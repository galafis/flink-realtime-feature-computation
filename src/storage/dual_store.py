"""
Dual Store Orchestrator

Coordinates writes to both Redis (online) and Cassandra (offline) stores
with exactly-once semantics, consistency verification, and automatic
failover handling.

Author: Gabriel Demetrios Lafis
"""

import json
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from src.utils.logger import setup_logger
from src.config.settings import RedisConfig, CassandraConfig
from src.storage.redis_store import RedisOnlineStore, FeatureValue
from src.storage.cassandra_store import (
    CassandraOfflineStore,
    HistoricalFeatureRecord,
)

logger = setup_logger(__name__)


class WriteStrategy(str, Enum):
    """Dual-write execution strategy."""
    SYNC_BOTH = "sync_both"
    ASYNC_OFFLINE = "async_offline"
    ONLINE_FIRST = "online_first"
    OFFLINE_FIRST = "offline_first"


class FailoverMode(str, Enum):
    """Behavior when one store fails."""
    FAIL_FAST = "fail_fast"
    BEST_EFFORT = "best_effort"
    QUEUE_RETRY = "queue_retry"


@dataclass
class DualWriteResult:
    """Result of a dual-store write operation."""
    online_success: bool
    offline_success: bool
    entity_id: str
    feature_name: str
    timestamp: float = field(default_factory=time.time)
    online_error: Optional[str] = None
    offline_error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.online_success and self.offline_success


@dataclass
class ConsistencyReport:
    """Report from a consistency verification check."""
    entity_id: str
    feature_name: str
    is_consistent: bool
    online_value: Optional[Any] = None
    offline_value: Optional[Any] = None
    online_version: Optional[int] = None
    offline_version: Optional[int] = None
    drift_seconds: float = 0.0
    details: str = ""


class DualStore:
    """
    Orchestrates writes and reads across Redis and Cassandra stores.

    Ensures feature data is consistently available in both the online
    (Redis) and offline (Cassandra) stores with configurable write
    strategies and failover modes.

    Features:
        - Exactly-once write semantics with idempotency keys
        - Configurable write strategies (sync, async offline, ordered)
        - Consistency verification between stores
        - Automatic failover with retry queue
        - Batch dual-write support
        - Health monitoring for both stores
    """

    def __init__(
        self,
        redis_config: Optional[RedisConfig] = None,
        cassandra_config: Optional[CassandraConfig] = None,
        write_strategy: WriteStrategy = WriteStrategy.SYNC_BOTH,
        failover_mode: FailoverMode = FailoverMode.BEST_EFFORT,
        max_retry_queue_size: int = 10000,
    ):
        self.online_store = RedisOnlineStore(redis_config)
        self.offline_store = CassandraOfflineStore(cassandra_config)
        self.write_strategy = write_strategy
        self.failover_mode = failover_mode
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._retry_queue: List[Tuple[HistoricalFeatureRecord, FeatureValue]] = []
        self._max_retry_queue = max_retry_queue_size
        self._lock = threading.Lock()
        self._idempotency_cache: Dict[str, float] = {}
        self._idempotency_ttl = 300.0
        self._metrics = {
            "dual_writes": 0,
            "dual_write_failures": 0,
            "online_only_writes": 0,
            "offline_only_writes": 0,
            "consistency_checks": 0,
            "inconsistencies_found": 0,
            "retries_queued": 0,
        }

    def connect(self) -> None:
        """Connect to both Redis and Cassandra."""
        self.online_store.connect()
        self.offline_store.connect()
        logger.info("DualStore connected to both stores")

    def disconnect(self) -> None:
        """Disconnect from both stores and shut down thread pool."""
        self._executor.shutdown(wait=True)
        self.online_store.disconnect()
        self.offline_store.disconnect()
        logger.info("DualStore disconnected")

    def write_feature(
        self,
        entity_id: str,
        feature_name: str,
        value: Any,
        event_time: Optional[datetime] = None,
        version: int = 1,
        ttl_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> DualWriteResult:
        """
        Write a feature value to both online and offline stores.

        Args:
            entity_id: Entity identifier.
            feature_name: Feature name.
            value: Feature value to store.
            event_time: Event timestamp (defaults to now).
            version: Feature version number.
            ttl_seconds: Optional TTL override.
            metadata: Optional metadata.

        Returns:
            DualWriteResult indicating success/failure of each store.
        """
        now = datetime.now(timezone.utc)
        event_ts = event_time or now
        meta = metadata or {}

        # Idempotency check
        idempotency_key = f"{entity_id}:{feature_name}:{event_ts.isoformat()}"
        if self._is_duplicate(idempotency_key):
            logger.debug(
                "Duplicate write detected, skipping",
                extra={"idempotency_key": idempotency_key},
            )
            return DualWriteResult(
                online_success=True,
                offline_success=True,
                entity_id=entity_id,
                feature_name=feature_name,
            )

        # Prepare store-specific records
        online_value = FeatureValue(
            value=value,
            version=version,
            timestamp=event_ts.timestamp(),
            ttl_seconds=ttl_seconds,
            metadata=meta,
        )
        offline_record = HistoricalFeatureRecord(
            entity_id=entity_id,
            feature_name=feature_name,
            feature_value=value,
            event_time=event_ts,
            ingestion_time=now,
            version=version,
            metadata=meta,
        )

        # Execute based on strategy
        if self.write_strategy == WriteStrategy.SYNC_BOTH:
            result = self._write_sync(
                entity_id, feature_name, online_value,
                offline_record, ttl_seconds,
            )
        elif self.write_strategy == WriteStrategy.ASYNC_OFFLINE:
            result = self._write_async_offline(
                entity_id, feature_name, online_value,
                offline_record, ttl_seconds,
            )
        elif self.write_strategy == WriteStrategy.ONLINE_FIRST:
            result = self._write_online_first(
                entity_id, feature_name, online_value,
                offline_record, ttl_seconds,
            )
        else:
            result = self._write_offline_first(
                entity_id, feature_name, online_value,
                offline_record, ttl_seconds,
            )

        # Mark idempotency key
        self._mark_processed(idempotency_key)

        # Update metrics
        self._metrics["dual_writes"] += 1
        if not result.success:
            self._metrics["dual_write_failures"] += 1
            if result.online_success and not result.offline_success:
                self._metrics["online_only_writes"] += 1
            elif result.offline_success and not result.online_success:
                self._metrics["offline_only_writes"] += 1

        return result

    def batch_write(
        self,
        records: List[Dict[str, Any]],
        ttl_seconds: Optional[int] = None,
    ) -> List[DualWriteResult]:
        """
        Write multiple feature values to both stores.

        Args:
            records: List of dicts with keys: entity_id, feature_name,
                     value, event_time (optional), version (optional).
            ttl_seconds: Optional TTL override.

        Returns:
            List of DualWriteResult for each record.
        """
        results = []
        for record in records:
            result = self.write_feature(
                entity_id=record["entity_id"],
                feature_name=record["feature_name"],
                value=record["value"],
                event_time=record.get("event_time"),
                version=record.get("version", 1),
                ttl_seconds=ttl_seconds,
                metadata=record.get("metadata"),
            )
            results.append(result)

        return results

    def read_feature_online(
        self, entity_id: str, feature_name: str
    ) -> Optional[FeatureValue]:
        """Read from the online store (Redis) with offline fallback."""
        value = self.online_store.get_feature(entity_id, feature_name)

        if value is None:
            # Fallback to offline store
            record = self.offline_store.get_latest(entity_id, feature_name)
            if record is not None:
                # Backfill online store
                value = FeatureValue(
                    value=record.feature_value,
                    version=record.version,
                    timestamp=record.event_time.timestamp(),
                    metadata=record.metadata,
                )
                self.online_store.set_feature(
                    entity_id, feature_name, value
                )
                logger.debug(
                    "Backfilled online store from offline",
                    extra={
                        "entity_id": entity_id,
                        "feature_name": feature_name,
                    },
                )

        return value

    def verify_consistency(
        self, entity_id: str, feature_name: str
    ) -> ConsistencyReport:
        """
        Verify that online and offline stores have consistent data.

        Compares the latest values in both stores and reports any drift.

        Args:
            entity_id: Entity identifier.
            feature_name: Feature name.

        Returns:
            ConsistencyReport with comparison details.
        """
        self._metrics["consistency_checks"] += 1

        online_val = self.online_store.get_feature(entity_id, feature_name)
        offline_rec = self.offline_store.get_latest(entity_id, feature_name)

        report = ConsistencyReport(
            entity_id=entity_id,
            feature_name=feature_name,
            is_consistent=True,
        )

        if online_val is None and offline_rec is None:
            report.details = "Both stores empty"
            return report

        if online_val is None and offline_rec is not None:
            report.is_consistent = False
            report.offline_value = offline_rec.feature_value
            report.offline_version = offline_rec.version
            report.details = "Missing from online store"
            self._metrics["inconsistencies_found"] += 1
            return report

        if online_val is not None and offline_rec is None:
            report.is_consistent = False
            report.online_value = online_val.value
            report.online_version = online_val.version
            report.details = "Missing from offline store"
            self._metrics["inconsistencies_found"] += 1
            return report

        # Both exist; compare values
        report.online_value = online_val.value
        report.offline_value = offline_rec.feature_value
        report.online_version = online_val.version
        report.offline_version = offline_rec.version

        # Check value consistency
        if str(online_val.value) != str(offline_rec.feature_value):
            report.is_consistent = False
            report.details = "Value mismatch"
            self._metrics["inconsistencies_found"] += 1

        # Check temporal drift
        online_ts = online_val.timestamp
        offline_ts = offline_rec.event_time.timestamp()
        report.drift_seconds = abs(online_ts - offline_ts)

        if report.drift_seconds > 60:
            report.is_consistent = False
            report.details = (
                f"Temporal drift: {report.drift_seconds:.1f}s"
            )
            self._metrics["inconsistencies_found"] += 1

        return report

    def repair_inconsistency(
        self,
        entity_id: str,
        feature_name: str,
        source: str = "offline",
    ) -> bool:
        """
        Repair an inconsistency by syncing one store from the other.

        Args:
            entity_id: Entity identifier.
            feature_name: Feature name.
            source: Which store to use as source ('online' or 'offline').

        Returns:
            True if repair was successful.
        """
        try:
            if source == "offline":
                record = self.offline_store.get_latest(
                    entity_id, feature_name
                )
                if record:
                    value = FeatureValue(
                        value=record.feature_value,
                        version=record.version,
                        timestamp=record.event_time.timestamp(),
                        metadata=record.metadata,
                    )
                    return self.online_store.set_feature(
                        entity_id, feature_name, value
                    )
            else:
                online_val = self.online_store.get_feature(
                    entity_id, feature_name
                )
                if online_val:
                    record = HistoricalFeatureRecord(
                        entity_id=entity_id,
                        feature_name=feature_name,
                        feature_value=online_val.value,
                        event_time=datetime.fromtimestamp(
                            online_val.timestamp, tz=timezone.utc
                        ),
                        version=online_val.version,
                        metadata=online_val.metadata,
                    )
                    return self.offline_store.write_feature(record)

            return False

        except Exception as e:
            logger.error(
                "Repair failed",
                extra={
                    "entity_id": entity_id,
                    "feature_name": feature_name,
                    "error": str(e),
                },
            )
            return False

    def health_check(self) -> Dict[str, bool]:
        """Check health of both stores."""
        return {
            "online": self.online_store.health_check(),
            "offline": self.offline_store.health_check(),
        }

    def process_retry_queue(self) -> Tuple[int, int]:
        """Process queued failed writes. Returns (success, failed) counts."""
        with self._lock:
            queue = list(self._retry_queue)
            self._retry_queue.clear()

        success = 0
        failed = 0

        for offline_record, online_value in queue:
            try:
                self.offline_store.write_feature(offline_record)
                success += 1
            except Exception:
                failed += 1
                if len(self._retry_queue) < self._max_retry_queue:
                    self._retry_queue.append((offline_record, online_value))

        return success, failed

    # --- Private write strategy implementations ---

    def _write_sync(
        self, entity_id, feature_name, online_value,
        offline_record, ttl_seconds,
    ) -> DualWriteResult:
        """Write synchronously to both stores."""
        online_ok = False
        offline_ok = False
        online_err = None
        offline_err = None

        try:
            online_ok = self.online_store.set_feature(
                entity_id, feature_name, online_value, ttl_seconds
            )
        except Exception as e:
            online_err = str(e)

        try:
            offline_ok = self.offline_store.write_feature(
                offline_record, ttl_seconds
            )
        except Exception as e:
            offline_err = str(e)

        if not offline_ok and self.failover_mode == FailoverMode.QUEUE_RETRY:
            self._enqueue_retry(offline_record, online_value)

        return DualWriteResult(
            online_success=online_ok,
            offline_success=offline_ok,
            entity_id=entity_id,
            feature_name=feature_name,
            online_error=online_err,
            offline_error=offline_err,
        )

    def _write_async_offline(
        self, entity_id, feature_name, online_value,
        offline_record, ttl_seconds,
    ) -> DualWriteResult:
        """Write to online synchronously, offline asynchronously."""
        online_ok = False
        online_err = None

        try:
            online_ok = self.online_store.set_feature(
                entity_id, feature_name, online_value, ttl_seconds
            )
        except Exception as e:
            online_err = str(e)

        # Submit offline write to thread pool
        self._executor.submit(
            self._async_offline_write, offline_record, ttl_seconds
        )

        return DualWriteResult(
            online_success=online_ok,
            offline_success=True,  # Assumed success (async)
            entity_id=entity_id,
            feature_name=feature_name,
            online_error=online_err,
        )

    def _write_online_first(
        self, entity_id, feature_name, online_value,
        offline_record, ttl_seconds,
    ) -> DualWriteResult:
        """Write to online first; proceed to offline only if online succeeds."""
        online_ok = False
        offline_ok = False
        online_err = None
        offline_err = None

        try:
            online_ok = self.online_store.set_feature(
                entity_id, feature_name, online_value, ttl_seconds
            )
        except Exception as e:
            online_err = str(e)

        if online_ok or self.failover_mode == FailoverMode.BEST_EFFORT:
            try:
                offline_ok = self.offline_store.write_feature(
                    offline_record, ttl_seconds
                )
            except Exception as e:
                offline_err = str(e)

        return DualWriteResult(
            online_success=online_ok,
            offline_success=offline_ok,
            entity_id=entity_id,
            feature_name=feature_name,
            online_error=online_err,
            offline_error=offline_err,
        )

    def _write_offline_first(
        self, entity_id, feature_name, online_value,
        offline_record, ttl_seconds,
    ) -> DualWriteResult:
        """Write to offline first; proceed to online only if offline succeeds."""
        online_ok = False
        offline_ok = False
        online_err = None
        offline_err = None

        try:
            offline_ok = self.offline_store.write_feature(
                offline_record, ttl_seconds
            )
        except Exception as e:
            offline_err = str(e)

        if offline_ok or self.failover_mode == FailoverMode.BEST_EFFORT:
            try:
                online_ok = self.online_store.set_feature(
                    entity_id, feature_name, online_value, ttl_seconds
                )
            except Exception as e:
                online_err = str(e)

        return DualWriteResult(
            online_success=online_ok,
            offline_success=offline_ok,
            entity_id=entity_id,
            feature_name=feature_name,
            online_error=online_err,
            offline_error=offline_err,
        )

    def _async_offline_write(
        self, record: HistoricalFeatureRecord, ttl_seconds: Optional[int]
    ) -> None:
        """Background task for async offline writes."""
        try:
            self.offline_store.write_feature(record, ttl_seconds)
        except Exception as e:
            logger.error(
                "Async offline write failed",
                extra={
                    "entity_id": record.entity_id,
                    "feature_name": record.feature_name,
                    "error": str(e),
                },
            )
            self._enqueue_retry(record, None)

    def _enqueue_retry(
        self, record: HistoricalFeatureRecord, online_value: Optional[FeatureValue]
    ) -> None:
        """Add a failed write to the retry queue."""
        with self._lock:
            if len(self._retry_queue) < self._max_retry_queue:
                self._retry_queue.append((record, online_value))
                self._metrics["retries_queued"] += 1

    def _is_duplicate(self, key: str) -> bool:
        """Check idempotency cache for duplicate writes."""
        now = time.time()
        with self._lock:
            if key in self._idempotency_cache:
                if now - self._idempotency_cache[key] < self._idempotency_ttl:
                    return True
            return False

    def _mark_processed(self, key: str) -> None:
        """Record a processed write in the idempotency cache."""
        with self._lock:
            self._idempotency_cache[key] = time.time()

            # Evict expired entries periodically
            if len(self._idempotency_cache) > 50000:
                cutoff = time.time() - self._idempotency_ttl
                self._idempotency_cache = {
                    k: v
                    for k, v in self._idempotency_cache.items()
                    if v > cutoff
                }

    @property
    def metrics(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "online_metrics": self.online_store.metrics,
            "offline_metrics": self.offline_store.metrics,
            "retry_queue_size": len(self._retry_queue),
        }

    def __enter__(self) -> "DualStore":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()
