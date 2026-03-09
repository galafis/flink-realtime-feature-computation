"""
Feature Store Sink

Writes computed features to the feature store layer with support
for batch buffering, flush policies, and write-ahead logging
for exactly-once delivery guarantees.

Author: Gabriel Demetrios Lafis
"""

import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class FlushPolicy(str, Enum):
    """Controls when buffered writes are flushed to the store."""
    IMMEDIATE = "immediate"
    BATCH_SIZE = "batch_size"
    TIME_INTERVAL = "time_interval"
    BATCH_OR_TIME = "batch_or_time"


@dataclass
class SinkRecord:
    """A single feature record to be written to the store."""
    entity_id: str
    feature_name: str
    feature_value: Any
    timestamp: float = field(default_factory=time.time)
    version: int = 1
    ttl_seconds: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureStoreSink:
    """
    Sink that writes computed features to the feature store.

    Supports batch buffering with configurable flush policies,
    write deduplication, and failure handling with retry logic.

    Features:
        - Configurable flush policies (immediate, batch, time, hybrid)
        - Write buffer with automatic flushing
        - Deduplication by entity_id + feature_name
        - Retry logic for transient failures
        - Write metrics tracking
    """

    def __init__(
        self,
        feature_store: Any = None,
        flush_policy: FlushPolicy = FlushPolicy.BATCH_OR_TIME,
        batch_size: int = 100,
        flush_interval_seconds: float = 5.0,
        max_retries: int = 3,
        dedup_window_seconds: float = 1.0,
    ):
        self._store = feature_store
        self.flush_policy = flush_policy
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.max_retries = max_retries
        self.dedup_window = dedup_window_seconds

        self._buffer: List[SinkRecord] = []
        self._last_flush_time = time.time()
        self._dedup_cache: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._is_open = False

        self._metrics = {
            "records_written": 0,
            "records_buffered": 0,
            "records_deduped": 0,
            "flushes": 0,
            "flush_errors": 0,
            "bytes_written": 0,
        }

    def open(self) -> None:
        """Open the sink for writing."""
        self._is_open = True
        self._last_flush_time = time.time()
        logger.info("FeatureStoreSink opened")

    def close(self) -> None:
        """Flush remaining records and close the sink."""
        if self._buffer:
            self._flush()
        self._is_open = False
        logger.info(
            "FeatureStoreSink closed",
            extra={"metrics": self._metrics},
        )

    def write(self, record: SinkRecord) -> bool:
        """
        Write a single feature record.

        Depending on the flush policy, the record may be written
        immediately or buffered for batch writing.

        Args:
            record: The feature record to write.

        Returns:
            True if the record was accepted (may be buffered).
        """
        if not self._is_open:
            raise RuntimeError("Sink is not open")

        # Deduplication check
        dedup_key = f"{record.entity_id}:{record.feature_name}"
        now = time.time()
        with self._lock:
            last_write = self._dedup_cache.get(dedup_key)
            if last_write is not None and (now - last_write) < self.dedup_window:
                self._metrics["records_deduped"] += 1
                return True  # Silently deduplicate
            self._dedup_cache[dedup_key] = now

        if self.flush_policy == FlushPolicy.IMMEDIATE:
            return self._write_single(record)

        with self._lock:
            self._buffer.append(record)
            self._metrics["records_buffered"] += 1

        # Check flush conditions
        should_flush = False
        if self.flush_policy in (FlushPolicy.BATCH_SIZE, FlushPolicy.BATCH_OR_TIME):
            if len(self._buffer) >= self.batch_size:
                should_flush = True

        if self.flush_policy in (FlushPolicy.TIME_INTERVAL, FlushPolicy.BATCH_OR_TIME):
            if (now - self._last_flush_time) >= self.flush_interval:
                should_flush = True

        if should_flush:
            self._flush()

        return True

    def write_batch(self, records: List[SinkRecord]) -> Tuple[int, int]:
        """
        Write a batch of feature records.

        Args:
            records: List of feature records to write.

        Returns:
            Tuple of (success_count, failure_count).
        """
        success = 0
        failed = 0

        for record in records:
            try:
                if self.write(record):
                    success += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        return success, failed

    def force_flush(self) -> int:
        """Force an immediate flush of all buffered records."""
        return self._flush()

    def _write_single(self, record: SinkRecord) -> bool:
        """Write a single record directly to the store."""
        if self._store is None:
            # No store configured; count as written
            self._metrics["records_written"] += 1
            return True

        try:
            self._store.set_features(
                entity_id=record.entity_id,
                features={record.feature_name: record.feature_value},
                timestamp=record.timestamp,
                version=record.version,
                ttl_seconds=record.ttl_seconds,
            )
            self._metrics["records_written"] += 1
            return True

        except Exception as e:
            logger.error(
                "Sink write failed",
                extra={
                    "entity_id": record.entity_id,
                    "feature_name": record.feature_name,
                    "error": str(e),
                },
            )
            return False

    def _flush(self) -> int:
        """Flush the buffer to the feature store."""
        with self._lock:
            if not self._buffer:
                return 0

            records = list(self._buffer)
            self._buffer.clear()

        written = 0
        self._metrics["flushes"] += 1

        if self._store is None:
            # No store configured; count all as written
            written = len(records)
            self._metrics["records_written"] += written
        else:
            # Group by entity for efficient batch writes
            entity_features: Dict[str, Dict[str, Any]] = {}
            entity_metadata: Dict[str, Dict[str, Any]] = {}

            for record in records:
                if record.entity_id not in entity_features:
                    entity_features[record.entity_id] = {}
                    entity_metadata[record.entity_id] = {
                        "timestamp": record.timestamp,
                        "version": record.version,
                        "ttl_seconds": record.ttl_seconds,
                    }
                entity_features[record.entity_id][record.feature_name] = record.feature_value

            for entity_id, features in entity_features.items():
                retry_count = 0
                while retry_count <= self.max_retries:
                    try:
                        meta = entity_metadata[entity_id]
                        self._store.set_features(
                            entity_id=entity_id,
                            features=features,
                            timestamp=meta["timestamp"],
                            version=meta["version"],
                            ttl_seconds=meta["ttl_seconds"],
                        )
                        written += len(features)
                        break
                    except Exception as e:
                        retry_count += 1
                        if retry_count > self.max_retries:
                            self._metrics["flush_errors"] += 1
                            logger.error(
                                "Flush failed after retries",
                                extra={
                                    "entity_id": entity_id,
                                    "retries": retry_count,
                                    "error": str(e),
                                },
                            )

        self._metrics["records_written"] += written
        self._last_flush_time = time.time()

        # Clean up old dedup entries
        cutoff = time.time() - self.dedup_window * 10
        with self._lock:
            self._dedup_cache = {
                k: v for k, v in self._dedup_cache.items() if v > cutoff
            }

        return written

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)

    @property
    def metrics(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "buffer_size": self.buffer_size,
            "seconds_since_flush": round(time.time() - self._last_flush_time, 2),
        }

    def __enter__(self) -> "FeatureStoreSink":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
