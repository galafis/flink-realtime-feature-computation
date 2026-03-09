"""
Cassandra Offline Feature Store

Wide-column storage for historical feature values with time-range
queries, TTL-based expiration, configurable compaction strategies,
and batch write support for backfill operations.

Author: Gabriel Demetrios Lafis
"""

import json
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from collections import defaultdict

from src.utils.logger import setup_logger
from src.config.settings import CassandraConfig

logger = setup_logger(__name__)


@dataclass
class HistoricalFeatureRecord:
    """A single historical feature record in the offline store."""
    entity_id: str
    feature_name: str
    feature_value: Any
    event_time: datetime
    ingestion_time: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)


CREATE_KEYSPACE_CQL = """
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{
    'class': '{replication_strategy}',
    'replication_factor': {replication_factor}
}};
"""

CREATE_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.features (
    entity_id text,
    feature_name text,
    event_time timestamp,
    feature_value text,
    version int,
    ingestion_time timestamp,
    metadata text,
    PRIMARY KEY ((entity_id, feature_name), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
AND default_time_to_live = {default_ttl}
AND gc_grace_seconds = {gc_grace_seconds}
AND compaction = {{
    'class': 'org.apache.cassandra.db.compaction.{compaction_strategy}',
    'compaction_window_size': {compaction_window_size},
    'compaction_window_unit': '{compaction_window_unit}'
}};
"""

CREATE_ENTITY_INDEX_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.features_by_entity (
    entity_id text,
    feature_name text,
    last_updated timestamp,
    PRIMARY KEY (entity_id, feature_name)
) WITH default_time_to_live = {default_ttl};
"""

INSERT_FEATURE_CQL = """
INSERT INTO {keyspace}.features
    (entity_id, feature_name, event_time, feature_value, version,
     ingestion_time, metadata)
VALUES (?, ?, ?, ?, ?, ?, ?)
USING TTL ?;
"""

QUERY_TIME_RANGE_CQL = """
SELECT entity_id, feature_name, event_time, feature_value,
       version, ingestion_time, metadata
FROM {keyspace}.features
WHERE entity_id = ? AND feature_name = ?
  AND event_time >= ? AND event_time <= ?
ORDER BY event_time DESC
LIMIT ?;
"""

QUERY_LATEST_CQL = """
SELECT entity_id, feature_name, event_time, feature_value,
       version, ingestion_time, metadata
FROM {keyspace}.features
WHERE entity_id = ? AND feature_name = ?
ORDER BY event_time DESC
LIMIT 1;
"""


class CassandraOfflineStore:
    """
    Cassandra-backed offline feature store for historical data.

    Features:
        - Wide-column storage optimized for time-series feature data
        - Time-range queries with configurable limits
        - TTL-based expiration aligned with compaction strategy
        - Batch writes for high-throughput ingestion and backfill
        - Prepared statements for low-latency repeated queries
        - Connection pooling with configurable consistency levels
    """

    def __init__(self, config: Optional[CassandraConfig] = None):
        self.config = config or CassandraConfig()
        self._cluster = None
        self._session = None
        self._prepared_stmts: Dict[str, Any] = {}
        self._is_connected = False
        self._metrics = {
            "writes": 0,
            "reads": 0,
            "batch_writes": 0,
            "errors": 0,
            "bytes_written": 0,
        }

    def connect(self) -> None:
        """Establish connection to the Cassandra cluster."""
        try:
            from cassandra.cluster import Cluster
            from cassandra.policies import (
                DCAwareRoundRobinPolicy,
                RetryPolicy,
                ConstantReconnectionPolicy,
            )
            from cassandra.auth import PlainTextAuthProvider

            auth = None
            if self.config.username and self.config.password:
                auth = PlainTextAuthProvider(
                    username=self.config.username,
                    password=self.config.password,
                )

            self._cluster = Cluster(
                contact_points=self.config.contact_points,
                port=self.config.port,
                auth_provider=auth,
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                reconnection_policy=ConstantReconnectionPolicy(delay=5.0),
                protocol_version=4,
            )
            self._session = self._cluster.connect()
            self._is_connected = True

            self._initialize_schema()
            self._prepare_statements()

            logger.info(
                "Connected to Cassandra",
                extra={
                    "contact_points": self.config.contact_points,
                    "keyspace": self.config.keyspace,
                },
            )

        except ImportError:
            logger.warning(
                "cassandra-driver not installed; using mock store"
            )
            self._session = _MockCassandraSession()
            self._is_connected = True

        except Exception as e:
            logger.error(
                "Failed to connect to Cassandra",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        """Close the Cassandra connection."""
        if self._cluster is not None:
            self._cluster.shutdown()
        self._is_connected = False
        logger.info("Disconnected from Cassandra")

    def write_feature(
        self,
        record: HistoricalFeatureRecord,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """
        Write a single feature record to the offline store.

        Args:
            record: The feature record to store.
            ttl_seconds: Optional TTL override.

        Returns:
            True if write succeeded.
        """
        self._ensure_connected()
        ttl = ttl_seconds or self.config.default_ttl_seconds

        try:
            serialized_value = json.dumps(record.feature_value, default=str)
            serialized_meta = json.dumps(record.metadata, default=str)

            self._session.execute(
                self._get_prepared("insert_feature"),
                (
                    record.entity_id,
                    record.feature_name,
                    record.event_time,
                    serialized_value,
                    record.version,
                    record.ingestion_time,
                    serialized_meta,
                    ttl,
                ),
            )
            self._metrics["writes"] += 1
            self._metrics["bytes_written"] += len(serialized_value)

            # Update entity index
            self._update_entity_index(
                record.entity_id, record.feature_name, record.event_time
            )

            return True

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Cassandra write failed",
                extra={
                    "entity_id": record.entity_id,
                    "feature_name": record.feature_name,
                    "error": str(e),
                },
            )
            return False

    def batch_write(
        self,
        records: List[HistoricalFeatureRecord],
        ttl_seconds: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Write a batch of feature records to the offline store.

        Uses unlogged batches for performance (records span multiple
        partitions, so logged batches add overhead without benefit).

        Args:
            records: List of feature records to store.
            ttl_seconds: Optional TTL override for all records.

        Returns:
            Tuple of (successful_count, failed_count).
        """
        self._ensure_connected()
        ttl = ttl_seconds or self.config.default_ttl_seconds

        success = 0
        failed = 0

        try:
            from cassandra.query import BatchStatement, BatchType

            # Group by partition for efficient batching
            partitions: Dict[str, List[HistoricalFeatureRecord]] = defaultdict(list)
            for record in records:
                partition_key = f"{record.entity_id}:{record.feature_name}"
                partitions[partition_key].append(record)

            for _partition_key, partition_records in partitions.items():
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)

                for record in partition_records:
                    serialized_value = json.dumps(
                        record.feature_value, default=str
                    )
                    serialized_meta = json.dumps(
                        record.metadata, default=str
                    )
                    batch.add(
                        self._get_prepared("insert_feature"),
                        (
                            record.entity_id,
                            record.feature_name,
                            record.event_time,
                            serialized_value,
                            record.version,
                            record.ingestion_time,
                            serialized_meta,
                            ttl,
                        ),
                    )

                self._session.execute(batch)
                success += len(partition_records)

        except ImportError:
            # Fallback: individual writes
            for record in records:
                if self.write_feature(record, ttl_seconds):
                    success += 1
                else:
                    failed += 1

        except Exception as e:
            failed = len(records) - success
            logger.error(
                "Cassandra batch write failed",
                extra={
                    "total": len(records),
                    "success": success,
                    "failed": failed,
                    "error": str(e),
                },
            )

        self._metrics["batch_writes"] += 1
        self._metrics["writes"] += success
        return success, failed

    def query_time_range(
        self,
        entity_id: str,
        feature_name: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
    ) -> List[HistoricalFeatureRecord]:
        """
        Query feature values within a time range.

        Args:
            entity_id: Entity identifier.
            feature_name: Feature name.
            start_time: Range start (inclusive).
            end_time: Range end (inclusive).
            limit: Maximum records to return.

        Returns:
            List of historical feature records, newest first.
        """
        self._ensure_connected()

        try:
            rows = self._session.execute(
                self._get_prepared("query_time_range"),
                (entity_id, feature_name, start_time, end_time, limit),
            )
            self._metrics["reads"] += 1

            records = []
            for row in rows:
                records.append(HistoricalFeatureRecord(
                    entity_id=row.entity_id,
                    feature_name=row.feature_name,
                    feature_value=json.loads(row.feature_value),
                    event_time=row.event_time,
                    ingestion_time=row.ingestion_time,
                    version=row.version,
                    metadata=json.loads(row.metadata) if row.metadata else {},
                ))

            return records

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Cassandra time-range query failed",
                extra={
                    "entity_id": entity_id,
                    "feature_name": feature_name,
                    "error": str(e),
                },
            )
            return []

    def get_latest(
        self, entity_id: str, feature_name: str
    ) -> Optional[HistoricalFeatureRecord]:
        """
        Retrieve the most recent feature value for an entity.

        Args:
            entity_id: Entity identifier.
            feature_name: Feature name.

        Returns:
            Most recent record, or None if not found.
        """
        self._ensure_connected()

        try:
            rows = self._session.execute(
                self._get_prepared("query_latest"),
                (entity_id, feature_name),
            )
            self._metrics["reads"] += 1

            for row in rows:
                return HistoricalFeatureRecord(
                    entity_id=row.entity_id,
                    feature_name=row.feature_name,
                    feature_value=json.loads(row.feature_value),
                    event_time=row.event_time,
                    ingestion_time=row.ingestion_time,
                    version=row.version,
                    metadata=json.loads(row.metadata) if row.metadata else {},
                )
            return None

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Cassandra latest query failed",
                extra={
                    "entity_id": entity_id,
                    "feature_name": feature_name,
                    "error": str(e),
                },
            )
            return None

    def get_entity_features(self, entity_id: str) -> List[str]:
        """List all feature names available for an entity."""
        self._ensure_connected()
        try:
            rows = self._session.execute(
                f"SELECT feature_name FROM {self.config.keyspace}"
                f".features_by_entity WHERE entity_id = %s",
                (entity_id,),
            )
            return [row.feature_name for row in rows]
        except Exception as e:
            logger.error(
                "Failed to list entity features",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            return []

    def health_check(self) -> bool:
        """Check Cassandra connectivity."""
        try:
            if self._session is not None:
                self._session.execute("SELECT now() FROM system.local")
                return True
        except Exception:
            pass
        return False

    def _initialize_schema(self) -> None:
        """Create keyspace and tables if they do not exist."""
        try:
            self._session.execute(
                CREATE_KEYSPACE_CQL.format(
                    keyspace=self.config.keyspace,
                    replication_strategy=self.config.replication_strategy,
                    replication_factor=self.config.replication_factor,
                )
            )
            self._session.set_keyspace(self.config.keyspace)

            self._session.execute(
                CREATE_TABLE_CQL.format(
                    keyspace=self.config.keyspace,
                    default_ttl=self.config.default_ttl_seconds,
                    gc_grace_seconds=self.config.gc_grace_seconds,
                    compaction_strategy=self.config.compaction_strategy,
                    compaction_window_size=self.config.compaction_window_size,
                    compaction_window_unit=self.config.compaction_window_unit,
                )
            )

            self._session.execute(
                CREATE_ENTITY_INDEX_CQL.format(
                    keyspace=self.config.keyspace,
                    default_ttl=self.config.default_ttl_seconds,
                )
            )

            logger.info("Cassandra schema initialized")

        except Exception as e:
            logger.warning(
                "Schema initialization skipped",
                extra={"reason": str(e)},
            )

    def _prepare_statements(self) -> None:
        """Prepare CQL statements for efficient repeated execution."""
        try:
            self._prepared_stmts["insert_feature"] = self._session.prepare(
                INSERT_FEATURE_CQL.format(keyspace=self.config.keyspace)
            )
            self._prepared_stmts["query_time_range"] = self._session.prepare(
                QUERY_TIME_RANGE_CQL.format(keyspace=self.config.keyspace)
            )
            self._prepared_stmts["query_latest"] = self._session.prepare(
                QUERY_LATEST_CQL.format(keyspace=self.config.keyspace)
            )
        except Exception as e:
            logger.warning(
                "Statement preparation skipped",
                extra={"reason": str(e)},
            )

    def _get_prepared(self, name: str) -> Any:
        """Retrieve a prepared statement or fall back to raw CQL."""
        return self._prepared_stmts.get(name, name)

    def _update_entity_index(
        self, entity_id: str, feature_name: str, event_time: datetime
    ) -> None:
        """Update the entity feature index table."""
        try:
            self._session.execute(
                f"INSERT INTO {self.config.keyspace}.features_by_entity "
                f"(entity_id, feature_name, last_updated) "
                f"VALUES (%s, %s, %s)",
                (entity_id, feature_name, event_time),
            )
        except Exception:
            pass  # Non-critical index update

    def _ensure_connected(self) -> None:
        if not self._is_connected:
            raise RuntimeError("Cassandra store is not connected")

    @property
    def metrics(self) -> Dict[str, int]:
        return dict(self._metrics)

    def __enter__(self) -> "CassandraOfflineStore":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()


class _MockRow:
    """Mock Cassandra row for testing."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _MockCassandraSession:
    """In-memory mock Cassandra session for testing."""

    def __init__(self):
        self._data: Dict[str, List[Dict]] = defaultdict(list)
        self._keyspace: Optional[str] = None

    def execute(self, query, params=None):
        if isinstance(query, str):
            if "SELECT now()" in query:
                return [_MockRow(now=datetime.now(timezone.utc))]
            return []
        return []

    def prepare(self, query):
        return query

    def set_keyspace(self, keyspace: str) -> None:
        self._keyspace = keyspace
