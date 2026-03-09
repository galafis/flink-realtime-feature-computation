"""
Engine Configuration Management

Provides typed, validated configuration dataclasses for every subsystem
of the feature computation engine. Supports loading from environment
variables, YAML files, and programmatic overrides.

Author: Gabriel Demetrios Lafis
"""

import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
from enum import Enum


class WindowType(str, Enum):
    """Supported Flink window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


class AggregationType(str, Enum):
    """Supported aggregation functions."""
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    STDDEV = "stddev"
    PERCENTILE = "percentile"
    DISTINCT_COUNT = "distinct_count"
    LAST_VALUE = "last_value"
    FIRST_VALUE = "first_value"


class ConsistencyLevel(str, Enum):
    """Cassandra consistency levels."""
    ONE = "ONE"
    QUORUM = "QUORUM"
    LOCAL_QUORUM = "LOCAL_QUORUM"
    ALL = "ALL"


@dataclass
class FlinkConfig:
    """
    Apache Flink cluster and job configuration.

    Controls parallelism, checkpointing, savepoints, and resource allocation
    for the Flink execution environment.
    """
    jobmanager_host: str = os.getenv("FLINK_JOBMANAGER_HOST", "localhost")
    jobmanager_port: int = int(os.getenv("FLINK_JOBMANAGER_PORT", "8081"))
    rest_port: int = int(os.getenv("FLINK_REST_PORT", "8081"))
    parallelism: int = int(os.getenv("FLINK_PARALLELISM", "4"))
    task_slots: int = int(os.getenv("FLINK_TASK_SLOTS", "4"))

    # Checkpointing
    checkpoint_interval_ms: int = int(
        os.getenv("FLINK_CHECKPOINT_INTERVAL", "60000")
    )
    checkpoint_timeout_ms: int = int(
        os.getenv("FLINK_CHECKPOINT_TIMEOUT", "120000")
    )
    checkpoint_min_pause_ms: int = int(
        os.getenv("FLINK_CHECKPOINT_MIN_PAUSE", "30000")
    )
    max_concurrent_checkpoints: int = 1
    checkpoint_storage: str = os.getenv(
        "FLINK_CHECKPOINT_STORAGE",
        "file:///opt/flink/checkpoints",
    )

    # Savepoints
    savepoint_directory: str = os.getenv(
        "FLINK_SAVEPOINT_DIR",
        "file:///opt/flink/savepoints",
    )

    # Resource management
    taskmanager_memory: str = os.getenv("FLINK_TM_MEMORY", "2048m")
    jobmanager_memory: str = os.getenv("FLINK_JM_MEMORY", "1024m")

    # Restart strategy
    restart_strategy: str = "fixed-delay"
    restart_attempts: int = 3
    restart_delay_ms: int = 10000

    @property
    def rest_url(self) -> str:
        return f"http://{self.jobmanager_host}:{self.rest_port}"


@dataclass
class KafkaConfig:
    """
    Apache Kafka connection and topic configuration.

    Supports both producer and consumer settings, including serialization,
    partitioning, and consumer group management.
    """
    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    schema_registry_url: str = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )

    # Topics
    feature_events_topic: str = os.getenv(
        "KAFKA_FEATURE_EVENTS_TOPIC", "feature-events"
    )
    feature_results_topic: str = os.getenv(
        "KAFKA_FEATURE_RESULTS_TOPIC", "feature-results"
    )
    dead_letter_topic: str = os.getenv(
        "KAFKA_DLQ_TOPIC", "feature-events-dlq"
    )

    # Consumer configuration
    consumer_group_id: str = os.getenv(
        "KAFKA_CONSUMER_GROUP", "feature-engine-consumer"
    )
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000

    # Producer configuration
    acks: str = "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 10
    buffer_memory: int = 33554432
    compression_type: str = "snappy"
    max_in_flight_requests: int = 5
    enable_idempotence: bool = True

    # Security (optional)
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism: Optional[str] = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username: Optional[str] = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password: Optional[str] = os.getenv("KAFKA_SASL_PASSWORD")

    @property
    def producer_config(self) -> Dict[str, Any]:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "batch.size": self.batch_size,
            "linger.ms": self.linger_ms,
            "buffer.memory": self.buffer_memory,
            "compression.type": self.compression_type,
            "max.in.flight.requests.per.connection": self.max_in_flight_requests,
            "enable.idempotence": self.enable_idempotence,
        }
        self._apply_security(config)
        return config

    @property
    def consumer_config(self) -> Dict[str, Any]:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.consumer_group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": self.enable_auto_commit,
            "max.poll.records": self.max_poll_records,
            "max.poll.interval.ms": self.max_poll_interval_ms,
            "session.timeout.ms": self.session_timeout_ms,
            "heartbeat.interval.ms": self.heartbeat_interval_ms,
        }
        self._apply_security(config)
        return config

    def _apply_security(self, config: Dict[str, Any]) -> None:
        if self.security_protocol != "PLAINTEXT":
            config["security.protocol"] = self.security_protocol
            if self.sasl_mechanism:
                config["sasl.mechanism"] = self.sasl_mechanism
            if self.sasl_username:
                config["sasl.jaas.config"] = (
                    f'org.apache.kafka.common.security.plain.PlainLoginModule '
                    f'required username="{self.sasl_username}" '
                    f'password="{self.sasl_password}";'
                )


@dataclass
class CassandraConfig:
    """
    Apache Cassandra connection and storage configuration.

    Manages contact points, keyspace settings, consistency levels,
    and compaction strategy for the offline feature store.
    """
    contact_points: List[str] = field(
        default_factory=lambda: os.getenv(
            "CASSANDRA_CONTACT_POINTS", "localhost"
        ).split(",")
    )
    port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
    keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "feature_store")
    replication_factor: int = int(
        os.getenv("CASSANDRA_REPLICATION_FACTOR", "3")
    )
    replication_strategy: str = "SimpleStrategy"

    # Connection pool
    min_connections: int = 2
    max_connections: int = 8
    connection_timeout_s: float = 10.0
    request_timeout_s: float = 30.0

    # Consistency
    read_consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
    write_consistency: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM

    # Table settings
    default_ttl_seconds: int = int(
        os.getenv("CASSANDRA_DEFAULT_TTL", "7776000")  # 90 days
    )
    compaction_strategy: str = "TimeWindowCompactionStrategy"
    compaction_window_size: int = 1
    compaction_window_unit: str = "DAYS"
    gc_grace_seconds: int = 864000  # 10 days

    # Authentication
    username: Optional[str] = os.getenv("CASSANDRA_USERNAME")
    password: Optional[str] = os.getenv("CASSANDRA_PASSWORD")

    @property
    def table_options(self) -> Dict[str, Any]:
        return {
            "default_time_to_live": self.default_ttl_seconds,
            "gc_grace_seconds": self.gc_grace_seconds,
            "compaction": {
                "class": (
                    f"org.apache.cassandra.db.compaction."
                    f"{self.compaction_strategy}"
                ),
                "compaction_window_size": self.compaction_window_size,
                "compaction_window_unit": self.compaction_window_unit,
            },
        }


@dataclass
class RedisConfig:
    """
    Redis connection and caching configuration.

    Manages connection pooling, TTL policies, and pipeline operations
    for the online feature store.
    """
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))
    password: Optional[str] = os.getenv("REDIS_PASSWORD")

    # Connection pool
    max_connections: int = int(os.getenv("REDIS_MAX_CONNECTIONS", "50"))
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    retry_on_timeout: bool = True

    # Feature store settings
    key_prefix: str = "feature:"
    default_ttl_seconds: int = int(
        os.getenv("REDIS_DEFAULT_TTL", "3600")  # 1 hour
    )
    version_prefix: str = "v:"
    metadata_prefix: str = "meta:"

    # Pipeline settings
    pipeline_batch_size: int = 100
    max_pipeline_size: int = 1000

    # Cluster mode
    cluster_mode: bool = os.getenv("REDIS_CLUSTER_MODE", "false").lower() == "true"
    sentinel_hosts: Optional[str] = os.getenv("REDIS_SENTINEL_HOSTS")
    sentinel_master: str = os.getenv("REDIS_SENTINEL_MASTER", "mymaster")

    @property
    def connection_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "socket_timeout": self.socket_timeout,
            "socket_connect_timeout": self.socket_connect_timeout,
            "retry_on_timeout": self.retry_on_timeout,
            "decode_responses": True,
            "max_connections": self.max_connections,
        }
        if self.password:
            kwargs["password"] = self.password
        return kwargs


@dataclass
class FeatureDefinition:
    """Definition of a single computed feature."""
    name: str
    source_topic: str
    key_field: str
    value_field: str
    aggregation: AggregationType
    window_type: WindowType
    window_size_seconds: int
    window_slide_seconds: Optional[int] = None
    session_gap_seconds: Optional[int] = None
    allowed_lateness_seconds: int = 60
    output_type: str = "float64"
    description: str = ""
    tags: List[str] = field(default_factory=list)
    ttl_seconds: Optional[int] = None


@dataclass
class FeatureConfig:
    """
    Feature computation configuration.

    Defines feature schemas, computation parameters, and quality thresholds
    for the real-time feature computation pipeline.
    """
    features: List[FeatureDefinition] = field(default_factory=list)
    feature_groups: Dict[str, List[str]] = field(default_factory=dict)

    # Quality thresholds
    max_null_ratio: float = 0.05
    max_stale_seconds: int = 300
    distribution_check_window: int = 1000
    distribution_drift_threshold: float = 0.1

    # Materialization
    materialization_interval_seconds: int = 60
    batch_write_size: int = 500

    @classmethod
    def from_yaml(cls, path: str) -> "FeatureConfig":
        """Load feature configuration from a YAML file."""
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        features = []
        for feat_data in data.get("features", []):
            features.append(FeatureDefinition(
                name=feat_data["name"],
                source_topic=feat_data["source_topic"],
                key_field=feat_data["key_field"],
                value_field=feat_data["value_field"],
                aggregation=AggregationType(feat_data["aggregation"]),
                window_type=WindowType(feat_data["window_type"]),
                window_size_seconds=feat_data["window_size_seconds"],
                window_slide_seconds=feat_data.get("window_slide_seconds"),
                session_gap_seconds=feat_data.get("session_gap_seconds"),
                allowed_lateness_seconds=feat_data.get(
                    "allowed_lateness_seconds", 60
                ),
                output_type=feat_data.get("output_type", "float64"),
                description=feat_data.get("description", ""),
                tags=feat_data.get("tags", []),
                ttl_seconds=feat_data.get("ttl_seconds"),
            ))

        return cls(
            features=features,
            feature_groups=data.get("feature_groups", {}),
            max_null_ratio=data.get("quality", {}).get("max_null_ratio", 0.05),
            max_stale_seconds=data.get("quality", {}).get(
                "max_stale_seconds", 300
            ),
            distribution_check_window=data.get("quality", {}).get(
                "distribution_check_window", 1000
            ),
            distribution_drift_threshold=data.get("quality", {}).get(
                "distribution_drift_threshold", 0.1
            ),
            materialization_interval_seconds=data.get(
                "materialization_interval_seconds", 60
            ),
            batch_write_size=data.get("batch_write_size", 500),
        )


@dataclass
class BackfillConfig:
    """
    Historical backfill configuration.

    Controls Spark-based offline computation parameters, batch sizes,
    and reconciliation settings between online and offline stores.
    """
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    spark_app_name: str = "feature-backfill"
    executor_memory: str = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    executor_cores: int = int(os.getenv("SPARK_EXECUTOR_CORES", "2"))
    num_executors: int = int(os.getenv("SPARK_NUM_EXECUTORS", "4"))

    # Backfill parameters
    batch_size: int = 10000
    max_concurrent_writes: int = 8
    write_timeout_seconds: int = 300

    # Reconciliation
    reconciliation_enabled: bool = True
    reconciliation_tolerance: float = 0.001
    reconciliation_sample_rate: float = 0.01

    # Time range
    default_lookback_days: int = 90
    max_lookback_days: int = 365

    @property
    def spark_config(self) -> Dict[str, str]:
        return {
            "spark.master": self.spark_master,
            "spark.app.name": self.spark_app_name,
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.instances": str(self.num_executors),
            "spark.sql.shuffle.partitions": "200",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        }


@dataclass
class EngineSettings:
    """
    Top-level configuration aggregating all subsystem configs.

    Provides a single entry point for initializing the entire engine
    with consistent settings.
    """
    flink: FlinkConfig = field(default_factory=FlinkConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    cassandra: CassandraConfig = field(default_factory=CassandraConfig)
    redis: RedisConfig = field(default_factory=RedisConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    backfill: BackfillConfig = field(default_factory=BackfillConfig)

    @classmethod
    def from_yaml(cls, config_dir: str) -> "EngineSettings":
        """Load all settings from a configuration directory."""
        config_path = Path(config_dir)

        settings = cls()

        feature_config_path = config_path / "feature_config.yaml"
        if feature_config_path.exists():
            settings.features = FeatureConfig.from_yaml(
                str(feature_config_path)
            )

        return settings

    def validate(self) -> List[str]:
        """Validate configuration and return list of warnings."""
        warnings = []

        if self.flink.parallelism < 1:
            warnings.append("Flink parallelism must be >= 1")

        if self.redis.default_ttl_seconds < 60:
            warnings.append(
                "Redis TTL < 60s may cause excessive cache misses"
            )

        if self.cassandra.replication_factor < 2:
            warnings.append(
                "Cassandra replication factor < 2 provides no fault tolerance"
            )

        if self.kafka.max_poll_records > 1000:
            warnings.append(
                "Kafka max_poll_records > 1000 may cause consumer timeouts"
            )

        return warnings
