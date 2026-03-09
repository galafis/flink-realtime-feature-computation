"""
Kafka Feature Event Producer

High-throughput, exactly-once producer for feature computation events.
Supports Avro-like serialization, custom partitioning strategies,
and asynchronous delivery with configurable callbacks.

Author: Gabriel Demetrios Lafis
"""

import json
import time
import hashlib
import struct
from typing import Any, Callable, Dict, List, Optional, Union
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from concurrent.futures import Future
from enum import Enum

from src.utils.logger import setup_logger, get_correlation_id
from src.config.settings import KafkaConfig

logger = setup_logger(__name__)


class PartitionStrategy(str, Enum):
    """Partitioning strategies for feature events."""
    KEY_HASH = "key_hash"
    ENTITY_BASED = "entity_based"
    ROUND_ROBIN = "round_robin"
    FEATURE_GROUP = "feature_group"


@dataclass
class FeatureEvent:
    """
    Structured feature event for Kafka transport.

    Represents a single feature computation event with metadata
    for tracing, versioning, and exactly-once processing.
    """
    entity_id: str
    feature_name: str
    feature_value: Any
    timestamp: float = field(default_factory=lambda: time.time())
    event_type: str = "feature_update"
    schema_version: int = 1
    correlation_id: str = field(default_factory=get_correlation_id)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def serialize(self) -> bytes:
        """Serialize to Avro-like binary format with schema header."""
        data = self.to_dict()
        # Magic byte + schema version (4 bytes) + JSON payload
        payload = json.dumps(data, default=str).encode("utf-8")
        header = struct.pack(">bI", 0, self.schema_version)
        return header + payload

    @classmethod
    def deserialize(cls, data: bytes) -> "FeatureEvent":
        """Deserialize from binary format."""
        magic, schema_version = struct.unpack(">bI", data[:5])
        if magic != 0:
            raise ValueError(f"Invalid magic byte: {magic}")
        payload = json.loads(data[5:].decode("utf-8"))
        payload["schema_version"] = schema_version
        return cls(**payload)


class FeatureEventProducer:
    """
    High-performance Kafka producer for feature events.

    Features:
        - Avro-like serialization with schema versioning
        - Configurable partitioning strategies (key hash, entity-based, round-robin)
        - Asynchronous delivery with callback support
        - Batch sending with automatic flushing
        - Dead-letter queue for failed messages
        - Delivery metrics and monitoring
    """

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        partition_strategy: PartitionStrategy = PartitionStrategy.KEY_HASH,
        on_delivery: Optional[Callable] = None,
    ):
        self.config = config or KafkaConfig()
        self.partition_strategy = partition_strategy
        self._on_delivery_callback = on_delivery
        self._producer = None
        self._is_running = False
        self._batch_buffer: List[FeatureEvent] = []
        self._batch_size = 100
        self._metrics = {
            "messages_sent": 0,
            "messages_failed": 0,
            "bytes_sent": 0,
            "batches_flushed": 0,
        }

    def start(self) -> None:
        """Initialize the Kafka producer with configured settings."""
        try:
            from confluent_kafka import Producer

            producer_config = {
                "bootstrap.servers": self.config.bootstrap_servers,
                "acks": self.config.acks,
                "retries": self.config.retries,
                "batch.size": self.config.batch_size,
                "linger.ms": self.config.linger_ms,
                "compression.type": self.config.compression_type,
                "enable.idempotence": self.config.enable_idempotence,
                "max.in.flight.requests.per.connection": (
                    self.config.max_in_flight_requests
                ),
            }

            if self.config.security_protocol != "PLAINTEXT":
                producer_config["security.protocol"] = (
                    self.config.security_protocol
                )

            self._producer = Producer(producer_config)
            self._is_running = True
            logger.info(
                "Kafka producer started",
                extra={
                    "bootstrap_servers": self.config.bootstrap_servers,
                    "partition_strategy": self.partition_strategy.value,
                },
            )
        except ImportError:
            logger.warning(
                "confluent_kafka not installed; using mock producer"
            )
            self._producer = _MockProducer()
            self._is_running = True

    def stop(self) -> None:
        """Flush pending messages and shut down the producer."""
        if self._batch_buffer:
            self.flush_batch()

        if self._producer is not None:
            self._producer.flush(timeout=30)
            self._is_running = False
            logger.info(
                "Kafka producer stopped",
                extra={"metrics": self._metrics},
            )

    def send(
        self,
        event: FeatureEvent,
        topic: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Send a single feature event to Kafka.

        Args:
            event: The feature event to publish.
            topic: Target topic (defaults to feature_events_topic).
            key: Partition key (defaults to entity_id).
            headers: Optional Kafka headers for metadata.
        """
        if not self._is_running:
            raise RuntimeError("Producer is not started")

        target_topic = topic or self.config.feature_events_topic
        partition_key = key or self._compute_partition_key(event)
        serialized_value = event.serialize()

        kafka_headers = [
            ("correlation_id", event.correlation_id.encode("utf-8")),
            ("schema_version", str(event.schema_version).encode("utf-8")),
            ("event_type", event.event_type.encode("utf-8")),
        ]
        if headers:
            kafka_headers.extend(
                (k, v.encode("utf-8")) for k, v in headers.items()
            )

        try:
            self._producer.produce(
                topic=target_topic,
                key=partition_key.encode("utf-8"),
                value=serialized_value,
                headers=kafka_headers,
                callback=self._delivery_callback,
            )
            self._metrics["messages_sent"] += 1
            self._metrics["bytes_sent"] += len(serialized_value)

            # Trigger periodic poll for callbacks
            self._producer.poll(0)

        except Exception as e:
            self._metrics["messages_failed"] += 1
            logger.error(
                "Failed to produce message",
                extra={
                    "topic": target_topic,
                    "entity_id": event.entity_id,
                    "error": str(e),
                },
                exc_info=True,
            )
            self._send_to_dlq(event, str(e))

    def send_batch(self, events: List[FeatureEvent], topic: Optional[str] = None) -> None:
        """
        Send a batch of feature events to Kafka.

        Args:
            events: List of feature events to publish.
            topic: Target topic for all events in the batch.
        """
        for event in events:
            self.send(event, topic=topic)

        # Flush after batch to ensure delivery
        if self._producer is not None:
            self._producer.flush(timeout=10)
            self._metrics["batches_flushed"] += 1

    def buffer_event(self, event: FeatureEvent) -> None:
        """
        Add an event to the internal batch buffer.

        The buffer is automatically flushed when it reaches batch_size.
        """
        self._batch_buffer.append(event)
        if len(self._batch_buffer) >= self._batch_size:
            self.flush_batch()

    def flush_batch(self) -> None:
        """Flush all buffered events to Kafka."""
        if not self._batch_buffer:
            return

        events = self._batch_buffer.copy()
        self._batch_buffer.clear()
        self.send_batch(events)

        logger.debug(
            "Batch flushed",
            extra={"batch_size": len(events)},
        )

    def _compute_partition_key(self, event: FeatureEvent) -> str:
        """Compute the partition key based on the configured strategy."""
        if self.partition_strategy == PartitionStrategy.KEY_HASH:
            return event.entity_id

        elif self.partition_strategy == PartitionStrategy.ENTITY_BASED:
            return event.entity_id

        elif self.partition_strategy == PartitionStrategy.FEATURE_GROUP:
            composite = f"{event.entity_id}:{event.feature_name}"
            return hashlib.md5(composite.encode()).hexdigest()

        else:  # ROUND_ROBIN
            return ""

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Handle delivery confirmation or failure."""
        if err is not None:
            self._metrics["messages_failed"] += 1
            logger.error(
                "Message delivery failed",
                extra={
                    "error": str(err),
                    "topic": msg.topic() if msg else "unknown",
                },
            )
        else:
            if self._on_delivery_callback:
                self._on_delivery_callback(msg)

    def _send_to_dlq(self, event: FeatureEvent, error_message: str) -> None:
        """Send failed events to the dead-letter queue."""
        try:
            event.metadata["error"] = error_message
            event.metadata["original_timestamp"] = event.timestamp
            event.timestamp = time.time()

            serialized = event.serialize()

            self._producer.produce(
                topic=self.config.dead_letter_topic,
                key=event.entity_id.encode("utf-8"),
                value=serialized,
                callback=lambda err, msg: None,
            )
        except Exception as dlq_error:
            logger.error(
                "Failed to send to DLQ",
                extra={
                    "entity_id": event.entity_id,
                    "error": str(dlq_error),
                },
            )

    @property
    def metrics(self) -> Dict[str, int]:
        return dict(self._metrics)

    def __enter__(self) -> "FeatureEventProducer":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


class _MockProducer:
    """In-memory mock producer for testing without Kafka."""

    def __init__(self):
        self.messages: List[Dict[str, Any]] = []

    def produce(self, topic: str, key: bytes, value: bytes,
                headers: list = None, callback: Callable = None) -> None:
        self.messages.append({
            "topic": topic,
            "key": key,
            "value": value,
            "headers": headers,
        })
        if callback:
            callback(None, _MockMessage(topic, key, value))

    def flush(self, timeout: int = 30) -> int:
        return 0

    def poll(self, timeout: float = 0) -> int:
        return 0


class _MockMessage:
    """Mock Kafka message for callback simulation."""

    def __init__(self, topic: str, key: bytes, value: bytes):
        self._topic = topic
        self._key = key
        self._value = value

    def topic(self) -> str:
        return self._topic

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes:
        return self._value
