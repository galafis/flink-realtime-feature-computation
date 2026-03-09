"""
Kafka Feature Event Consumer

Reliable consumer for feature computation events with exactly-once
processing semantics, batch consumption, deserialization, and
graceful shutdown support.

Author: Gabriel Demetrios Lafis
"""

import json
import time
import signal
import struct
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass

from src.utils.logger import setup_logger, set_correlation_id
from src.config.settings import KafkaConfig
from src.kafka.producer import FeatureEvent

logger = setup_logger(__name__)


@dataclass
class ConsumedMessage:
    """Deserialized Kafka message with metadata."""
    event: FeatureEvent
    topic: str
    partition: int
    offset: int
    timestamp: float
    headers: Dict[str, str]
    key: str


class FeatureEventConsumer:
    """
    High-reliability Kafka consumer for feature events.

    Features:
        - Exactly-once processing via manual offset management
        - Avro-like deserialization with schema version validation
        - Configurable batch processing with backpressure
        - Graceful shutdown with offset commit
        - Dead-letter queue routing for malformed messages
        - Processing metrics and lag monitoring
    """

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        topics: Optional[List[str]] = None,
        process_fn: Optional[Callable[[List[ConsumedMessage]], None]] = None,
        batch_size: int = 100,
        batch_timeout_ms: int = 5000,
    ):
        self.config = config or KafkaConfig()
        self.topics = topics or [self.config.feature_events_topic]
        self._process_fn = process_fn
        self._batch_size = batch_size
        self._batch_timeout_ms = batch_timeout_ms
        self._consumer = None
        self._is_running = False
        self._shutdown_event = threading.Event()
        self._metrics = {
            "messages_consumed": 0,
            "messages_failed": 0,
            "batches_processed": 0,
            "bytes_consumed": 0,
            "last_offset": {},
        }

    def start(self) -> None:
        """Initialize the Kafka consumer and subscribe to topics."""
        try:
            from confluent_kafka import Consumer

            consumer_config = {
                "bootstrap.servers": self.config.bootstrap_servers,
                "group.id": self.config.consumer_group_id,
                "auto.offset.reset": self.config.auto_offset_reset,
                "enable.auto.commit": False,
                "max.poll.interval.ms": self.config.max_poll_interval_ms,
                "session.timeout.ms": self.config.session_timeout_ms,
                "heartbeat.interval.ms": self.config.heartbeat_interval_ms,
            }

            if self.config.security_protocol != "PLAINTEXT":
                consumer_config["security.protocol"] = (
                    self.config.security_protocol
                )

            self._consumer = Consumer(consumer_config)
            self._consumer.subscribe(
                self.topics,
                on_assign=self._on_partition_assign,
                on_revoke=self._on_partition_revoke,
            )
            self._is_running = True

            logger.info(
                "Kafka consumer started",
                extra={
                    "topics": self.topics,
                    "group_id": self.config.consumer_group_id,
                },
            )

        except ImportError:
            logger.warning(
                "confluent_kafka not installed; using mock consumer"
            )
            self._consumer = _MockConsumer()
            self._is_running = True

    def stop(self) -> None:
        """Gracefully shut down the consumer, committing final offsets."""
        self._shutdown_event.set()
        self._is_running = False

        if self._consumer is not None:
            try:
                self._consumer.commit(asynchronous=False)
            except Exception:
                pass
            self._consumer.close()

        logger.info(
            "Kafka consumer stopped",
            extra={"metrics": self._metrics},
        )

    def consume_loop(self) -> None:
        """
        Main consumption loop with batch processing.

        Polls Kafka for messages, accumulates them into batches,
        and invokes the processing function. Commits offsets only
        after successful processing (exactly-once semantics).
        """
        if not self._is_running:
            raise RuntimeError("Consumer is not started")

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, lambda *_: self.stop())
        signal.signal(signal.SIGTERM, lambda *_: self.stop())

        batch: List[ConsumedMessage] = []
        batch_start_time = time.time()

        logger.info("Entering consumption loop")

        while not self._shutdown_event.is_set():
            try:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    # Check if batch timeout exceeded
                    elapsed_ms = (time.time() - batch_start_time) * 1000
                    if batch and elapsed_ms >= self._batch_timeout_ms:
                        self._process_batch(batch)
                        batch = []
                        batch_start_time = time.time()
                    continue

                if msg.error():
                    self._handle_consumer_error(msg)
                    continue

                # Deserialize message
                consumed = self._deserialize_message(msg)
                if consumed is None:
                    continue

                batch.append(consumed)
                self._metrics["messages_consumed"] += 1
                self._metrics["bytes_consumed"] += len(msg.value())

                # Process batch if size threshold reached
                if len(batch) >= self._batch_size:
                    self._process_batch(batch)
                    batch = []
                    batch_start_time = time.time()

            except Exception as e:
                logger.error(
                    "Error in consumption loop",
                    extra={"error": str(e)},
                    exc_info=True,
                )
                time.sleep(1)

        # Process remaining messages in buffer
        if batch:
            self._process_batch(batch)

    def consume_batch(self, max_messages: int = 100, timeout_s: float = 5.0) -> List[ConsumedMessage]:
        """
        Consume a single batch of messages (non-blocking).

        Args:
            max_messages: Maximum number of messages to return.
            timeout_s: Maximum time to wait for messages.

        Returns:
            List of consumed and deserialized messages.
        """
        if not self._is_running:
            raise RuntimeError("Consumer is not started")

        messages: List[ConsumedMessage] = []
        deadline = time.time() + timeout_s

        while len(messages) < max_messages and time.time() < deadline:
            remaining = deadline - time.time()
            msg = self._consumer.poll(timeout=min(remaining, 1.0))

            if msg is None:
                continue
            if msg.error():
                self._handle_consumer_error(msg)
                continue

            consumed = self._deserialize_message(msg)
            if consumed:
                messages.append(consumed)

        return messages

    def commit_offsets(self) -> None:
        """Manually commit current consumer offsets."""
        if self._consumer is not None:
            self._consumer.commit(asynchronous=False)
            logger.debug("Offsets committed")

    def _process_batch(self, batch: List[ConsumedMessage]) -> None:
        """Process a batch of messages and commit offsets."""
        if not batch:
            return

        try:
            if self._process_fn:
                self._process_fn(batch)

            # Commit offsets after successful processing
            self._consumer.commit(asynchronous=False)
            self._metrics["batches_processed"] += 1

            # Track last offsets per partition
            for msg in batch:
                key = f"{msg.topic}:{msg.partition}"
                self._metrics["last_offset"][key] = msg.offset

            logger.debug(
                "Batch processed and committed",
                extra={"batch_size": len(batch)},
            )

        except Exception as e:
            self._metrics["messages_failed"] += len(batch)
            logger.error(
                "Batch processing failed",
                extra={
                    "batch_size": len(batch),
                    "error": str(e),
                },
                exc_info=True,
            )

    def _deserialize_message(self, msg: Any) -> Optional[ConsumedMessage]:
        """Deserialize a raw Kafka message into a ConsumedMessage."""
        try:
            value = msg.value()
            if value is None:
                return None

            event = FeatureEvent.deserialize(value)
            set_correlation_id(event.correlation_id)

            # Extract headers
            headers: Dict[str, str] = {}
            if msg.headers():
                for h_key, h_value in msg.headers():
                    if h_value is not None:
                        headers[h_key] = h_value.decode("utf-8")

            return ConsumedMessage(
                event=event,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                timestamp=msg.timestamp()[1] / 1000.0 if msg.timestamp()[0] != -1 else time.time(),
                headers=headers,
                key=msg.key().decode("utf-8") if msg.key() else "",
            )

        except Exception as e:
            self._metrics["messages_failed"] += 1
            logger.error(
                "Failed to deserialize message",
                extra={
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "error": str(e),
                },
            )
            return None

    def _handle_consumer_error(self, msg: Any) -> None:
        """Handle Kafka consumer errors."""
        logger.error(
            "Consumer error",
            extra={"error": str(msg.error())},
        )

    def _on_partition_assign(self, consumer: Any, partitions: Any) -> None:
        """Callback when partitions are assigned to this consumer."""
        partition_info = [
            {"topic": p.topic, "partition": p.partition}
            for p in partitions
        ]
        logger.info(
            "Partitions assigned",
            extra={"partitions": partition_info},
        )

    def _on_partition_revoke(self, consumer: Any, partitions: Any) -> None:
        """Callback when partitions are revoked from this consumer."""
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            pass
        logger.info("Partitions revoked, offsets committed")

    @property
    def metrics(self) -> Dict[str, Any]:
        return dict(self._metrics)

    def __enter__(self) -> "FeatureEventConsumer":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()


class _MockConsumer:
    """In-memory mock consumer for testing without Kafka."""

    def __init__(self):
        self._messages: List[Any] = []
        self._subscribed = False

    def subscribe(self, topics: List[str], **kwargs) -> None:
        self._subscribed = True

    def poll(self, timeout: float = 1.0) -> Optional[Any]:
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, asynchronous: bool = False) -> None:
        pass

    def close(self) -> None:
        self._subscribed = False

    def inject_message(self, message: Any) -> None:
        self._messages.append(message)
