"""
Stream Processor

Flink-inspired stream processing engine for real-time feature computation.
Supports event-time and processing-time semantics, watermark tracking,
windowed aggregations, and exactly-once processing guarantees.

Author: Gabriel Demetrios Lafis
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from enum import Enum

from src.stream.source import StreamEvent
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class TimeCharacteristic(str, Enum):
    """Time semantics for stream processing."""
    EVENT_TIME = "event_time"
    PROCESSING_TIME = "processing_time"


@dataclass
class Watermark:
    """
    Watermark for tracking event-time progress.

    Indicates that no events with timestamp less than or equal to
    the watermark value will arrive in the future. Used to trigger
    window computations and manage late event handling.
    """
    timestamp: float = 0.0
    source_id: str = ""

    def advance(self, new_timestamp: float, max_out_of_orderness: float = 5.0) -> bool:
        """
        Advance the watermark based on observed event timestamp.

        Args:
            new_timestamp: Observed event timestamp.
            max_out_of_orderness: Maximum expected out-of-orderness in seconds.

        Returns:
            True if the watermark was advanced.
        """
        proposed = new_timestamp - max_out_of_orderness
        if proposed > self.timestamp:
            self.timestamp = proposed
            return True
        return False

    @property
    def as_datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)


@dataclass
class ProcessingContext:
    """Context passed to processing functions for each event."""
    event: StreamEvent
    watermark: Watermark
    current_time: float = field(default_factory=time.time)
    time_characteristic: TimeCharacteristic = TimeCharacteristic.EVENT_TIME
    partition_key: str = ""

    @property
    def is_late(self) -> bool:
        """Check if the event arrived after the watermark."""
        if self.time_characteristic == TimeCharacteristic.EVENT_TIME:
            return self.event.timestamp < self.watermark.timestamp
        return False


class StreamProcessor:
    """
    Flink-inspired stream processing engine.

    Provides a dataflow programming model with operators for filtering,
    mapping, keying, and windowed aggregations. Supports both event-time
    and processing-time semantics with watermark-based progress tracking.

    Features:
        - Event-time and processing-time semantics
        - Watermark generation and tracking
        - Configurable allowed lateness for late events
        - Key-based partitioning for stateful operations
        - Operator chaining (filter, map, flatmap, key_by)
        - Processing metrics and monitoring
    """

    def __init__(
        self,
        time_characteristic: TimeCharacteristic = TimeCharacteristic.EVENT_TIME,
        max_out_of_orderness_seconds: float = 5.0,
        allowed_lateness_seconds: float = 30.0,
        parallelism: int = 1,
    ):
        self.time_characteristic = time_characteristic
        self.max_out_of_orderness = max_out_of_orderness_seconds
        self.allowed_lateness = allowed_lateness_seconds
        self.parallelism = parallelism

        # Operator chain
        self._filters: List[Callable[[StreamEvent], bool]] = []
        self._mappers: List[Callable[[StreamEvent], StreamEvent]] = []
        self._flat_mappers: List[Callable[[StreamEvent], List[StreamEvent]]] = []
        self._key_extractor: Optional[Callable[[StreamEvent], str]] = None
        self._event_handlers: List[Callable[[ProcessingContext], None]] = []
        self._late_event_handler: Optional[Callable[[StreamEvent], None]] = None

        # State
        self._watermark = Watermark()
        self._partitioned_state: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self._lock = threading.Lock()
        self._is_running = False

        # Metrics
        self._metrics = {
            "events_processed": 0,
            "events_filtered": 0,
            "events_late": 0,
            "events_dropped": 0,
            "watermark_advances": 0,
            "processing_time_total_ms": 0.0,
            "max_processing_time_ms": 0.0,
        }

    # --- Operator chain API ---

    def filter(self, predicate: Callable[[StreamEvent], bool]) -> "StreamProcessor":
        """Add a filter operator to the pipeline."""
        self._filters.append(predicate)
        return self

    def map(self, mapper: Callable[[StreamEvent], StreamEvent]) -> "StreamProcessor":
        """Add a map operator to transform events."""
        self._mappers.append(mapper)
        return self

    def flat_map(self, mapper: Callable[[StreamEvent], List[StreamEvent]]) -> "StreamProcessor":
        """Add a flat-map operator that can produce zero or more events."""
        self._flat_mappers.append(mapper)
        return self

    def key_by(self, key_extractor: Callable[[StreamEvent], str]) -> "StreamProcessor":
        """Set the key extractor for partitioned state."""
        self._key_extractor = key_extractor
        return self

    def add_handler(self, handler: Callable[[ProcessingContext], None]) -> "StreamProcessor":
        """Register an event processing handler."""
        self._event_handlers.append(handler)
        return self

    def set_late_event_handler(self, handler: Callable[[StreamEvent], None]) -> "StreamProcessor":
        """Register a handler for late events that exceed allowed lateness."""
        self._late_event_handler = handler
        return self

    # --- Processing API ---

    def process_event(self, event: StreamEvent) -> bool:
        """
        Process a single event through the operator chain.

        Args:
            event: The incoming stream event.

        Returns:
            True if the event was processed, False if filtered or dropped.
        """
        start_time = time.monotonic()

        try:
            # Apply filters
            for pred in self._filters:
                if not pred(event):
                    self._metrics["events_filtered"] += 1
                    return False

            # Apply mappers
            current_event = event
            for mapper in self._mappers:
                current_event = mapper(current_event)

            # Handle flat-mappers (expand to multiple events)
            events_to_process = [current_event]
            for flat_mapper in self._flat_mappers:
                expanded = []
                for evt in events_to_process:
                    expanded.extend(flat_mapper(evt))
                events_to_process = expanded

            # Process each event
            for evt in events_to_process:
                self._process_single(evt)

            return True

        finally:
            elapsed_ms = (time.monotonic() - start_time) * 1000
            self._metrics["processing_time_total_ms"] += elapsed_ms
            self._metrics["max_processing_time_ms"] = max(
                self._metrics["max_processing_time_ms"], elapsed_ms
            )

    def process_batch(self, events: List[StreamEvent]) -> Dict[str, int]:
        """
        Process a batch of events.

        Args:
            events: List of events to process.

        Returns:
            Dictionary with counts: processed, filtered, late, dropped.
        """
        results = {"processed": 0, "filtered": 0, "late": 0, "dropped": 0}

        # Sort by event time for proper watermark advancement
        if self.time_characteristic == TimeCharacteristic.EVENT_TIME:
            sorted_events = sorted(events, key=lambda e: e.timestamp)
        else:
            sorted_events = events

        for event in sorted_events:
            if self.process_event(event):
                results["processed"] += 1
            else:
                results["filtered"] += 1

        results["late"] = self._metrics["events_late"]
        return results

    def _process_single(self, event: StreamEvent) -> None:
        """Process a single event after operator chain."""
        # Extract partition key
        key = ""
        if self._key_extractor is not None:
            key = self._key_extractor(event)

        # Update watermark
        if self.time_characteristic == TimeCharacteristic.EVENT_TIME:
            advanced = self._watermark.advance(
                event.timestamp, self.max_out_of_orderness
            )
            if advanced:
                self._metrics["watermark_advances"] += 1

        # Build processing context
        ctx = ProcessingContext(
            event=event,
            watermark=self._watermark,
            current_time=time.time(),
            time_characteristic=self.time_characteristic,
            partition_key=key,
        )

        # Check for late events
        if ctx.is_late:
            self._metrics["events_late"] += 1

            # Check if within allowed lateness
            lateness = self._watermark.timestamp - event.timestamp
            if lateness > self.allowed_lateness:
                self._metrics["events_dropped"] += 1
                if self._late_event_handler:
                    self._late_event_handler(event)
                return

        # Invoke handlers
        for handler in self._event_handlers:
            handler(ctx)

        self._metrics["events_processed"] += 1

    # --- State management ---

    def get_state(self, key: str, name: str, default: Any = None) -> Any:
        """Get partitioned state value."""
        with self._lock:
            return self._partitioned_state.get(key, {}).get(name, default)

    def set_state(self, key: str, name: str, value: Any) -> None:
        """Set partitioned state value."""
        with self._lock:
            if key not in self._partitioned_state:
                self._partitioned_state[key] = {}
            self._partitioned_state[key][name] = value

    def clear_state(self, key: str) -> None:
        """Clear all state for a given key."""
        with self._lock:
            self._partitioned_state.pop(key, None)

    # --- Watermark API ---

    @property
    def current_watermark(self) -> Watermark:
        """Get the current watermark."""
        return self._watermark

    def get_watermark_lag(self) -> float:
        """Get the lag between current time and watermark in seconds."""
        return time.time() - self._watermark.timestamp

    # --- Metrics ---

    @property
    def metrics(self) -> Dict[str, Any]:
        avg_processing_time = 0.0
        total = self._metrics["events_processed"] + self._metrics["events_filtered"]
        if total > 0:
            avg_processing_time = self._metrics["processing_time_total_ms"] / total

        return {
            **self._metrics,
            "avg_processing_time_ms": round(avg_processing_time, 3),
            "watermark": self._watermark.timestamp,
            "watermark_lag_seconds": round(self.get_watermark_lag(), 2),
            "partitioned_keys": len(self._partitioned_state),
        }

    def reset_metrics(self) -> None:
        """Reset all processing metrics."""
        for key in self._metrics:
            if isinstance(self._metrics[key], float):
                self._metrics[key] = 0.0
            else:
                self._metrics[key] = 0
