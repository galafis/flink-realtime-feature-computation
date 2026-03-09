"""
Complex Event Processing (CEP)

Detects temporal patterns in event streams such as sequences,
absence conditions, and frequency-based anomalies. Generates
alerts when patterns are matched.

Author: Gabriel Demetrios Lafis
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

from src.stream.source import StreamEvent
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class PatternType(str, Enum):
    """Types of event patterns to detect."""
    SEQUENCE = "sequence"          # A followed by B within time
    ABSENCE = "absence"            # A not followed by B within time
    FREQUENCY = "frequency"        # N occurrences of A within time
    THRESHOLD = "threshold"        # Value exceeds threshold


@dataclass
class PatternDefinition:
    """
    Defines a temporal event pattern for detection.

    Attributes:
        name: Unique pattern name.
        pattern_type: Type of pattern (sequence, absence, frequency, threshold).
        event_types: Ordered list of event types in the pattern.
        time_window_seconds: Maximum time span for the pattern.
        key_field: Field to partition pattern matching by (e.g., user_id).
        conditions: Additional conditions for pattern matching.
        min_occurrences: Minimum event count for frequency patterns.
        threshold_field: Field to check for threshold patterns.
        threshold_value: Threshold value for threshold patterns.
        description: Human-readable description of the pattern.
    """
    name: str
    pattern_type: str
    event_types: List[str]
    time_window_seconds: float
    key_field: str = "user_id"
    conditions: Dict[str, Any] = field(default_factory=dict)
    min_occurrences: int = 1
    threshold_field: str = ""
    threshold_value: float = 0.0
    description: str = ""


@dataclass
class PatternMatch:
    """Represents a detected pattern match."""
    pattern_name: str
    pattern_type: str
    entity_id: str
    matched_events: List[Dict[str, Any]]
    match_time: float = field(default_factory=time.time)
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def event_count(self) -> int:
        return len(self.matched_events)

    @property
    def time_span_seconds(self) -> float:
        if len(self.matched_events) < 2:
            return 0.0
        timestamps = [e.get("timestamp", 0) for e in self.matched_events]
        return max(timestamps) - min(timestamps)


@dataclass
class Alert:
    """Alert generated from a pattern match."""
    alert_id: str
    pattern_name: str
    severity: str
    entity_id: str
    message: str
    match: PatternMatch
    created_at: float = field(default_factory=time.time)
    acknowledged: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "pattern_name": self.pattern_name,
            "severity": self.severity,
            "entity_id": self.entity_id,
            "message": self.message,
            "event_count": self.match.event_count,
            "time_span_seconds": self.match.time_span_seconds,
            "created_at": self.created_at,
        }


class ComplexEventProcessor:
    """
    Complex Event Processing engine for pattern detection in streams.

    Maintains per-key state machines that track partial pattern matches
    and emit alerts when complete patterns are detected.

    Features:
        - Sequence detection (A -> B within time window)
        - Absence detection (A without B within time window)
        - Frequency detection (N events of type A within time)
        - Threshold detection (value exceeds configured threshold)
        - Per-key state management with automatic cleanup
        - Alert generation with severity levels
        - Configurable alert callbacks

    Usage:
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="cart_abandonment",
            pattern_type="absence",
            event_types=["add_to_cart", "purchase"],
            time_window_seconds=1800,
        ))
        alerts = cep.process_event(event)
    """

    def __init__(
        self,
        on_alert: Optional[Callable[[Alert], None]] = None,
        state_cleanup_interval_seconds: float = 60.0,
    ):
        self._patterns: Dict[str, PatternDefinition] = {}
        self._on_alert = on_alert
        self._cleanup_interval = state_cleanup_interval_seconds
        self._last_cleanup = time.time()
        self._lock = threading.Lock()

        # State: pattern_name -> key -> list of partial match events
        self._state: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(list)
        )

        # Frequency counters: pattern_name -> key -> list of timestamps
        self._frequency_state: Dict[str, Dict[str, List[float]]] = defaultdict(
            lambda: defaultdict(list)
        )

        self._alert_counter = 0
        self._metrics = {
            "events_processed": 0,
            "patterns_checked": 0,
            "matches_found": 0,
            "alerts_generated": 0,
        }

    def register_pattern(self, pattern: PatternDefinition) -> None:
        """Register a new pattern for detection."""
        with self._lock:
            self._patterns[pattern.name] = pattern
            logger.info(
                "Pattern registered",
                extra={
                    "pattern": pattern.name,
                    "type": pattern.pattern_type,
                    "event_types": pattern.event_types,
                },
            )

    def unregister_pattern(self, name: str) -> bool:
        """Remove a pattern from detection."""
        with self._lock:
            if name in self._patterns:
                del self._patterns[name]
                self._state.pop(name, None)
                self._frequency_state.pop(name, None)
                return True
            return False

    def process_event(self, event: StreamEvent) -> List[Alert]:
        """
        Process a single event against all registered patterns.

        Args:
            event: The incoming stream event.

        Returns:
            List of alerts generated from pattern matches.
        """
        alerts: List[Alert] = []
        self._metrics["events_processed"] += 1

        with self._lock:
            for name, pattern in self._patterns.items():
                self._metrics["patterns_checked"] += 1

                # Extract key
                if pattern.key_field == "user_id":
                    key = event.user_id
                else:
                    key = str(event.properties.get(pattern.key_field, event.user_id))

                if pattern.pattern_type == PatternType.SEQUENCE:
                    alert = self._check_sequence(pattern, key, event)
                elif pattern.pattern_type == PatternType.ABSENCE:
                    alert = self._check_absence(pattern, key, event)
                elif pattern.pattern_type == PatternType.FREQUENCY:
                    alert = self._check_frequency(pattern, key, event)
                elif pattern.pattern_type == PatternType.THRESHOLD:
                    alert = self._check_threshold(pattern, key, event)
                else:
                    alert = None

                if alert is not None:
                    alerts.append(alert)
                    self._metrics["alerts_generated"] += 1

                    if self._on_alert:
                        self._on_alert(alert)

        # Periodic state cleanup
        if (time.time() - self._last_cleanup) > self._cleanup_interval:
            self._cleanup_expired_state()
            self._last_cleanup = time.time()

        return alerts

    def process_events(self, events: List[StreamEvent]) -> List[Alert]:
        """Process a batch of events and return all generated alerts."""
        all_alerts: List[Alert] = []
        for event in events:
            all_alerts.extend(self.process_event(event))
        return all_alerts

    def check_pending_absences(self, current_time: float) -> List[Alert]:
        """
        Check for absence patterns whose time window has expired.

        Called periodically to detect cases where an expected event
        did NOT occur within the time window.

        Args:
            current_time: Current timestamp for expiry check.

        Returns:
            List of absence alerts.
        """
        alerts: List[Alert] = []

        with self._lock:
            for name, pattern in self._patterns.items():
                if pattern.pattern_type != PatternType.ABSENCE:
                    continue

                state = self._state[name]
                expired_keys = []

                for key, events in state.items():
                    if not events:
                        continue

                    first_event = events[0]
                    first_time = first_event.get("timestamp", 0)

                    # Check if the window has expired without the completing event
                    if (current_time - first_time) >= pattern.time_window_seconds:
                        # Absence confirmed: first event occurred but completing event did not
                        match = PatternMatch(
                            pattern_name=name,
                            pattern_type=PatternType.ABSENCE,
                            entity_id=key,
                            matched_events=list(events),
                            metadata={"absence_type": pattern.event_types[-1]},
                        )
                        alert = self._create_alert(pattern, match)
                        alerts.append(alert)
                        expired_keys.append(key)

                        if self._on_alert:
                            self._on_alert(alert)

                for key in expired_keys:
                    del state[key]

        return alerts

    # --- Pattern detection methods ---

    def _check_sequence(
        self, pattern: PatternDefinition, key: str, event: StreamEvent
    ) -> Optional[Alert]:
        """Detect sequential event patterns (A followed by B)."""
        state = self._state[pattern.name]
        event_dict = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "properties": dict(event.properties),
        }

        expected_types = pattern.event_types

        if event.event_type == expected_types[0]:
            # Start of sequence
            state[key] = [event_dict]
            return None

        if key not in state or not state[key]:
            return None

        partial = state[key]
        next_idx = len(partial)

        if next_idx < len(expected_types) and event.event_type == expected_types[next_idx]:
            # Check time window
            first_time = partial[0].get("timestamp", 0)
            if (event.timestamp - first_time) <= pattern.time_window_seconds:
                partial.append(event_dict)

                # Check if pattern is complete
                if len(partial) == len(expected_types):
                    match = PatternMatch(
                        pattern_name=pattern.name,
                        pattern_type=PatternType.SEQUENCE,
                        entity_id=key,
                        matched_events=list(partial),
                    )
                    self._metrics["matches_found"] += 1
                    state[key] = []
                    return self._create_alert(pattern, match)
            else:
                # Window expired; reset
                state[key] = []

        return None

    def _check_absence(
        self, pattern: PatternDefinition, key: str, event: StreamEvent
    ) -> Optional[Alert]:
        """
        Track absence patterns (A not followed by B within time).

        The actual absence alert is generated by check_pending_absences()
        when the time window expires without seeing the completing event.
        """
        state = self._state[pattern.name]
        event_dict = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "timestamp": event.timestamp,
            "properties": dict(event.properties),
        }

        if len(pattern.event_types) < 2:
            return None

        trigger_type = pattern.event_types[0]
        completing_type = pattern.event_types[-1]

        if event.event_type == trigger_type:
            # Start tracking for absence
            state[key] = [event_dict]
            return None

        if event.event_type == completing_type and key in state and state[key]:
            # Completing event arrived; absence NOT detected, clear state
            first_time = state[key][0].get("timestamp", 0)
            if (event.timestamp - first_time) <= pattern.time_window_seconds:
                state[key] = []

        return None

    def _check_frequency(
        self, pattern: PatternDefinition, key: str, event: StreamEvent
    ) -> Optional[Alert]:
        """Detect frequency-based patterns (N occurrences within time)."""
        if not pattern.event_types or event.event_type not in pattern.event_types:
            return None

        freq_state = self._frequency_state[pattern.name]
        timestamps = freq_state[key]
        now = event.timestamp

        # Add current timestamp
        timestamps.append(now)

        # Remove expired timestamps
        cutoff = now - pattern.time_window_seconds
        freq_state[key] = [t for t in timestamps if t >= cutoff]
        timestamps = freq_state[key]

        if len(timestamps) >= pattern.min_occurrences:
            match = PatternMatch(
                pattern_name=pattern.name,
                pattern_type=PatternType.FREQUENCY,
                entity_id=key,
                matched_events=[
                    {"event_type": event.event_type, "timestamp": t}
                    for t in timestamps
                ],
                metadata={"count": len(timestamps)},
            )
            self._metrics["matches_found"] += 1
            freq_state[key] = []  # Reset after match
            return self._create_alert(pattern, match)

        return None

    def _check_threshold(
        self, pattern: PatternDefinition, key: str, event: StreamEvent
    ) -> Optional[Alert]:
        """Detect threshold-crossing events."""
        if pattern.event_types and event.event_type not in pattern.event_types:
            return None

        if not pattern.threshold_field:
            return None

        value = event.properties.get(pattern.threshold_field)
        if value is None:
            return None

        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return None

        if numeric_value >= pattern.threshold_value:
            match = PatternMatch(
                pattern_name=pattern.name,
                pattern_type=PatternType.THRESHOLD,
                entity_id=key,
                matched_events=[{
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "timestamp": event.timestamp,
                    "field": pattern.threshold_field,
                    "value": numeric_value,
                    "threshold": pattern.threshold_value,
                }],
                metadata={
                    "value": numeric_value,
                    "threshold": pattern.threshold_value,
                    "field": pattern.threshold_field,
                },
            )
            self._metrics["matches_found"] += 1
            return self._create_alert(pattern, match)

        return None

    def _create_alert(self, pattern: PatternDefinition, match: PatternMatch) -> Alert:
        """Create an alert from a pattern match."""
        self._alert_counter += 1
        alert_id = f"ALERT-{self._alert_counter:06d}"

        # Determine severity based on pattern type
        severity_map = {
            PatternType.THRESHOLD: "HIGH",
            PatternType.FREQUENCY: "MEDIUM",
            PatternType.ABSENCE: "MEDIUM",
            PatternType.SEQUENCE: "LOW",
        }
        severity = severity_map.get(pattern.pattern_type, "INFO")

        message = (
            f"Pattern '{pattern.name}' detected for entity '{match.entity_id}': "
            f"{pattern.description or pattern.pattern_type}"
        )

        return Alert(
            alert_id=alert_id,
            pattern_name=pattern.name,
            severity=severity,
            entity_id=match.entity_id,
            message=message,
            match=match,
        )

    def _cleanup_expired_state(self) -> None:
        """Remove expired partial matches from state."""
        now = time.time()
        cleaned = 0

        for name, pattern in self._patterns.items():
            # Clean sequence/absence state
            state = self._state.get(name, {})
            expired_keys = []
            for key, events in state.items():
                if events:
                    first_time = events[0].get("timestamp", 0)
                    if (now - first_time) > pattern.time_window_seconds * 2:
                        expired_keys.append(key)

            for key in expired_keys:
                del state[key]
                cleaned += 1

            # Clean frequency state
            freq = self._frequency_state.get(name, {})
            cutoff = now - pattern.time_window_seconds
            for key in list(freq.keys()):
                freq[key] = [t for t in freq[key] if t >= cutoff]
                if not freq[key]:
                    del freq[key]
                    cleaned += 1

        if cleaned > 0:
            logger.debug(
                "CEP state cleanup",
                extra={"cleaned_entries": cleaned},
            )

    @property
    def metrics(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "registered_patterns": list(self._patterns.keys()),
            "active_state_keys": sum(
                len(v) for v in self._state.values()
            ),
        }
