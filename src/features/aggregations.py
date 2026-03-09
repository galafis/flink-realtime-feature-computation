"""
Windowed Aggregation Engine

Provides Flink-style window implementations (tumbling, sliding, session)
with support for multiple aggregation functions, late event handling,
and watermark-triggered computation.

Author: Gabriel Demetrios Lafis
"""

import math
import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class AggregationFunction(str, Enum):
    """Supported aggregation functions for windowed computations."""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    DISTINCT_COUNT = "distinct_count"
    PERCENTILE = "percentile"
    STDDEV = "stddev"
    FIRST_VALUE = "first_value"
    LAST_VALUE = "last_value"


@dataclass
class WindowBounds:
    """Defines the time boundaries of a window instance."""
    start: float  # epoch seconds
    end: float    # epoch seconds

    @property
    def duration_seconds(self) -> float:
        return self.end - self.start

    def contains(self, timestamp: float) -> bool:
        return self.start <= timestamp < self.end

    def __hash__(self) -> int:
        return hash((round(self.start, 3), round(self.end, 3)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, WindowBounds):
            return False
        return (
            abs(self.start - other.start) < 0.001
            and abs(self.end - other.end) < 0.001
        )


@dataclass
class WindowState:
    """Accumulated state within a single window instance."""
    bounds: WindowBounds
    values: List[float] = field(default_factory=list)
    distinct_values: Set[str] = field(default_factory=set)
    count: int = 0
    sum_value: float = 0.0
    min_value: float = float("inf")
    max_value: float = float("-inf")
    first_value: Optional[float] = None
    first_timestamp: float = float("inf")
    last_value: Optional[float] = None
    last_timestamp: float = 0.0
    created_at: float = field(default_factory=time.time)

    def add(self, value: float, timestamp: float = 0.0, distinct_key: Optional[str] = None) -> None:
        """Add a value to the window state."""
        self.values.append(value)
        self.count += 1
        self.sum_value += value

        if value < self.min_value:
            self.min_value = value
        if value > self.max_value:
            self.max_value = value

        if timestamp < self.first_timestamp:
            self.first_timestamp = timestamp
            self.first_value = value
        if timestamp >= self.last_timestamp:
            self.last_timestamp = timestamp
            self.last_value = value

        if distinct_key is not None:
            self.distinct_values.add(distinct_key)

    def compute(self, function: AggregationFunction, percentile_value: float = 95.0) -> Optional[float]:
        """
        Compute the aggregation result for this window.

        Args:
            function: The aggregation function to apply.
            percentile_value: Percentile value (0-100) for PERCENTILE function.

        Returns:
            The computed aggregate value, or None if no data.
        """
        if self.count == 0:
            return None

        if function == AggregationFunction.COUNT:
            return float(self.count)

        elif function == AggregationFunction.SUM:
            return self.sum_value

        elif function == AggregationFunction.AVG:
            return self.sum_value / self.count

        elif function == AggregationFunction.MIN:
            return self.min_value if self.min_value != float("inf") else None

        elif function == AggregationFunction.MAX:
            return self.max_value if self.max_value != float("-inf") else None

        elif function == AggregationFunction.DISTINCT_COUNT:
            return float(len(self.distinct_values))

        elif function == AggregationFunction.PERCENTILE:
            if not self.values:
                return None
            sorted_vals = sorted(self.values)
            idx = (percentile_value / 100.0) * (len(sorted_vals) - 1)
            lower = int(math.floor(idx))
            upper = min(int(math.ceil(idx)), len(sorted_vals) - 1)
            if lower == upper:
                return sorted_vals[lower]
            fraction = idx - lower
            return sorted_vals[lower] * (1 - fraction) + sorted_vals[upper] * fraction

        elif function == AggregationFunction.STDDEV:
            if self.count < 2:
                return 0.0
            mean = self.sum_value / self.count
            variance = sum((v - mean) ** 2 for v in self.values) / (self.count - 1)
            return math.sqrt(variance)

        elif function == AggregationFunction.FIRST_VALUE:
            return self.first_value

        elif function == AggregationFunction.LAST_VALUE:
            return self.last_value

        return None


class TumblingWindow:
    """
    Tumbling (fixed-size, non-overlapping) window implementation.

    Each event belongs to exactly one window based on its timestamp.
    Windows are triggered when the watermark passes the window end time.

    Example:
        window_size=60 seconds creates windows:
        [0,60), [60,120), [120,180), ...
    """

    def __init__(
        self,
        size_seconds: float,
        allowed_lateness_seconds: float = 0.0,
        offset_seconds: float = 0.0,
    ):
        self.size_seconds = size_seconds
        self.allowed_lateness = allowed_lateness_seconds
        self.offset = offset_seconds

    def assign_window(self, timestamp: float) -> WindowBounds:
        """Assign a timestamp to its tumbling window."""
        adjusted = timestamp - self.offset
        window_start = (adjusted // self.size_seconds) * self.size_seconds + self.offset
        return WindowBounds(
            start=window_start,
            end=window_start + self.size_seconds,
        )

    def is_window_ready(self, window: WindowBounds, watermark: float) -> bool:
        """Check if a window should be triggered based on the watermark."""
        return watermark >= window.end

    def is_late_but_allowed(self, window: WindowBounds, watermark: float) -> bool:
        """Check if a late event is within the allowed lateness period."""
        return (
            watermark >= window.end
            and watermark < (window.end + self.allowed_lateness)
        )


class SlidingWindow:
    """
    Sliding (overlapping) window implementation.

    Each event may belong to multiple windows. Windows are defined
    by a size and a slide interval.

    Example:
        size=60s, slide=15s creates windows that start every 15 seconds
        and span 60 seconds, so each event belongs to 4 windows.
    """

    def __init__(
        self,
        size_seconds: float,
        slide_seconds: float,
        allowed_lateness_seconds: float = 0.0,
    ):
        if slide_seconds > size_seconds:
            raise ValueError("Slide interval cannot exceed window size")

        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds
        self.allowed_lateness = allowed_lateness_seconds

    def assign_windows(self, timestamp: float) -> List[WindowBounds]:
        """Assign a timestamp to all overlapping sliding windows."""
        windows = []
        # Find the earliest window that contains this timestamp
        last_start = (timestamp // self.slide_seconds) * self.slide_seconds
        window_start = last_start - self.size_seconds + self.slide_seconds

        while window_start <= timestamp:
            window_end = window_start + self.size_seconds
            if window_start <= timestamp < window_end:
                windows.append(WindowBounds(start=window_start, end=window_end))
            window_start += self.slide_seconds

        return windows

    def is_window_ready(self, window: WindowBounds, watermark: float) -> bool:
        """Check if a window should be triggered."""
        return watermark >= window.end


class SessionWindow:
    """
    Session window implementation with inactivity gap detection.

    Sessions are created per-key and remain open as long as events
    continue arriving within the session gap. A session closes when
    no events arrive for longer than the gap duration.

    Example:
        gap=300s (5 minutes) - a new session starts after 5 minutes
        of inactivity for a given key.
    """

    def __init__(
        self,
        gap_seconds: float,
        max_session_seconds: float = 3600.0,
    ):
        self.gap_seconds = gap_seconds
        self.max_session_seconds = max_session_seconds
        self._sessions: Dict[str, WindowBounds] = {}
        self._lock = threading.Lock()

    def assign_window(self, key: str, timestamp: float) -> WindowBounds:
        """
        Assign a timestamp to a session window for the given key.

        May extend an existing session or create a new one.

        Args:
            key: Partition key (e.g., user_id).
            timestamp: Event timestamp.

        Returns:
            The (potentially merged) session window bounds.
        """
        with self._lock:
            existing = self._sessions.get(key)

            if existing is None:
                # Create new session
                session = WindowBounds(
                    start=timestamp,
                    end=timestamp + self.gap_seconds,
                )
                self._sessions[key] = session
                return session

            # Check if event falls within or extends the session
            if timestamp < existing.end + self.gap_seconds:
                # Extend session
                existing.start = min(existing.start, timestamp)
                existing.end = max(existing.end, timestamp + self.gap_seconds)

                # Cap session duration
                if existing.end - existing.start > self.max_session_seconds:
                    existing.end = existing.start + self.max_session_seconds

                return existing
            else:
                # Gap exceeded; create new session
                session = WindowBounds(
                    start=timestamp,
                    end=timestamp + self.gap_seconds,
                )
                self._sessions[key] = session
                return session

    def get_closed_sessions(self, watermark: float) -> Dict[str, WindowBounds]:
        """
        Retrieve sessions that have been closed (gap exceeded by watermark).

        Args:
            watermark: Current watermark timestamp.

        Returns:
            Dictionary of key -> closed session bounds.
        """
        closed = {}
        with self._lock:
            expired_keys = []
            for key, session in self._sessions.items():
                if watermark >= session.end:
                    closed[key] = session
                    expired_keys.append(key)

            for key in expired_keys:
                del self._sessions[key]

        return closed

    @property
    def active_session_count(self) -> int:
        with self._lock:
            return len(self._sessions)


class WindowAggregator:
    """
    Unified windowed aggregation engine.

    Manages window assignment, state accumulation, and result computation
    across tumbling, sliding, and session windows with support for
    multiple aggregation functions and late event handling.

    Features:
        - Multiple window types (tumbling, sliding, session)
        - Pluggable aggregation functions
        - Key-based partitioned window state
        - Watermark-triggered computation
        - Late event handling with allowed lateness
        - Window state cleanup and garbage collection
    """

    def __init__(self):
        # window_key -> { window_bounds_hash -> WindowState }
        self._window_states: Dict[str, Dict[int, WindowState]] = defaultdict(dict)
        self._lock = threading.Lock()
        self._metrics = {
            "events_added": 0,
            "windows_created": 0,
            "windows_triggered": 0,
            "late_events": 0,
        }

    def add_event_tumbling(
        self,
        key: str,
        value: float,
        timestamp: float,
        window: TumblingWindow,
        distinct_key: Optional[str] = None,
    ) -> WindowBounds:
        """
        Add an event to its tumbling window.

        Args:
            key: Partition key.
            value: Numeric value to aggregate.
            timestamp: Event timestamp.
            window: TumblingWindow instance.
            distinct_key: Optional key for distinct count.

        Returns:
            The WindowBounds the event was assigned to.
        """
        bounds = window.assign_window(timestamp)
        self._add_to_window(key, bounds, value, timestamp, distinct_key)
        return bounds

    def add_event_sliding(
        self,
        key: str,
        value: float,
        timestamp: float,
        window: SlidingWindow,
        distinct_key: Optional[str] = None,
    ) -> List[WindowBounds]:
        """
        Add an event to all its sliding windows.

        Returns:
            List of WindowBounds the event was assigned to.
        """
        all_bounds = window.assign_windows(timestamp)
        for bounds in all_bounds:
            self._add_to_window(key, bounds, value, timestamp, distinct_key)
        return all_bounds

    def add_event_session(
        self,
        key: str,
        value: float,
        timestamp: float,
        session_window: SessionWindow,
        distinct_key: Optional[str] = None,
    ) -> WindowBounds:
        """
        Add an event to its session window.

        Returns:
            The session WindowBounds.
        """
        bounds = session_window.assign_window(key, timestamp)
        self._add_to_window(key, bounds, value, timestamp, distinct_key)
        return bounds

    def trigger_windows(
        self,
        watermark: float,
        aggregation: AggregationFunction,
        window_impl: Any = None,
        percentile_value: float = 95.0,
    ) -> List[Tuple[str, WindowBounds, float]]:
        """
        Trigger all windows whose end time has been reached by the watermark.

        Args:
            watermark: Current watermark timestamp.
            aggregation: Aggregation function to compute.
            window_impl: Window implementation for ready check (optional).
            percentile_value: Percentile value for percentile aggregation.

        Returns:
            List of (key, window_bounds, computed_value) for triggered windows.
        """
        results = []

        with self._lock:
            keys_to_clean = []

            for key, windows in self._window_states.items():
                triggered_hashes = []

                for w_hash, state in windows.items():
                    is_ready = watermark >= state.bounds.end
                    if window_impl is not None and hasattr(window_impl, "is_window_ready"):
                        is_ready = window_impl.is_window_ready(state.bounds, watermark)

                    if is_ready:
                        result = state.compute(aggregation, percentile_value)
                        if result is not None:
                            results.append((key, state.bounds, result))
                        triggered_hashes.append(w_hash)
                        self._metrics["windows_triggered"] += 1

                for w_hash in triggered_hashes:
                    del windows[w_hash]

                if not windows:
                    keys_to_clean.append(key)

            for key in keys_to_clean:
                del self._window_states[key]

        return results

    def get_window_state(self, key: str, bounds: WindowBounds) -> Optional[WindowState]:
        """Get the current state of a specific window."""
        with self._lock:
            windows = self._window_states.get(key, {})
            return windows.get(hash(bounds))

    def get_active_windows(self, key: Optional[str] = None) -> int:
        """Count active (not yet triggered) windows."""
        with self._lock:
            if key is not None:
                return len(self._window_states.get(key, {}))
            return sum(len(w) for w in self._window_states.values())

    def cleanup_expired(self, cutoff_timestamp: float) -> int:
        """Remove window states that are too old to be useful."""
        removed = 0
        with self._lock:
            for key in list(self._window_states.keys()):
                windows = self._window_states[key]
                expired = [
                    h for h, s in windows.items()
                    if s.bounds.end < cutoff_timestamp
                ]
                for h in expired:
                    del windows[h]
                    removed += 1
                if not windows:
                    del self._window_states[key]
        return removed

    def _add_to_window(
        self,
        key: str,
        bounds: WindowBounds,
        value: float,
        timestamp: float,
        distinct_key: Optional[str] = None,
    ) -> None:
        """Add a value to the appropriate window state."""
        with self._lock:
            w_hash = hash(bounds)
            if w_hash not in self._window_states[key]:
                self._window_states[key][w_hash] = WindowState(bounds=bounds)
                self._metrics["windows_created"] += 1

            self._window_states[key][w_hash].add(value, timestamp, distinct_key)
            self._metrics["events_added"] += 1

    @property
    def metrics(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "active_windows": self.get_active_windows(),
        }
