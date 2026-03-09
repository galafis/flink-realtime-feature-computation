"""
Stream Processing Metrics

Provides comprehensive metrics collection for the real-time feature
computation pipeline, including event throughput, processing latency,
feature freshness, backpressure detection, and window statistics.

Author: Gabriel Demetrios Lafis
"""

import time
import threading
import statistics
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class LatencyHistogram:
    """Tracks latency distribution with configurable window size."""
    window_size: int = 1000
    _values: deque = field(default_factory=lambda: deque(maxlen=1000))

    def __post_init__(self):
        self._values = deque(maxlen=self.window_size)

    def record(self, value_ms: float) -> None:
        """Record a latency measurement."""
        self._values.append(value_ms)

    @property
    def count(self) -> int:
        return len(self._values)

    @property
    def mean(self) -> float:
        if not self._values:
            return 0.0
        return statistics.mean(self._values)

    @property
    def median(self) -> float:
        if not self._values:
            return 0.0
        return statistics.median(self._values)

    @property
    def p95(self) -> float:
        if len(self._values) < 20:
            return self.max
        sorted_vals = sorted(self._values)
        idx = int(len(sorted_vals) * 0.95)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    @property
    def p99(self) -> float:
        if len(self._values) < 100:
            return self.max
        sorted_vals = sorted(self._values)
        idx = int(len(sorted_vals) * 0.99)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]

    @property
    def min(self) -> float:
        return min(self._values) if self._values else 0.0

    @property
    def max(self) -> float:
        return max(self._values) if self._values else 0.0

    def summary(self) -> Dict[str, float]:
        return {
            "count": self.count,
            "mean_ms": round(self.mean, 3),
            "median_ms": round(self.median, 3),
            "p95_ms": round(self.p95, 3),
            "p99_ms": round(self.p99, 3),
            "min_ms": round(self.min, 3),
            "max_ms": round(self.max, 3),
        }


@dataclass
class ThroughputCounter:
    """Tracks events per second over a sliding window."""
    window_seconds: float = 60.0
    _timestamps: deque = field(default_factory=lambda: deque())
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self._timestamps = deque()
        self._lock = threading.Lock()

    def record(self) -> None:
        """Record a single event occurrence."""
        now = time.time()
        with self._lock:
            self._timestamps.append(now)
            self._cleanup(now)

    def record_batch(self, count: int) -> None:
        """Record multiple events at once."""
        now = time.time()
        with self._lock:
            for _ in range(count):
                self._timestamps.append(now)
            self._cleanup(now)

    @property
    def rate(self) -> float:
        """Current events per second."""
        now = time.time()
        with self._lock:
            self._cleanup(now)
            if not self._timestamps:
                return 0.0
            elapsed = now - self._timestamps[0]
            if elapsed < 0.001:
                return float(len(self._timestamps))
            return len(self._timestamps) / elapsed

    @property
    def count_in_window(self) -> int:
        """Total events in the current window."""
        now = time.time()
        with self._lock:
            self._cleanup(now)
            return len(self._timestamps)

    def _cleanup(self, now: float) -> None:
        """Remove timestamps outside the window."""
        cutoff = now - self.window_seconds
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()


class StreamMetrics:
    """
    Comprehensive metrics collection for the stream processing pipeline.

    Tracks:
        - Event throughput (events/second, total count)
        - Processing latency (mean, median, p95, p99)
        - Feature freshness (time since last computation)
        - Backpressure indicators
        - Window statistics (active windows, triggered windows)
        - Error rates and types

    Thread-safe for concurrent metric updates from multiple operators.

    Usage:
        metrics = StreamMetrics()
        metrics.record_event_processed(latency_ms=2.5)
        metrics.record_feature_computed("user_123", "spend_1h")
        report = metrics.get_report()
    """

    def __init__(self, report_interval_seconds: float = 10.0):
        self._report_interval = report_interval_seconds
        self._start_time = time.time()
        self._lock = threading.Lock()

        # Core counters
        self._events_processed = 0
        self._events_filtered = 0
        self._events_late = 0
        self._events_dropped = 0
        self._features_computed = 0
        self._errors = 0

        # Latency tracking
        self._processing_latency = LatencyHistogram(window_size=1000)
        self._feature_latency = LatencyHistogram(window_size=500)

        # Throughput tracking
        self._event_throughput = ThroughputCounter(window_seconds=60.0)
        self._feature_throughput = ThroughputCounter(window_seconds=60.0)

        # Feature freshness: feature_name -> last_computed_timestamp
        self._feature_freshness: Dict[str, float] = {}

        # Backpressure detection
        self._backpressure_events: deque = deque(maxlen=100)
        self._buffer_sizes: deque = deque(maxlen=100)

        # Error tracking
        self._error_types: Dict[str, int] = {}

        # Window statistics
        self._active_windows = 0
        self._triggered_windows = 0

        # Report history
        self._last_report_time = time.time()
        self._report_history: List[Dict[str, Any]] = []

    def record_event_processed(self, latency_ms: float = 0.0) -> None:
        """Record that an event was successfully processed."""
        with self._lock:
            self._events_processed += 1
        self._processing_latency.record(latency_ms)
        self._event_throughput.record()

    def record_event_filtered(self) -> None:
        """Record that an event was filtered out."""
        with self._lock:
            self._events_filtered += 1

    def record_event_late(self) -> None:
        """Record a late-arriving event."""
        with self._lock:
            self._events_late += 1

    def record_event_dropped(self) -> None:
        """Record a dropped event (exceeded allowed lateness)."""
        with self._lock:
            self._events_dropped += 1

    def record_feature_computed(
        self,
        entity_id: str,
        feature_name: str,
        latency_ms: float = 0.0,
    ) -> None:
        """Record a feature computation completion."""
        with self._lock:
            self._features_computed += 1
            self._feature_freshness[feature_name] = time.time()
        self._feature_latency.record(latency_ms)
        self._feature_throughput.record()

    def record_error(self, error_type: str = "unknown") -> None:
        """Record a processing error."""
        with self._lock:
            self._errors += 1
            self._error_types[error_type] = self._error_types.get(error_type, 0) + 1

    def record_backpressure(self, buffer_utilization: float) -> None:
        """
        Record backpressure indicator.

        Args:
            buffer_utilization: Buffer fullness ratio (0.0 to 1.0).
        """
        self._backpressure_events.append({
            "timestamp": time.time(),
            "utilization": buffer_utilization,
        })
        self._buffer_sizes.append(buffer_utilization)

    def update_window_stats(self, active: int, triggered: int) -> None:
        """Update window statistics."""
        with self._lock:
            self._active_windows = active
            self._triggered_windows += triggered

    def get_feature_freshness(self) -> Dict[str, float]:
        """
        Get feature freshness as seconds since last computation.

        Returns:
            Dictionary of feature_name -> seconds_since_last_update.
        """
        now = time.time()
        with self._lock:
            return {
                name: round(now - ts, 2)
                for name, ts in self._feature_freshness.items()
            }

    def get_backpressure_status(self) -> Dict[str, Any]:
        """Assess current backpressure level."""
        if not self._buffer_sizes:
            return {"level": "none", "avg_utilization": 0.0}

        avg_util = sum(self._buffer_sizes) / len(self._buffer_sizes)

        if avg_util > 0.9:
            level = "critical"
        elif avg_util > 0.7:
            level = "high"
        elif avg_util > 0.5:
            level = "moderate"
        else:
            level = "none"

        return {
            "level": level,
            "avg_utilization": round(avg_util, 3),
            "max_utilization": round(max(self._buffer_sizes), 3) if self._buffer_sizes else 0.0,
            "samples": len(self._buffer_sizes),
        }

    def get_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive metrics report.

        Returns:
            Dictionary with all collected metrics.
        """
        uptime = time.time() - self._start_time

        with self._lock:
            report = {
                "uptime_seconds": round(uptime, 1),
                "events": {
                    "total_processed": self._events_processed,
                    "filtered": self._events_filtered,
                    "late": self._events_late,
                    "dropped": self._events_dropped,
                    "throughput_eps": round(self._event_throughput.rate, 2),
                },
                "features": {
                    "total_computed": self._features_computed,
                    "throughput_fps": round(self._feature_throughput.rate, 2),
                    "freshness": self.get_feature_freshness(),
                },
                "latency": {
                    "processing": self._processing_latency.summary(),
                    "feature_computation": self._feature_latency.summary(),
                },
                "windows": {
                    "active": self._active_windows,
                    "total_triggered": self._triggered_windows,
                },
                "errors": {
                    "total": self._errors,
                    "by_type": dict(self._error_types),
                    "error_rate": round(
                        self._errors / max(self._events_processed, 1), 6
                    ),
                },
                "backpressure": self.get_backpressure_status(),
            }

        return report

    def should_report(self) -> bool:
        """Check if it is time for a periodic report."""
        now = time.time()
        if (now - self._last_report_time) >= self._report_interval:
            self._last_report_time = now
            return True
        return False

    def log_report(self) -> None:
        """Log the current metrics report."""
        report = self.get_report()
        self._report_history.append(report)

        # Keep last 100 reports
        if len(self._report_history) > 100:
            self._report_history = self._report_history[-100:]

        logger.info(
            "Stream metrics report",
            extra={
                "events_total": report["events"]["total_processed"],
                "throughput_eps": report["events"]["throughput_eps"],
                "features_total": report["features"]["total_computed"],
                "p95_latency_ms": report["latency"]["processing"]["p95_ms"],
                "error_rate": report["errors"]["error_rate"],
            },
        )

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._events_processed = 0
            self._events_filtered = 0
            self._events_late = 0
            self._events_dropped = 0
            self._features_computed = 0
            self._errors = 0
            self._error_types.clear()
            self._feature_freshness.clear()
            self._active_windows = 0
            self._triggered_windows = 0
            self._start_time = time.time()

    @property
    def events_processed(self) -> int:
        return self._events_processed

    @property
    def features_computed(self) -> int:
        return self._features_computed
