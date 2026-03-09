"""
Tests for the Stream Metrics module.

Validates latency tracking, throughput counting, feature
freshness, and report generation.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.monitoring.metrics import StreamMetrics, LatencyHistogram, ThroughputCounter


class TestLatencyHistogram:
    """Tests for LatencyHistogram."""

    def test_record_and_mean(self):
        h = LatencyHistogram()
        for v in [10, 20, 30]:
            h.record(v)
        assert h.mean == 20.0

    def test_min_max(self):
        h = LatencyHistogram()
        for v in [5, 15, 25]:
            h.record(v)
        assert h.min == 5.0
        assert h.max == 25.0

    def test_empty_histogram(self):
        h = LatencyHistogram()
        assert h.mean == 0.0
        assert h.count == 0

    def test_summary(self):
        h = LatencyHistogram()
        for v in range(100):
            h.record(float(v))
        s = h.summary()
        assert s["count"] == 100
        assert s["mean_ms"] > 0


class TestThroughputCounter:
    """Tests for ThroughputCounter."""

    def test_record_and_count(self):
        tc = ThroughputCounter(window_seconds=60.0)
        tc.record()
        tc.record()
        tc.record()
        assert tc.count_in_window == 3

    def test_batch_record(self):
        tc = ThroughputCounter(window_seconds=60.0)
        tc.record_batch(10)
        assert tc.count_in_window == 10


class TestStreamMetrics:
    """Tests for StreamMetrics."""

    def test_record_events(self):
        m = StreamMetrics()
        m.record_event_processed(latency_ms=1.0)
        m.record_event_processed(latency_ms=2.0)
        assert m.events_processed == 2

    def test_record_features(self):
        m = StreamMetrics()
        m.record_feature_computed("user_1", "feat_1", 1.0)
        assert m.features_computed == 1

    def test_feature_freshness(self):
        m = StreamMetrics()
        m.record_feature_computed("user_1", "feat_1")
        freshness = m.get_feature_freshness()
        assert "feat_1" in freshness
        assert freshness["feat_1"] < 1.0  # Just recorded

    def test_error_tracking(self):
        m = StreamMetrics()
        m.record_error("parse_error")
        m.record_error("parse_error")
        m.record_error("timeout")

        report = m.get_report()
        assert report["errors"]["total"] == 3
        assert report["errors"]["by_type"]["parse_error"] == 2

    def test_backpressure(self):
        m = StreamMetrics()
        m.record_backpressure(0.3)
        m.record_backpressure(0.5)
        status = m.get_backpressure_status()
        assert status["level"] == "none"  # avg 0.4 < 0.5

    def test_full_report(self):
        m = StreamMetrics()
        m.record_event_processed(1.0)
        m.record_feature_computed("u", "f", 0.5)
        m.record_error("test")

        report = m.get_report()
        assert "events" in report
        assert "features" in report
        assert "latency" in report
        assert "errors" in report
        assert "backpressure" in report
        assert report["events"]["total_processed"] == 1
