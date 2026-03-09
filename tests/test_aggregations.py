"""
Tests for the Windowed Aggregation Engine.

Validates tumbling, sliding, and session windows, aggregation
functions, late event handling, and window triggering.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.features.aggregations import (
    WindowAggregator,
    TumblingWindow,
    SlidingWindow,
    SessionWindow,
    WindowState,
    WindowBounds,
    AggregationFunction,
)


class TestTumblingWindow:
    """Tests for TumblingWindow."""

    def test_assign_window(self):
        tw = TumblingWindow(size_seconds=60)
        bounds = tw.assign_window(100.0)
        assert bounds.start == 60.0
        assert bounds.end == 120.0

    def test_same_window_for_close_timestamps(self):
        tw = TumblingWindow(size_seconds=60)
        b1 = tw.assign_window(61.0)
        b2 = tw.assign_window(119.0)
        assert b1 == b2

    def test_different_windows(self):
        tw = TumblingWindow(size_seconds=60)
        b1 = tw.assign_window(59.0)
        b2 = tw.assign_window(61.0)
        assert b1 != b2

    def test_window_ready(self):
        tw = TumblingWindow(size_seconds=60)
        bounds = WindowBounds(start=60.0, end=120.0)
        assert tw.is_window_ready(bounds, watermark=120.0) is True
        assert tw.is_window_ready(bounds, watermark=119.9) is False


class TestSlidingWindow:
    """Tests for SlidingWindow."""

    def test_multiple_window_assignment(self):
        sw = SlidingWindow(size_seconds=60, slide_seconds=30)
        windows = sw.assign_windows(45.0)
        assert len(windows) >= 1

    def test_slide_cannot_exceed_size(self):
        with pytest.raises(ValueError):
            SlidingWindow(size_seconds=30, slide_seconds=60)

    def test_overlapping_windows(self):
        sw = SlidingWindow(size_seconds=60, slide_seconds=15)
        windows = sw.assign_windows(50.0)
        # With 60s window and 15s slide, an event can belong to up to 4 windows
        assert len(windows) >= 2


class TestSessionWindow:
    """Tests for SessionWindow."""

    def test_create_new_session(self):
        sw = SessionWindow(gap_seconds=30)
        bounds = sw.assign_window("user_1", 100.0)
        assert bounds.start == 100.0
        assert bounds.end == 130.0

    def test_extend_existing_session(self):
        sw = SessionWindow(gap_seconds=30)
        sw.assign_window("user_1", 100.0)
        bounds = sw.assign_window("user_1", 120.0)
        assert bounds.start == 100.0
        assert bounds.end == 150.0  # extended to 120 + 30

    def test_new_session_after_gap(self):
        sw = SessionWindow(gap_seconds=30)
        b1 = sw.assign_window("user_1", 100.0)
        b2 = sw.assign_window("user_1", 200.0)  # 200 > 130 (end of session)
        assert b2.start == 200.0  # New session

    def test_closed_sessions(self):
        sw = SessionWindow(gap_seconds=30)
        sw.assign_window("user_1", 100.0)
        closed = sw.get_closed_sessions(watermark=131.0)
        assert "user_1" in closed

    def test_active_session_count(self):
        sw = SessionWindow(gap_seconds=30)
        sw.assign_window("user_1", 100.0)
        sw.assign_window("user_2", 100.0)
        assert sw.active_session_count == 2


class TestWindowState:
    """Tests for WindowState aggregation functions."""

    def _make_state(self, values):
        bounds = WindowBounds(start=0, end=60)
        state = WindowState(bounds=bounds)
        for v in values:
            state.add(v, timestamp=time.time())
        return state

    def test_count(self):
        state = self._make_state([1, 2, 3, 4, 5])
        assert state.compute(AggregationFunction.COUNT) == 5.0

    def test_sum(self):
        state = self._make_state([10, 20, 30])
        assert state.compute(AggregationFunction.SUM) == 60.0

    def test_avg(self):
        state = self._make_state([10, 20, 30])
        assert state.compute(AggregationFunction.AVG) == 20.0

    def test_min(self):
        state = self._make_state([5, 3, 8, 1, 9])
        assert state.compute(AggregationFunction.MIN) == 1.0

    def test_max(self):
        state = self._make_state([5, 3, 8, 1, 9])
        assert state.compute(AggregationFunction.MAX) == 9.0

    def test_distinct_count(self):
        bounds = WindowBounds(start=0, end=60)
        state = WindowState(bounds=bounds)
        state.add(1.0, distinct_key="a")
        state.add(2.0, distinct_key="b")
        state.add(3.0, distinct_key="a")  # duplicate key
        assert state.compute(AggregationFunction.DISTINCT_COUNT) == 2.0

    def test_percentile(self):
        state = self._make_state(list(range(1, 101)))  # 1..100
        p50 = state.compute(AggregationFunction.PERCENTILE, percentile_value=50.0)
        assert 49 <= p50 <= 51

    def test_stddev(self):
        state = self._make_state([10, 10, 10])
        assert state.compute(AggregationFunction.STDDEV) == 0.0

    def test_empty_state(self):
        bounds = WindowBounds(start=0, end=60)
        state = WindowState(bounds=bounds)
        assert state.compute(AggregationFunction.COUNT) is None


class TestWindowAggregator:
    """Tests for the WindowAggregator."""

    def test_tumbling_aggregation(self):
        agg = WindowAggregator()
        tw = TumblingWindow(size_seconds=60)

        for i in range(5):
            agg.add_event_tumbling("user_1", float(i + 1), 70.0 + i, tw)

        results = agg.trigger_windows(
            watermark=130.0,
            aggregation=AggregationFunction.SUM,
            window_impl=tw,
        )
        assert len(results) == 1
        key, bounds, value = results[0]
        assert key == "user_1"
        assert value == 15.0  # 1+2+3+4+5

    def test_active_windows(self):
        agg = WindowAggregator()
        tw = TumblingWindow(size_seconds=60)
        agg.add_event_tumbling("user_1", 1.0, 70.0, tw)
        agg.add_event_tumbling("user_2", 1.0, 70.0, tw)
        assert agg.get_active_windows() == 2
