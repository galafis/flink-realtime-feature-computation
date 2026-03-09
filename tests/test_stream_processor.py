"""
Tests for the Stream Processor.

Validates event processing, watermark management, filtering,
mapping, and late event handling.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.stream.processor import (
    StreamProcessor,
    TimeCharacteristic,
    Watermark,
    ProcessingContext,
)
from src.stream.source import StreamEvent


class TestWatermark:
    """Tests for the Watermark class."""

    def test_initial_watermark(self):
        wm = Watermark()
        assert wm.timestamp == 0.0

    def test_advance_watermark(self):
        wm = Watermark()
        advanced = wm.advance(100.0, max_out_of_orderness=5.0)
        assert advanced is True
        assert wm.timestamp == 95.0

    def test_watermark_no_regression(self):
        wm = Watermark()
        wm.advance(100.0, max_out_of_orderness=5.0)
        advanced = wm.advance(90.0, max_out_of_orderness=5.0)
        assert advanced is False
        assert wm.timestamp == 95.0  # Still at 95, not 85


class TestStreamProcessor:
    """Tests for the StreamProcessor."""

    def _make_event(self, user_id="user_001", event_type="page_view",
                    ts=None, props=None):
        return StreamEvent(
            user_id=user_id,
            event_type=event_type,
            timestamp=ts or time.time(),
            properties=props or {},
        )

    def test_process_single_event(self):
        proc = StreamProcessor()
        handler_called = []
        proc.add_handler(lambda ctx: handler_called.append(ctx.event.user_id))
        event = self._make_event()
        result = proc.process_event(event)
        assert result is True
        assert len(handler_called) == 1

    def test_filter_events(self):
        proc = StreamProcessor()
        proc.filter(lambda e: e.event_type == "purchase")

        processed = []
        proc.add_handler(lambda ctx: processed.append(ctx.event))

        proc.process_event(self._make_event(event_type="page_view"))
        proc.process_event(self._make_event(event_type="purchase"))

        assert len(processed) == 1
        assert processed[0].event_type == "purchase"

    def test_map_events(self):
        proc = StreamProcessor()

        def add_enrichment(event):
            event.properties["enriched"] = True
            return event

        proc.map(add_enrichment)

        enriched_events = []
        proc.add_handler(lambda ctx: enriched_events.append(ctx.event))

        proc.process_event(self._make_event())
        assert enriched_events[0].properties["enriched"] is True

    def test_key_by(self):
        proc = StreamProcessor()
        proc.key_by(lambda e: e.user_id)

        keys = []
        proc.add_handler(lambda ctx: keys.append(ctx.partition_key))

        proc.process_event(self._make_event(user_id="user_A"))
        proc.process_event(self._make_event(user_id="user_B"))

        assert keys == ["user_A", "user_B"]

    def test_late_event_detection(self):
        proc = StreamProcessor(
            time_characteristic=TimeCharacteristic.EVENT_TIME,
            max_out_of_orderness_seconds=5.0,
            allowed_lateness_seconds=30.0,
        )
        proc.add_handler(lambda ctx: None)

        base_ts = time.time()

        # Process events to advance watermark
        for i in range(10):
            proc.process_event(self._make_event(ts=base_ts + i * 10))

        # Now send a late event
        proc.process_event(self._make_event(ts=base_ts - 100))

        assert proc.metrics["events_late"] > 0

    def test_process_batch(self):
        proc = StreamProcessor()
        proc.add_handler(lambda ctx: None)

        events = [self._make_event(ts=time.time() + i) for i in range(20)]
        results = proc.process_batch(events)

        assert results["processed"] == 20
        assert results["filtered"] == 0

    def test_metrics(self):
        proc = StreamProcessor()
        proc.add_handler(lambda ctx: None)
        proc.process_event(self._make_event())

        m = proc.metrics
        assert m["events_processed"] == 1
        assert "watermark" in m
        assert "avg_processing_time_ms" in m

    def test_state_management(self):
        proc = StreamProcessor()
        proc.set_state("user_1", "count", 5)
        assert proc.get_state("user_1", "count") == 5
        proc.clear_state("user_1")
        assert proc.get_state("user_1", "count") is None
