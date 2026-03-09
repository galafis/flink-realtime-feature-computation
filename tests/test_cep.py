"""
Tests for the Complex Event Processor.

Validates pattern detection for sequences, absences,
frequency, and threshold patterns.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.features.cep import (
    ComplexEventProcessor,
    PatternDefinition,
    PatternType,
    Alert,
)
from src.stream.source import StreamEvent


class TestComplexEventProcessor:
    """Tests for the ComplexEventProcessor."""

    def _make_event(self, user_id="user_001", event_type="page_view",
                    ts=None, props=None):
        return StreamEvent(
            user_id=user_id,
            event_type=event_type,
            timestamp=ts or time.time(),
            properties=props or {},
        )

    def test_sequence_pattern_detection(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="browse_buy",
            pattern_type="sequence",
            event_types=["product_click", "purchase"],
            time_window_seconds=300,
        ))

        base = time.time()

        # First event in sequence
        alerts = cep.process_event(self._make_event(
            event_type="product_click", ts=base
        ))
        assert len(alerts) == 0

        # Second event completes the sequence
        alerts = cep.process_event(self._make_event(
            event_type="purchase", ts=base + 60
        ))
        assert len(alerts) == 1
        assert alerts[0].pattern_name == "browse_buy"

    def test_sequence_timeout(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="browse_buy",
            pattern_type="sequence",
            event_types=["product_click", "purchase"],
            time_window_seconds=10,
        ))

        base = time.time()

        cep.process_event(self._make_event(
            event_type="product_click", ts=base
        ))
        # Purchase too late (outside 10s window)
        alerts = cep.process_event(self._make_event(
            event_type="purchase", ts=base + 20
        ))
        assert len(alerts) == 0

    def test_threshold_pattern(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="high_spend",
            pattern_type="threshold",
            event_types=["purchase"],
            time_window_seconds=300,
            threshold_field="total_amount",
            threshold_value=100.0,
        ))

        # Below threshold
        alerts = cep.process_event(self._make_event(
            event_type="purchase",
            props={"total_amount": 50.0},
        ))
        assert len(alerts) == 0

        # Above threshold
        alerts = cep.process_event(self._make_event(
            event_type="purchase",
            props={"total_amount": 150.0},
        ))
        assert len(alerts) == 1
        assert alerts[0].severity == "HIGH"

    def test_frequency_pattern(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="rapid_search",
            pattern_type="frequency",
            event_types=["search"],
            time_window_seconds=60,
            min_occurrences=3,
        ))

        base = time.time()
        alerts = []
        for i in range(3):
            result = cep.process_event(self._make_event(
                event_type="search", ts=base + i
            ))
            alerts.extend(result)

        assert len(alerts) == 1
        assert alerts[0].pattern_name == "rapid_search"

    def test_absence_pattern(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="cart_abandon",
            pattern_type="absence",
            event_types=["add_to_cart", "purchase"],
            time_window_seconds=10,
        ))

        base = time.time()

        # Trigger event
        cep.process_event(self._make_event(
            event_type="add_to_cart", ts=base
        ))

        # Check for absences after window expires
        alerts = cep.check_pending_absences(base + 15)
        assert len(alerts) == 1
        assert alerts[0].pattern_name == "cart_abandon"

    def test_absence_cancelled_by_completing_event(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="cart_abandon",
            pattern_type="absence",
            event_types=["add_to_cart", "purchase"],
            time_window_seconds=60,
        ))

        base = time.time()

        cep.process_event(self._make_event(event_type="add_to_cart", ts=base))
        cep.process_event(self._make_event(event_type="purchase", ts=base + 30))

        # No absence since purchase occurred
        alerts = cep.check_pending_absences(base + 120)
        assert len(alerts) == 0

    def test_metrics(self):
        cep = ComplexEventProcessor()
        cep.register_pattern(PatternDefinition(
            name="test", pattern_type="threshold",
            event_types=["purchase"], time_window_seconds=60,
            threshold_field="total_amount", threshold_value=100.0,
        ))
        cep.process_event(self._make_event(
            event_type="purchase", props={"total_amount": 200.0}
        ))
        m = cep.metrics
        assert m["events_processed"] == 1
        assert m["matches_found"] == 1
