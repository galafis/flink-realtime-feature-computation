"""
Tests for the Kafka Source Simulator.

Validates event generation, distribution, session management,
and late event simulation.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.stream.source import KafkaSourceSimulator, StreamEvent, EventType


class TestStreamEvent:
    """Tests for the StreamEvent dataclass."""

    def test_create_event(self):
        event = StreamEvent(
            user_id="user_001",
            event_type="page_view",
            timestamp=time.time(),
            properties={"page": "/home"},
        )
        assert event.user_id == "user_001"
        assert event.event_type == "page_view"
        assert event.properties["page"] == "/home"
        assert event.event_id  # auto-generated UUID

    def test_event_to_dict(self):
        event = StreamEvent(
            user_id="user_002",
            event_type="purchase",
            properties={"total_amount": 99.99},
        )
        d = event.to_dict()
        assert d["user_id"] == "user_002"
        assert d["event_type"] == "purchase"
        assert d["properties"]["total_amount"] == 99.99

    def test_event_from_dict(self):
        data = {
            "event_id": "test-id",
            "user_id": "user_003",
            "event_type": "search",
            "timestamp": 1700000000.0,
            "properties": {"query": "shoes"},
        }
        event = StreamEvent.from_dict(data)
        assert event.event_id == "test-id"
        assert event.user_id == "user_003"
        assert event.get_property("query") == "shoes"

    def test_event_time_property(self):
        ts = 1700000000.0
        event = StreamEvent(timestamp=ts)
        dt = event.event_time
        assert dt.year == 2023
        assert dt.month == 11


class TestKafkaSourceSimulator:
    """Tests for the KafkaSourceSimulator."""

    def test_generate_events_count(self):
        source = KafkaSourceSimulator(num_users=10, seed=42)
        events = source.generate_events(100)
        assert len(events) == 100

    def test_events_have_valid_structure(self):
        source = KafkaSourceSimulator(num_users=5, seed=42)
        events = source.generate_events(50)

        for event in events:
            assert event.event_id
            assert event.user_id.startswith("user_")
            assert event.event_type in [e.value for e in EventType]
            assert event.timestamp > 0
            assert isinstance(event.properties, dict)

    def test_event_type_distribution(self):
        source = KafkaSourceSimulator(num_users=10, seed=42)
        events = source.generate_events(1000)

        type_counts = {}
        for e in events:
            type_counts[e.event_type] = type_counts.get(e.event_type, 0) + 1

        # Page views should be the most common
        assert type_counts.get("page_view", 0) > 100
        # Purchases should be less common
        assert type_counts.get("purchase", 0) < type_counts.get("page_view", 0)

    def test_late_events_generated(self):
        source = KafkaSourceSimulator(
            num_users=5,
            late_event_probability=1.0,  # 100% late
            max_late_seconds=10,
            seed=42,
        )
        events = source.generate_events(50)
        base_time = time.time()

        # All events should have timestamps in the past
        late_count = sum(1 for e in events if e.timestamp < base_time - 1)
        assert late_count > 0

    def test_session_management(self):
        source = KafkaSourceSimulator(num_users=3, seed=42)
        events = source.generate_events(30)

        sessions = set()
        for e in events:
            sid = e.properties.get("session_id")
            if sid:
                sessions.add(sid)

        assert len(sessions) > 0  # At least some sessions created

    def test_event_count_tracking(self):
        source = KafkaSourceSimulator(num_users=5, seed=42)
        assert source.event_count == 0
        source.generate_events(25)
        assert source.event_count == 25
        source.generate_events(10)
        assert source.event_count == 35

    def test_stream_events_generator(self):
        source = KafkaSourceSimulator(num_users=5, seed=42)
        events = list(source.stream_events(max_events=10))
        assert len(events) == 10
