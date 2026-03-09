"""
Tests for the Feature Pipeline.

Validates feature registration, event routing, computation
triggering, and result callbacks.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.features.feature_pipeline import (
    FeaturePipeline,
    FeatureDefinition,
    ComputedFeature,
)
from src.stream.source import StreamEvent


class TestFeaturePipeline:
    """Tests for the FeaturePipeline."""

    def _make_event(self, user_id="user_001", event_type="purchase",
                    ts=None, props=None):
        return StreamEvent(
            user_id=user_id,
            event_type=event_type,
            timestamp=ts or 100.0,
            properties=props or {"total_amount": 50.0},
        )

    def test_register_feature(self):
        pipeline = FeaturePipeline()
        fd = FeatureDefinition(
            name="test_count",
            aggregation="count",
            window_type="tumbling",
            window_size_seconds=60,
            source_field="_count",
        )
        pipeline.register_feature(fd)
        defs = pipeline.get_feature_definitions()
        assert "test_count" in defs

    def test_unregister_feature(self):
        pipeline = FeaturePipeline()
        fd = FeatureDefinition(
            name="test_feat",
            aggregation="count",
            window_type="tumbling",
            window_size_seconds=60,
            source_field="_count",
        )
        pipeline.register_feature(fd)
        assert pipeline.unregister_feature("test_feat") is True
        assert pipeline.unregister_feature("nonexistent") is False

    def test_event_routing_with_filter(self):
        pipeline = FeaturePipeline()
        pipeline.register_feature(FeatureDefinition(
            name="purchase_sum",
            aggregation="sum",
            window_type="tumbling",
            window_size_seconds=60,
            source_field="total_amount",
            filter_event_type="purchase",
        ))

        # Purchase event should be routed
        routed = pipeline.process_event(self._make_event(event_type="purchase"))
        assert routed == 1

        # Page view should not be routed
        routed = pipeline.process_event(self._make_event(event_type="page_view"))
        assert routed == 0

    def test_trigger_computation(self):
        results = []
        pipeline = FeaturePipeline(
            on_feature_computed=lambda f: results.append(f),
        )
        pipeline.register_feature(FeatureDefinition(
            name="spend_sum",
            aggregation="sum",
            window_type="tumbling",
            window_size_seconds=60,
            source_field="total_amount",
            filter_event_type="purchase",
        ))

        # Add events in the window [60, 120)
        for i in range(5):
            pipeline.process_event(self._make_event(ts=70.0 + i, props={"total_amount": 10.0}))

        # Trigger with watermark past window end
        computed = pipeline.trigger(watermark=121.0)
        assert len(computed) >= 1

        total = sum(c.feature_value for c in computed if c.feature_name == "spend_sum")
        assert total == 50.0  # 5 * 10.0

    def test_multiple_features(self):
        pipeline = FeaturePipeline()
        pipeline.register_feature(FeatureDefinition(
            name="f1", aggregation="count", window_type="tumbling",
            window_size_seconds=60, source_field="_count",
        ))
        pipeline.register_feature(FeatureDefinition(
            name="f2", aggregation="count", window_type="tumbling",
            window_size_seconds=60, source_field="_count",
        ))

        routed = pipeline.process_event(self._make_event())
        assert routed == 2  # Both features should receive the event

    def test_metrics(self):
        pipeline = FeaturePipeline()
        pipeline.register_feature(FeatureDefinition(
            name="test", aggregation="count", window_type="tumbling",
            window_size_seconds=60, source_field="_count",
        ))
        pipeline.process_event(self._make_event())
        m = pipeline.metrics
        assert m["events_processed"] == 1
        assert m["features_registered"] == 1
