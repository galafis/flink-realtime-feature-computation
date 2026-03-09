"""
Tests for the Feature Store Sink.

Validates buffering, flush policies, deduplication,
and write operations.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.stream.sink import FeatureStoreSink, SinkRecord, FlushPolicy
from src.store.feature_store import FeatureStore


class TestFeatureStoreSink:
    """Tests for FeatureStoreSink."""

    def test_immediate_write(self):
        store = FeatureStore(enable_offline=False)
        sink = FeatureStoreSink(
            feature_store=store,
            flush_policy=FlushPolicy.IMMEDIATE,
        )
        sink.open()

        record = SinkRecord(
            entity_id="user_1",
            feature_name="feat_a",
            feature_value=42.0,
        )
        assert sink.write(record) is True
        assert store.get_feature("user_1", "feat_a") == 42.0

        sink.close()

    def test_batch_buffering(self):
        store = FeatureStore(enable_offline=False)
        sink = FeatureStoreSink(
            feature_store=store,
            flush_policy=FlushPolicy.BATCH_SIZE,
            batch_size=5,
        )
        sink.open()

        # Write 4 records (below batch size)
        for i in range(4):
            sink.write(SinkRecord(
                entity_id="user_1", feature_name=f"f{i}", feature_value=float(i)
            ))

        assert sink.buffer_size == 4
        assert store.get_feature("user_1", "f0") is None  # Not flushed yet

        # Write 5th record, triggers flush
        sink.write(SinkRecord(
            entity_id="user_1", feature_name="f4", feature_value=4.0
        ))

        # After flush, features should be in the store
        assert sink.buffer_size == 0

        sink.close()

    def test_force_flush(self):
        sink = FeatureStoreSink(
            flush_policy=FlushPolicy.BATCH_SIZE,
            batch_size=100,
        )
        sink.open()

        sink.write(SinkRecord(
            entity_id="u1", feature_name="f", feature_value=1.0
        ))
        assert sink.buffer_size == 1

        flushed = sink.force_flush()
        assert flushed == 1
        assert sink.buffer_size == 0

        sink.close()

    def test_deduplication(self):
        sink = FeatureStoreSink(
            flush_policy=FlushPolicy.BATCH_SIZE,
            batch_size=100,
            dedup_window_seconds=10.0,
        )
        sink.open()

        record = SinkRecord(entity_id="u1", feature_name="f", feature_value=1.0)
        sink.write(record)
        sink.write(record)  # Should be deduped

        assert sink.metrics["records_deduped"] == 1

        sink.close()

    def test_write_batch(self):
        sink = FeatureStoreSink(flush_policy=FlushPolicy.IMMEDIATE)
        sink.open()

        records = [
            SinkRecord(entity_id="u1", feature_name=f"f{i}", feature_value=float(i))
            for i in range(5)
        ]
        success, failed = sink.write_batch(records)
        assert success == 5
        assert failed == 0

        sink.close()

    def test_metrics(self):
        sink = FeatureStoreSink(flush_policy=FlushPolicy.IMMEDIATE)
        sink.open()
        sink.write(SinkRecord(entity_id="u1", feature_name="f", feature_value=1.0))
        m = sink.metrics
        assert m["records_written"] >= 1
        sink.close()

    def test_context_manager(self):
        with FeatureStoreSink(flush_policy=FlushPolicy.IMMEDIATE) as sink:
            sink.write(SinkRecord(
                entity_id="u1", feature_name="f", feature_value=1.0
            ))
        # Should not raise after close
