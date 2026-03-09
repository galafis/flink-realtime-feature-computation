"""
Tests for the unified Feature Store.

Validates online get/set, TTL expiration, versioning,
point-in-time lookups, and batch operations.
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.store.feature_store import FeatureStore


class TestFeatureStore:
    """Tests for the FeatureStore."""

    def test_set_and_get_features(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("user_1", {"spend": 100.0, "clicks": 5})
        features = store.get_features("user_1")
        assert features["spend"] == 100.0
        assert features["clicks"] == 5

    def test_get_specific_features(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("user_1", {"a": 1, "b": 2, "c": 3})
        features = store.get_features("user_1", ["a", "c"])
        assert features == {"a": 1, "c": 3}

    def test_get_nonexistent_entity(self):
        store = FeatureStore(enable_offline=False)
        features = store.get_features("nonexistent")
        assert features == {}

    def test_ttl_expiration(self):
        store = FeatureStore(enable_offline=False, default_ttl_seconds=3600)
        store.set_features("user_1", {"feat": 1.0}, ttl_seconds=1)

        # Feature should exist immediately
        assert store.get_feature("user_1", "feat") == 1.0

        # Manually expire by manipulating the stored feature
        stored = store.get_feature_with_metadata("user_1", "feat")
        stored.expires_at = time.time() - 1  # Force expiration

        assert store.get_feature("user_1", "feat") is None

    def test_version_history(self):
        store = FeatureStore(enable_offline=False, enable_versioning=True)
        store.set_features("user_1", {"feat": 1.0}, version=1)
        store.set_features("user_1", {"feat": 2.0}, version=2)
        store.set_features("user_1", {"feat": 3.0}, version=3)

        history = store.get_feature_version_history("user_1", "feat")
        assert len(history) == 3
        assert history[-1]["version"] == 3

    def test_point_in_time_lookup(self):
        store = FeatureStore(enable_offline=False, enable_versioning=True)

        t1, t2, t3 = 1000.0, 2000.0, 3000.0
        store.set_features("user_1", {"feat": 10.0}, timestamp=t1)
        store.set_features("user_1", {"feat": 20.0}, timestamp=t2)
        store.set_features("user_1", {"feat": 30.0}, timestamp=t3)

        result = store.get_features_at_time("user_1", ["feat"], point_in_time=1500.0)
        assert result["feat"] == 10.0

        result = store.get_features_at_time("user_1", ["feat"], point_in_time=2500.0)
        assert result["feat"] == 20.0

    def test_batch_get(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("u1", {"feat": 1.0})
        store.set_features("u2", {"feat": 2.0})

        batch = store.batch_get(["u1", "u2", "u3"], ["feat"])
        assert batch["u1"]["feat"] == 1.0
        assert batch["u2"]["feat"] == 2.0
        assert batch["u3"] == {}

    def test_delete_feature(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("user_1", {"feat": 1.0})
        assert store.delete_feature("user_1", "feat") is True
        assert store.get_feature("user_1", "feat") is None

    def test_delete_entity(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("user_1", {"a": 1.0, "b": 2.0})
        assert store.delete_entity("user_1") is True
        assert store.get_features("user_1") == {}

    def test_list_entities(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("user_1", {"f": 1})
        store.set_features("user_2", {"f": 2})
        entities = store.list_entities()
        assert set(entities) == {"user_1", "user_2"}

    def test_metrics(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("u1", {"f": 1})
        store.get_features("u1")
        store.get_features("u_missing")

        m = store.metrics
        assert m["online_sets"] > 0
        assert m["online_gets"] > 0
        assert m["entity_count"] == 1

    def test_cleanup_expired(self):
        store = FeatureStore(enable_offline=False)
        store.set_features("u1", {"f": 1}, ttl_seconds=1)

        # Force expiration
        stored = store.get_feature_with_metadata("u1", "f")
        stored.expires_at = time.time() - 1

        removed = store.cleanup_expired()
        assert removed == 1
