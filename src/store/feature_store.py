"""
Unified Feature Store

Provides a single interface for online (dict-based / Redis-like) and
offline (file-based) feature serving with versioning, TTL management,
and point-in-time lookups.

Author: Gabriel Demetrios Lafis
"""

import json
import os
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class StoredFeature:
    """Internal representation of a stored feature value."""
    value: Any
    version: int = 1
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    expires_at: Optional[float] = None

    def __post_init__(self):
        if self.ttl_seconds is not None and self.expires_at is None:
            self.expires_at = self.timestamp + self.ttl_seconds

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "version": self.version,
            "timestamp": self.timestamp,
            "ttl_seconds": self.ttl_seconds,
            "metadata": self.metadata,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StoredFeature":
        return cls(
            value=data["value"],
            version=data.get("version", 1),
            timestamp=data.get("timestamp", 0.0),
            ttl_seconds=data.get("ttl_seconds"),
            metadata=data.get("metadata", {}),
            expires_at=data.get("expires_at"),
        )


class FeatureStore:
    """
    Unified feature store with online and offline serving layers.

    The online store uses an in-memory dictionary (simulating Redis)
    for sub-millisecond feature lookups. The offline store uses
    file-based persistence (simulating Cassandra) for historical
    queries and batch training data extraction.

    Features:
        - Online store: in-memory dict with TTL expiration
        - Offline store: append-only file-based log
        - Feature versioning with version history
        - TTL-based automatic expiration
        - Point-in-time feature retrieval
        - Batch get/set operations
        - Feature metadata and lineage tracking
        - Thread-safe concurrent access

    Usage:
        store = FeatureStore()
        store.set_features("user_123", {"spend_1h": 45.99, "click_count": 12})
        features = store.get_features("user_123", ["spend_1h", "click_count"])
    """

    def __init__(
        self,
        offline_storage_path: Optional[str] = None,
        default_ttl_seconds: int = 3600,
        enable_offline: bool = True,
        enable_versioning: bool = True,
    ):
        self.default_ttl = default_ttl_seconds
        self.enable_offline = enable_offline
        self.enable_versioning = enable_versioning
        self._offline_path = offline_storage_path

        # Online store: entity_id -> feature_name -> StoredFeature
        self._online: Dict[str, Dict[str, StoredFeature]] = defaultdict(dict)

        # Version history: entity_id -> feature_name -> [StoredFeature]
        self._versions: Dict[str, Dict[str, List[StoredFeature]]] = defaultdict(
            lambda: defaultdict(list)
        )

        self._lock = threading.RLock()
        self._metrics = {
            "online_gets": 0,
            "online_sets": 0,
            "online_hits": 0,
            "online_misses": 0,
            "offline_writes": 0,
            "offline_reads": 0,
            "expired_evictions": 0,
        }

        # Initialize offline store path
        if self.enable_offline and self._offline_path:
            os.makedirs(self._offline_path, exist_ok=True)

    def set_features(
        self,
        entity_id: str,
        features: Dict[str, Any],
        timestamp: Optional[float] = None,
        version: int = 1,
        ttl_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Store multiple features for an entity.

        Writes to both online and offline stores.

        Args:
            entity_id: Entity identifier (e.g., user_id).
            features: Dictionary of feature_name -> value.
            timestamp: Event timestamp (defaults to now).
            version: Feature version number.
            ttl_seconds: TTL override (defaults to store default).
            metadata: Optional metadata for all features.

        Returns:
            True if all writes succeeded.
        """
        ts = timestamp or time.time()
        ttl = ttl_seconds or self.default_ttl
        meta = metadata or {}

        with self._lock:
            for feature_name, value in features.items():
                stored = StoredFeature(
                    value=value,
                    version=version,
                    timestamp=ts,
                    ttl_seconds=ttl,
                    metadata=meta,
                )

                # Write to online store
                self._online[entity_id][feature_name] = stored
                self._metrics["online_sets"] += 1

                # Track version history
                if self.enable_versioning:
                    self._versions[entity_id][feature_name].append(stored)
                    # Keep last 10 versions
                    if len(self._versions[entity_id][feature_name]) > 10:
                        self._versions[entity_id][feature_name] = (
                            self._versions[entity_id][feature_name][-10:]
                        )

        # Write to offline store
        if self.enable_offline:
            self._write_offline(entity_id, features, ts, version, meta)

        return True

    def get_features(
        self,
        entity_id: str,
        feature_names: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Retrieve features for an entity from the online store.

        Args:
            entity_id: Entity identifier.
            feature_names: Specific features to retrieve (None = all).

        Returns:
            Dictionary of feature_name -> value.
        """
        result: Dict[str, Any] = {}

        with self._lock:
            entity_features = self._online.get(entity_id, {})
            self._metrics["online_gets"] += 1

            if not entity_features:
                self._metrics["online_misses"] += 1
                return result

            names = feature_names or list(entity_features.keys())

            for name in names:
                stored = entity_features.get(name)
                if stored is None:
                    self._metrics["online_misses"] += 1
                    continue

                if stored.is_expired:
                    # Evict expired feature
                    del entity_features[name]
                    self._metrics["expired_evictions"] += 1
                    self._metrics["online_misses"] += 1
                    continue

                result[name] = stored.value
                self._metrics["online_hits"] += 1

        return result

    def get_feature(self, entity_id: str, feature_name: str) -> Optional[Any]:
        """Get a single feature value."""
        features = self.get_features(entity_id, [feature_name])
        return features.get(feature_name)

    def get_feature_with_metadata(
        self, entity_id: str, feature_name: str
    ) -> Optional[StoredFeature]:
        """Get a feature with its full metadata."""
        with self._lock:
            entity_features = self._online.get(entity_id, {})
            stored = entity_features.get(feature_name)
            if stored is not None and not stored.is_expired:
                return stored
            return None

    def get_feature_version_history(
        self, entity_id: str, feature_name: str
    ) -> List[Dict[str, Any]]:
        """Retrieve the version history for a feature."""
        with self._lock:
            versions = self._versions.get(entity_id, {}).get(feature_name, [])
            return [v.to_dict() for v in versions]

    def get_features_at_time(
        self,
        entity_id: str,
        feature_names: List[str],
        point_in_time: float,
    ) -> Dict[str, Any]:
        """
        Point-in-time feature retrieval for training data generation.

        Returns the feature values as they were at a specific timestamp.

        Args:
            entity_id: Entity identifier.
            feature_names: Features to retrieve.
            point_in_time: Target timestamp.

        Returns:
            Feature values as they were at the specified time.
        """
        result: Dict[str, Any] = {}

        with self._lock:
            entity_versions = self._versions.get(entity_id, {})

            for name in feature_names:
                versions = entity_versions.get(name, [])
                # Find the latest version at or before point_in_time
                best = None
                for v in versions:
                    if v.timestamp <= point_in_time:
                        best = v
                    else:
                        break

                if best is not None:
                    result[name] = best.value

        return result

    def batch_get(
        self,
        entity_ids: List[str],
        feature_names: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Batch retrieve features for multiple entities.

        Args:
            entity_ids: List of entity identifiers.
            feature_names: Features to retrieve for each entity.

        Returns:
            Nested dictionary: entity_id -> feature_name -> value.
        """
        result: Dict[str, Dict[str, Any]] = {}
        for entity_id in entity_ids:
            result[entity_id] = self.get_features(entity_id, feature_names)
        return result

    def delete_feature(self, entity_id: str, feature_name: str) -> bool:
        """Delete a specific feature from the online store."""
        with self._lock:
            entity_features = self._online.get(entity_id, {})
            if feature_name in entity_features:
                del entity_features[feature_name]
                return True
            return False

    def delete_entity(self, entity_id: str) -> bool:
        """Delete all features for an entity."""
        with self._lock:
            if entity_id in self._online:
                del self._online[entity_id]
                self._versions.pop(entity_id, None)
                return True
            return False

    def list_entities(self) -> List[str]:
        """List all entities with stored features."""
        with self._lock:
            return list(self._online.keys())

    def list_features(self, entity_id: str) -> List[str]:
        """List all feature names for an entity."""
        with self._lock:
            entity_features = self._online.get(entity_id, {})
            return [
                name for name, f in entity_features.items()
                if not f.is_expired
            ]

    def get_entity_count(self) -> int:
        """Get the total number of entities in the store."""
        with self._lock:
            return len(self._online)

    def get_feature_count(self) -> int:
        """Get the total number of stored features."""
        with self._lock:
            return sum(len(f) for f in self._online.values())

    def cleanup_expired(self) -> int:
        """Remove all expired features from the online store."""
        removed = 0
        with self._lock:
            for entity_id in list(self._online.keys()):
                features = self._online[entity_id]
                expired = [
                    name for name, f in features.items()
                    if f.is_expired
                ]
                for name in expired:
                    del features[name]
                    removed += 1
                if not features:
                    del self._online[entity_id]

        self._metrics["expired_evictions"] += removed
        return removed

    def export_offline_snapshot(self, output_path: str) -> int:
        """
        Export current online store state to a JSON file for offline use.

        Args:
            output_path: Path to write the snapshot file.

        Returns:
            Number of features exported.
        """
        snapshot: Dict[str, Dict[str, Any]] = {}
        count = 0

        with self._lock:
            for entity_id, features in self._online.items():
                entity_data = {}
                for name, stored in features.items():
                    if not stored.is_expired:
                        entity_data[name] = stored.to_dict()
                        count += 1
                if entity_data:
                    snapshot[entity_id] = entity_data

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, default=str)

        logger.info(
            "Offline snapshot exported",
            extra={"path": output_path, "features": count},
        )
        return count

    def _write_offline(
        self,
        entity_id: str,
        features: Dict[str, Any],
        timestamp: float,
        version: int,
        metadata: Dict[str, Any],
    ) -> None:
        """Append feature records to the offline log file."""
        if not self._offline_path:
            return

        try:
            log_path = os.path.join(self._offline_path, "feature_log.jsonl")
            with open(log_path, "a", encoding="utf-8") as f:
                for name, value in features.items():
                    record = {
                        "entity_id": entity_id,
                        "feature_name": name,
                        "value": value,
                        "timestamp": timestamp,
                        "version": version,
                        "metadata": metadata,
                        "ingestion_time": time.time(),
                    }
                    f.write(json.dumps(record, default=str) + "\n")
                    self._metrics["offline_writes"] += 1

        except Exception as e:
            logger.error(
                "Offline write failed",
                extra={"entity_id": entity_id, "error": str(e)},
            )

    @property
    def metrics(self) -> Dict[str, Any]:
        hit_rate = 0.0
        total_gets = self._metrics["online_hits"] + self._metrics["online_misses"]
        if total_gets > 0:
            hit_rate = self._metrics["online_hits"] / total_gets

        return {
            **self._metrics,
            "hit_rate": round(hit_rate, 4),
            "entity_count": self.get_entity_count(),
            "feature_count": self.get_feature_count(),
        }

    def __repr__(self) -> str:
        return (
            f"FeatureStore(entities={self.get_entity_count()}, "
            f"features={self.get_feature_count()}, "
            f"offline={'enabled' if self.enable_offline else 'disabled'})"
        )
