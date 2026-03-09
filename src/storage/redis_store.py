"""
Redis Online Feature Store

Low-latency feature serving layer with TTL-based caching,
pipeline operations for batch access, feature versioning,
and connection pool management.

Author: Gabriel Demetrios Lafis
"""

import json
import time
import threading
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field

from src.utils.logger import setup_logger
from src.config.settings import RedisConfig

logger = setup_logger(__name__)


@dataclass
class FeatureValue:
    """Typed feature value with metadata for the online store."""
    value: Any
    version: int = 1
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def serialize(self) -> str:
        return json.dumps({
            "v": self.value,
            "ver": self.version,
            "ts": self.timestamp,
            "meta": self.metadata,
        })

    @classmethod
    def deserialize(cls, data: str) -> "FeatureValue":
        parsed = json.loads(data)
        return cls(
            value=parsed["v"],
            version=parsed.get("ver", 1),
            timestamp=parsed.get("ts", 0.0),
            metadata=parsed.get("meta", {}),
        )


class RedisOnlineStore:
    """
    Redis-backed online feature store for low-latency feature serving.

    Features:
        - Sub-millisecond feature lookups with connection pooling
        - TTL-based automatic expiration per feature
        - Pipeline operations for batch get/set (100x throughput)
        - Feature versioning with atomic version bumps
        - Health checks and connection monitoring
        - Thread-safe access with connection pool
    """

    def __init__(self, config: Optional[RedisConfig] = None):
        self.config = config or RedisConfig()
        self._client = None
        self._pool = None
        self._is_connected = False
        self._metrics = {
            "gets": 0,
            "sets": 0,
            "hits": 0,
            "misses": 0,
            "pipeline_ops": 0,
            "errors": 0,
        }

    def connect(self) -> None:
        """Establish connection to Redis with connection pooling."""
        try:
            import redis

            self._pool = redis.ConnectionPool(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                decode_responses=True,
            )
            self._client = redis.Redis(connection_pool=self._pool)
            self._client.ping()
            self._is_connected = True

            logger.info(
                "Connected to Redis",
                extra={
                    "host": self.config.host,
                    "port": self.config.port,
                    "db": self.config.db,
                },
            )

        except ImportError:
            logger.warning("redis package not installed; using mock store")
            self._client = _MockRedis()
            self._is_connected = True

        except Exception as e:
            logger.error(
                "Failed to connect to Redis",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise

    def disconnect(self) -> None:
        """Close the Redis connection pool."""
        if self._pool is not None:
            self._pool.disconnect()
        self._is_connected = False
        logger.info("Disconnected from Redis")

    def get_feature(
        self, entity_id: str, feature_name: str
    ) -> Optional[FeatureValue]:
        """
        Retrieve a single feature value for an entity.

        Args:
            entity_id: Entity identifier (e.g., user_id, item_id).
            feature_name: Feature name to retrieve.

        Returns:
            FeatureValue if found, None otherwise.
        """
        self._ensure_connected()
        key = self._make_key(entity_id, feature_name)

        try:
            data = self._client.get(key)
            self._metrics["gets"] += 1

            if data is None:
                self._metrics["misses"] += 1
                return None

            self._metrics["hits"] += 1
            return FeatureValue.deserialize(data)

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Redis GET failed",
                extra={"key": key, "error": str(e)},
            )
            return None

    def set_feature(
        self,
        entity_id: str,
        feature_name: str,
        feature_value: FeatureValue,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """
        Store a feature value for an entity.

        Args:
            entity_id: Entity identifier.
            feature_name: Feature name.
            feature_value: The feature value to store.
            ttl_seconds: Optional TTL override (defaults to config TTL).

        Returns:
            True if successful, False otherwise.
        """
        self._ensure_connected()
        key = self._make_key(entity_id, feature_name)
        ttl = ttl_seconds or feature_value.ttl_seconds or self.config.default_ttl_seconds

        try:
            serialized = feature_value.serialize()
            self._client.setex(key, ttl, serialized)
            self._metrics["sets"] += 1

            # Store version mapping
            version_key = self._make_version_key(entity_id, feature_name)
            self._client.set(version_key, str(feature_value.version))

            return True

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Redis SET failed",
                extra={"key": key, "error": str(e)},
            )
            return False

    def batch_get(
        self, entity_id: str, feature_names: List[str]
    ) -> Dict[str, Optional[FeatureValue]]:
        """
        Retrieve multiple features for an entity using pipeline.

        Args:
            entity_id: Entity identifier.
            feature_names: List of feature names to retrieve.

        Returns:
            Dictionary mapping feature names to their values (or None).
        """
        self._ensure_connected()
        keys = [self._make_key(entity_id, fn) for fn in feature_names]

        try:
            pipe = self._client.pipeline(transaction=False)
            for key in keys:
                pipe.get(key)
            results = pipe.execute()
            self._metrics["pipeline_ops"] += 1
            self._metrics["gets"] += len(keys)

            output: Dict[str, Optional[FeatureValue]] = {}
            for fn, data in zip(feature_names, results):
                if data is not None:
                    output[fn] = FeatureValue.deserialize(data)
                    self._metrics["hits"] += 1
                else:
                    output[fn] = None
                    self._metrics["misses"] += 1

            return output

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Redis pipeline GET failed",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            return {fn: None for fn in feature_names}

    def batch_set(
        self,
        entity_id: str,
        features: Dict[str, FeatureValue],
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        """
        Store multiple features for an entity using pipeline.

        Args:
            entity_id: Entity identifier.
            features: Dictionary of feature name -> FeatureValue.
            ttl_seconds: Optional TTL override.

        Returns:
            True if all writes succeeded, False otherwise.
        """
        self._ensure_connected()
        ttl = ttl_seconds or self.config.default_ttl_seconds

        try:
            pipe = self._client.pipeline(transaction=True)

            for feature_name, feature_value in features.items():
                key = self._make_key(entity_id, feature_name)
                effective_ttl = feature_value.ttl_seconds or ttl
                serialized = feature_value.serialize()
                pipe.setex(key, effective_ttl, serialized)

                version_key = self._make_version_key(entity_id, feature_name)
                pipe.set(version_key, str(feature_value.version))

            pipe.execute()
            self._metrics["pipeline_ops"] += 1
            self._metrics["sets"] += len(features)

            return True

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Redis pipeline SET failed",
                extra={"entity_id": entity_id, "error": str(e)},
            )
            return False

    def multi_entity_get(
        self,
        entity_ids: List[str],
        feature_name: str,
    ) -> Dict[str, Optional[FeatureValue]]:
        """
        Retrieve a feature across multiple entities using pipeline.

        Args:
            entity_ids: List of entity identifiers.
            feature_name: Feature name to retrieve for all entities.

        Returns:
            Dictionary mapping entity IDs to their feature values.
        """
        self._ensure_connected()
        keys = [self._make_key(eid, feature_name) for eid in entity_ids]

        try:
            pipe = self._client.pipeline(transaction=False)
            for key in keys:
                pipe.get(key)
            results = pipe.execute()
            self._metrics["pipeline_ops"] += 1

            output: Dict[str, Optional[FeatureValue]] = {}
            for eid, data in zip(entity_ids, results):
                if data is not None:
                    output[eid] = FeatureValue.deserialize(data)
                else:
                    output[eid] = None

            return output

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "Multi-entity GET failed",
                extra={"feature_name": feature_name, "error": str(e)},
            )
            return {eid: None for eid in entity_ids}

    def get_feature_version(
        self, entity_id: str, feature_name: str
    ) -> Optional[int]:
        """Get the current version number for a feature."""
        self._ensure_connected()
        version_key = self._make_version_key(entity_id, feature_name)
        try:
            version = self._client.get(version_key)
            return int(version) if version else None
        except Exception:
            return None

    def delete_feature(self, entity_id: str, feature_name: str) -> bool:
        """Delete a feature value and its version mapping."""
        self._ensure_connected()
        key = self._make_key(entity_id, feature_name)
        version_key = self._make_version_key(entity_id, feature_name)

        try:
            pipe = self._client.pipeline(transaction=True)
            pipe.delete(key)
            pipe.delete(version_key)
            pipe.execute()
            return True
        except Exception as e:
            logger.error(
                "Redis DELETE failed",
                extra={"key": key, "error": str(e)},
            )
            return False

    def health_check(self) -> bool:
        """Check Redis connectivity."""
        try:
            if self._client is not None:
                return self._client.ping()
        except Exception:
            pass
        return False

    def get_ttl(self, entity_id: str, feature_name: str) -> int:
        """Get remaining TTL in seconds for a feature key."""
        self._ensure_connected()
        key = self._make_key(entity_id, feature_name)
        try:
            return self._client.ttl(key)
        except Exception:
            return -1

    @property
    def metrics(self) -> Dict[str, int]:
        return dict(self._metrics)

    def _make_key(self, entity_id: str, feature_name: str) -> str:
        return f"{self.config.key_prefix}{entity_id}:{feature_name}"

    def _make_version_key(self, entity_id: str, feature_name: str) -> str:
        return (
            f"{self.config.key_prefix}"
            f"{self.config.version_prefix}{entity_id}:{feature_name}"
        )

    def _ensure_connected(self) -> None:
        if not self._is_connected:
            raise RuntimeError("Redis store is not connected")

    def __enter__(self) -> "RedisOnlineStore":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()


class _MockRedis:
    """In-memory mock Redis for testing without a Redis server."""

    def __init__(self):
        self._data: Dict[str, str] = {}
        self._ttl: Dict[str, float] = {}
        self._lock = threading.Lock()

    def ping(self) -> bool:
        return True

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            if key in self._ttl and time.time() > self._ttl[key]:
                del self._data[key]
                del self._ttl[key]
                return None
            return self._data.get(key)

    def set(self, key: str, value: str) -> bool:
        with self._lock:
            self._data[key] = value
            return True

    def setex(self, key: str, ttl: int, value: str) -> bool:
        with self._lock:
            self._data[key] = value
            self._ttl[key] = time.time() + ttl
            return True

    def delete(self, *keys: str) -> int:
        count = 0
        with self._lock:
            for key in keys:
                if key in self._data:
                    del self._data[key]
                    self._ttl.pop(key, None)
                    count += 1
        return count

    def ttl(self, key: str) -> int:
        with self._lock:
            if key in self._ttl:
                remaining = self._ttl[key] - time.time()
                return max(0, int(remaining))
            if key in self._data:
                return -1
            return -2

    def pipeline(self, transaction: bool = True) -> "_MockPipeline":
        return _MockPipeline(self)


class _MockPipeline:
    """Mock Redis pipeline for batch operations."""

    def __init__(self, mock_redis: _MockRedis):
        self._redis = mock_redis
        self._commands: List[Tuple[str, tuple]] = []

    def get(self, key: str) -> "_MockPipeline":
        self._commands.append(("get", (key,)))
        return self

    def set(self, key: str, value: str) -> "_MockPipeline":
        self._commands.append(("set", (key, value)))
        return self

    def setex(self, key: str, ttl: int, value: str) -> "_MockPipeline":
        self._commands.append(("setex", (key, ttl, value)))
        return self

    def delete(self, key: str) -> "_MockPipeline":
        self._commands.append(("delete", (key,)))
        return self

    def execute(self) -> List[Any]:
        results = []
        for cmd, args in self._commands:
            method = getattr(self._redis, cmd)
            results.append(method(*args))
        self._commands.clear()
        return results
