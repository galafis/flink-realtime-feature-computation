"""
Schema Registry for Feature Events

Manages schema versioning, compatibility validation, and evolution
tracking for feature event schemas. Provides a lightweight registry
that can operate standalone or integrate with Confluent Schema Registry.

Author: Gabriel Demetrios Lafis
"""

import json
import hashlib
import time
import threading
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass, field
from collections import OrderedDict

from src.utils.logger import setup_logger
from src.config.settings import KafkaConfig

logger = setup_logger(__name__)


class CompatibilityMode(str, Enum):
    """Schema compatibility modes following Avro conventions."""
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


class SchemaType(str, Enum):
    """Supported schema formats."""
    AVRO = "AVRO"
    JSON = "JSON"


@dataclass
class SchemaVersion:
    """A versioned schema entry in the registry."""
    schema_id: int
    version: int
    subject: str
    schema_type: SchemaType
    schema_str: str
    fingerprint: str
    created_at: float = field(default_factory=time.time)
    is_deprecated: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CompatibilityResult:
    """Result of a schema compatibility check."""
    is_compatible: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


# Default feature event schema definition
FEATURE_EVENT_SCHEMA = {
    "type": "record",
    "name": "FeatureEvent",
    "namespace": "com.featureengine.events",
    "fields": [
        {"name": "entity_id", "type": "string"},
        {"name": "feature_name", "type": "string"},
        {"name": "feature_value", "type": ["null", "double", "string", "long"]},
        {"name": "timestamp", "type": "double"},
        {"name": "event_type", "type": "string", "default": "feature_update"},
        {"name": "schema_version", "type": "int", "default": 1},
        {"name": "correlation_id", "type": "string"},
        {
            "name": "metadata",
            "type": {"type": "map", "values": "string"},
            "default": {},
        },
    ],
}


class SchemaRegistry:
    """
    Feature event schema registry with versioning and compatibility checks.

    Provides:
        - Schema registration with automatic versioning
        - Compatibility validation (backward, forward, full)
        - Schema fingerprinting for deduplication
        - Evolution tracking and deprecation
        - Thread-safe concurrent access
        - Optional integration with Confluent Schema Registry
    """

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD,
        use_external_registry: bool = False,
    ):
        self.config = config or KafkaConfig()
        self.compatibility_mode = compatibility_mode
        self._use_external = use_external_registry
        self._lock = threading.RLock()
        self._schemas: Dict[str, List[SchemaVersion]] = {}
        self._schema_by_id: Dict[int, SchemaVersion] = {}
        self._next_id = 1
        self._external_client = None

        # Register default schema
        self.register_schema(
            subject="feature-events-value",
            schema_str=json.dumps(FEATURE_EVENT_SCHEMA),
            schema_type=SchemaType.AVRO,
        )

    def _connect_external(self) -> None:
        """Connect to external Confluent Schema Registry if configured."""
        if self._use_external and self._external_client is None:
            try:
                from confluent_kafka.schema_registry import SchemaRegistryClient
                self._external_client = SchemaRegistryClient({
                    "url": self.config.schema_registry_url,
                })
                logger.info(
                    "Connected to external schema registry",
                    extra={"url": self.config.schema_registry_url},
                )
            except ImportError:
                logger.warning(
                    "confluent_kafka.schema_registry not available; "
                    "using in-memory registry"
                )
                self._use_external = False

    def register_schema(
        self,
        subject: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SchemaVersion:
        """
        Register a new schema version for a subject.

        Args:
            subject: Schema subject name (e.g., 'feature-events-value').
            schema_str: JSON string of the schema definition.
            schema_type: Schema format (AVRO or JSON).
            metadata: Optional metadata to attach to the schema version.

        Returns:
            The registered SchemaVersion.

        Raises:
            ValueError: If the schema is incompatible with existing versions.
        """
        with self._lock:
            fingerprint = self._compute_fingerprint(schema_str)

            # Check for duplicate registration
            if subject in self._schemas:
                for existing in self._schemas[subject]:
                    if existing.fingerprint == fingerprint:
                        logger.debug(
                            "Schema already registered",
                            extra={
                                "subject": subject,
                                "version": existing.version,
                            },
                        )
                        return existing

                # Validate compatibility
                compat_result = self.check_compatibility(subject, schema_str)
                if not compat_result.is_compatible:
                    raise ValueError(
                        f"Schema incompatible with {self.compatibility_mode.value}: "
                        f"{'; '.join(compat_result.errors)}"
                    )

            # Create new version
            version_num = (
                len(self._schemas.get(subject, [])) + 1
            )
            schema_version = SchemaVersion(
                schema_id=self._next_id,
                version=version_num,
                subject=subject,
                schema_type=schema_type,
                schema_str=schema_str,
                fingerprint=fingerprint,
                metadata=metadata or {},
            )

            self._schemas.setdefault(subject, []).append(schema_version)
            self._schema_by_id[self._next_id] = schema_version
            self._next_id += 1

            logger.info(
                "Schema registered",
                extra={
                    "subject": subject,
                    "version": version_num,
                    "schema_id": schema_version.schema_id,
                },
            )
            return schema_version

    def get_schema(self, schema_id: int) -> Optional[SchemaVersion]:
        """Retrieve a schema by its global ID."""
        with self._lock:
            return self._schema_by_id.get(schema_id)

    def get_latest_schema(self, subject: str) -> Optional[SchemaVersion]:
        """Retrieve the latest non-deprecated schema for a subject."""
        with self._lock:
            versions = self._schemas.get(subject, [])
            for v in reversed(versions):
                if not v.is_deprecated:
                    return v
            return None

    def get_schema_by_version(
        self, subject: str, version: int
    ) -> Optional[SchemaVersion]:
        """Retrieve a specific schema version for a subject."""
        with self._lock:
            versions = self._schemas.get(subject, [])
            for v in versions:
                if v.version == version:
                    return v
            return None

    def get_all_versions(self, subject: str) -> List[SchemaVersion]:
        """Return all schema versions for a subject."""
        with self._lock:
            return list(self._schemas.get(subject, []))

    def get_subjects(self) -> List[str]:
        """Return all registered subjects."""
        with self._lock:
            return list(self._schemas.keys())

    def check_compatibility(
        self,
        subject: str,
        new_schema_str: str,
    ) -> CompatibilityResult:
        """
        Check whether a new schema is compatible with existing versions.

        Validates based on the configured compatibility mode.

        Args:
            subject: Schema subject to validate against.
            new_schema_str: The new schema to check.

        Returns:
            CompatibilityResult with compatibility status and any errors.
        """
        with self._lock:
            existing = self._schemas.get(subject, [])
            if not existing:
                return CompatibilityResult(is_compatible=True)

            errors: List[str] = []
            warnings: List[str] = []

            try:
                new_schema = json.loads(new_schema_str)
            except json.JSONDecodeError as e:
                return CompatibilityResult(
                    is_compatible=False,
                    errors=[f"Invalid JSON schema: {e}"],
                )

            latest = existing[-1]
            try:
                old_schema = json.loads(latest.schema_str)
            except json.JSONDecodeError:
                return CompatibilityResult(is_compatible=True)

            if self.compatibility_mode in (
                CompatibilityMode.BACKWARD,
                CompatibilityMode.BACKWARD_TRANSITIVE,
                CompatibilityMode.FULL,
                CompatibilityMode.FULL_TRANSITIVE,
            ):
                backward_errors = self._check_backward_compat(
                    old_schema, new_schema
                )
                errors.extend(backward_errors)

            if self.compatibility_mode in (
                CompatibilityMode.FORWARD,
                CompatibilityMode.FORWARD_TRANSITIVE,
                CompatibilityMode.FULL,
                CompatibilityMode.FULL_TRANSITIVE,
            ):
                forward_errors = self._check_forward_compat(
                    old_schema, new_schema
                )
                errors.extend(forward_errors)

            if self.compatibility_mode == CompatibilityMode.NONE:
                return CompatibilityResult(is_compatible=True)

            return CompatibilityResult(
                is_compatible=len(errors) == 0,
                errors=errors,
                warnings=warnings,
            )

    def deprecate_schema(self, subject: str, version: int) -> bool:
        """Mark a schema version as deprecated."""
        with self._lock:
            schema = self.get_schema_by_version(subject, version)
            if schema:
                schema.is_deprecated = True
                logger.info(
                    "Schema deprecated",
                    extra={"subject": subject, "version": version},
                )
                return True
            return False

    def get_evolution_history(self, subject: str) -> List[Dict[str, Any]]:
        """
        Return the evolution history for a subject, tracking field changes
        between consecutive versions.
        """
        with self._lock:
            versions = self._schemas.get(subject, [])
            history = []

            for i, version in enumerate(versions):
                entry: Dict[str, Any] = {
                    "version": version.version,
                    "schema_id": version.schema_id,
                    "created_at": version.created_at,
                    "is_deprecated": version.is_deprecated,
                    "changes": [],
                }

                if i > 0:
                    prev = versions[i - 1]
                    changes = self._diff_schemas(
                        prev.schema_str, version.schema_str
                    )
                    entry["changes"] = changes

                history.append(entry)

            return history

    def _check_backward_compat(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> List[str]:
        """Check backward compatibility (new reader, old writer)."""
        errors = []
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

        # New required fields without defaults break backward compat
        for name, field_def in new_fields.items():
            if name not in old_fields:
                if "default" not in field_def:
                    errors.append(
                        f"New field '{name}' has no default value "
                        f"(breaks backward compatibility)"
                    )
        return errors

    def _check_forward_compat(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
    ) -> List[str]:
        """Check forward compatibility (old reader, new writer)."""
        errors = []
        old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
        new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

        # Removed fields that had no default break forward compat
        for name, field_def in old_fields.items():
            if name not in new_fields:
                if "default" not in field_def:
                    errors.append(
                        f"Removed field '{name}' had no default "
                        f"(breaks forward compatibility)"
                    )
        return errors

    def _diff_schemas(
        self, old_schema_str: str, new_schema_str: str
    ) -> List[str]:
        """Compute a human-readable diff between two schema versions."""
        changes = []
        try:
            old = json.loads(old_schema_str)
            new = json.loads(new_schema_str)

            old_fields = {f["name"] for f in old.get("fields", [])}
            new_fields = {f["name"] for f in new.get("fields", [])}

            for added in new_fields - old_fields:
                changes.append(f"Added field: {added}")
            for removed in old_fields - new_fields:
                changes.append(f"Removed field: {removed}")

        except (json.JSONDecodeError, KeyError):
            changes.append("Unable to parse schema diff")

        return changes

    @staticmethod
    def _compute_fingerprint(schema_str: str) -> str:
        """Compute a canonical fingerprint for schema deduplication."""
        normalized = json.dumps(
            json.loads(schema_str), sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
