"""
Feature Pipeline

Orchestrates real-time feature computation by connecting stream
events to windowed aggregations and materializing results to the
feature store. Supports declarative feature definitions and
automatic pipeline construction.

Author: Gabriel Demetrios Lafis
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from src.stream.source import StreamEvent
from src.stream.processor import StreamProcessor, ProcessingContext, TimeCharacteristic
from src.features.aggregations import (
    WindowAggregator,
    TumblingWindow,
    SlidingWindow,
    SessionWindow,
    AggregationFunction,
    WindowBounds,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class WindowType(str, Enum):
    """Window type for feature definitions."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class FeatureDefinition:
    """
    Declarative definition of a computed feature.

    Describes what to compute (aggregation), over what time range (window),
    from which event field (source_field), and how to key the computation.

    Attributes:
        name: Unique feature name (e.g., "purchase_count_1h").
        aggregation: Aggregation function to apply.
        window_type: Type of window (tumbling, sliding, session).
        window_size_seconds: Window size in seconds.
        source_field: Event property field to extract the value from.
        key_field: Event field to use as partition key (default: user_id).
        filter_event_type: Only consider events of this type (optional).
        slide_seconds: Slide interval for sliding windows.
        session_gap_seconds: Inactivity gap for session windows.
        allowed_lateness_seconds: Grace period for late events.
        default_value: Default value when no data is available.
        description: Human-readable description.
        percentile_value: Percentile (0-100) for percentile aggregation.
    """
    name: str
    aggregation: str  # maps to AggregationFunction
    window_type: str  # maps to WindowType
    window_size_seconds: float
    source_field: str
    key_field: str = "user_id"
    filter_event_type: Optional[str] = None
    slide_seconds: Optional[float] = None
    session_gap_seconds: Optional[float] = None
    allowed_lateness_seconds: float = 30.0
    default_value: float = 0.0
    description: str = ""
    percentile_value: float = 95.0

    @property
    def agg_function(self) -> AggregationFunction:
        return AggregationFunction(self.aggregation)


@dataclass
class ComputedFeature:
    """Result of a feature computation."""
    entity_id: str
    feature_name: str
    feature_value: float
    window_start: float
    window_end: float
    computed_at: float = field(default_factory=time.time)
    event_count: int = 0


class FeaturePipeline:
    """
    Orchestrates real-time feature computation from stream events.

    Connects declarative FeatureDefinitions to the windowed aggregation
    engine, automatically routing events and triggering computations
    based on watermark progress.

    Features:
        - Declarative feature registration
        - Automatic window assignment per feature definition
        - Multi-feature computation from single event stream
        - Watermark-based triggering with configurable intervals
        - Feature result callbacks for materialization
        - Pipeline metrics and monitoring

    Usage:
        pipeline = FeaturePipeline()
        pipeline.register_feature(FeatureDefinition(
            name="purchase_count_1h",
            aggregation="count",
            window_type="tumbling",
            window_size_seconds=3600,
            source_field="total_amount",
            filter_event_type="purchase",
        ))
        pipeline.process_event(event)
        results = pipeline.trigger(watermark)
    """

    def __init__(
        self,
        trigger_interval_seconds: float = 1.0,
        on_feature_computed: Optional[Callable[[ComputedFeature], None]] = None,
    ):
        self._features: Dict[str, FeatureDefinition] = {}
        self._aggregators: Dict[str, WindowAggregator] = {}
        self._windows: Dict[str, Any] = {}  # Window implementations
        self._trigger_interval = trigger_interval_seconds
        self._last_trigger_time = 0.0
        self._on_feature_computed = on_feature_computed
        self._lock = threading.Lock()

        self._metrics = {
            "events_processed": 0,
            "features_computed": 0,
            "features_registered": 0,
            "trigger_count": 0,
        }

    def register_feature(self, definition: FeatureDefinition) -> None:
        """
        Register a new feature definition in the pipeline.

        Creates the appropriate window implementation and aggregator
        for the feature.

        Args:
            definition: The feature definition to register.
        """
        with self._lock:
            self._features[definition.name] = definition
            self._aggregators[definition.name] = WindowAggregator()

            # Create window implementation
            if definition.window_type == WindowType.TUMBLING:
                self._windows[definition.name] = TumblingWindow(
                    size_seconds=definition.window_size_seconds,
                    allowed_lateness_seconds=definition.allowed_lateness_seconds,
                )
            elif definition.window_type == WindowType.SLIDING:
                slide = definition.slide_seconds or (definition.window_size_seconds / 4)
                self._windows[definition.name] = SlidingWindow(
                    size_seconds=definition.window_size_seconds,
                    slide_seconds=slide,
                    allowed_lateness_seconds=definition.allowed_lateness_seconds,
                )
            elif definition.window_type == WindowType.SESSION:
                gap = definition.session_gap_seconds or 300.0
                self._windows[definition.name] = SessionWindow(
                    gap_seconds=gap,
                )

            self._metrics["features_registered"] += 1
            logger.info(
                "Feature registered",
                extra={
                    "feature": definition.name,
                    "aggregation": definition.aggregation,
                    "window_type": definition.window_type,
                    "window_size": definition.window_size_seconds,
                },
            )

    def unregister_feature(self, name: str) -> bool:
        """Remove a feature definition from the pipeline."""
        with self._lock:
            if name in self._features:
                del self._features[name]
                del self._aggregators[name]
                del self._windows[name]
                return True
            return False

    def process_event(self, event: StreamEvent) -> int:
        """
        Process a single event through all registered features.

        Routes the event to appropriate feature aggregators based
        on event type filters and extracts the source field value.

        Args:
            event: The incoming stream event.

        Returns:
            Number of feature aggregators the event was routed to.
        """
        routed = 0

        with self._lock:
            for name, definition in self._features.items():
                # Apply event type filter
                if definition.filter_event_type is not None:
                    if event.event_type != definition.filter_event_type:
                        continue

                # Extract key
                if definition.key_field == "user_id":
                    key = event.user_id
                else:
                    key = str(event.properties.get(definition.key_field, event.user_id))

                # Extract value
                value = self._extract_value(event, definition)
                if value is None:
                    continue

                # Route to aggregator
                aggregator = self._aggregators[name]
                window_impl = self._windows[name]

                if isinstance(window_impl, TumblingWindow):
                    aggregator.add_event_tumbling(
                        key=key,
                        value=value,
                        timestamp=event.timestamp,
                        window=window_impl,
                        distinct_key=str(event.properties.get("product_id", event.event_id)),
                    )
                elif isinstance(window_impl, SlidingWindow):
                    aggregator.add_event_sliding(
                        key=key,
                        value=value,
                        timestamp=event.timestamp,
                        window=window_impl,
                        distinct_key=str(event.properties.get("product_id", event.event_id)),
                    )
                elif isinstance(window_impl, SessionWindow):
                    aggregator.add_event_session(
                        key=key,
                        value=value,
                        timestamp=event.timestamp,
                        session_window=window_impl,
                        distinct_key=str(event.properties.get("product_id", event.event_id)),
                    )

                routed += 1

        self._metrics["events_processed"] += 1
        return routed

    def process_events(self, events: List[StreamEvent]) -> int:
        """Process a batch of events. Returns total routing count."""
        total = 0
        for event in events:
            total += self.process_event(event)
        return total

    def trigger(self, watermark: float) -> List[ComputedFeature]:
        """
        Trigger computation for all ready windows across all features.

        Called when the watermark advances. Computes aggregation results
        for windows whose end time has been reached.

        Args:
            watermark: Current watermark timestamp.

        Returns:
            List of computed feature results.
        """
        all_results: List[ComputedFeature] = []

        with self._lock:
            for name, definition in self._features.items():
                aggregator = self._aggregators[name]
                window_impl = self._windows[name]

                triggered = aggregator.trigger_windows(
                    watermark=watermark,
                    aggregation=definition.agg_function,
                    window_impl=window_impl,
                    percentile_value=definition.percentile_value,
                )

                for key, bounds, value in triggered:
                    computed = ComputedFeature(
                        entity_id=key,
                        feature_name=name,
                        feature_value=value,
                        window_start=bounds.start,
                        window_end=bounds.end,
                        event_count=0,
                    )
                    all_results.append(computed)

                    if self._on_feature_computed:
                        self._on_feature_computed(computed)

            self._metrics["features_computed"] += len(all_results)
            self._metrics["trigger_count"] += 1

        return all_results

    def trigger_if_due(self, watermark: float) -> List[ComputedFeature]:
        """
        Trigger computation only if the trigger interval has elapsed.

        Args:
            watermark: Current watermark timestamp.

        Returns:
            List of computed results, or empty list if not due.
        """
        now = time.time()
        if (now - self._last_trigger_time) < self._trigger_interval:
            return []

        self._last_trigger_time = now
        return self.trigger(watermark)

    def get_feature_definitions(self) -> Dict[str, FeatureDefinition]:
        """Return all registered feature definitions."""
        with self._lock:
            return dict(self._features)

    def get_feature_definition(self, name: str) -> Optional[FeatureDefinition]:
        """Return a specific feature definition by name."""
        return self._features.get(name)

    def _extract_value(self, event: StreamEvent, definition: FeatureDefinition) -> Optional[float]:
        """Extract the numeric value from an event for aggregation."""
        source = definition.source_field

        # Special pseudo-fields
        if source == "_count" or source == "__count__":
            return 1.0

        if source == "_timestamp":
            return event.timestamp

        # Try properties first, then top-level event fields
        raw_value = event.properties.get(source)

        if raw_value is None:
            raw_value = getattr(event, source, None)

        if raw_value is None:
            # For count aggregations, missing field is treated as 1.0
            if definition.aggregation == "count":
                return 1.0
            return None

        try:
            return float(raw_value)
        except (ValueError, TypeError):
            return None

    @property
    def metrics(self) -> Dict[str, Any]:
        aggregator_metrics = {}
        with self._lock:
            for name, agg in self._aggregators.items():
                aggregator_metrics[name] = agg.metrics
        return {
            **self._metrics,
            "registered_features": list(self._features.keys()),
            "aggregator_details": aggregator_metrics,
        }
