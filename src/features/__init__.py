"""
Feature computation module for real-time feature extraction.

Provides feature pipelines, windowed aggregations, and
complex event processing for streaming data.
"""

from src.features.feature_pipeline import FeaturePipeline, FeatureDefinition
from src.features.aggregations import (
    WindowAggregator,
    TumblingWindow,
    SlidingWindow,
    SessionWindow,
)
from src.features.cep import ComplexEventProcessor, PatternDefinition

__all__ = [
    "FeaturePipeline",
    "FeatureDefinition",
    "WindowAggregator",
    "TumblingWindow",
    "SlidingWindow",
    "SessionWindow",
    "ComplexEventProcessor",
    "PatternDefinition",
]
