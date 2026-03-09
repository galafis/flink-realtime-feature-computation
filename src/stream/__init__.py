"""
Stream processing module for real-time feature computation.

Provides Flink-inspired stream processing with event sources,
windowed aggregations, and feature store sinks.
"""

from src.stream.processor import StreamProcessor
from src.stream.source import KafkaSourceSimulator, StreamEvent
from src.stream.sink import FeatureStoreSink

__all__ = ["StreamProcessor", "KafkaSourceSimulator", "StreamEvent", "FeatureStoreSink"]
