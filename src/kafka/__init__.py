"""
Kafka integration module for feature event streaming.

Provides producers, consumers, and schema management for
real-time feature event pipelines.
"""

from src.kafka.producer import FeatureEventProducer
from src.kafka.consumer import FeatureEventConsumer
from src.kafka.schema_registry import SchemaRegistry

__all__ = ["FeatureEventProducer", "FeatureEventConsumer", "SchemaRegistry"]
