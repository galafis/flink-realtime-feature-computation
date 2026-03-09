"""
Feature store module for online and offline feature serving.

Provides a unified FeatureStore abstraction over in-memory
online store (Redis-like) and file-based offline store.
"""

from src.store.feature_store import FeatureStore

__all__ = ["FeatureStore"]
