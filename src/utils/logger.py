"""
Structured Logging Configuration

Provides centralized, structured logging with JSON formatting,
correlation IDs for distributed tracing, and configurable log levels
per module. Integrates with Flink job monitoring and Kafka event tracking.

Author: Gabriel Demetrios Lafis
"""

import logging
import logging.handlers
import json
import os
import sys
import uuid
import threading
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from contextvars import ContextVar

# Context variable for correlation ID propagation across async boundaries
_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> str:
    """Retrieve the current correlation ID, generating one if absent."""
    cid = _correlation_id.get()
    if cid is None:
        cid = str(uuid.uuid4())
        _correlation_id.set(cid)
    return cid


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current execution context."""
    _correlation_id.set(correlation_id)


class StructuredJsonFormatter(logging.Formatter):
    """
    JSON log formatter that produces structured log entries suitable
    for ingestion by centralized logging systems (ELK, Splunk, CloudWatch).

    Each log entry includes:
        - timestamp (ISO 8601 with timezone)
        - level
        - logger name
        - message
        - correlation_id for distributed tracing
        - module, function, line number
        - thread information
        - optional extra fields
    """

    RESERVED_ATTRS = {
        "name", "msg", "args", "created", "relativeCreated",
        "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "pathname", "filename", "module", "levelno", "levelname",
        "thread", "threadName", "process", "processName", "message",
        "msecs", "taskName",
    }

    def __init__(self, service_name: str = "flink-feature-engine"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service_name,
            "correlation_id": get_correlation_id(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.threadName,
            "process": record.process,
        }

        # Include exception info if present
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # Include any extra fields passed via the `extra` parameter
        for key, value in record.__dict__.items():
            if key not in self.RESERVED_ATTRS and not key.startswith("_"):
                try:
                    json.dumps(value)
                    log_entry[key] = value
                except (TypeError, ValueError):
                    log_entry[key] = str(value)

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class CorrelationFilter(logging.Filter):
    """Injects the correlation ID into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id()
        return True


class MetricsCounter:
    """Thread-safe counter for log-level metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: Dict[str, int] = {}

    def increment(self, level: str) -> None:
        with self._lock:
            self._counts[level] = self._counts.get(level, 0) + 1

    def get_counts(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._counts)

    def reset(self) -> None:
        with self._lock:
            self._counts.clear()


_metrics = MetricsCounter()


class MetricsHandler(logging.Handler):
    """Handler that counts log entries per level for monitoring."""

    def emit(self, record: logging.LogRecord) -> None:
        _metrics.increment(record.levelname)


def get_log_metrics() -> Dict[str, int]:
    """Return the current log-level counters."""
    return _metrics.get_counts()


def setup_logger(
    name: str = "flink_feature_engine",
    level: str = "INFO",
    log_file: Optional[str] = None,
    json_format: bool = True,
    max_bytes: int = 50 * 1024 * 1024,
    backup_count: int = 5,
    service_name: str = "flink-feature-engine",
) -> logging.Logger:
    """
    Configure and return a logger instance.

    Args:
        name: Logger name (typically module path).
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Optional path for file-based logging with rotation.
        json_format: If True, emit JSON-structured logs; otherwise plain text.
        max_bytes: Maximum size per log file before rotation.
        backup_count: Number of rotated log files to retain.
        service_name: Service identifier embedded in each log entry.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    logger.addFilter(CorrelationFilter())

    if json_format:
        formatter = StructuredJsonFormatter(service_name=service_name)
    else:
        formatter = logging.Formatter(
            fmt=(
                "%(asctime)s | %(levelname)-8s | %(name)s | "
                "%(module)s:%(funcName)s:%(lineno)d | %(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler with rotation
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Metrics handler
    logger.addHandler(MetricsHandler())

    return logger


# Module-level convenience logger
logger = setup_logger(
    level=os.getenv("LOG_LEVEL", "INFO"),
    json_format=os.getenv("LOG_FORMAT", "json").lower() == "json",
)
