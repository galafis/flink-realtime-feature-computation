"""
Kafka Source Simulator

Simulates a Kafka consumer producing realistic e-commerce events
for the stream processing pipeline. Generates page views, searches,
add-to-cart actions, purchases, and other user interaction events
with configurable rates and distributions.

Author: Gabriel Demetrios Lafis
"""

import random
import time
import uuid
import threading
from typing import Any, Dict, Generator, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class EventType(str, Enum):
    """Supported e-commerce event types."""
    PAGE_VIEW = "page_view"
    SEARCH = "search"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"
    CHECKOUT_START = "checkout_start"
    PRODUCT_CLICK = "product_click"
    WISHLIST_ADD = "wishlist_add"
    LOGIN = "login"
    LOGOUT = "logout"


@dataclass
class StreamEvent:
    """
    Structured streaming event matching the Flink event schema.

    Carries all necessary metadata for event-time processing,
    partitioning, and feature extraction.
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    event_type: str = ""
    timestamp: float = field(default_factory=time.time)
    properties: Dict[str, Any] = field(default_factory=dict)

    @property
    def event_time(self) -> datetime:
        """Convert epoch timestamp to datetime."""
        return datetime.fromtimestamp(self.timestamp, tz=timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StreamEvent":
        return cls(**data)

    def get_property(self, key: str, default: Any = None) -> Any:
        """Safely retrieve a nested property value."""
        return self.properties.get(key, default)


# --- Realistic data generators ---

_PRODUCT_CATALOG = [
    {"product_id": "P001", "name": "Wireless Headphones", "category": "electronics", "price": 79.99},
    {"product_id": "P002", "name": "Running Shoes", "category": "sports", "price": 129.99},
    {"product_id": "P003", "name": "Coffee Maker", "category": "kitchen", "price": 49.99},
    {"product_id": "P004", "name": "Laptop Stand", "category": "electronics", "price": 34.99},
    {"product_id": "P005", "name": "Yoga Mat", "category": "sports", "price": 24.99},
    {"product_id": "P006", "name": "Smart Watch", "category": "electronics", "price": 199.99},
    {"product_id": "P007", "name": "Backpack", "category": "accessories", "price": 59.99},
    {"product_id": "P008", "name": "Desk Lamp", "category": "home", "price": 39.99},
    {"product_id": "P009", "name": "Bluetooth Speaker", "category": "electronics", "price": 44.99},
    {"product_id": "P010", "name": "Water Bottle", "category": "sports", "price": 19.99},
    {"product_id": "P011", "name": "Phone Case", "category": "accessories", "price": 14.99},
    {"product_id": "P012", "name": "Mechanical Keyboard", "category": "electronics", "price": 89.99},
]

_SEARCH_QUERIES = [
    "wireless headphones", "running shoes", "coffee maker", "laptop accessories",
    "yoga equipment", "smart watch deals", "backpack waterproof", "desk lamp led",
    "bluetooth speaker portable", "fitness tracker", "phone accessories",
    "mechanical keyboard rgb", "usb hub", "monitor stand", "webcam hd",
]

_PAGES = [
    "/", "/products", "/categories/electronics", "/categories/sports",
    "/categories/kitchen", "/categories/home", "/deals", "/new-arrivals",
    "/bestsellers", "/account", "/cart", "/wishlist", "/checkout",
]

_REFERRERS = [
    "google.com", "facebook.com", "instagram.com", "twitter.com",
    "direct", "email_campaign", "affiliate_partner", "tiktok.com",
]

_DEVICES = ["mobile", "desktop", "tablet"]
_BROWSERS = ["chrome", "firefox", "safari", "edge"]
_OS_LIST = ["windows", "macos", "ios", "android", "linux"]


class KafkaSourceSimulator:
    """
    Simulates a Kafka source producing realistic e-commerce stream events.

    Generates events with realistic temporal patterns, user sessions,
    and product interactions. Supports configurable event rates,
    user counts, and probability distributions.

    Features:
        - Realistic user session simulation with sequential behavior
        - Configurable event type distribution
        - Support for out-of-order and late events (for watermark testing)
        - Thread-safe event generation
        - Bounded and unbounded stream modes
    """

    def __init__(
        self,
        num_users: int = 50,
        events_per_second: float = 10.0,
        late_event_probability: float = 0.05,
        max_late_seconds: int = 30,
        seed: Optional[int] = None,
    ):
        self.num_users = num_users
        self.events_per_second = events_per_second
        self.late_event_probability = late_event_probability
        self.max_late_seconds = max_late_seconds
        self._rng = random.Random(seed)
        self._user_ids = [f"user_{i:04d}" for i in range(num_users)]
        self._user_sessions: Dict[str, Dict[str, Any]] = {}
        self._event_count = 0
        self._is_running = False
        self._lock = threading.Lock()

        # Event type distribution weights
        self._event_weights: Dict[str, float] = {
            EventType.PAGE_VIEW: 0.35,
            EventType.SEARCH: 0.15,
            EventType.PRODUCT_CLICK: 0.15,
            EventType.ADD_TO_CART: 0.12,
            EventType.REMOVE_FROM_CART: 0.03,
            EventType.CHECKOUT_START: 0.05,
            EventType.PURCHASE: 0.05,
            EventType.WISHLIST_ADD: 0.04,
            EventType.LOGIN: 0.03,
            EventType.LOGOUT: 0.03,
        }

    def generate_events(self, count: int) -> List[StreamEvent]:
        """
        Generate a fixed batch of realistic stream events.

        Args:
            count: Number of events to generate.

        Returns:
            List of StreamEvent objects with realistic properties.
        """
        events = []
        base_time = time.time()

        for i in range(count):
            event = self._generate_single_event(
                base_time + (i / max(self.events_per_second, 0.1))
            )
            events.append(event)

        with self._lock:
            self._event_count += count

        return events

    def stream_events(
        self,
        max_events: Optional[int] = None,
        duration_seconds: Optional[float] = None,
    ) -> Generator[StreamEvent, None, None]:
        """
        Generate a continuous stream of events (generator).

        Args:
            max_events: Maximum events to generate (None for unbounded).
            duration_seconds: Maximum duration in seconds (None for unbounded).

        Yields:
            StreamEvent objects at the configured rate.
        """
        self._is_running = True
        count = 0
        start_time = time.time()
        interval = 1.0 / max(self.events_per_second, 0.1)

        try:
            while self._is_running:
                if max_events is not None and count >= max_events:
                    break
                if duration_seconds is not None and (time.time() - start_time) >= duration_seconds:
                    break

                event = self._generate_single_event(time.time())
                yield event

                count += 1
                with self._lock:
                    self._event_count += 1

                # Throttle to target rate
                elapsed = time.time() - start_time
                expected_time = count * interval
                sleep_time = expected_time - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

        finally:
            self._is_running = False

    def stop(self) -> None:
        """Signal the stream generator to stop."""
        self._is_running = False

    def _generate_single_event(self, base_timestamp: float) -> StreamEvent:
        """Generate a single realistic event."""
        user_id = self._rng.choice(self._user_ids)
        event_type = self._weighted_choice()

        # Simulate late events by shifting timestamp backwards
        timestamp = base_timestamp
        if self._rng.random() < self.late_event_probability:
            late_offset = self._rng.uniform(1, self.max_late_seconds)
            timestamp -= late_offset

        # Build event-specific properties
        properties = self._build_properties(user_id, event_type)

        return StreamEvent(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            event_type=event_type,
            timestamp=timestamp,
            properties=properties,
        )

    def _weighted_choice(self) -> str:
        """Select an event type based on configured weights."""
        types = list(self._event_weights.keys())
        weights = list(self._event_weights.values())
        return self._rng.choices(types, weights=weights, k=1)[0]

    def _build_properties(self, user_id: str, event_type: str) -> Dict[str, Any]:
        """Build realistic properties based on event type."""
        # Common properties
        properties: Dict[str, Any] = {
            "device": self._rng.choice(_DEVICES),
            "browser": self._rng.choice(_BROWSERS),
            "os": self._rng.choice(_OS_LIST),
            "session_id": self._get_or_create_session(user_id),
            "referrer": self._rng.choice(_REFERRERS),
        }

        if event_type == EventType.PAGE_VIEW:
            properties["page"] = self._rng.choice(_PAGES)
            properties["duration_seconds"] = round(self._rng.expovariate(1.0 / 30.0), 1)

        elif event_type == EventType.SEARCH:
            properties["query"] = self._rng.choice(_SEARCH_QUERIES)
            properties["results_count"] = self._rng.randint(0, 150)
            properties["page"] = "/search"

        elif event_type in (EventType.PRODUCT_CLICK, EventType.ADD_TO_CART, EventType.WISHLIST_ADD):
            product = self._rng.choice(_PRODUCT_CATALOG)
            properties["product_id"] = product["product_id"]
            properties["product_name"] = product["name"]
            properties["category"] = product["category"]
            properties["price"] = product["price"]
            if event_type == EventType.ADD_TO_CART:
                properties["quantity"] = self._rng.randint(1, 3)

        elif event_type == EventType.REMOVE_FROM_CART:
            product = self._rng.choice(_PRODUCT_CATALOG)
            properties["product_id"] = product["product_id"]
            properties["price"] = product["price"]
            properties["quantity"] = 1

        elif event_type == EventType.CHECKOUT_START:
            num_items = self._rng.randint(1, 5)
            items = self._rng.choices(_PRODUCT_CATALOG, k=num_items)
            properties["cart_total"] = round(sum(i["price"] for i in items), 2)
            properties["item_count"] = num_items
            properties["page"] = "/checkout"

        elif event_type == EventType.PURCHASE:
            num_items = self._rng.randint(1, 5)
            items = self._rng.choices(_PRODUCT_CATALOG, k=num_items)
            total = round(sum(i["price"] for i in items), 2)
            properties["order_id"] = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            properties["total_amount"] = total
            properties["item_count"] = num_items
            properties["payment_method"] = self._rng.choice(
                ["credit_card", "debit_card", "paypal", "pix"]
            )
            properties["items"] = [
                {"product_id": i["product_id"], "price": i["price"]} for i in items
            ]

        elif event_type == EventType.LOGIN:
            properties["method"] = self._rng.choice(["email", "google", "facebook", "github"])

        return properties

    def _get_or_create_session(self, user_id: str) -> str:
        """Get existing session or create a new one for the user."""
        now = time.time()
        session = self._user_sessions.get(user_id)

        # Create new session if none exists or if session expired (30 min gap)
        if session is None or (now - session["last_active"]) > 1800:
            session_id = f"sess_{uuid.uuid4().hex[:12]}"
            self._user_sessions[user_id] = {
                "session_id": session_id,
                "last_active": now,
                "started_at": now,
            }
            return session_id

        session["last_active"] = now
        return session["session_id"]

    @property
    def event_count(self) -> int:
        with self._lock:
            return self._event_count

    @property
    def active_sessions(self) -> int:
        now = time.time()
        return sum(
            1 for s in self._user_sessions.values()
            if (now - s["last_active"]) < 1800
        )
