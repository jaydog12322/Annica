# -*- coding: utf-8 -*-
"""
throttler.py
------------
Throttler Module - Rate Limiting for Kiwoom API

Implements token bucket rate limiting according to Kiwoom constraints:
- Orders: 5 per second (includes cancels)
- Data requests: 5 per second
- Token reservation for cancel/hedge operations

Key Features:
- Separate buckets for orders and queries
- Token reservation system
- Auto-pause on high utilization
- Queue management for burst handling
"""

import logging
import time
import threading
from typing import Dict, Optional, List
from dataclasses import dataclass
from enum import Enum
from collections import deque
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

logger = logging.getLogger(__name__)


class TokenType(Enum):
    """Token bucket types"""
    ORDERS = "orders"
    QUERIES = "queries"


@dataclass
class TokenRequest:
    """Token request details"""
    token_type: TokenType
    count: int
    requester_id: str
    priority: int = 1  # 0=highest (cancels/hedge), 1=normal
    timestamp: float = 0.0

    def __post_init__(self):
        if self.timestamp == 0.0:
            self.timestamp = time.time()


@dataclass
class TokenResponse:
    """Token allocation response"""
    granted: bool
    tokens_allocated: int
    wait_time_ms: float = 0.0
    reason: str = ""


class TokenBucket:
    """
    Token bucket implementation for rate limiting.

    Thread-safe token bucket with configurable rate and capacity.
    """

    def __init__(self, rate_per_second: int, capacity: int = None):
        self.rate = rate_per_second
        self.capacity = capacity or rate_per_second
        self.tokens = float(self.capacity)
        self.last_update = time.time()
        self.lock = threading.Lock()

        logger.debug(f"TokenBucket created: rate={rate_per_second}/s, capacity={self.capacity}")

    def request_tokens(self, count: int) -> bool:
        """
        Request tokens from bucket.

        Args:
            count: Number of tokens requested

        Returns:
            True if tokens granted, False if insufficient
        """
        with self.lock:
            self._refill()

            if self.tokens >= count:
                self.tokens -= count
                return True
            else:
                return False

    def get_available_tokens(self) -> int:
        """Get current number of available tokens"""
        with self.lock:
            self._refill()
            return int(self.tokens)

    def get_wait_time_ms(self, count: int) -> float:
        """
        Get estimated wait time for token availability.

        Args:
            count: Number of tokens needed

        Returns:
            Wait time in milliseconds
        """
        with self.lock:
            self._refill()

            if self.tokens >= count:
                return 0.0

            tokens_needed = count - self.tokens
            wait_time_s = tokens_needed / self.rate
            return wait_time_s * 1000.0

    def _refill(self):
        """Refill tokens based on elapsed time"""
        now = time.time()
        elapsed = now - self.last_update

        tokens_to_add = elapsed * self.rate
        self.tokens = min(self.capacity, self.tokens + tokens_to_add)
        self.last_update = now

    def get_utilization(self) -> float:
        """Get current utilization (0.0 to 1.0)"""
        with self.lock:
            self._refill()
            return 1.0 - (self.tokens / self.capacity)


class Throttler(QObject):
    """
    Central throttler for all Kiwoom API rate limiting.

    Manages separate token buckets for orders and queries,
    with reservation system and auto-pause functionality.

    Signals:
        auto_pause_triggered: Emitted when auto-pause activates
        utilization_warning: Emitted on high utilization
    """

    # Signals
    auto_pause_triggered = pyqtSignal(bool)  # True=paused, False=resumed
    utilization_warning = pyqtSignal(str, float)  # bucket_type, utilization

    def __init__(self, config):
        super().__init__()

        self.config = config
        self.throttling_config = config.throttling

        # Token buckets
        orders_rate = self.throttling_config.orders_bucket_per_sec
        queries_rate = self.throttling_config.queries_bucket_per_sec

        self.orders_bucket = TokenBucket(orders_rate)
        self.queries_bucket = TokenBucket(queries_rate)

        # Reservation system
        self.reserved_order_tokens = self.throttling_config.min_tokens_free_to_start_new_pair
        self.currently_reserved = 0

        # Auto-pause system
        self.auto_pause_config = config.telemetry.orders_utilization_autopause
        self.auto_pause_enabled = self.auto_pause_config["enabled"]
        self.auto_pause_threshold = self.auto_pause_config["threshold"]
        self.auto_pause_sustain_seconds = self.auto_pause_config["sustain_seconds"]

        self.is_auto_paused = False
        self.high_utilization_start_time = None

        # Request queue for fairness
        self.request_queue: deque = deque()
        self.queue_lock = threading.Lock()

        # Statistics
        self.stats = {
            "orders_granted": 0,
            "orders_denied": 0,
            "queries_granted": 0,
            "queries_denied": 0,
            "auto_pause_events": 0,
            "peak_utilization": 0.0
        }

        # Monitoring timer
        self.monitor_timer = QTimer()
        self.monitor_timer.timeout.connect(self._monitor_utilization)
        self.monitor_timer.start(1000)  # Check every second

        logger.info(f"Throttler initialized: orders={orders_rate}/s, queries={queries_rate}/s, "
                    f"reserved={self.reserved_order_tokens}, auto_pause={self.auto_pause_enabled}")

    def request_tokens(self, token_type: TokenType, count: int,
                       requester_id: str = "", priority: int = 1) -> TokenResponse:
        """
        Request tokens from appropriate bucket.

        Args:
            token_type: ORDERS or QUERIES
            count: Number of tokens requested
            requester_id: ID of requesting component
            priority: 0=highest (cancel/hedge), 1=normal

        Returns:
            TokenResponse with grant status
        """
        request = TokenRequest(
            token_type=token_type,
            count=count,
            requester_id=requester_id,
            priority=priority
        )

        # Check auto-pause for new orders
        if token_type == TokenType.ORDERS and priority > 0 and self.is_auto_paused:
            return TokenResponse(
                granted=False,
                tokens_allocated=0,
                reason="auto_paused"
            )

        # Select appropriate bucket
        bucket = self.orders_bucket if token_type == TokenType.ORDERS else self.queries_bucket

        # Check reservation for orders
        if token_type == TokenType.ORDERS:
            available_tokens = bucket.get_available_tokens()
            effective_available = available_tokens - self.reserved_order_tokens

            if priority > 0 and effective_available < count:
                # Normal priority request, but would violate reservation
                wait_time = bucket.get_wait_time_ms(count + self.reserved_order_tokens)

                self.stats["orders_denied"] += 1

                return TokenResponse(
                    granted=False,
                    tokens_allocated=0,
                    wait_time_ms=wait_time,
                    reason="insufficient_tokens_after_reservation"
                )

        # Attempt to get tokens
        granted = bucket.request_tokens(count)

        if granted:
            if token_type == TokenType.ORDERS:
                self.stats["orders_granted"] += 1
            else:
                self.stats["queries_granted"] += 1

            logger.debug(f"Tokens granted: {count} {token_type.value} to {requester_id}")

            return TokenResponse(
                granted=True,
                tokens_allocated=count
            )
        else:
            # Denied - calculate wait time
            wait_time = bucket.get_wait_time_ms(count)

            if token_type == TokenType.ORDERS:
                self.stats["orders_denied"] += 1
            else:
                self.stats["queries_denied"] += 1

            logger.debug(f"Tokens denied: {count} {token_type.value} to {requester_id}, "
                         f"wait={wait_time:.1f}ms")

            return TokenResponse(
                granted=False,
                tokens_allocated=0,
                wait_time_ms=wait_time,
                reason="rate_limit_exceeded"
            )

    def request_order_tokens(self, count: int, requester_id: str = "",
                             priority: int = 1) -> TokenResponse:
        """Convenience method for order token requests"""
        return self.request_tokens(TokenType.ORDERS, count, requester_id, priority)

    def request_query_tokens(self, count: int, requester_id: str = "") -> TokenResponse:
        """Convenience method for query token requests"""
        return self.request_tokens(TokenType.QUERIES, count, requester_id)

    def reserve_order_tokens(self, count: int) -> bool:
        """
        Reserve additional order tokens (beyond the configured minimum).

        Args:
            count: Additional tokens to reserve

        Returns:
            True if reservation successful
        """
        available = self.orders_bucket.get_available_tokens()
        total_reserved = self.reserved_order_tokens + count

        if available >= total_reserved:
            self.currently_reserved += count
            logger.debug(f"Reserved {count} additional order tokens (total reserved: {total_reserved})")
            return True
        else:
            logger.debug(f"Cannot reserve {count} tokens - insufficient available")
            return False

    def release_reserved_tokens(self, count: int):
        """Release previously reserved tokens"""
        self.currently_reserved = max(0, self.currently_reserved - count)
        logger.debug(f"Released {count} reserved tokens (remaining reserved: {self.currently_reserved})")

    def _monitor_utilization(self):
        """Monitor utilization and trigger auto-pause if needed"""
        if not self.auto_pause_enabled:
            return

        orders_utilization = self.orders_bucket.get_utilization()

        # Update peak utilization
        self.stats["peak_utilization"] = max(self.stats["peak_utilization"], orders_utilization)

        # Check for high utilization
        if orders_utilization >= self.auto_pause_threshold:
            if self.high_utilization_start_time is None:
                self.high_utilization_start_time = time.time()
                logger.debug(f"High utilization detected: {orders_utilization:.1%}")

            # Check if sustained long enough
            sustained_time = time.time() - self.high_utilization_start_time
            if sustained_time >= self.auto_pause_sustain_seconds and not self.is_auto_paused:
                self._trigger_auto_pause(True)
        else:
            # Utilization dropped
            if self.high_utilization_start_time is not None:
                self.high_utilization_start_time = None

            if self.is_auto_paused:
                self._trigger_auto_pause(False)

        # Emit warning on high utilization
        if orders_utilization >= 0.7:  # 70% warning threshold
            self.utilization_warning.emit("orders", orders_utilization)

    def _trigger_auto_pause(self, paused: bool):
        """Trigger or release auto-pause"""
        if paused != self.is_auto_paused:
            self.is_auto_paused = paused

            if paused:
                self.stats["auto_pause_events"] += 1
                logger.warning("AUTO-PAUSE TRIGGERED: High order utilization sustained")
            else:
                logger.info("AUTO-PAUSE RELEASED: Order utilization normalized")

            self.auto_pause_triggered.emit(paused)

    def get_status(self) -> Dict[str, any]:
        """Get current throttler status"""
        orders_available = self.orders_bucket.get_available_tokens()
        queries_available = self.queries_bucket.get_available_tokens()

        return {
            "orders": {
                "available": orders_available,
                "capacity": self.orders_bucket.capacity,
                "utilization": self.orders_bucket.get_utilization(),
                "reserved": self.reserved_order_tokens + self.currently_reserved
            },
            "queries": {
                "available": queries_available,
                "capacity": self.queries_bucket.capacity,
                "utilization": self.queries_bucket.get_utilization()
            },
            "auto_pause": {
                "enabled": self.auto_pause_enabled,
                "active": self.is_auto_paused,
                "threshold": self.auto_pause_threshold
            },
            "stats": self.stats.copy()
        }

    def reset_statistics(self):
        """Reset throttling statistics"""
        self.stats = {
            "orders_granted": 0,
            "orders_denied": 0,
            "queries_granted": 0,
            "queries_denied": 0,
            "auto_pause_events": 0,
            "peak_utilization": 0.0
        }
        logger.info("Throttler statistics reset")

    def force_auto_pause(self, paused: bool):
        """Manually force auto-pause state (for testing/emergency)"""
        self._trigger_auto_pause(paused)
        if paused:
            logger.warning("AUTO-PAUSE MANUALLY FORCED")
        else:
            logger.info("AUTO-PAUSE MANUALLY RELEASED")

    def get_effective_order_capacity(self) -> int:
        """Get effective order capacity after reservations"""
        return max(0, self.orders_bucket.capacity - self.reserved_order_tokens - self.currently_reserved)

    def can_start_new_pair(self) -> bool:
        """
        Check if we can start a new trading pair.

        Returns:
            True if sufficient tokens available for new pair
        """
        if self.is_auto_paused:
            return False

        # Need at least min_tokens_free_to_start_new_pair available
        min_required = self.throttling_config.min_tokens_free_to_start_new_pair
        available = self.orders_bucket.get_available_tokens()

        return available >= min_required
