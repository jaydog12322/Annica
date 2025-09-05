# -*- coding: utf-8 -*-
"""
router.py
---------
Router Module - Order Style Selection and Venue Routing

Converts arbitrage signals into specific order intents with venue routing
and order type selection according to the master plan.

Key Features:
- Direct venue routing (no SOR)
- Take on rich side (IOC/Market), Post on cheap side (Limit/Mid)
- NXT mid-price order handling (hoga=29, price=0)
- Proper Kiwoom order type mapping
"""

import logging
from typing import List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from src.core.spread_engine import ArbitrageSignal

logger = logging.getLogger(__name__)


class OrderSide(Enum):
    """Order side enumeration"""
    BUY = 1
    SELL = 2


class OrderType(Enum):
    """Order type enumeration"""
    LIMIT = "00"  # 지정가
    MARKET = "03"  # 시장가
    LIMIT_IOC = "10"  # 지정가IOC
    MARKET_IOC = "13"  # 시장가IOC
    NXT_MID = "29"  # NXT 중간가


class Venue(Enum):
    """Trading venue enumeration"""
    KRX = "KRX"
    NXT = "NXT"


@dataclass
class OrderIntent:
    """Order intent containing all details needed for execution"""
    # Identification
    pair_id: str  # UUID for this pair
    leg: str  # "A" or "B"

    # Symbol and venue
    symbol: str  # Base symbol code (e.g., "005930")
    venue: Venue  # KRX or NXT

    # Order details
    side: OrderSide  # BUY or SELL
    quantity: int  # Number of shares
    price: int  # Price in KRW (0 for market/IOC/mid orders)
    order_type: OrderType  # Order type enum

    # Execution strategy
    is_take_leg: bool  # True for immediate execution (rich side)
    is_hedge_leg: bool  # True for hedge/post leg (cheap side)

    # Kiwoom-specific
    hoga_gb: str  # 호가구분 code for Kiwoom
    kiwoom_order_type: int  # Order type for SendOrder (1=buy, 2=sell, 21=NXT buy, 22=NXT sell)

    # Timing
    priority: int = 0  # 0=highest priority (take leg), 1=normal (hedge leg)

    def __str__(self):
        return (f"OrderIntent({self.leg}: {self.side.name} {self.quantity} {self.symbol} "
                f"@ {self.venue.value} {self.price} {self.order_type.value})")


@dataclass
class RoutingDecision:
    """Complete routing decision for an arbitrage signal"""
    pair_id: str
    take_leg: OrderIntent  # Immediate execution leg
    hedge_leg: OrderIntent  # Post/hedge leg
    expected_edge_krw: float

    def get_order_intents(self) -> List[OrderIntent]:
        """Get both order intents as a list"""
        return [self.take_leg, self.hedge_leg]


class Router:
    """
    Router for converting arbitrage signals to order intents.

    Implements the routing strategy from the master plan:
    - Take on rich side (IOC/Market)
    - Post on cheap side (Limit or NXT Mid)
    - Direct venue routing only (no SOR)
    """

    def __init__(self, config):
        self.config = config
        self.router_config = config.router

        # Routing preferences
        self.entry_prefer = self.router_config.entry_leg["prefer"]  # "ioc_or_market"
        self.hedge_prefer = self.router_config.hedge_leg["prefer"]  # "limit_or_mid"
        self.allow_nxt_mid = self.router_config.hedge_leg["allow_nxt_mid_price"]

        logger.info(f"Router initialized: entry={self.entry_prefer}, hedge={self.hedge_prefer}, "
                    f"nxt_mid={self.allow_nxt_mid}")

    def route_signal(self, arbitrage_signal: ArbitrageSignal) -> RoutingDecision:
        """
        Convert arbitrage signal to routing decision with order intents.

        Args:
            arbitrage_signal: Signal from spread engine

        Returns:
            RoutingDecision with take and hedge legs
        """
        try:
            # Generate unique pair ID
            pair_id = self._generate_pair_id(arbitrage_signal)

            # Determine take and hedge legs
            take_leg = self._create_take_leg(arbitrage_signal, pair_id)
            hedge_leg = self._create_hedge_leg(arbitrage_signal, pair_id)

            routing_decision = RoutingDecision(
                pair_id=pair_id,
                take_leg=take_leg,
                hedge_leg=hedge_leg,
                expected_edge_krw=arbitrage_signal.net_edge_krw
            )

            logger.info(f"Routed signal: {routing_decision.pair_id} - "
                        f"Take: {take_leg.venue.value} {take_leg.side.name}, "
                        f"Hedge: {hedge_leg.venue.value} {hedge_leg.side.name}")

            return routing_decision

        except Exception as e:
            logger.error(f"Failed to route signal for {arbitrage_signal.symbol}: {e}")
            raise

    def _generate_pair_id(self, signal: ArbitrageSignal) -> str:
        """Generate unique pair ID"""
        import uuid
        import time

        # Format: symbol_timestamp_uuid_short
        timestamp = int(time.time() * 1000) % 1000000  # Last 6 digits of milliseconds
        uuid_short = str(uuid.uuid4())[:8]

        return f"{signal.symbol}_{timestamp}_{uuid_short}"

    def _create_take_leg(self, signal: ArbitrageSignal, pair_id: str) -> OrderIntent:
        """
        Create the take leg (immediate execution on rich side).

        Rich side = the venue where we can sell at a higher price
        """
        # The rich side is where we SELL (take their bid)
        if signal.sell_venue == "KRX":
            venue = Venue.KRX
            side = OrderSide.SELL
            price = signal.sell_price
            kiwoom_order_type = 2  # KRX sell
        else:  # NXT
            venue = Venue.NXT
            side = OrderSide.SELL
            price = signal.sell_price
            kiwoom_order_type = 22  # NXT sell

        # Choose order type for immediate execution
        order_type, hoga_gb = self._get_take_order_type(venue)

        return OrderIntent(
            pair_id=pair_id,
            leg="A",  # Take leg is always A
            symbol=signal.symbol,
            venue=venue,
            side=side,
            quantity=min(signal.max_qty, 1),  # Start with 1 share for pilot
            price=0 if order_type in [OrderType.MARKET, OrderType.MARKET_IOC] else price,
            order_type=order_type,
            is_take_leg=True,
            is_hedge_leg=False,
            hoga_gb=hoga_gb,
            kiwoom_order_type=kiwoom_order_type,
            priority=0  # Highest priority
        )

    def _create_hedge_leg(self, signal: ArbitrageSignal, pair_id: str) -> OrderIntent:
        """
        Create the hedge leg (post on cheap side to earn edge).

        Cheap side = the venue where we can buy at a lower price
        """
        # The cheap side is where we BUY (hit their ask)
        if signal.buy_venue == "KRX":
            venue = Venue.KRX
            side = OrderSide.BUY
            price = signal.buy_price
            kiwoom_order_type = 1  # KRX buy
        else:  # NXT
            venue = Venue.NXT
            side = OrderSide.BUY
            price = signal.buy_price
            kiwoom_order_type = 21  # NXT buy

        # Choose order type for hedge (post)
        order_type, hoga_gb = self._get_hedge_order_type(venue, signal)

        return OrderIntent(
            pair_id=pair_id,
            leg="B",  # Hedge leg is always B
            symbol=signal.symbol,
            venue=venue,
            side=side,
            quantity=min(signal.max_qty, 1),  # Start with 1 share for pilot
            price=0 if order_type == OrderType.NXT_MID else price,
            order_type=order_type,
            is_take_leg=False,
            is_hedge_leg=True,
            hoga_gb=hoga_gb,
            kiwoom_order_type=kiwoom_order_type,
            priority=1  # Normal priority
        )

    def _get_take_order_type(self, venue: Venue) -> Tuple[OrderType, str]:
        """
        Get order type for take leg (immediate execution).

        Returns:
            (OrderType, hoga_gb_code)
        """
        if self.entry_prefer == "ioc_or_market":
            # Prefer IOC for more control, fallback to market if needed
            if venue == Venue.KRX:
                return OrderType.MARKET_IOC, "13"  # 시장가IOC
            else:  # NXT
                return OrderType.MARKET_IOC, "13"  # NXT also supports IOC
        else:
            # Fallback to market order
            return OrderType.MARKET, "03"

    def _get_hedge_order_type(self, venue: Venue, signal: ArbitrageSignal) -> Tuple[OrderType, str]:
        """
        Get order type for hedge leg (post to earn edge).

        Returns:
            (OrderType, hoga_gb_code)
        """
        if self.hedge_prefer == "limit_or_mid":
            # Use NXT mid-price if allowed and on NXT venue
            if venue == Venue.NXT and self.allow_nxt_mid:
                # Check if spread is wide enough to safely use mid-price
                if self._should_use_nxt_mid(signal):
                    return OrderType.NXT_MID, "29"

            # Default to limit order
            return OrderType.LIMIT, "00"
        else:
            # Fallback to limit
            return OrderType.LIMIT, "00"

    def _should_use_nxt_mid(self, signal: ArbitrageSignal) -> bool:
        """
        Determine if we should use NXT mid-price order.

        Only use mid-price when:
        1. Edge is safely above fees + buffer
        2. Spread is wide enough that mid won't hurt us

        Args:
            signal: Arbitrage signal

        Returns:
            True if mid-price is safe to use
        """
        # Conservative approach: only use mid if edge is very comfortable
        safety_multiplier = 1.5  # Require 1.5x the minimum edge
        min_safe_edge = signal.total_fees_krw * safety_multiplier

        return signal.net_edge_krw >= min_safe_edge

    def create_cancel_intent(self, original_intent: OrderIntent, order_number: str) -> OrderIntent:
        """
        Create cancel intent from original order intent.

        Args:
            original_intent: Original order intent to cancel
            order_number: Kiwoom order number to cancel

        Returns:
            Cancel order intent
        """
        # Map to cancel order types
        if original_intent.venue == Venue.KRX:
            kiwoom_order_type = 3  # KRX cancel
        else:  # NXT
            kiwoom_order_type = 23  # NXT cancel

        cancel_intent = OrderIntent(
            pair_id=original_intent.pair_id,
            leg=original_intent.leg,
            symbol=original_intent.symbol,
            venue=original_intent.venue,
            side=original_intent.side,
            quantity=original_intent.quantity,
            price=0,  # Not used for cancels
            order_type=OrderType.MARKET,  # Placeholder for cancel
            is_take_leg=original_intent.is_take_leg,
            is_hedge_leg=original_intent.is_hedge_leg,
            hoga_gb="00",  # Not used for cancels
            kiwoom_order_type=kiwoom_order_type,
            priority=0  # High priority for cancels
        )

        return cancel_intent

    def create_escalation_intent(self, original_intent: OrderIntent) -> OrderIntent:
        """
        Create escalated intent (limit -> IOC/Market) for hedge timeout.

        Args:
            original_intent: Original hedge intent that timed out

        Returns:
            Escalated order intent
        """
        # Create new intent with more aggressive order type
        escalated_intent = OrderIntent(
            pair_id=original_intent.pair_id,
            leg=original_intent.leg,
            symbol=original_intent.symbol,
            venue=original_intent.venue,
            side=original_intent.side,
            quantity=original_intent.quantity,
            price=0,  # Market/IOC uses price=0
            order_type=OrderType.MARKET_IOC,
            is_take_leg=original_intent.is_take_leg,
            is_hedge_leg=original_intent.is_hedge_leg,
            hoga_gb="13",  # 시장가IOC
            kiwoom_order_type=original_intent.kiwoom_order_type,
            priority=0  # High priority for escalation
        )

        logger.info(f"Created escalation intent: {escalated_intent}")

        return escalated_intent

    def get_venue_symbol_code(self, intent: OrderIntent) -> str:
        """
        Get the symbol code to use for Kiwoom API based on venue.

        Args:
            intent: Order intent

        Returns:
            Symbol code for API call
        """
        if intent.venue == Venue.KRX:
            return intent.symbol  # Base code for KRX
        else:  # NXT
            return f"{intent.symbol}_NX"  # Add _NX suffix for NXT

    def get_routing_statistics(self) -> dict:
        """Get routing statistics for monitoring"""
        return {
            "entry_preference": self.entry_prefer,
            "hedge_preference": self.hedge_prefer,
            "nxt_mid_enabled": self.allow_nxt_mid
        }