# -*- coding: utf-8 -*-
"""
pair_manager.py
---------------
Pair Manager - Manages Paired Trades and Hedging

Implements the state machine for paired arbitrage trades according to the master plan.
Handles the complete lifecycle from signal → entry → hedge → flat.

Key Features:
- Pair state machine (CANDIDATE → ENTRY → HEDGE → FLAT)
- t_hedge timeout and escalation (limit → IOC → market)
- Inventory tracking to ensure flat positions
- Risk limits and concurrency control
"""

import logging
import time
import uuid
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

from src.core.spread_engine import ArbitrageSignal
from src.core.router import Router, RoutingDecision, OrderIntent
from src.core.throttler import Throttler
from src.kiwoom.execution_gateway import ExecutionGateway, ExecutionEvent
from src.core.session_state import SessionStateManager

logger = logging.getLogger(__name__)
exec_logger = logging.getLogger('execution')


class PairState(Enum):
    """Pair trade states"""
    CANDIDATE = "CANDIDATE"  # Signal received, checking admission
    ENTRY_TAKE_SENT = "ENTRY_TAKE_SENT"  # Take leg sent
    ENTRY_TAKE_FILLED = "ENTRY_TAKE_FILLED"  # Take leg filled, hedge leg sent
    HEDGE_POST_PENDING = "HEDGE_POST_PENDING"  # Waiting for hedge fill
    CANCEL_POST_SENT = "CANCEL_POST_SENT"  # Cancelling hedge leg
    HEDGE_IOC_SENT = "HEDGE_IOC_SENT"  # Escalated to IOC/market
    PAIRED_DONE = "PAIRED_DONE"  # Both legs filled, flat
    FAILED = "FAILED"  # Failed/rejected
    COOLDOWN = "COOLDOWN"  # In cooldown period


@dataclass
class PairTrade:
    """Complete pair trade record"""
    pair_id: str
    symbol: str

    # Original signal
    signal: ArbitrageSignal
    routing_decision: RoutingDecision

    # State tracking
    state: PairState = PairState.CANDIDATE
    start_time: float = field(default_factory=time.time)

    # Order tracking
    take_order_id: str = ""
    hedge_order_id: str = ""
    cancel_order_id: str = ""
    escalation_order_id: str = ""

    # Execution tracking
    take_filled_qty: int = 0
    take_fill_price: float = 0.0
    hedge_filled_qty: int = 0
    hedge_fill_price: float = 0.0

    # Timing
    take_fill_time: float = 0.0
    hedge_fill_time: float = 0.0
    unhedged_start_time: float = 0.0

    # Timers
    hedge_timeout_timer: Optional[QTimer] = None
    cooldown_timer: Optional[QTimer] = None

    # Results
    realized_edge_krw: float = 0.0
    is_profitable: bool = False


class PairManager(QObject):
    """
    Pair Manager for complete arbitrage trade lifecycle.

    Signals:
        pair_state_changed: Emitted when pair state changes
        pair_completed: Emitted when pair trade completes
        unhedged_timeout: Emitted when hedge timeout occurs
        inventory_update: Emitted when position changes
    """

    # Signals
    pair_state_changed = pyqtSignal(str, str)  # pair_id, new_state
    pair_completed = pyqtSignal(object)  # PairTrade
    unhedged_timeout = pyqtSignal(str)  # pair_id
    inventory_update = pyqtSignal(dict)  # inventory by symbol

    def __init__(self, router: Router, throttler: Throttler,
                 execution_gateway: ExecutionGateway, session_state: SessionStateManager, config):
        super().__init__()

        self.router = router
        self.throttler = throttler
        self.execution = execution_gateway
        self.session_state = session_state
        self.config = config

        # Configuration
        self.execution_config = config.execution
        self.t_hedge_ms = self.execution_config.t_hedge_ms
        self.max_concurrent_symbols = self.execution_config.max_concurrent_symbols
        self.max_pairs_per_symbol = self.execution_config.max_outstanding_pairs_per_symbol

        # Active trades
        self.active_pairs: Dict[str, PairTrade] = {}  # pair_id -> PairTrade
        self.symbol_pairs: Dict[str, List[str]] = {}  # symbol -> [pair_ids]
        self.order_to_pair: Dict[str, str] = {}  # order_id -> pair_id

        # Inventory tracking
        self.inventory: Dict[str, int] = {}  # symbol -> net position

        # Statistics
        self.stats = {
            "total_pairs": 0,
            "completed_pairs": 0,
            "failed_pairs": 0,
            "profitable_pairs": 0,
            "total_profit_krw": 0.0,
            "avg_unhedged_time_ms": 0.0
        }

        # Connect to execution events
        self.execution.execution_event.connect(self._on_execution_event)

        logger.info(f"PairManager initialized: t_hedge={self.t_hedge_ms}ms, "
                    f"max_concurrent={self.max_concurrent_symbols}, "
                    f"max_per_symbol={self.max_pairs_per_symbol}")

    def handle_signal(self, arbitrage_signal: ArbitrageSignal) -> bool:
        """
        Process arbitrage signal and potentially start new pair trade.

        Args:
            arbitrage_signal: Signal from SpreadEngine

        Returns:
            True if signal was accepted and pair started
        """
        try:
            # Check admission criteria
            if not self._check_admission(arbitrage_signal):
                return False

            # Route the signal
            routing_decision = self.router.route_signal(arbitrage_signal)

            # Create pair trade
            pair_trade = PairTrade(
                pair_id=routing_decision.pair_id,
                symbol=arbitrage_signal.symbol,
                signal=arbitrage_signal,
                routing_decision=routing_decision
            )

            # Add to tracking
            self.active_pairs[pair_trade.pair_id] = pair_trade

            if pair_trade.symbol not in self.symbol_pairs:
                self.symbol_pairs[pair_trade.symbol] = []
            self.symbol_pairs[pair_trade.symbol].append(pair_trade.pair_id)

            # Start the pair trade
            self._start_pair_trade(pair_trade)

            self.stats["total_pairs"] += 1

            logger.info(f"Started pair trade: {pair_trade.pair_id} - {arbitrage_signal}")

            return True

        except Exception as e:
            logger.error(f"Failed to handle signal for {arbitrage_signal.symbol}: {e}")
            return False

    def _check_admission(self, signal: ArbitrageSignal) -> bool:
        """Check if signal meets admission criteria"""

        # Check session state
        if not self.session_state.can_open_new_positions():
            logger.debug(f"Signal rejected - session not allowing new positions: {signal.symbol}")
            return False

        # Check concurrent symbols limit
        active_symbols = len(self.symbol_pairs)
        if active_symbols >= self.max_concurrent_symbols:
            logger.debug(f"Signal rejected - max concurrent symbols ({self.max_concurrent_symbols}): {signal.symbol}")
            return False

        # Check per-symbol limit
        symbol_pair_count = len(self.symbol_pairs.get(signal.symbol, []))
        if symbol_pair_count >= self.max_pairs_per_symbol:
            logger.debug(f"Signal rejected - max pairs per symbol ({self.max_pairs_per_symbol}): {signal.symbol}")
            return False

        # Check throttler capacity
        if not self.throttler.can_start_new_pair():
            logger.debug(f"Signal rejected - throttler capacity: {signal.symbol}")
            return False

        return True

    def _start_pair_trade(self, pair_trade: PairTrade):
        """Start pair trade by sending take leg"""
        try:
            # Send take leg (immediate execution on rich side)
            take_intent = pair_trade.routing_decision.take_leg
            take_order_id = self.execution.send_order_intent(take_intent)

            if take_order_id:
                pair_trade.take_order_id = take_order_id
                pair_trade.state = PairState.ENTRY_TAKE_SENT

                # Map order to pair
                self.order_to_pair[take_order_id] = pair_trade.pair_id

                self._emit_state_change(pair_trade)

                exec_logger.info(f"PAIR_TAKE_SENT,{pair_trade.pair_id},{take_order_id},"
                                 f"{take_intent.symbol},{take_intent.venue.value}")
            else:
                # Failed to send take leg
                self._fail_pair(pair_trade, "failed_to_send_take_leg")

        except Exception as e:
            logger.error(f"Failed to start pair trade {pair_trade.pair_id}: {e}")
            self._fail_pair(pair_trade, f"exception: {e}")

    def _on_execution_event(self, event: ExecutionEvent):
        """Handle execution events from ExecutionGateway"""
        try:
            # Find the pair for this order
            pair_id = self.order_to_pair.get(event.order_id)
            if not pair_id:
                return  # Not our order

            pair_trade = self.active_pairs.get(pair_id)
            if not pair_trade:
                logger.warning(f"Execution event for unknown pair: {pair_id}")
                return

            # Route to appropriate handler
            if event.event_type == "ORDER_REJECTED":
                self._handle_order_rejected(pair_trade, event)
            elif event.event_type == "TRADE_FILL":
                self._handle_trade_fill(pair_trade, event)
            elif event.event_type == "ORDER_CANCELLED":
                self._handle_order_cancelled(pair_trade, event)
            elif event.event_type == "ORDER_TIMEOUT":
                self._handle_order_timeout(pair_trade, event)

        except Exception as e:
            logger.error(f"Error handling execution event: {e}")

    def _handle_trade_fill(self, pair_trade: PairTrade, event: ExecutionEvent):
        """Handle trade fill event"""

        if event.order_id == pair_trade.take_order_id:
            # Take leg filled
            pair_trade.take_filled_qty = event.data["filled_qty"]
            pair_trade.take_fill_price = event.data["avg_fill_price"]
            pair_trade.take_fill_time = event.timestamp
            pair_trade.unhedged_start_time = event.timestamp

            if pair_trade.state == PairState.ENTRY_TAKE_SENT:
                # Send hedge leg
                self._send_hedge_leg(pair_trade)

            exec_logger.info(f"PAIR_TAKE_FILLED,{pair_trade.pair_id},{event.order_id},"
                             f"{pair_trade.take_fill_price},{pair_trade.take_filled_qty}")

        elif event.order_id == pair_trade.hedge_order_id or event.order_id == pair_trade.escalation_order_id:
            # Hedge leg filled
            pair_trade.hedge_filled_qty = event.data["filled_qty"]
            pair_trade.hedge_fill_price = event.data["avg_fill_price"]
            pair_trade.hedge_fill_time = event.timestamp

            # Both legs filled - pair complete
            self._complete_pair(pair_trade)

            exec_logger.info(f"PAIR_HEDGE_FILLED,{pair_trade.pair_id},{event.order_id},"
                             f"{pair_trade.hedge_fill_price},{pair_trade.hedge_filled_qty}")

    def _handle_order_rejected(self, pair_trade: PairTrade, event: ExecutionEvent):
        """Handle order rejection"""
        reason = event.data.get("reason", "unknown")

        if event.order_id == pair_trade.take_order_id:
            logger.warning(f"Take leg rejected: {pair_trade.pair_id} - {reason}")
            self._fail_pair(pair_trade, f"take_rejected: {reason}")
        else:
            logger.warning(f"Hedge leg rejected: {pair_trade.pair_id} - {reason}")
            # If hedge fails but take filled, we have unhedged exposure
            if pair_trade.take_filled_qty > 0:
                self._escalate_hedge(pair_trade, "hedge_rejected")
            else:
                self._fail_pair(pair_trade, f"hedge_rejected: {reason}")

    def _handle_order_cancelled(self, pair_trade: PairTrade, event: ExecutionEvent):
        """Handle order cancellation"""
        if event.order_id == pair_trade.hedge_order_id:
            # Hedge order cancelled - escalate to IOC/market
            self._escalate_hedge(pair_trade, "hedge_cancelled")

    def _handle_order_timeout(self, pair_trade: PairTrade, event: ExecutionEvent):
        """Handle order timeout"""
        timeout_type = event.data.get("timeout_type", "unknown")

        if event.order_id == pair_trade.take_order_id:
            logger.warning(f"Take leg timeout: {pair_trade.pair_id} - {timeout_type}")
            self._fail_pair(pair_trade, f"take_timeout: {timeout_type}")
        else:
            logger.warning(f"Hedge leg timeout: {pair_trade.pair_id} - {timeout_type}")
            if pair_trade.take_filled_qty > 0:
                self._escalate_hedge(pair_trade, f"hedge_timeout: {timeout_type}")

    def _send_hedge_leg(self, pair_trade: PairTrade):
        """Send hedge leg after take leg fills"""
        try:
            hedge_intent = pair_trade.routing_decision.hedge_leg
            hedge_intent.quantity = pair_trade.take_filled_qty  # Match filled quantity

            hedge_order_id = self.execution.send_order_intent(hedge_intent)

            if hedge_order_id:
                pair_trade.hedge_order_id = hedge_order_id
                pair_trade.state = PairState.HEDGE_POST_PENDING

                # Map order to pair
                self.order_to_pair[hedge_order_id] = pair_trade.pair_id

                # Start hedge timeout timer
                self._start_hedge_timeout(pair_trade)

                self._emit_state_change(pair_trade)

                exec_logger.info(f"PAIR_HEDGE_SENT,{pair_trade.pair_id},{hedge_order_id},"
                                 f"{hedge_intent.symbol},{hedge_intent.venue.value}")
            else:
                # Failed to send hedge
                self._escalate_hedge(pair_trade, "failed_to_send_hedge")

        except Exception as e:
            logger.error(f"Failed to send hedge leg {pair_trade.pair_id}: {e}")
            self._escalate_hedge(pair_trade, f"hedge_exception: {e}")

    def _start_hedge_timeout(self, pair_trade: PairTrade):
        """Start hedge timeout timer"""
        if pair_trade.hedge_timeout_timer:
            pair_trade.hedge_timeout_timer.stop()

        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self._handle_hedge_timeout(pair_trade.pair_id))
        timer.start(self.t_hedge_ms)

        pair_trade.hedge_timeout_timer = timer

    def _handle_hedge_timeout(self, pair_id: str):
        """Handle hedge timeout"""
        pair_trade = self.active_pairs.get(pair_id)
        if not pair_trade or pair_trade.state != PairState.HEDGE_POST_PENDING:
            return

        logger.warning(f"Hedge timeout: {pair_id} after {self.t_hedge_ms}ms")

        # Cancel hedge order and escalate
        if pair_trade.hedge_order_id:
            cancel_success = self.execution.cancel_order(pair_trade.hedge_order_id)
            if cancel_success:
                pair_trade.state = PairState.CANCEL_POST_SENT
                self._emit_state_change(pair_trade)

        # Emit timeout event
        self.unhedged_timeout.emit(pair_id)

        exec_logger.warning(f"HEDGE_TIMEOUT,{pair_id},{self.t_hedge_ms}")

    def _escalate_hedge(self, pair_trade: PairTrade, reason: str):
        """Escalate hedge to IOC/market for immediate execution"""
        try:
            logger.info(f"Escalating hedge: {pair_trade.pair_id} - {reason}")

            # Create escalation intent (more aggressive order type)
            original_hedge = pair_trade.routing_decision.hedge_leg
            escalation_intent = self.router.create_escalation_intent(original_hedge)
            escalation_intent.quantity = pair_trade.take_filled_qty  # Match filled quantity

            escalation_order_id = self.execution.send_order_intent(escalation_intent)

            if escalation_order_id:
                pair_trade.escalation_order_id = escalation_order_id
                pair_trade.state = PairState.HEDGE_IOC_SENT

                # Map order to pair
                self.order_to_pair[escalation_order_id] = pair_trade.pair_id

                self._emit_state_change(pair_trade)

                exec_logger.info(f"PAIR_ESCALATED,{pair_trade.pair_id},{escalation_order_id},{reason}")
            else:
                # Failed to escalate - this is serious
                logger.error(f"Failed to escalate hedge: {pair_trade.pair_id}")
                self._fail_pair(pair_trade, f"escalation_failed: {reason}")

        except Exception as e:
            logger.error(f"Exception escalating hedge {pair_trade.pair_id}: {e}")
            self._fail_pair(pair_trade, f"escalation_exception: {e}")

    def _complete_pair(self, pair_trade: PairTrade):
        """Complete pair trade when both legs are filled"""
        try:
            # Stop timers
            if pair_trade.hedge_timeout_timer:
                pair_trade.hedge_timeout_timer.stop()

            # Calculate results
            self._calculate_pair_results(pair_trade)

            # Update inventory
            self._update_inventory(pair_trade)

            # Update state
            pair_trade.state = PairState.PAIRED_DONE
            self._emit_state_change(pair_trade)

            # Update statistics
            self.stats["completed_pairs"] += 1
            if pair_trade.is_profitable:
                self.stats["profitable_pairs"] += 1
            self.stats["total_profit_krw"] += pair_trade.realized_edge_krw

            # Calculate unhedged time
            if pair_trade.hedge_fill_time > pair_trade.unhedged_start_time:
                unhedged_time_ms = (pair_trade.hedge_fill_time - pair_trade.unhedged_start_time) * 1000
                current_avg = self.stats["avg_unhedged_time_ms"]
                completed = self.stats["completed_pairs"]
                self.stats["avg_unhedged_time_ms"] = (current_avg * (completed - 1) + unhedged_time_ms) / completed

            # Emit completion
            self.pair_completed.emit(pair_trade)

            exec_logger.info(f"PAIR_DONE,{pair_trade.pair_id},"
                             f"{pair_trade.realized_edge_krw:.2f},"
                             f"{pair_trade.is_profitable}")

            # Clean up after short delay
            self._schedule_cleanup(pair_trade.pair_id, 5000)  # 5 second delay

        except Exception as e:
            logger.error(f"Error completing pair {pair_trade.pair_id}: {e}")

    def _calculate_pair_results(self, pair_trade: PairTrade):
        """Calculate realized P&L for completed pair"""
        if pair_trade.take_filled_qty != pair_trade.hedge_filled_qty:
            logger.warning(f"Quantity mismatch in pair {pair_trade.pair_id}: "
                           f"take={pair_trade.take_filled_qty}, hedge={pair_trade.hedge_filled_qty}")

        # Use smaller quantity for calculation
        qty = min(pair_trade.take_filled_qty, pair_trade.hedge_filled_qty)

        # Calculate gross P&L (sell_price - buy_price) * qty
        take_intent = pair_trade.routing_decision.take_leg
        hedge_intent = pair_trade.routing_decision.hedge_leg

        if take_intent.side.name == "SELL":
            # Take leg was sell, hedge leg was buy
            gross_pnl = (pair_trade.take_fill_price - pair_trade.hedge_fill_price) * qty
        else:
            # Take leg was buy, hedge leg was sell
            gross_pnl = (pair_trade.hedge_fill_price - pair_trade.take_fill_price) * qty

        # Subtract fees (simplified - should use actual venue fees)
        estimated_fees = pair_trade.signal.total_fees_krw * qty
        net_pnl = gross_pnl - estimated_fees

        pair_trade.realized_edge_krw = net_pnl
        pair_trade.is_profitable = net_pnl > 0

        logger.info(f"Pair results: {pair_trade.pair_id} - "
                    f"gross={gross_pnl:.2f}, fees={estimated_fees:.2f}, net={net_pnl:.2f}")

    def _update_inventory(self, pair_trade: PairTrade):
        """Update inventory tracking"""
        symbol = pair_trade.symbol

        # Since this is arbitrage, net position should be zero
        # But track intermediate states for monitoring
        if symbol not in self.inventory:
            self.inventory[symbol] = 0

        # For arbitrage pairs, net effect should be zero
        # But we can track the gross position temporarily

        self.inventory_update.emit(self.inventory.copy())

    def _fail_pair(self, pair_trade: PairTrade, reason: str):
        """Mark pair as failed"""
        logger.warning(f"Pair failed: {pair_trade.pair_id} - {reason}")

        # Stop timers
        if pair_trade.hedge_timeout_timer:
            pair_trade.hedge_timeout_timer.stop()

        pair_trade.state = PairState.FAILED
        self._emit_state_change(pair_trade)

        self.stats["failed_pairs"] += 1

        exec_logger.warning(f"PAIR_FAILED,{pair_trade.pair_id},{reason}")

        # Clean up
        self._schedule_cleanup(pair_trade.pair_id, 1000)  # 1 second delay

    def _emit_state_change(self, pair_trade: PairTrade):
        """Emit pair state change signal"""
        self.pair_state_changed.emit(pair_trade.pair_id, pair_trade.state.value)

    def _schedule_cleanup(self, pair_id: str, delay_ms: int):
        """Schedule pair cleanup after delay"""
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self._cleanup_pair(pair_id))
        timer.start(delay_ms)

    def _cleanup_pair(self, pair_id: str):
        """Clean up completed/failed pair"""
        pair_trade = self.active_pairs.get(pair_id)
        if not pair_trade:
            return

        # Remove from tracking
        symbol = pair_trade.symbol

        if symbol in self.symbol_pairs:
            if pair_id in self.symbol_pairs[symbol]:
                self.symbol_pairs[symbol].remove(pair_id)
            if not self.symbol_pairs[symbol]:
                del self.symbol_pairs[symbol]

        # Remove order mappings
        for order_id in [pair_trade.take_order_id, pair_trade.hedge_order_id,
                         pair_trade.cancel_order_id, pair_trade.escalation_order_id]:
            if order_id and order_id in self.order_to_pair:
                del self.order_to_pair[order_id]

        # Remove from active pairs
        del self.active_pairs[pair_id]

        logger.debug(f"Cleaned up pair: {pair_id}")

    def get_active_pairs(self) -> List[dict]:
        """Get list of active pairs for monitoring"""
        active = []
        for pair_id, pair_trade in self.active_pairs.items():
            unhedged_time_ms = 0
            if pair_trade.unhedged_start_time > 0:
                if pair_trade.hedge_fill_time > 0:
                    unhedged_time_ms = (pair_trade.hedge_fill_time - pair_trade.unhedged_start_time) * 1000
                else:
                    unhedged_time_ms = (time.time() - pair_trade.unhedged_start_time) * 1000

            active.append({
                "pair_id": pair_id,
                "symbol": pair_trade.symbol,
                "state": pair_trade.state.value,
                "expected_edge_krw": pair_trade.signal.net_edge_krw,
                "take_filled_qty": pair_trade.take_filled_qty,
                "hedge_filled_qty": pair_trade.hedge_filled_qty,
                "unhedged_time_ms": unhedged_time_ms,
                "age_seconds": time.time() - pair_trade.start_time
            })
        return active

    def get_statistics(self) -> dict:
        """Get pair manager statistics"""
        active_count = len(self.active_pairs)
        active_symbols = len(self.symbol_pairs)

        return {
            "active_pairs": active_count,
            "active_symbols": active_symbols,
            "max_concurrent_symbols": self.max_concurrent_symbols,
            "max_pairs_per_symbol": self.max_pairs_per_symbol,
            **self.stats
        }

    def force_close_all_pairs(self):
        """Emergency close all active pairs"""
        logger.warning("Force closing all active pairs")

        for pair_id in list(self.active_pairs.keys()):
            pair_trade = self.active_pairs[pair_id]

            # Try to cancel any pending orders
            if pair_trade.hedge_order_id:
                self.execution.cancel_order(pair_trade.hedge_order_id)

            self._fail_pair(pair_trade, "force_close")

        exec_logger.warning("FORCE_CLOSE_ALL_PAIRS")

    def can_accept_new_signal(self, symbol: str) -> bool:
        """Check if we can accept a new signal for symbol"""
        if not self.session_state.can_open_new_positions():
            return False

        active_symbols = len(self.symbol_pairs)
        if active_symbols >= self.max_concurrent_symbols:
            return False

        symbol_pair_count = len(self.symbol_pairs.get(symbol, []))
        if symbol_pair_count >= self.max_pairs_per_symbol:
            return False

        return self.throttler.can_start_new_pair()