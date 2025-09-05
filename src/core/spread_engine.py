# -*- coding: utf-8 -*-
"""
spread_engine.py
----------------
Spread Engine Module

Core engine for computing cross-venue arbitrage opportunities.
Implements micro-batch processing with cooldown logic as per master plan.

Key Features:
- Micro-batch processing (every ~10ms)
- Two-candidate edge calculation (KRX<->NXT)
- Fee-aware edge thresholds
- Per-symbol cooldown to prevent thrashing
"""

import logging
import time
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

from src.core.market_data import MarketDataManager, QuoteSnapshot
from src.core.session_state import SessionStateManager

logger = logging.getLogger(__name__)


@dataclass
class ArbitrageSignal:
    """Arbitrage signal containing trade opportunity details"""
    symbol: str

    # Direction details
    buy_venue: str  # "KRX" or "NXT"
    sell_venue: str  # "KRX" or "NXT"
    buy_price: int  # Price to buy (ask)
    sell_price: int  # Price to sell (bid)

    # Quantity and edge
    max_qty: int  # Max quantity based on visible size
    edge_krw: float  # Edge in KRW
    edge_bps: float  # Edge in basis points

    # Fees
    total_fees_krw: float
    net_edge_krw: float

    # Timing
    timestamp: float

    def __str__(self):
        return (f"Signal({self.symbol}: {self.buy_venue}@{self.buy_price} -> "
                f"{self.sell_venue}@{self.sell_price}, edge={self.edge_bps:.1f}bps)")


class SpreadEngine(QObject):
    """
    Spread calculation engine with micro-batch processing.

    Signals:
        signal_generated: Emitted when arbitrage opportunity found
        batch_processed: Emitted after each micro-batch with stats
    """

    # Signals
    signal_generated = pyqtSignal(object)  # ArbitrageSignal
    batch_processed = pyqtSignal(dict)  # Batch statistics

    def __init__(self, market_data: MarketDataManager, session_state: SessionStateManager, config):
        super().__init__()

        self.market_data = market_data
        self.session_state = session_state
        self.config = config
        self.spread_config = config.spread_engine

        # Micro-batch timer
        self.batch_timer = QTimer()
        self.batch_timer.timeout.connect(self._process_batch)
        self.batch_interval_ms = self.spread_config.batch_interval_ms

        # Cooldown tracking
        self.symbol_cooldowns: Dict[str, float] = {}  # symbol -> cooldown_until_timestamp
        self.cooldown_duration = self.spread_config.cooldown_ms / 1000.0  # Convert to seconds

        # Fee calculation
        self.krx_fees_bps = config.fees.krx["broker_bps"]
        self.nxt_fees_bps = config.fees.nxt["broker_bps"] + config.fees.nxt.get("regulatory_bps", 0)

        # Edge threshold
        self.min_edge_ticks = self.spread_config.min_net_ticks_after_fees
        self.min_visible_qty = self.spread_config.also_require_min_visible_qty

        # Statistics
        self.batch_count = 0
        self.signals_generated = 0
        self.symbols_in_cooldown = 0

        logger.info(f"SpreadEngine initialized: batch_interval={self.batch_interval_ms}ms, "
                    f"cooldown={self.cooldown_duration}s, min_edge={self.min_edge_ticks} ticks")

    def start(self):
        """Start the micro-batch processing timer"""
        if not self.batch_timer.isActive():
            self.batch_timer.start(self.batch_interval_ms)
            logger.info("SpreadEngine started")

    def stop(self):
        """Stop the micro-batch processing timer"""
        if self.batch_timer.isActive():
            self.batch_timer.stop()
            logger.info("SpreadEngine stopped")

    def _process_batch(self):
        """Process one micro-batch of dirty symbols"""
        batch_start_time = time.time()

        # Only process if trading is allowed
        if not self.session_state.can_trade():
            return

        # Get dirty symbols from market data
        dirty_symbols = self.market_data.get_dirty_symbols()

        # Filter out symbols in cooldown
        eligible_symbols = self._filter_cooldown_symbols(dirty_symbols)

        # Process each eligible symbol
        signals_this_batch = 0
        symbols_processed = 0

        for symbol in eligible_symbols:
            quote = self.market_data.get_quote(symbol)
            if quote and self._is_quote_valid(quote):
                signal = self._calculate_edge(symbol, quote)
                if signal:
                    self.signal_generated.emit(signal)
                    signals_this_batch += 1
                    self.signals_generated += 1
                    logger.debug(f"Generated signal: {signal}")
                else:
                    # No signal generated - enter cooldown
                    self._enter_cooldown(symbol)

                symbols_processed += 1

        # Update statistics
        batch_duration_ms = (time.time() - batch_start_time) * 1000
        self.batch_count += 1
        self.symbols_in_cooldown = len([s for s, t in self.symbol_cooldowns.items()
                                        if t > time.time()])

        # Emit batch statistics
        stats = {
            "batch_number": self.batch_count,
            "dirty_symbols": len(dirty_symbols),
            "eligible_symbols": len(eligible_symbols),
            "symbols_processed": symbols_processed,
            "signals_generated": signals_this_batch,
            "symbols_in_cooldown": self.symbols_in_cooldown,
            "batch_duration_ms": batch_duration_ms,
            "total_signals": self.signals_generated
        }

        self.batch_processed.emit(stats)

        if batch_duration_ms > self.batch_interval_ms * 0.8:
            logger.warning(f"Batch processing slow: {batch_duration_ms:.1f}ms "
                           f"(target: {self.batch_interval_ms}ms)")

    def _filter_cooldown_symbols(self, symbols: Set[str]) -> List[str]:
        """Filter out symbols that are still in cooldown"""
        current_time = time.time()
        eligible = []

        for symbol in symbols:
            cooldown_until = self.symbol_cooldowns.get(symbol, 0)
            if current_time >= cooldown_until:
                eligible.append(symbol)
                # Clean up expired cooldown
                if symbol in self.symbol_cooldowns:
                    del self.symbol_cooldowns[symbol]

        return eligible

    def _is_quote_valid(self, quote: QuoteSnapshot) -> bool:
        """Check if quote has valid data for edge calculation"""
        # Check that we have valid bid/ask from both venues
        krx_valid = quote.krx_bid > 0 and quote.krx_ask > 0 and quote.krx_ask > quote.krx_bid
        nxt_valid = quote.nxt_bid > 0 and quote.nxt_ask > 0 and quote.nxt_ask > quote.nxt_bid

        # Check minimum visible quantity
        krx_size_ok = min(quote.krx_bid_size, quote.krx_ask_size) >= self.min_visible_qty
        nxt_size_ok = min(quote.nxt_bid_size, quote.nxt_ask_size) >= self.min_visible_qty

        return krx_valid and nxt_valid and krx_size_ok and nxt_size_ok

    def _calculate_edge(self, symbol: str, quote: QuoteSnapshot) -> Optional[ArbitrageSignal]:
        """
        Calculate arbitrage edge for a symbol.

        Returns:
            ArbitrageSignal if edge meets threshold, None otherwise
        """
        try:
            # Calculate two possible directions

            # Direction 1: Buy KRX, Sell NXT
            krx_nxt_edge = self._calculate_direction_edge(
                symbol=symbol,
                buy_venue="KRX", buy_price=quote.krx_ask, buy_size=quote.krx_ask_size,
                sell_venue="NXT", sell_price=quote.nxt_bid, sell_size=quote.nxt_bid_size
            )

            # Direction 2: Buy NXT, Sell KRX
            nxt_krx_edge = self._calculate_direction_edge(
                symbol=symbol,
                buy_venue="NXT", buy_price=quote.nxt_ask, buy_size=quote.nxt_ask_size,
                sell_venue="KRX", sell_price=quote.krx_bid, sell_size=quote.krx_bid_size
            )

            # Choose the better direction
            best_signal = None
            if krx_nxt_edge and nxt_krx_edge:
                best_signal = krx_nxt_edge if krx_nxt_edge.net_edge_krw > nxt_krx_edge.net_edge_krw else nxt_krx_edge
            elif krx_nxt_edge:
                best_signal = krx_nxt_edge
            elif nxt_krx_edge:
                best_signal = nxt_krx_edge

            # Check if edge meets minimum threshold
            if best_signal and self._meets_edge_threshold(symbol, best_signal):
                return best_signal

            return None

        except Exception as e:
            logger.error(f"Error calculating edge for {symbol}: {e}")
            return None

    def _calculate_direction_edge(self, symbol: str, buy_venue: str, buy_price: int, buy_size: int,
                                  sell_venue: str, sell_price: int, sell_size: int) -> Optional[ArbitrageSignal]:
        """Calculate edge for a specific direction"""

        # Check for crossed book (sell price must be higher than buy price)
        if sell_price <= buy_price:
            return None

        # Calculate gross edge
        gross_edge_krw = sell_price - buy_price

        # Calculate fees
        buy_fees_krw = (buy_price * self._get_venue_fees_bps(buy_venue)) / 10000
        sell_fees_krw = (sell_price * self._get_venue_fees_bps(sell_venue)) / 10000
        total_fees_krw = buy_fees_krw + sell_fees_krw

        # Net edge
        net_edge_krw = gross_edge_krw - total_fees_krw

        # Edge in basis points
        edge_bps = (net_edge_krw / buy_price) * 10000

        # Max quantity (limited by smaller side)
        max_qty = min(buy_size, sell_size, 10)  # Cap at 10 for pilot

        return ArbitrageSignal(
            symbol=symbol,
            buy_venue=buy_venue,
            sell_venue=sell_venue,
            buy_price=buy_price,
            sell_price=sell_price,
            max_qty=max_qty,
            edge_krw=gross_edge_krw,
            edge_bps=edge_bps,
            total_fees_krw=total_fees_krw,
            net_edge_krw=net_edge_krw,
            timestamp=time.time()
        )

    def _get_venue_fees_bps(self, venue: str) -> float:
        """Get fees in basis points for a venue"""
        if venue == "KRX":
            return self.krx_fees_bps
        elif venue == "NXT":
            return self.nxt_fees_bps
        else:
            logger.warning(f"Unknown venue: {venue}")
            return 0.0

    def _meets_edge_threshold(self, symbol: str, signal: ArbitrageSignal) -> bool:
        """Check if signal meets minimum edge threshold"""

        # Get tick size for symbol (simplified - assume 1 KRW tick for now)
        tick_size = self._get_tick_size(symbol)
        min_edge_krw = tick_size * self.min_edge_ticks

        meets_threshold = signal.net_edge_krw >= min_edge_krw

        if not meets_threshold:
            logger.debug(f"Edge below threshold: {symbol} net_edge={signal.net_edge_krw:.1f} "
                         f"min_required={min_edge_krw:.1f}")

        return meets_threshold

    def _get_tick_size(self, symbol: str) -> int:
        """
        Get tick size for symbol.

        TODO: Implement proper tick size table lookup
        For now, simplified logic based on price ranges
        """
        quote = self.market_data.get_quote(symbol)
        if not quote:
            return 1

        # Use average price for tick size determination
        avg_price = (quote.krx_ask + quote.krx_bid) / 2 if quote.krx_ask > 0 and quote.krx_bid > 0 else 50000

        # Simplified tick size logic (Korean market)
        if avg_price < 2000:
            return 1
        elif avg_price < 5000:
            return 5
        elif avg_price < 20000:
            return 10
        elif avg_price < 50000:
            return 50
        elif avg_price < 200000:
            return 100
        elif avg_price < 500000:
            return 500
        else:
            return 1000

    def _enter_cooldown(self, symbol: str):
        """Enter cooldown for a symbol"""
        cooldown_until = time.time() + self.cooldown_duration
        self.symbol_cooldowns[symbol] = cooldown_until

        logger.debug(f"Symbol {symbol} entered cooldown for {self.cooldown_duration}s")

    def get_statistics(self) -> Dict[str, any]:
        """Get engine statistics for monitoring"""
        return {
            "is_running": self.batch_timer.isActive(),
            "batch_interval_ms": self.batch_interval_ms,
            "total_batches": self.batch_count,
            "total_signals": self.signals_generated,
            "symbols_in_cooldown": len([s for s, t in self.symbol_cooldowns.items()
                                        if t > time.time()]),
            "cooldown_duration_ms": self.cooldown_duration * 1000,
            "min_edge_ticks": self.min_edge_ticks,
            "signals_per_batch": self.signals_generated / max(self.batch_count, 1)
        }

    def clear_cooldowns(self):
        """Clear all symbol cooldowns (for testing or manual override)"""
        cleared_count = len(self.symbol_cooldowns)
        self.symbol_cooldowns.clear()
        logger.info(f"Cleared {cleared_count} symbol cooldowns")

    def set_batch_interval(self, interval_ms: int):
        """Change micro-batch interval"""
        if interval_ms != self.batch_interval_ms:
            self.batch_interval_ms = interval_ms
            if self.batch_timer.isActive():
                self.batch_timer.stop()
                self.batch_timer.start(interval_ms)
            logger.info(f"Batch interval changed to {interval_ms}ms")
            