# -*- coding: utf-8 -*-
"""
market_data.py
--------------
Market Data Management Module

Handles real-time L1 data subscription and management for both KRX and NXT venues.
Implements screen sharding and maintains per-symbol snapshots according to the
master plan specifications.

Key Features:
- Screen sharding (≤100 symbols per screen)
- Per-venue L1 data (bid/ask/size)
- Dirty flagging for changed quotes
- No AL feed (per requirements)
"""

import logging
import time
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

from src.kiwoom.kiwoom_connector import KiwoomConnector

logger = logging.getLogger(__name__)


@dataclass
class QuoteSnapshot:
    """Per-symbol, per-venue quote snapshot"""
    symbol: str

    # KRX data
    krx_bid: int = 0
    krx_ask: int = 0
    krx_bid_size: int = 0
    krx_ask_size: int = 0
    krx_last_update: float = 0.0

    # NXT data
    nxt_bid: int = 0
    nxt_ask: int = 0
    nxt_bid_size: int = 0
    nxt_ask_size: int = 0
    nxt_last_update: float = 0.0

    # State flags
    is_dirty: bool = False
    both_venues_touched: bool = False


class MarketDataManager(QObject):
    """
    Market data manager for real-time L1 quote handling.

    Signals:
        quote_updated: Emitted when quote changes (symbol, venue)
        subscription_status: Emitted when subscription status changes
    """

    # Signals
    quote_updated = pyqtSignal(str, str)  # symbol, venue
    subscription_status = pyqtSignal(str, bool)  # screen_no, success

    # FID codes for real-time data
    # 41: 매도호가1, 51: 매수호가1, 61: 매도호가수량1, 71: 매수호가수량1
    KRX_QUOTE_FIDS = "41;51;61;71"
    NXT_QUOTE_FIDS = "41;51;61;71"  # Same FIDs for NXT

    def __init__(self, kiwoom_connector: KiwoomConnector, config):
        super().__init__()

        self.kiwoom = kiwoom_connector
        self.config = config

        # Symbol universe and quote storage
        self.symbols: List[str] = []  # Will be loaded from config
        self.quotes: Dict[str, QuoteSnapshot] = {}
        self.dirty_symbols: Set[str] = set()

        # Screen allocation for sharding
        self.screen_shards: Dict[str, List[str]] = {}  # screen_no -> symbol list
        self.symbol_to_screen: Dict[str, str] = {}  # symbol -> screen_no
        self.max_symbols_per_screen = 100

        # Subscription state
        self.active_subscriptions: Set[str] = set()  # screen numbers

        # Connect to Kiwoom real-time events
        self.kiwoom.real_data_received.connect(self._on_real_data)

        logger.info("MarketDataManager initialized")

    def load_symbol_universe(self, symbols: List[str]):
        """
        Load symbol universe and initialize quote snapshots.

        Args:
            symbols: List of KRX symbol codes
        """
        self.symbols = symbols.copy()

        # Initialize quote snapshots
        self.quotes.clear()
        for symbol in self.symbols:
            self.quotes[symbol] = QuoteSnapshot(symbol=symbol)

        # Create screen shards
        self._create_screen_shards()

        logger.info(f"Loaded {len(self.symbols)} symbols across {len(self.screen_shards)} screen shards")

    def _create_screen_shards(self):
        """Create screen number allocation for symbol sharding"""
        self.screen_shards.clear()
        self.symbol_to_screen.clear()

        # Get available screen numbers from config
        available_screens = self.config.kiwoom.screen_numbers["marketdata"]

        # Distribute symbols across screens
        current_screen_idx = 0
        symbols_in_current_screen = 0

        for symbol in self.symbols:
            # Get current screen number
            screen_no = str(available_screens[current_screen_idx])

            # Initialize screen if first symbol
            if screen_no not in self.screen_shards:
                self.screen_shards[screen_no] = []

            # Add symbol to current screen
            self.screen_shards[screen_no].append(symbol)
            self.symbol_to_screen[symbol] = screen_no
            symbols_in_current_screen += 1

            # Move to next screen if current one is full
            if symbols_in_current_screen >= self.max_symbols_per_screen:
                current_screen_idx = (current_screen_idx + 1) % len(available_screens)
                symbols_in_current_screen = 0

        # Log shard distribution
        for screen_no, symbols in self.screen_shards.items():
            logger.info(f"Screen {screen_no}: {len(symbols)} symbols")

    def subscribe_real_time_data(self) -> bool:
        """
        Subscribe to real-time data for all symbols (both KRX and NXT).

        Returns:
            True if all subscriptions successful
        """
        success_count = 0
        total_shards = len(self.screen_shards)

        for screen_no, symbols in self.screen_shards.items():
            if self._subscribe_screen_shard(screen_no, symbols):
                success_count += 1
                self.active_subscriptions.add(screen_no)

        logger.info(f"Real-time subscriptions: {success_count}/{total_shards} screens successful")

        return success_count == total_shards

    def _subscribe_screen_shard(self, screen_no: str, symbols: List[str]) -> bool:
        """
        Subscribe to real-time data for one screen shard (both venues).

        Args:
            screen_no: Screen number
            symbols: List of base symbols for this screen

        Returns:
            True if subscription successful
        """
        try:
            # Create code lists for both venues
            krx_codes = symbols.copy()  # Base codes for KRX
            nxt_codes = [f"{symbol}_NX" for symbol in symbols]  # Add _NX suffix for NXT

            # Combine all codes for this screen
            all_codes = krx_codes + nxt_codes
            code_list = ";".join(all_codes)

            # Subscribe to both KRX and NXT quotes
            result = self.kiwoom.set_real_reg(
                screen_no=screen_no,
                code_list=code_list,
                fid_list=self.KRX_QUOTE_FIDS,  # Same FIDs work for both venues
                real_type="0"  # Register
            )

            if result != 0:
                logger.error(f"Failed to subscribe screen {screen_no}: error {result}")
                self.subscription_status.emit(screen_no, False)
                return False

            logger.info(f"Subscribed screen {screen_no} with {len(symbols)} symbols "
                        f"({len(krx_codes)} KRX + {len(nxt_codes)} NXT)")
            self.subscription_status.emit(screen_no, True)
            return True

        except Exception as e:
            logger.error(f"Exception subscribing screen {screen_no}: {e}")
            self.subscription_status.emit(screen_no, False)
            return False

    def _on_real_data(self, code: str, real_type: str, real_data: str):
        """
        Handle real-time data from Kiwoom.

        Args:
            code: Symbol code
            real_type: Real-time data type
            real_data: Real data (not used directly)
        """
        try:
            # Determine venue and base symbol
            base_code = code[:-3] if code.endswith('_NX') else code
            if base_code not in self.quotes:
                return

            quote = self.quotes[base_code]
            is_nxt = code.endswith('_NX')

            # Previous values for change detection
            if is_nxt:
                old_bid, old_ask = quote.nxt_bid, quote.nxt_ask
                old_bid_size, old_ask_size = quote.nxt_bid_size, quote.nxt_ask_size
            else:
                old_bid, old_ask = quote.krx_bid, quote.krx_ask
                old_bid_size, old_ask_size = quote.krx_bid_size, quote.krx_ask_size

            # Extract real-time data using new FIDs
            ask_price = self._parse_int(self.kiwoom.get_comm_real_data(code, 41))  # 매도호가1
            bid_price = self._parse_int(self.kiwoom.get_comm_real_data(code, 51))  # 매수호가1
            ask_size = self._parse_int(self.kiwoom.get_comm_real_data(code, 61))  # 매도호가수량1
            bid_size = self._parse_int(self.kiwoom.get_comm_real_data(code, 71))  # 매수호가수량1

            now = time.time()

            if is_nxt:
                quote.nxt_bid = bid_price if bid_price > 0 else quote.nxt_bid
                quote.nxt_ask = ask_price if ask_price > 0 else quote.nxt_ask
                quote.nxt_bid_size = bid_size if bid_size > 0 else quote.nxt_bid_size
                quote.nxt_ask_size = ask_size if ask_size > 0 else quote.nxt_ask_size
                quote.nxt_last_update = now
                changed = (
                        quote.nxt_bid != old_bid or quote.nxt_ask != old_ask or
                        quote.nxt_bid_size != old_bid_size or quote.nxt_ask_size != old_ask_size
                )
                venue = "NXT"
            else:
                quote.krx_bid = bid_price if bid_price > 0 else quote.krx_bid
                quote.krx_ask = ask_price if ask_price > 0 else quote.krx_ask
                quote.krx_bid_size = bid_size if bid_size > 0 else quote.krx_bid_size
                quote.krx_ask_size = ask_size if ask_size > 0 else quote.krx_ask_size
                quote.krx_last_update = now
                changed = (
                        quote.krx_bid != old_bid or quote.krx_ask != old_ask or
                        quote.krx_bid_size != old_bid_size or quote.krx_ask_size != old_ask_size
                )
                venue = "KRX"

            if changed:
                quote.is_dirty = True
                self.dirty_symbols.add(base_code)
                logger.debug(
                    f"Quote updated: {base_code} {venue} bid={bid_price} ask={ask_price} "
                    f"bid_size={bid_size} ask_size={ask_size}"
                )
                self.quote_updated.emit(base_code, venue)

        except Exception as e:
            logger.error(f"Error processing real data for {code}: {e}")

    def _parse_int(self, value: str) -> int:
        """Parse string to int, handling Kiwoom format quirks"""
        try:
            # Remove any formatting characters
            clean_value = value.replace(",", "").replace("+", "").replace("-", "").strip()
            if not clean_value:
                return 0
            return int(clean_value)
        except (ValueError, AttributeError):
            return 0

    def get_dirty_symbols(self) -> Set[str]:
        """
        Get and clear the dirty symbols set.

        Returns:
            Set of symbols that have changed since last call
        """
        dirty = self.dirty_symbols.copy()
        self.dirty_symbols.clear()

        # Clear dirty flags
        for symbol in dirty:
            if symbol in self.quotes:
                self.quotes[symbol].is_dirty = False

        return dirty

    def get_quote(self, symbol: str) -> Optional[QuoteSnapshot]:
        """
        Get quote snapshot for symbol.

        Args:
            symbol: Symbol code

        Returns:
            QuoteSnapshot or None if not found
        """
        return self.quotes.get(symbol)

    def get_all_quotes(self) -> Dict[str, QuoteSnapshot]:
        """Get all quote snapshots"""
        return self.quotes.copy()

    def unsubscribe_all(self):
        """Unsubscribe from all real-time data"""
        for screen_no in self.active_subscriptions.copy():
            try:
                result = self.kiwoom.unregister_real(screen_no, "ALL")
                if result == 0:
                    logger.info(f"Unsubscribed screen {screen_no}")
                else:
                    logger.warning(f"Failed to unsubscribe screen {screen_no}: {result}")

                self.active_subscriptions.remove(screen_no)

            except Exception as e:
                logger.error(f"Exception unsubscribing screen {screen_no}: {e}")

    def get_subscription_status(self) -> Dict[str, bool]:
        """Get subscription status for all screens"""
        status = {}
        for screen_no in self.screen_shards.keys():
            status[screen_no] = screen_no in self.active_subscriptions
        return status

    def is_symbol_ready(self, symbol: str) -> bool:
        """
        Check if symbol has valid data from both venues.

        Args:
            symbol: Symbol code

        Returns:
            True if both KRX and NXT have recent data
        """
        quote = self.quotes.get(symbol)
        if not quote:
            return False

        current_time = time.time()
        max_age_seconds = 5.0  # 5 second staleness threshold

        krx_fresh = (current_time - quote.krx_last_update) < max_age_seconds
        nxt_fresh = (current_time - quote.nxt_last_update) < max_age_seconds

        return krx_fresh and nxt_fresh and quote.krx_bid > 0 and quote.krx_ask > 0

    def get_ready_symbols(self) -> List[str]:
        """Get list of symbols with fresh data from both venues"""
        ready = []
        for symbol in self.symbols:
            if self.is_symbol_ready(symbol):
                ready.append(symbol)
        return ready

    def get_statistics(self) -> Dict[str, any]:
        """Get market data statistics for monitoring"""
        total_symbols = len(self.symbols)
        ready_symbols = len(self.get_ready_symbols())
        active_screens = len(self.active_subscriptions)
        total_screens = len(self.screen_shards)

        return {
            "total_symbols": total_symbols,
            "ready_symbols": ready_symbols,
            "active_screens": active_screens,
            "total_screens": total_screens,
            "subscription_rate": active_screens / total_screens if total_screens > 0 else 0,
            "ready_rate": ready_symbols / total_symbols if total_symbols > 0 else 0
        }