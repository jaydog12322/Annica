# -*- coding: utf-8 -*-
"""
session_state.py
----------------
Session State Management Module

Manages trading session state based on:
- KRX trading hours (09:00-15:20)
- NXT session signals via FID 215 (P/Q/R/S/T/U/V)
- User-defined overlap window (09:00:32-15:19:50)

Only allows trading during the safe overlap period.
"""

import logging
from datetime import datetime, time
from enum import Enum
from typing import Optional
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

logger = logging.getLogger(__name__)


class NXTSessionState(Enum):
    """NXT session states from FID 215"""
    PRE_OPEN = "P"  # 프리 시작 (08:00)
    PRE_CLOSE = "Q"  # 프리 종료 (08:50)
    MAIN_OPEN = "R"  # 메인 시작 (09:00:30)
    MAIN_CLOSE = "S"  # 메인 종료 (15:20)
    AFTER_START = "T"  # 애프터 시작 (15:30)
    AFTER_MID = "U"  # 애프터 중간
    AFTER_CLOSE = "V"  # 애프터 종료 (20:00)


class TradingState(Enum):
    """Overall trading state"""
    DISARMED = "DISARMED"  # Not trading
    ARMED = "ARMED"  # Ready to trade
    TRADING = "TRADING"  # Actively trading
    CLOSING = "CLOSING"  # Winding down


class SessionStateManager(QObject):
    """
    Manages trading session state and enforces overlap window rules.

    Signals:
        state_changed: Emitted when trading state changes
        session_update: Emitted when session info updates
    """

    # Signals
    state_changed = pyqtSignal(str)  # TradingState
    session_update = pyqtSignal(dict)  # Session info dict

    def __init__(self, config):
        super().__init__()

        self.config = config
        self.session_config = config.sessions

        # Current state
        self.trading_state = TradingState.DISARMED
        self.nxt_session_state: Optional[NXTSessionState] = None
        self.krx_trading_hours = True  # Assume KRX is open for now

        # Timers for state checking
        self.state_check_timer = QTimer()
        self.state_check_timer.timeout.connect(self._check_trading_state)
        self.state_check_timer.start(1000)  # Check every second

        logger.info("SessionStateManager initialized")

    def update_nxt_session(self, fid_215_value: str):
        """
        Update NXT session state from FID 215 value.

        Args:
            fid_215_value: Session state code (P/Q/R/S/T/U/V)
        """
        try:
            old_state = self.nxt_session_state
            self.nxt_session_state = NXTSessionState(fid_215_value)

            logger.info(f"NXT session updated: {old_state} -> {self.nxt_session_state}")

            # Trigger state evaluation
            self._evaluate_trading_state()

            # Emit session update
            self.session_update.emit(self._get_session_info())

        except ValueError:
            logger.warning(f"Unknown NXT session state: {fid_215_value}")

    def _check_trading_state(self):
        """Periodic check of trading state based on time"""
        current_time = datetime.now().time()

        # Check KRX trading hours (09:00-15:20)
        krx_start = time(9, 0)
        krx_end = time(15, 20)

        old_krx_state = self.krx_trading_hours
        self.krx_trading_hours = krx_start <= current_time <= krx_end

        if old_krx_state != self.krx_trading_hours:
            logger.info(f"KRX trading hours changed: {self.krx_trading_hours}")
            self._evaluate_trading_state()

    def _evaluate_trading_state(self):
        """Evaluate and update trading state based on all conditions"""
        old_state = self.trading_state

        # Check if we should be armed/trading
        if self._should_be_trading():
            if self.trading_state == TradingState.DISARMED:
                self.trading_state = TradingState.ARMED
            elif self.trading_state == TradingState.ARMED:
                # Could transition to TRADING when first trade occurs
                pass
        else:
            if self.trading_state in [TradingState.TRADING, TradingState.ARMED]:
                self.trading_state = TradingState.CLOSING
                # Will transition to DISARMED when all positions are flat

        # Log state change
        if old_state != self.trading_state:
            logger.info(f"Trading state changed: {old_state} -> {self.trading_state}")
            self.state_changed.emit(self.trading_state.value)

    def _should_be_trading(self) -> bool:
        """
        Determine if we should be in trading state based on all conditions.

        Returns:
            True if all conditions met for trading
        """
        if not self.session_config.arm_only_in_overlap:
            return True

        # Check overlap window
        if not self._in_overlap_window():
            return False

        # Check KRX trading hours
        if not self.krx_trading_hours:
            return False

        # Check NXT main session (if using FID 215 signals)
        if self.session_config.use_fid_215_signals:
            if self.nxt_session_state != NXTSessionState.MAIN_OPEN:
                return False
        else:
            # Fallback to time-based check for NXT main (09:00:30-15:20)
            current_time = datetime.now().time()
            nxt_start = time(9, 0, 30)
            nxt_end = time(15, 20)
            if not (nxt_start <= current_time <= nxt_end):
                return False

        return True

    def _in_overlap_window(self) -> bool:
        """Check if current time is within user-defined overlap window"""
        current_time = datetime.now().time()

        # Parse overlap window times
        start_str = self.session_config.overlap_window["start"]  # "09:00:32"
        end_str = self.session_config.overlap_window["end"]  # "15:19:50"

        try:
            start_parts = start_str.split(":")
            start_time = time(int(start_parts[0]), int(start_parts[1]), int(start_parts[2]))

            end_parts = end_str.split(":")
            end_time = time(int(end_parts[0]), int(end_parts[1]), int(end_parts[2]))

            return start_time <= current_time <= end_time

        except (ValueError, IndexError) as e:
            logger.error(f"Invalid overlap window format: {e}")
            return False

    def can_trade(self) -> bool:
        """
        Check if trading is currently allowed.

        Returns:
            True if trading is allowed
        """
        return self.trading_state in [TradingState.ARMED, TradingState.TRADING]

    def can_open_new_positions(self) -> bool:
        """
        Check if new positions can be opened.

        Returns:
            True if new positions allowed
        """
        return self.trading_state == TradingState.ARMED

    def should_close_positions(self) -> bool:
        """
        Check if positions should be closed.

        Returns:
            True if in closing state
        """
        return self.trading_state == TradingState.CLOSING

    def force_disarm(self):
        """Force trading state to disarmed (emergency stop)"""
        old_state = self.trading_state
        self.trading_state = TradingState.DISARMED

        logger.warning(f"Trading FORCE DISARMED: {old_state} -> {self.trading_state}")
        self.state_changed.emit(self.trading_state.value)

    def manual_arm(self):
        """Manually arm trading (override time checks)"""
        if self.trading_state == TradingState.DISARMED:
            self.trading_state = TradingState.ARMED
            logger.info("Trading MANUALLY ARMED")
            self.state_changed.emit(self.trading_state.value)

    def transition_to_trading(self):
        """Transition from ARMED to TRADING when first trade occurs"""
        if self.trading_state == TradingState.ARMED:
            self.trading_state = TradingState.TRADING
            logger.info("Trading state: ARMED -> TRADING")
            self.state_changed.emit(self.trading_state.value)

    def _get_session_info(self) -> dict:
        """Get current session information"""
        current_time = datetime.now()

        return {
            "current_time": current_time.strftime("%H:%M:%S"),
            "trading_state": self.trading_state.value,
            "nxt_session": self.nxt_session_state.value if self.nxt_session_state else "UNKNOWN",
            "krx_trading_hours": self.krx_trading_hours,
            "in_overlap_window": self._in_overlap_window(),
            "can_trade": self.can_trade(),
            "can_open_new": self.can_open_new_positions(),
            "should_close": self.should_close_positions()
        }

    def get_status_text(self) -> str:
        """Get human-readable status text for GUI"""
        info = self._get_session_info()

        if self.trading_state == TradingState.DISARMED:
            return f"DISARMED - {info['current_time']}"
        elif self.trading_state == TradingState.ARMED:
            return f"ARMED - {info['current_time']} - NXT:{info['nxt_session']}"
        elif self.trading_state == TradingState.TRADING:
            return f"TRADING - {info['current_time']} - NXT:{info['nxt_session']}"
        elif self.trading_state == TradingState.CLOSING:
            return f"CLOSING - {info['current_time']}"

        return f"UNKNOWN - {info['current_time']}"