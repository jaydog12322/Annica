# -*- coding: utf-8 -*-
"""vi_lister.py
-----------------
Real-time Volatility Interruption (VI) tracker.

This module queries Kiwoom via the ``OPT10054`` TR to obtain the current list
of symbols under a VI halt and registers for subsequent real-time
``"VI발동/해제"`` events.  The class maintains an in-memory set of halted
symbols and exposes a simple ``is_in_vi`` helper so other components can check
status before processing orders or market data.

The implementation gracefully degrades when the Kiwoom control is not
available (e.g. during unit tests on non-Windows platforms).
"""

from __future__ import annotations

import logging
from typing import Set

from PyQt5.QtCore import QObject, pyqtSignal

from src.kiwoom.kiwoom_connector import KiwoomConnector

logger = logging.getLogger(__name__)


class VILister(QObject):
    """Maintain the set of symbols currently in a VI halt."""

    # Emitted whenever a symbol's VI status changes (symbol, in_vi)
    vi_status_changed = pyqtSignal(str, bool)

    RQ_NAME = "VI_LIST"
    TR_CODE = "OPT10054"
    REAL_TYPE = "VI발동/해제"

    def __init__(self, kiwoom: KiwoomConnector, screen_no: str):
        super().__init__()

        self.kiwoom = kiwoom
        self.screen_no = str(screen_no)
        self._vi_symbols: Set[str] = set()

        # Hook into Kiwoom callbacks
        self.kiwoom.tr_data_received.connect(self._on_tr_data)
        self.kiwoom.real_data_received.connect(self._on_real_data)

    # ------------------------------------------------------------------
    def start(self) -> None:
        """Request the initial VI list and register for real-time updates."""
        try:
            # ``OPT10054`` takes no input parameters; simply issue the request.
            result = self.kiwoom.comm_rq_data(
                self.RQ_NAME, self.TR_CODE, 0, self.screen_no
            )
            if result != 0:
                logger.error("VI list request failed: %s", result)
        except Exception:
            logger.exception("Failed to request VI list")

    # ------------------------------------------------------------------
    def _on_tr_data(
        self,
        screen_no: str,
        rq_name: str,
        tr_code: str,
        record_name: str,
        prev_next: str,
        data_len: int,
        err_code: str,
        msg: str,
        splm_msg: str,
    ) -> None:
        """Handle TR data for the initial VI list."""
        if rq_name != self.RQ_NAME:
            return

        try:
            cnt = self.kiwoom.get_repeat_cnt(tr_code, "output")
            new_set: Set[str] = set()
            for i in range(cnt):
                code = (
                    self.kiwoom.get_comm_data(tr_code, "output", i, "종목코드")
                    .strip()
                )
                if code:
                    new_set.add(code)

            added = new_set - self._vi_symbols
            removed = self._vi_symbols - new_set
            self._vi_symbols = new_set

            for sym in added:
                self.vi_status_changed.emit(sym, True)
            for sym in removed:
                self.vi_status_changed.emit(sym, False)

            logger.info("Loaded %d VI symbols", len(self._vi_symbols))
        except Exception:
            logger.exception("Error processing VI TR data")

    # ------------------------------------------------------------------
    def _on_real_data(self, code: str, real_type: str, real_data: str) -> None:
        """Handle real-time VI events."""
        if real_type != self.REAL_TYPE:
            return

        event = real_data.strip()
        in_vi = event == "1"  # 1=발동, other=해제

        if in_vi:
            if code not in self._vi_symbols:
                self._vi_symbols.add(code)
                self.vi_status_changed.emit(code, True)
        else:
            if code in self._vi_symbols:
                self._vi_symbols.discard(code)
                self.vi_status_changed.emit(code, False)

    # ------------------------------------------------------------------
    def is_in_vi(self, symbol: str) -> bool:
        """Return ``True`` if *symbol* is currently in a VI halt."""
        return symbol in self._vi_symbols


__all__ = ["VILister"]