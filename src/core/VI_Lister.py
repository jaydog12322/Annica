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
from typing import Set, Dict

from PyQt5.QtCore import QObject, pyqtSignal

from src.kiwoom.kiwoom_connector import KiwoomConnector

logger = logging.getLogger(__name__)


class VILister(QObject):
    """Maintain the set of symbols currently in a VI halt."""

    # Emitted whenever a symbol's VI status changes (symbol, in_vi, info)
    vi_status_changed = pyqtSignal(str, bool, dict)

    RQ_NAME = "VI_LIST"
    TR_CODE = "OPT10054"
    REAL_TYPE = "VI발동/해제"

    def __init__(self, kiwoom: KiwoomConnector, screen_no: str):
        super().__init__()

        self.kiwoom = kiwoom
        self.screen_no = str(screen_no)
        self._vi_symbols: Set[str] = set()
        # Map of symbol -> latest VI details
        self._vi_info: Dict[str, Dict[str, str]] = {}

        # FID mappings for real-time VI fields (per KOA documentation)
        self.VI_FIDS = {
            "trigger_time": 1223,  # 매매체결처리시각
            "release_time": 1224,  # VI 해제시각
            "trigger_type": 9068,  # VI발동구분
            "trigger_price": 1221,  # VI 발동가격
            "name": 302,  # 종목명
        }

        # Hook into Kiwoom callbacks
        self.kiwoom.tr_data_received.connect(self._on_tr_data)
        self.kiwoom.real_data_received.connect(self._on_real_data)

    # ------------------------------------------------------------------
    def start(self) -> None:
        """Request the initial VI list and register for real-time updates."""
        try:
            # Use connector convenience to avoid -300 (missing/invalid inputs)
            result = self.kiwoom.request_vi_list(self.screen_no, rq_name=self.RQ_NAME)
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
            # Be tolerant to record/table naming differences
            rec_primary = "발동종목"
            rec_fallback = "output"

            cnt = self.kiwoom.get_repeat_cnt(tr_code, rec_primary)
            if cnt == 0:
                cnt = self.kiwoom.get_repeat_cnt(tr_code, rec_fallback)

            new_set: Set[str] = set()
            for i in range(cnt):
                code = self.kiwoom.get_comm_data(tr_code, rec_primary, i, "종목코드").strip()
                if not code:
                    code = self.kiwoom.get_comm_data(tr_code, rec_fallback, i, "종목코드").strip()
                if code:
                    info = {
                        "trigger_time": self.kiwoom.get_comm_data(tr_code, rec_primary, i, "발동시간").strip()
                                        or self.kiwoom.get_comm_data(tr_code, rec_fallback, i, "발동시간").strip(),
                        "release_time": self.kiwoom.get_comm_data(tr_code, rec_primary, i, "해제시간").strip()
                                        or self.kiwoom.get_comm_data(tr_code, rec_fallback, i, "해제시간").strip(),
                        "trigger_type": self.kiwoom.get_comm_data(tr_code, rec_primary, i, "VI발동구분").strip()
                                        or self.kiwoom.get_comm_data(tr_code, rec_fallback, i, "VI발동구분").strip(),
                        "trigger_price": self.kiwoom.get_comm_data(tr_code, rec_primary, i, "발동가격").strip()
                                         or self.kiwoom.get_comm_data(tr_code, rec_fallback, i, "발동가격").strip(),
                        "name": self.kiwoom.get_comm_data(tr_code, rec_primary, i, "종목명").strip()
                                or self.kiwoom.get_comm_data(tr_code, rec_fallback, i, "종목명").strip(),
                    }
                    self._vi_info[code] = info
                    new_set.add(code)

            added = new_set - self._vi_symbols
            removed = self._vi_symbols - new_set
            self._vi_symbols = new_set

            for sym in added:
                self.vi_status_changed.emit(sym, True, self._vi_info.get(sym, {}).copy())
            for sym in removed:
                info = self._vi_info.pop(sym, {})
                self.vi_status_changed.emit(sym, False, info)

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

        info = self._vi_info.setdefault(code, {})
        for key, fid in self.VI_FIDS.items():
            try:
                value = self.kiwoom.get_comm_real_data(code, fid).strip()
            except Exception:
                value = ""
            if value:
                info[key] = value

        if in_vi:
            if code not in self._vi_symbols:
                self._vi_symbols.add(code)

        else:
            if code in self._vi_symbols:
                self._vi_symbols.discard(code)
            # Once released remove stored info after notifying listeners
        self.vi_status_changed.emit(code, in_vi, info.copy())
        if not in_vi:
            self._vi_info.pop(code, None)

    # ------------------------------------------------------------------
    def is_in_vi(self, code: str) -> bool:
        """Return True if *code* is currently in a VI halt."""
        return code in self._vi_symbols
