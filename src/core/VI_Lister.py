# -*- coding: utf-8 -*-
"""vi_lister.py
-----------------
Real-time Volatility Interruption (VI) tracker – 4-column feed only.

Columns (GUI-bound):
1) Ticker# (종목코드), 2) 종목이름, 3) Trigger Price, 4) 현재 VI 진입여부

Behavior:
- On start(): request OPT10054 once (snapshot) and auto-register VI real stream.
- Emit rows to GUI when in_vi == True (enter) and emit a False row on 해제 so GUI can remove.
"""

from __future__ import annotations

import logging
from typing import Set, Dict

from PyQt5.QtCore import QObject, pyqtSignal

from src.kiwoom.kiwoom_connector import KiwoomConnector

logger = logging.getLogger(__name__)


class VILister(QObject):
    """Maintain the set of symbols currently in a VI halt."""

    # Signal payload: (symbol, in_vi, row_dict)
    # row_dict keys: "Ticker#", "종목이름", "Trigger Price", "현재 VI 진입여부"
    vi_status_changed = pyqtSignal(str, bool, dict)

    RQ_NAME = "VI_LIST"
    TR_CODE = "OPT10054"
    REAL_TYPES = ("VI발동/해제", "주식VI발동/해제")

    # Real-time FID map (subset needed for our 4 columns)
    VI_FIDS = {
        "name": 302,            # 종목명
        "trigger_price": 1221,  # VI 발동가격
        "trigger_type": 9068,   # VI발동구분 (1=발동, 2=해제)
    }

    def __init__(self, kiwoom: KiwoomConnector, screen_no: str):
        super().__init__()
        self.kiwoom = kiwoom
        self.screen_no = str(screen_no)

        # Set of symbols currently in VI (for quick membership checks)
        self._vi_symbols: Set[str] = set()
        # Cache minimal info by code
        self._vi_info: Dict[str, Dict[str, str]] = {}

        # Hook into Kiwoom callbacks
        self.kiwoom.tr_data_received.connect(self._on_tr_data)
        self.kiwoom.real_data_received.connect(self._on_real_data)

    # ------------------------------------------------------------------
    @staticmethod
    def _norm_code(code: str) -> str:
        """Normalize codes: strip leading 'A' and zero-pad to 6 digits."""
        return (code or "").lstrip("A").zfill(6)

    # ------------------------------------------------------------------
    def start(self) -> None:
        """Request the initial VI list and register for real-time updates."""
        try:
            # Custom wrapper in KiwoomConnector that calls CommRqData for OPT10054
            result = self.kiwoom.request_vi_list(self.screen_no, rq_name=self.RQ_NAME)
            if result != 0:
                logger.error("VI list request failed: %s", result)
        except Exception:
            logger.exception("Failed to request VI list")

        # Register minimal FIDs for realtime VI events
        try:
            fid_str = ";".join(str(fid) for fid in self.VI_FIDS.values())
            # Register on this screen; code_list "ALL" / real_type "0" overwrite
            self.kiwoom.set_real_reg(self.screen_no, "ALL", fid_str, "0")
            logger.info("Registered VI realtime FIDs on screen %s", self.screen_no)
        except Exception:
            logger.exception("Failed to SetRealReg for VI")

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
        """Handle TR data for the initial VI list (snapshot)."""
        if rq_name != self.RQ_NAME:
            return

        try:
            # TR 레코드명은 브로커 버전에 따라 다를 수 있어 넉넉히 시도
            rec_candidates = ["발동종목", "output", "주식VI발동", "주식VI발동해제"]

            def _get_repeat_cnt_any() -> int:
                for rec in rec_candidates:
                    try:
                        c = self.kiwoom.get_repeat_cnt(tr_code, rec)
                    except Exception:
                        c = 0
                    if c and c > 0:
                        return c
                return 0

            def _get_item(tr: str, recs, i: int, item_names) -> str:
                if isinstance(recs, str):
                    recs = [recs]
                if isinstance(item_names, str):
                    item_names = [item_names]
                for rec in recs:
                    for item in item_names:
                        try:
                            v = (self.kiwoom.get_comm_data(tr, rec, i, item) or "").strip()
                        except Exception:
                            v = ""
                        if v:
                            return v
                return ""

            cnt = _get_repeat_cnt_any()
            if cnt <= 0:
                logger.info("No initial VI rows returned")
                return

            new_set: Set[str] = set()
            for i in range(cnt):
                code = _get_item(tr_code, rec_candidates, i, ["종목코드"])
                if not code:
                    continue
                code = self._norm_code(code)  # normalize snapshot codes

                name = _get_item(tr_code, rec_candidates, i, ["종목명", "한글종목명"])
                trig_price = _get_item(tr_code, rec_candidates, i, ["발동가격", "발동가", "기준가격", "기준가"])

                # 스냅샷 단계에서는 모두 in_vi = True 로 간주(발동 목록이니까)
                new_set.add(code)
                self._vi_info[code] = {"name": name or "", "trigger_price": trig_price or ""}

                # === emit True row ===
                row = {
                    "Ticker#": code,
                    "종목이름": name or "",
                    "Trigger Price": trig_price or "",
                    "현재 VI 진입여부": True,
                }
                self.vi_status_changed.emit(code, True, row)

            # 내부 상태 갱신
            self._vi_symbols = new_set
            logger.info("Loaded %d VI symbols (snapshot)", len(self._vi_symbols))

        except Exception:
            logger.exception("Error processing VI TR data")

    # ------------------------------------------------------------------
    def _on_real_data(self, code: str, real_type: str, real_data: str) -> None:
        """Handle real-time VI events: emit True on enter, False on release."""
        # Kiwoom sends VI events under these real types once OPT10054 was requested.
        if real_type not in self.REAL_TYPES:
            return

        # Normalize incoming code first (often '######' without 'A')
        code = self._norm_code(code)

        # 1) Determine in_vi by FID 9068 (fallback: keep last known state)
        try:
            vi_flag = (self.kiwoom.get_comm_real_data(code, self.VI_FIDS["trigger_type"]) or "").strip()
        except Exception:
            vi_flag = ""

        if vi_flag == "1":
            in_vi = True
        elif vi_flag == "2":
            in_vi = False
        else:
            # Fallback to previous known membership if flag missing
            in_vi = code in self._vi_symbols

        # 2) Read/retain fields needed for our 4 columns
        def _read_fid(fid: int) -> str:
            try:
                return (self.kiwoom.get_comm_real_data(code, fid) or "").strip()
            except Exception:
                return ""

        name = _read_fid(self.VI_FIDS["name"]) or self._vi_info.get(code, {}).get("name", "")
        trig_price = _read_fid(self.VI_FIDS["trigger_price"]) or self._vi_info.get(code, {}).get("trigger_price", "")

        # Update caches
        if name:
            self._vi_info.setdefault(code, {})["name"] = name
        if trig_price:
            self._vi_info.setdefault(code, {})["trigger_price"] = trig_price

        # 3) State transition handling + emits
        if in_vi:
            # Ensure membership and emit a True row
            if code not in self._vi_symbols:
                logger.debug("VI enter detected (add): %s", code)
            self._vi_symbols.add(code)

            row = {
                "Ticker#": code,
                "종목이름": name or "",
                "Trigger Price": trig_price or "",
                "현재 VI 진입여부": True,
            }
            self.vi_status_changed.emit(code, True, row)
        else:
            # If previously present, remove and emit False to let GUI drop the row
            if code in self._vi_symbols:
                logger.debug("VI release detected (remove): %s", code)
                self._vi_symbols.remove(code)

            # Always emit False on release so GUI can remove any lingering row
            row = {
                "Ticker#": code,
                "종목이름": name or "",
                "Trigger Price": trig_price or "",
                "현재 VI 진입여부": False,
            }
            self.vi_status_changed.emit(code, False, row)
