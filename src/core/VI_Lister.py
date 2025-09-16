# -*- coding: utf-8 -*-
"""vi_lister.py
-----------------
Real-time Volatility Interruption (VI) tracker – 4-column feed only.

Columns (GUI-bound):
1) Ticker# (종목코드), 2) 종목이름, 3) Trigger Price, 4) 현재 VI 진입여부(True only emitted)

Behavior:
- On start(): request OPT10054 once (snapshot) and auto-register VI real stream.
- Emit rows to GUI only when in_vi == True (both on initial snapshot and on realtime updates).
"""

from __future__ import annotations

import logging
from typing import Set, Dict

from PyQt5.QtCore import QObject, pyqtSignal

from src.kiwoom.kiwoom_connector import KiwoomConnector

logger = logging.getLogger(__name__)


class VILister(QObject):
    """Maintain the set of symbols currently in a VI halt."""

    # Emitted only when a symbol is confirmed to be in VI (True).
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
        "trigger_type": 9068,   # VI발동구분 (1=발동, 2=해제 등)
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
    def start(self) -> None:
        """Request the initial VI list and register for real-time updates."""
        try:
            # Custom wrapper in KiwoomConnector that calls CommRqData for OPT10054
            result = self.kiwoom.request_vi_list(self.screen_no, rq_name=self.RQ_NAME)
            if result != 0:
                logger.error("VI list request failed: %s", result)
        except Exception:
            logger.exception("Failed to request VI list")

        # (Optional) Register a minimal FID set for redundancy/field fill
        try:
            fid_str = ";".join(str(fid) for fid in self.VI_FIDS.values())
            # Register on this screen; code_list "ALL" (broker allows) / real_type "0" overwrite
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

                name = _get_item(tr_code, rec_candidates, i, ["종목명", "한글종목명"])
                trig_price = _get_item(tr_code, rec_candidates, i, ["발동가격", "발동가", "기준가격", "기준가"])

                # 스냅샷 단계에서는 모두 in_vi = True 로 간주(발동 목록이니까)
                new_set.add(code)
                self._vi_info[code] = {"name": name, "trigger_price": trig_price}

                # === emit only True rows ===
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
        """Handle real-time VI events: only emit when currently in VI."""
        # Kiwoom sends VI events under these real types once OPT10054 was requested.
        if real_type not in self.REAL_TYPES:
            return

        # 1) Determine in_vi by FID 9068 (fallback: keep last known state)
        prev_in_vi = code in self._vi_symbols
        try:
            vi_flag = (self.kiwoom.get_comm_real_data(code, self.VI_FIDS["trigger_type"]) or "").strip()
        except Exception:
            vi_flag = ""
        if vi_flag:
            in_vi = (vi_flag == "1")  # (common: 1=발동, 2=해제)
        else:
            in_vi = prev_in_vi  # Broker sometimes omits field on partial packets

        # 2) Cache minimal fields for our 4 columns
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

        # Maintain internal set
        if in_vi:
            self._vi_symbols.add(code)
        else:
            self._vi_symbols.discard(code)

        # 3) Emit when entering VI or when leaving so GUI can clear the row
        should_emit = False
        emit_flag = in_vi
        if in_vi:
            should_emit = True
        elif prev_in_vi and not in_vi:
            should_emit = True
            emit_flag = False

        elif vi_flag == "2":  # explicit 해제 flag even if we missed the entry
            should_emit = True
            emit_flag = False

        if should_emit:
            row = {
                "Ticker#": code,
                "종목이름": name or "",
                "Trigger Price": trig_price or "",
                "현재 VI 진입여부": emit_flag,
            }
            self.vi_status_changed.emit(code, emit_flag, row)

    # ------------------------------------------------------------------
    def is_in_vi(self, code: str) -> bool:
        """Return True if *code* is currently in a VI halt."""
        return code in self._vi_symbols
