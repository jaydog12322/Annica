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
            result = self.kiwoom.request_vi_list(self.screen_no, rq_name=self.RQ_NAME)
            if result != 0:
                logger.error("VI list request failed: %s", result)
        except Exception:
            logger.exception("Failed to request VI list")

        # [ADD] VI 실시간 FID 등록
        try:
            vi_fids = [
                9001, 302, 13, 14, 9008, 9075,  # 코드/이름/누적
                9068, 9069,  # 발동구분/방향
                1221, 1223, 1224, 1225,  # 발동가/체결시각/해제시각/적용구분
                1236, 1237, 1238, 1239,  # 기준가/괴리율(정/동)
                1489, 1490, 1279
            ]
            fid_str = ";".join(map(str, vi_fids))
            # 화면번호는 VI 전용으로, code_list="ALL", real_type="0"(덮어쓰기)
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
    def _fmt_hms(s: str) -> str:
        s = (s or "").strip()
        return f"{s[0:2]}:{s[2:4]}:{s[4:6]}" if len(s) >= 6 and s.isdigit() else s

    def _dump_vi_fields(self, code: str):
        probe = [9001, 302, 13, 14, 9008, 9075, 9068, 9069, 1221, 1223, 1224, 1225, 1236, 1237, 1238, 1239, 1489, 1490,
                 1279]
        got = {}
        for fid in probe:
            try:
                v = (self.kiwoom.get_comm_real_data(code, fid) or "").strip()
            except Exception:
                v = ""
            if v:
                got[fid] = v
        logger.info("[VI DEBUG] %s %s", code, got)

    def _on_real_data(self, code: str, real_type: str, real_data: str) -> None:
        if real_type != self.REAL_TYPE:  # "VI발동/해제"
            return

        # 1) FID 9068로 발동/해제 판정
        try:
            vi_flag = (self.kiwoom.get_comm_real_data(code, 9068) or "").strip()
        except Exception:
            vi_flag = ""
        in_vi = (vi_flag == "1")  # 1=발동, 2=해제 (브로커별 표현 다를 수 있어 보조룰 추가)
        if vi_flag not in ("1", "2"):
            # 보조룰: 해제시각(1224)이 채워지면 해제로 간주
            try:
                rel_probe = (self.kiwoom.get_comm_real_data(code, 1224) or "").strip()
            except Exception:
                rel_probe = ""
            if rel_probe:
                in_vi = False

        # 2) 값 채우기 (키 이름 유지: trigger_time/release_time/trigger_type/trigger_price/name)
        info = self._vi_info.setdefault(code, {})
        for key, fid in self.VI_FIDS.items():
            try:
                value = (self.kiwoom.get_comm_real_data(code, fid) or "").strip()
            except Exception:
                value = ""
            if key in ("trigger_time", "release_time"):
                value = _fmt_hms(value)
            if value != "":
                info[key] = value

        # 3) 상태 업데이트 & 송출
        if in_vi:
            self._vi_symbols.add(code)
        else:
            self._vi_symbols.discard(code)

        # [임시] 실제 값 들어오는지 로그로 확인 (문제 해결되면 제거)
        self._dump_vi_fields(code)

        gui_row = {
            "Code": code,
            "Trigger Price": info.get("trigger_price", ""),
            "Trigger Time": info.get("trigger_time", ""),
            "Release Time": info.get("release_time", ""),
            "Trigger Type": info.get("trigger_type", ""),
            "Name": info.get("name", ""),
        }
        self.vi_status_changed.emit(code, in_vi, gui_row)

        if not in_vi:
            self._vi_info.pop(code, None)

    # ------------------------------------------------------------------
    def is_in_vi(self, code: str) -> bool:
        """Return True if *code* is currently in a VI halt."""
        return code in self._vi_symbols
