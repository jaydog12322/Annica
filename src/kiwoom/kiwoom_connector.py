# -*- coding: utf-8 -*-
"""kiwoom_connector.py
---------------------
Kiwoom OpenAPI+ connector used by the KRX–NXT arbitrage system.

This module provides a thin, *event-driven* wrapper around the Kiwoom COM control:
- Login flow (CommConnect → OnEventConnect)
- TR request/response wrappers (CommRqData, GetCommData, GetRepeatCnt)
- Real-time registration helpers (SetRealReg, GetCommRealData, DisconnectRealData)
- Basic SendOrder passthrough (kept minimal; ExecutionGateway owns routing)

Design goals:
- Keep all Kiwoom calls on the GUI thread (Qt event loop).
- Gracefully degrade (stub returns) when QAxWidget is unavailable (e.g., CI/Linux).
- Mirror Kiwoom events via Qt signals so other modules can subscribe safely.
"""

from __future__ import annotations

import logging
from typing import Optional

from PyQt5.QtCore import QObject, QEventLoop, pyqtSignal

try:
    # QAxWidget is available only on Windows
    from PyQt5.QAxContainer import QAxWidget  # type: ignore
except Exception:  # pragma: no cover - executed on non-Windows environments
    QAxWidget = None  # type: ignore

logger = logging.getLogger(__name__)


class KiwoomConnector(QObject):
    """Minimal Kiwoom OpenAPI+ wrapper focused on login & core API calls."""

    # ------------------------------------------------------------------
    # Signals (mirror Kiwoom control event signatures)
    # ------------------------------------------------------------------
    tr_data_received = pyqtSignal(
        str,  # sScrNo
        str,  # sRQName
        str,  # sTrCode
        str,  # sRecordName
        str,  # sPrevNext
        int,  # nDataLength
        str,  # sErrorCode
        str,  # sMessage
        str,  # sSplmMsg
    )
    real_data_received = pyqtSignal(str, str, str)
    msg_received = pyqtSignal(str, str, str, str)
    chejan_data_received = pyqtSignal(str, int, str)

    def __init__(self) -> None:
        super().__init__()

        self._api: Optional[QAxWidget] = None
        self._login_event_loop: Optional[QEventLoop] = None
        self._login_result: Optional[int] = None
        self.logged_in: bool = False

        if QAxWidget is not None:
            # Instantiate Kiwoom control
            self._api = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

            # Wire Kiwoom events to our signals
            self._api.OnReceiveTrData.connect(self.tr_data_received)
            self._api.OnReceiveRealData.connect(self.real_data_received)
            self._api.OnReceiveMsg.connect(self.msg_received)
            self._api.OnReceiveChejanData.connect(self.chejan_data_received)
            self._api.OnEventConnect.connect(self._on_event_connect)

            logger.info("Kiwoom control loaded")
        else:  # pragma: no cover
            logger.warning("QAxWidget not available; KiwoomConnector running in stub mode")

    # ==================================================================
    # Login handling
    # ==================================================================
    def login(self, show_account_pw: bool = False) -> bool:
        """Launch Kiwoom login window and block until OnEventConnect."""
        if self._api is None:
            logger.debug("Kiwoom login skipped – running without API")
            return False

        self._login_event_loop = QEventLoop()
        self._login_result = None

        logger.info("Initiating Kiwoom login")
        try:
            self._api.dynamicCall("CommConnect()")
            self._login_event_loop.exec_()
        except Exception:
            logger.exception("CommConnect failed")
            return False

        success = (self._login_result == 0)
        self.logged_in = success

        if success and show_account_pw:
            # Open the account-password UI managed by Kiwoom (once per device)
            try:
                self._api.dynamicCall("KOA_Functions(QString, QString)", "ShowAccountWindow", "")
            except Exception:
                logger.exception("Failed to open Kiwoom account-password window")

        if success:
            logger.info("Kiwoom login successful")
        else:
            logger.error("Kiwoom login failed: %s", self._login_result)
        return success

    def _on_event_connect(self, err_code: int) -> None:
        """Kiwoom OnEventConnect callback."""
        self._login_result = err_code
        if self._login_event_loop is not None:
            self._login_event_loop.exit()

    # ==================================================================
    # Convenience wrappers (GetLoginInfo, etc.)
    # ==================================================================
    def get_login_info(self, tag: str) -> str:
        """Wrapper for GetLoginInfo(QString)."""
        if self._api is None:
            return ""
        try:
            return str(self._api.dynamicCall("GetLoginInfo(QString)", tag))
        except Exception:
            logger.exception("GetLoginInfo failed: %s", tag)
            return ""

    # ==================================================================
    # TR request/response wrappers
    # ==================================================================
    def set_input_value(self, fid_name: str, value: str) -> None:
        """SetInputValue(QString, QString) – must be called before CommRqData."""
        if self._api is None:
            return
        try:
            self._api.dynamicCall("SetInputValue(QString, QString)", fid_name, value)
        except Exception:
            logger.exception("SetInputValue failed: %s=%s", fid_name, value)

    def comm_rq_data(self, rq_name: str, tr_code: str, prev_next: int, screen_no: str) -> int:
        """CommRqData(QString, QString, int, QString) -> int (0=OK, <0 error)."""
        if self._api is None:
            return -1
        try:
            ret = self._api.dynamicCall(
                "CommRqData(QString, QString, int, QString)",
                rq_name,
                tr_code,
                int(prev_next),
                str(screen_no),
            )
            return int(ret)
        except Exception:
            logger.exception("CommRqData failed: rq=%s tr=%s", rq_name, tr_code)
            return -1

    def get_repeat_cnt(self, tr_code: str, record_name: str) -> int:
        """GetRepeatCnt(QString, QString) -> int."""
        if self._api is None:
            return 0
        try:
            ret = self._api.dynamicCall("GetRepeatCnt(QString, QString)", tr_code, record_name)
            return int(ret)
        except Exception:
            logger.exception("GetRepeatCnt failed: tr=%s rec=%s", tr_code, record_name)
            return 0

    def get_comm_data(self, tr_code: str, record_name: str, index: int, item_name: str) -> str:
        """GetCommData(QString, QString, int, QString) -> QString (trimmed)."""
        if self._api is None:
            return ""
        try:
            out = self._api.dynamicCall(
                "GetCommData(QString, QString, int, QString)",
                tr_code,
                record_name,
                int(index),
                item_name,
            )
            return str(out).strip()
        except Exception:
            logger.exception(
                "GetCommData failed: tr=%s rec=%s idx=%s item=%s",
                tr_code, record_name, index, item_name,
            )
            return ""

    # (Optional) GetCommDataEx if you need nested table output in one call:
    # def get_comm_data_ex(self, tr_code: str, record_name: str) -> list[list[str]]:
    #     if self._api is None:
    #         return []
    #     try:
    #         data = self._api.dynamicCall("GetCommDataEx(QString, QString)", tr_code, record_name)
    #         return data
    #     except Exception:
    #         logger.exception("GetCommDataEx failed: tr=%s rec=%s", tr_code, record_name)
    #         return []

    # ==================================================================
    # Real-time registration helpers
    # ==================================================================
    def set_real_reg(self, screen_no: str, code_list: str, fid_list: str, real_type: str) -> int:
        """SetRealReg(QString, QString, QString, QString) -> int."""
        if self._api is None:
            return -1
        try:
            ret = self._api.dynamicCall(
                "SetRealReg(QString, QString, QString, QString)",
                str(screen_no),
                str(code_list),
                str(fid_list),
                str(real_type),
            )
            return int(ret)
        except Exception:
            logger.exception("SetRealReg failed (scr=%s)", screen_no)
            return -1

    def set_real_remove(self, screen_no: str, code: str) -> int:
        """SetRealRemove(QString, QString) -> int (unregister specific code)."""
        if self._api is None:
            return -1
        try:
            ret = self._api.dynamicCall(
                "SetRealRemove(QString, QString)", str(screen_no), str(code)
            )
            return int(ret)
        except Exception:
            logger.exception("SetRealRemove failed (scr=%s, code=%s)", screen_no, code)
            return -1

    def disconnect_real_data(self, screen_no: str) -> int:
        """DisconnectRealData(QString) -> int (unregister all on screen)."""
        if self._api is None:
            return -1
        try:
            ret = self._api.dynamicCall("DisconnectRealData(QString)", str(screen_no))
            return int(ret)
        except Exception:
            logger.exception("DisconnectRealData failed (scr=%s)", screen_no)
            return -1

    def get_comm_real_data(self, code: str, fid: int) -> str:
        """GetCommRealData(QString, int) -> QString."""
        if self._api is None:
            return ""
        try:
            out = self._api.dynamicCall("GetCommRealData(QString,int)", str(code), int(fid))
            return str(out)
        except Exception:
            logger.exception("GetCommRealData failed (code=%s, fid=%s)", code, fid)
            return ""

    # ==================================================================
    # Orders (kept minimal – ExecutionGateway owns routing/policies)
    # ==================================================================
    def send_order(
        self,
        rq_name: str,
        screen_no: str,
        acc_no: str,
        order_type: int,
        code: str,
        qty: int,
        price: int,
        hoga: str,
        order_no: str,
    ) -> int:
        """SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)."""
        if self._api is None:
            return -1
        try:
            ret = self._api.dynamicCall(
                "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                rq_name, screen_no, acc_no, int(order_type), code,
                int(qty), int(price), hoga, order_no,
            )
            return int(ret)
        except Exception:
            logger.exception("SendOrder failed (code=%s, qty=%s)", code, qty)
            return -1

    # ==================================================================
    # High-level TR convenience: OPT10054 (VI list)
    # ==================================================================
    def request_vi_list(
        self,
        screen_no: str,
        *,
        시장구분: str = "000",          # 000=전체, 001=코스피, 101=코스닥
        장전구분: str = "0",            # 0=전체, 1=정규, 2=시간외단일가
        종목코드: str = "",             # 공백이면 시장구분 전체 조회
        발동구분: str = "0",            # 0=전체, 1=정적, 2=동적, 3=동+정
        제외종목: str = "000000000",     # 9자리 플래그(모두 포함)
        거래량구분: str = "0",          # 0=미사용
        최소거래량: str = "",           # 미사용 시 공백
        최대거래량: str = "",           # 미사용 시 공백
        거래대금구분: str = "0",        # 0=미사용
        최소거래대금: str = "",         # 미사용 시 공백
        최대거래대금: str = "",         # 미사용 시 공백
        발동방향: str = "0",            # 0=전체
        rq_name: str = "VI_LIST",
    ) -> int:
        """Issue OPT10054 request with safe defaults for '변동성완화장치 발동종목현황'."""
        if self._api is None:
            return -1
        try:
            # Required inputs
            self.set_input_value("시장구분", 시장구분)
            self.set_input_value("장전구분", 장전구분)
            self.set_input_value("종목코드", 종목코드)
            self.set_input_value("발동구분", 발동구분)
            self.set_input_value("제외종목", 제외종목)
            self.set_input_value("거래량구분", 거래량구분)
            self.set_input_value("최소거래량", 최소거래량)
            self.set_input_value("최대거래량", 최대거래량)
            self.set_input_value("거래대금구분", 거래대금구분)
            self.set_input_value("최소거래대금", 최소거래대금)
            self.set_input_value("최대거래대금", 최대거래대금)
            self.set_input_value("발동방향", 발동방향)

            return self.comm_rq_data(rq_name, "OPT10054", 0, str(screen_no))
        except Exception:
            logger.exception("request_vi_list failed (scr=%s)", screen_no)
            return -1

    # ==================================================================
    # Aliases (if other modules use CamelCase naming)
    # ==================================================================
    # Kiwoom docs/examples often use CamelCase; provide thin aliases so both
    # styles work without touching call sites elsewhere.
    SetInputValue = set_input_value
    CommRqData = comm_rq_data
    GetRepeatCnt = get_repeat_cnt
    GetCommData = get_comm_data
    SetRealReg = set_real_reg
    SetRealRemove = set_real_remove
    DisconnectRealData = disconnect_real_data
    GetCommRealData = get_comm_real_data
    SendOrder = send_order
    RequestVIList = request_vi_list  # convenience alias
