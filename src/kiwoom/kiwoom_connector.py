# -*- coding: utf-8 -*-
"""kiwoom_connector.py
---------------------
Kiwoom OpenAPI+ connector used by the KRX-NXT arbitrage system.

This module provides a thin wrapper around the Kiwoom COM control in order
 to manage login and basic message routing. Only the pieces required for the
initial GUI prototype are implemented. The implementation follows the
workflow described in the blueprint and Kiwoom guideline documents.

The real Kiwoom control is only available on Windows. The class gracefully
falls back to a stub when the QAxWidget cannot be created (e.g. during
continuous integration on Linux).
"""

from __future__ import annotations

import logging
from typing import Optional

from PyQt5.QtCore import QObject, QEventLoop, pyqtSignal

try:
    # QAxWidget is only available on Windows platforms
    from PyQt5.QAxContainer import QAxWidget  # type: ignore
except Exception:  # pragma: no cover - executed on non-Windows environments
    QAxWidget = None  # type: ignore


logger = logging.getLogger(__name__)


class KiwoomConnector(QObject):
    """Minimal Kiwoom OpenAPI+ wrapper.

    The class focuses on the login sequence required by the trading system.
    Additional request/response helpers will be layered on top in later
    revisions.
    """

    # Signals mirrored from the underlying control. They are defined even in
    # stub mode so other modules can safely connect to them.  The Kiwoom
    # ``OnReceiveTrData`` event provides nine arguments, so our re‑emitted
    # signal must declare the same signature for the connection to succeed.
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
            # Instantiate the actual Kiwoom control
            self._api = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
            # Connect Kiwoom events to our Qt signals
            self._api.OnReceiveTrData.connect(self.tr_data_received)
            self._api.OnReceiveRealData.connect(self.real_data_received)
            self._api.OnReceiveMsg.connect(self.msg_received)
            self._api.OnReceiveChejanData.connect(self.chejan_data_received)
            self._api.OnEventConnect.connect(self._on_event_connect)
            logger.info("Kiwoom control loaded")
        else:  # pragma: no cover - running on non-Windows
            logger.warning("QAxWidget not available; KiwoomConnector running in stub mode")

    # ------------------------------------------------------------------
    # Login handling
    # ------------------------------------------------------------------
    def login(self, show_account_pw: bool = False) -> bool:
        """Login to the Kiwoom API."""
        if self._api is None:
            logger.debug("Kiwoom login skipped – running without API")
            return False

        self._login_event_loop = QEventLoop()
        self._login_result = None

        logger.info("Initiating Kiwoom login")
        self._api.dynamicCall("CommConnect()")
        self._login_event_loop.exec_()

        success = self._login_result == 0
        self.logged_in = success
        if success and show_account_pw:
            try:
                self._api.dynamicCall("KOA_Functions(QString, QString)", "ShowAccountWindow", "")
            except Exception:
                logger.exception("Failed to open account-password window")

        if success:
            logger.info("Kiwoom login successful")
        else:
            logger.error("Kiwoom login failed: %s", self._login_result)
        return success

    def _on_event_connect(self, err_code: int) -> None:
        """Handle the OnEventConnect callback from Kiwoom."""
        self._login_result = err_code
        if self._login_event_loop is not None:
            self._login_event_loop.exit()

    # Convenience helpers
    def get_login_info(self, tag: str) -> str:
        if self._api is None:
            return ""
        return self._api.dynamicCall("GetLoginInfo(QString)", tag)

    def set_real_reg(self, screen_no: str, code_list: str, fid_list: str, real_type: str) -> int:
        if self._api is None:
            return -1
        return int(
            self._api.dynamicCall(
                "SetRealReg(QString, QString, QString, QString)",
                screen_no,
                code_list,
                fid_list,
                real_type,
            )
        )

        # ------------------------------------------------------------------
        # Real-time data helpers
        # ------------------------------------------------------------------
        def get_comm_real_data(self, code: str, fid: int) -> str:
            """Wrapper for ``GetCommRealData``.

            Returns an empty string when the underlying API is unavailable
            (e.g. during unit tests on non-Windows platforms).
            """
            if self._api is None:
                return ""
            return str(self._api.dynamicCall("GetCommRealData(QString,int)", code, fid))

        def unregister_real(self, screen_no: str, code: str) -> int:
            """Unregister real-time data for *code* on *screen_no*.

            Passing ``"ALL"`` as *code* removes all registrations on the screen.
            """
            if self._api is None:
                return -1
            return int(
                self._api.dynamicCall("SetRealRemove(QString, QString)", screen_no, code)
            )

        # ------------------------------------------------------------------
        # TR (transaction request) helpers
        # ------------------------------------------------------------------
        def set_input_value(self, name: str, value: str) -> None:
            """Set input value prior to a TR request."""
            if self._api is None:
                return
            self._api.dynamicCall("SetInputValue(QString, QString)", name, value)

        def comm_rq_data(self, rq_name: str, tr_code: str, prev_next: int, screen_no: str) -> int:
            """Send a TR request via ``CommRqData``."""
            if self._api is None:
                return -1
            return int(
                self._api.dynamicCall(
                    "CommRqData(QString, QString, int, QString)",
                    rq_name,
                    tr_code,
                    prev_next,
                    screen_no,
                )
            )

        def get_comm_data(self, tr_code: str, record_name: str, index: int, item_name: str) -> str:
            """Retrieve data from the last TR response."""
            if self._api is None:
                return ""
            return str(
                self._api.dynamicCall(
                    "GetCommData(QString, QString, int, QString)",
                    tr_code,
                    record_name,
                    index,
                    item_name,
                )
            )

        def get_repeat_cnt(self, tr_code: str, record_name: str) -> int:
            """Return the number of rows in the last TR response."""
            if self._api is None:
                return 0
            return int(
                self._api.dynamicCall(
                    "GetRepeatCnt(QString, QString)", tr_code, record_name
                )
            )