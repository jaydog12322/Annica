# -*- coding: utf-8 -*-
"""
kiwoom_connector.py
-------------------
Kiwoom OpenAPI+ Connection and Event Handling Module

This module handles the core Kiwoom API connection, login process, and event routing
according to the Kiwoom OpenAPI+ guidelines.

Key Features:
- Proper login sequence (CommConnect -> OnEventConnect)
- Account password window handling
- Event-driven architecture for TR and real-time data
- Rate limiting enforcement (5 orders/sec, 5 queries/sec)
- Session state management via FID 215
"""

import logging
import time
from typing import Dict, Any, Optional, Callable
from PyQt5.QtCore import QObject, pyqtSignal, QTimer, QEventLoop
from PyQt5.QtWidgets import QInputDialog, QLineEdit

try:  # QAxContainer is only available on Windows
    from PyQt5.QAxContainer import QAxWidget  # type: ignore
except Exception:  # pragma: no cover - platform fallback
    class _DummySignal:
        """Lightweight stand-in for pyqtSignal in headless tests."""

        def connect(self, *args, **kwargs):  # noqa: D401 - stub connect
            """Ignore all connections."""
            return None
    class QAxWidget(QObject):  # minimal stub for headless test environments
        def __init__(self, *args, **kwargs):
            super().__init__()
            # provide dummy event attributes so event wiring doesn't fail
            self.OnEventConnect = _DummySignal()
            self.OnReceiveTrData = _DummySignal()
            self.OnReceiveRealData = _DummySignal()
            self.OnReceiveChejanData = _DummySignal()
            self.OnReceiveMsg = _DummySignal()

        def dynamicCall(self, *args, **kwargs):  # noqa: D401 - stub method
            """Return 0 for all calls."""
            return 0

        def winId(self):
            """Return a fake window handle."""
            return 0

logger = logging.getLogger(__name__)


class KiwoomConnector(QObject):
    """
    Main Kiwoom API connector class.

    Handles connection, login, and provides interfaces for:
    - TR requests (CommRqData)
    - Real-time data registration (SetRealReg)
    - Order sending (SendOrder, NXT orders)
    - Event processing (OnReceiveTrData, OnReceiveChejanData, etc.)
    """

    # PyQt signals for event communication
    connected = pyqtSignal(int)  # OnEventConnect event
    tr_data_received = pyqtSignal(str, str, str, str, str)  # OnReceiveTrData
    real_data_received = pyqtSignal(str, str, str)  # OnReceiveRealData
    chejan_data_received = pyqtSignal(str, int, str)  # OnReceiveChejanData
    msg_received = pyqtSignal(str, str, str, str)  # OnReceiveMsg

    def __init__(self):
        super().__init__()

        # Initialize Kiwoom OCX control
        # The widget must obtain a native window handle before calling
        # ``CommConnect`` or Kiwoom will terminate with
        # "핸들값이 없습니다 프로그램을 종료합니다".  Constructing the control with
        # the CLSID immediately loads the COM object and calling ``winId`` forces
        # creation of the underlying HWND.
        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.winId()  # Ensure window handle is created eagerly

        # Connection state
        self.is_connected = False
        self.account_list = []
        self.account = ""
        self.user_id = ""
        self.account_pw = ""

        # Rate limiting
        self.last_tr_time = 0
        self.last_order_time = 0
        self.tr_limit_ms = 200  # 5 requests per second = 200ms interval
        self.order_limit_ms = 200  # 5 orders per second = 200ms interval

        # Event handlers storage
        self.tr_handlers: Dict[str, Callable] = {}
        self.real_handlers: Dict[str, Callable] = {}

        # Connect OCX events to our handlers
        self._connect_events()

        logger.info("KiwoomConnector initialized")

    def _connect_events(self):
        """Connect OCX events to our event handlers"""
        try:
            self.ocx.OnEventConnect.connect(self._on_event_connect)
            self.ocx.OnReceiveTrData.connect(self._on_receive_tr_data)
            self.ocx.OnReceiveRealData.connect(self._on_receive_real_data)
            self.ocx.OnReceiveChejanData.connect(self._on_receive_chejan_data)
            self.ocx.OnReceiveMsg.connect(self._on_receive_msg)

            logger.info("Kiwoom OCX events connected")

        except Exception as e:
            logger.error(f"Failed to connect OCX events: {e}")
            raise

    def login(self, show_account_pw: bool = False, timeout_ms: int = 10000) -> bool:
        """Connect to Kiwoom and optionally prompt for account password.

        This wraps the ``CommConnect`` sequence and waits synchronously for the
        ``OnEventConnect`` callback before returning.  If ``show_account_pw`` is
        ``True`` the Kiwoom account password window will be displayed once the
        session is established so the user can save their password locally as
        required by the OpenAPI guidelines.

        Args:
            show_account_pw: Whether to open the account-password window after
                login so the user can store the password.
            timeout_ms: Maximum time to wait for the ``OnEventConnect`` signal.

        Returns:
            ``True`` if login succeeded, ``False`` otherwise.
        """

        if self.is_connected:
            return True

        if not self.connect_to_server():
            return False

        loop = QEventLoop()
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        self.connected.connect(loop.quit)
        timer.start(timeout_ms)
        loop.exec_()

        if not self.is_connected:
            logger.error("Kiwoom login failed or timed out")
            return False

        if show_account_pw:
            self.show_account_window()

        return True

    def connect_to_server(self) -> bool:
        """
        Initiate connection to Kiwoom server.
        This will open the Kiwoom login window.
        """
        try:
            logger.info("Initiating connection to Kiwoom server...")

            # Call CommConnect - this opens the login window
            result = self.ocx.dynamicCall("CommConnect()")

            if result == 0:
                logger.info("CommConnect() called successfully - login window should appear")
                return True
            else:
                logger.error(f"CommConnect() failed with error code: {result}")
                return False

        except Exception as e:
            logger.error(f"Exception in connect_to_server: {e}")
            return False

    def _on_event_connect(self, err_code: int):
        """
        Handle OnEventConnect event.
        err_code: 0 = success, others = error codes
        """
        logger.info(f"OnEventConnect received with error code: {err_code}")

        if err_code == 0:
            self.is_connected = True
            logger.info("Successfully connected to Kiwoom server")

            # Get login info
            self._get_login_info()

        else:
            self.is_connected = False
            logger.error(f"Failed to connect to Kiwoom server. Error code: {err_code}")

        # Emit signal for GUI/other components
        self.connected.emit(err_code)

    def _get_login_info(self):
        """Get user login information after successful connection"""
        try:
            # Get account list
            account_cnt = self.ocx.dynamicCall("GetLoginInfo(QString)", "ACCOUNT_CNT")
            accounts = self.ocx.dynamicCall("GetLoginInfo(QString)", "ACCLIST")
            self.user_id = self.ocx.dynamicCall("GetLoginInfo(QString)", "USER_ID")
            user_name = self.ocx.dynamicCall("GetLoginInfo(QString)", "USER_NAME")

            if accounts:
                self.account_list = accounts.split(';')[:-1]  # Remove last empty element

            logger.info(f"Login info - User: {user_name} ({self.user_id})")
            logger.info(f"Available accounts: {self.account_list}")

            # Set default account if available
            if self.account_list:
                self.account = self.account_list[0]
                logger.info(f"Default account set to: {self.account}")

        except Exception as e:
            logger.error(f"Failed to get login info: {e}")

    def show_account_window(self):
        """
        Show account password window for saving credentials.
        Should be called once after first login or when account password needs to be updated.
        """
        try:
            logger.info("Opening account password window...")
            result = self.ocx.dynamicCall('KOA_Functions(QString, QString)',
                                          "ShowAccountWindow", "")
            logger.info(f"ShowAccountWindow result: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to show account window: {e}")
            return False

    def prompt_account_password(self) -> str:
        """Prompt the user for their 4-digit account password."""
        try:
            pw, ok = QInputDialog.getText(
                None,
                "Account Password",
                "Enter 4-digit account password:",
                QLineEdit.Password,
            )
            if ok:
                self.account_pw = pw
            return self.account_pw
        except Exception as e:
            logger.error(f"Failed to get account password: {e}")
            return ""

    def set_input_value(self, id: str, value: str):
        """Set input value for TR requests"""
        try:
            self.ocx.dynamicCall("SetInputValue(QString, QString)", id, value)
        except Exception as e:
            logger.error(f"Failed to set input value {id}={value}: {e}")

    def comm_rq_data(self, rq_name: str, tr_code: str, next: int, screen_no: str) -> int:
        """
        Send TR request with rate limiting.

        Args:
            rq_name: Request name
            tr_code: Transaction code
            next: 0=first request, 2=next request
            screen_no: Screen number

        Returns:
            0 = success, others = error codes
        """
        # Rate limiting check
        current_time = time.time() * 1000
        if current_time - self.last_tr_time < self.tr_limit_ms:
            wait_time = self.tr_limit_ms - (current_time - self.last_tr_time)
            logger.warning(f"TR rate limit hit, waiting {wait_time:.1f}ms")
            time.sleep(wait_time / 1000)

        try:
            result = self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)",
                                          rq_name, tr_code, next, screen_no)
            self.last_tr_time = time.time() * 1000

            if result != 0:
                logger.error(f"CommRqData failed. rq_name={rq_name}, tr_code={tr_code}, error={result}")

            return result

        except Exception as e:
            logger.error(f"Exception in comm_rq_data: {e}")
            return -1

    def set_real_reg(self, screen_no: str, code_list: str, fid_list: str, real_type: str) -> int:
        """
        Register real-time data.

        Args:
            screen_no: Screen number
            code_list: Symbol codes separated by ';'
            fid_list: FID numbers separated by ';'
            real_type: "0"=register, "1"=unregister

        Returns:
            0 = success, others = error codes
        """
        try:
            result = self.ocx.dynamicCall("SetRealReg(QString, QString, QString, QString)",
                                          screen_no, code_list, fid_list, real_type)

            if result != 0:
                logger.error(f"SetRealReg failed. screen={screen_no}, error={result}")
            else:
                logger.debug(
                    f"Real-time registration successful. screen={screen_no}, codes={len(code_list.split(';'))}")

            return result

        except Exception as e:
            logger.error(f"Exception in set_real_reg: {e}")
            return -1

    def send_order(self, rq_name: str, screen_no: str, acc_no: str, order_type: int,
                   code: str, qty: int, price: int, hoga_gb: str, org_order_no: str) -> str:
        """
        Send regular KRX order with rate limiting.

        Args:
            rq_name: Request name
            screen_no: Screen number
            acc_no: Account number
            order_type: 1=buy, 2=sell, 3=cancel, 4=modify
            code: Symbol code
            qty: Quantity
            price: Price (0 for market orders)
            hoga_gb: Order type code (00=limit, 03=market, etc.)
            org_order_no: Original order number (for cancel/modify)

        Returns:
            Order number if successful, empty string if failed
        """
        # Rate limiting check
        current_time = time.time() * 1000
        if current_time - self.last_order_time < self.order_limit_ms:
            wait_time = self.order_limit_ms - (current_time - self.last_order_time)
            logger.warning(f"Order rate limit hit, waiting {wait_time:.1f}ms")
            time.sleep(wait_time / 1000)

        try:
            result = self.ocx.dynamicCall(
                "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                [rq_name, screen_no, acc_no, order_type, code, qty, price, hoga_gb, org_order_no])

            self.last_order_time = time.time() * 1000

            logger.info(f"Order sent: {rq_name}, code={code}, qty={qty}, price={price}, hoga={hoga_gb}")

            return result

        except Exception as e:
            logger.error(f"Exception in send_order: {e}")
            return ""

    def send_nxt_order(self, order_type: int, rq_name: str, screen_no: str, acc_no: str,
                       code: str, qty: int, price: int, hoga_gb: str, org_order_no: str = "") -> str:
        """
        Send NXT-specific order.

        Args:
            order_type: 21=NXT buy, 22=NXT sell, 23=NXT cancel, 25=NXT modify
            Other args same as send_order

        Returns:
            Order number if successful, empty string if failed
        """
        # Rate limiting check (same as regular orders)
        current_time = time.time() * 1000
        if current_time - self.last_order_time < self.order_limit_ms:
            wait_time = self.order_limit_ms - (current_time - self.last_order_time)
            logger.warning(f"NXT order rate limit hit, waiting {wait_time:.1f}ms")
            time.sleep(wait_time / 1000)

        try:
            result = self.ocx.dynamicCall(
                "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                [rq_name, screen_no, acc_no, order_type, code, qty, price, hoga_gb, org_order_no])

            self.last_order_time = time.time() * 1000

            logger.info(f"NXT order sent: type={order_type}, code={code}, qty={qty}, price={price}, hoga={hoga_gb}")

            return result

        except Exception as e:
            logger.error(f"Exception in send_nxt_order: {e}")
            return ""

    def get_comm_data(self, tr_code: str, rq_name: str, index: int, item_name: str) -> str:
        """Get data from TR response"""
        try:
            return self.ocx.dynamicCall("GetCommData(QString, QString, int, QString)",
                                        tr_code, rq_name, index, item_name).strip()
        except Exception as e:
            logger.error(f"Exception in get_comm_data: {e}")
            return ""

    def get_comm_real_data(self, code: str, fid: int) -> str:
        """Get real-time data"""
        try:
            return self.ocx.dynamicCall("GetCommRealData(QString, int)", code, fid).strip()
        except Exception as e:
            logger.error(f"Exception in get_comm_real_data: {e}")
            return ""

    def get_chejan_data(self, fid: int) -> str:
        """Get Chejan data"""
        try:
            return self.ocx.dynamicCall("GetChejanData(int)", fid).strip()
        except Exception as e:
            logger.error(f"Exception in get_chejan_data: {e}")
            return ""

    def _on_receive_tr_data(self, screen_no: str, rq_name: str, tr_code: str,
                            record_name: str, next: str):
        """Handle OnReceiveTrData event"""
        logger.debug(f"TR Data received: screen={screen_no}, rq_name={rq_name}, tr_code={tr_code}")

        # Call registered handler if exists
        if rq_name in self.tr_handlers:
            try:
                self.tr_handlers[rq_name](screen_no, rq_name, tr_code, record_name, next)
            except Exception as e:
                logger.error(f"Error in TR handler for {rq_name}: {e}")

        # Emit signal for other components
        self.tr_data_received.emit(screen_no, rq_name, tr_code, record_name, next)

    def _on_receive_real_data(self, code: str, real_type: str, real_data: str):
        """Handle OnReceiveRealData event"""
        logger.debug(f"Real data received: code={code}, type={real_type}")

        # Call registered handler if exists
        handler_key = f"{real_type}_{code}"
        if handler_key in self.real_handlers:
            try:
                self.real_handlers[handler_key](code, real_type, real_data)
            except Exception as e:
                logger.error(f"Error in real data handler for {handler_key}: {e}")

        # Emit signal for other components
        self.real_data_received.emit(code, real_type, real_data)

    def _on_receive_chejan_data(self, gubun: str, item_cnt: int, fid_list: str):
        """
        Handle OnReceiveChejanData event.
        gubun: 0=order/fill, 1=balance
        """
        logger.debug(f"Chejan data received: gubun={gubun}, item_cnt={item_cnt}")

        # Emit signal for ExecutionGateway to handle
        self.chejan_data_received.emit(gubun, item_cnt, fid_list)

    def _on_receive_msg(self, screen_no: str, rq_name: str, tr_code: str, msg: str):
        """Handle OnReceiveMsg event"""
        logger.info(f"Message received: screen={screen_no}, rq_name={rq_name}, msg={msg}")

        # Emit signal for other components
        self.msg_received.emit(screen_no, rq_name, tr_code, msg)

    def register_tr_handler(self, rq_name: str, handler: Callable):
        """Register handler for specific TR request"""
        self.tr_handlers[rq_name] = handler
        logger.debug(f"TR handler registered for {rq_name}")

    def register_real_handler(self, real_type: str, code: str, handler: Callable):
        """Register handler for specific real-time data"""
        handler_key = f"{real_type}_{code}"
        self.real_handlers[handler_key] = handler
        logger.debug(f"Real data handler registered for {handler_key}")

    def unregister_real(self, screen_no: str, code_list: str = "ALL"):
        """Unregister real-time data"""
        try:
            result = self.ocx.dynamicCall("SetRealRemove(QString, QString)", screen_no, code_list)
            logger.debug(f"Real-time unregistration: screen={screen_no}, result={result}")
            return result
        except Exception as e:
            logger.error(f"Exception in unregister_real: {e}")
            return -1

    def disconnect(self):
        """Disconnect from Kiwoom server"""
        try:
            if self.is_connected:
                self.ocx.dynamicCall("CommTerminate()")
                self.is_connected = False
                logger.info("Disconnected from Kiwoom server")
        except Exception as e:
            logger.error(f"Exception in disconnect: {e}")

    def get_server_gubun(self) -> str:
        """Get server type (실서버/모의투자)"""
        try:
            return self.ocx.dynamicCall("GetLoginInfo(QString)", "GetServerGubun")
        except Exception as e:
            logger.error(f"Exception in get_server_gubun: {e}")
            return ""