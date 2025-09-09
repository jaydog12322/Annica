#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Replay recorded market data through the entire trading stack."""

import sys
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd
from PyQt5.QtCore import QObject, QTimer, pyqtSignal
from PyQt5.QtWidgets import QApplication

from src.core.config_manager import ConfigManager
from src.core.session_state import SessionStateManager
from src.core.spread_engine import SpreadEngine
from src.core.router import Router
from src.core.throttler import Throttler
from src.kiwoom.execution_gateway import ExecutionGateway
from src.core.pair_manager import PairManager
from src.utils.logger import setup_logging


# Default parameters; can be overridden via CLI
DEFAULT_DATA_FILE = Path("data/market_data_20250909.xlsx")
DEFAULT_INTERVAL_MS = 0
DEFAULT_MAX_ROWS = None


class MockKiwoomConnector(QObject):
    """Mock Kiwoom connector providing quote replay and order simulation."""

    # Signals matching KiwoomConnector
    connected = pyqtSignal(int)
    tr_data_received = pyqtSignal(str, str, str, str, str)
    real_data_received = pyqtSignal(str, str, str)
    chejan_data_received = pyqtSignal(str, int, str)
    msg_received = pyqtSignal(str, str, str, str)
    finished = pyqtSignal()

    def __init__(self, data: pd.DataFrame, interval_ms: int = 0):
        super().__init__()
        self._data = data.reset_index(drop=True)
        self._interval = interval_ms
        self._index = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._emit_next)
        self._current_values = {}
        self._registered_codes = set()

        # Order simulation state
        self.is_connected = True
        self.account = "1234567890"
        self.account_list = [self.account]
        self.user_id = "tester"
        self.order_counter = 1000
        self.pending_orders = {}

    # ---- Kiwoom API stubs -------------------------------------------------
    def set_real_reg(self, screen_no: str, code_list: str, fid_list: str, real_type: str):

        self._registered_codes.update(code_list.split(";"))
        return 0

    def unregister_real(self, screen_no: str, code_list: str = "ALL"):
        return 0

    def get_comm_real_data(self, code: str, fid: int) -> str:
        return str(self._current_values.get(code, {}).get(fid, ""))

    # ---- Order interface ---------------------------------------------------
    def send_order(self, rq_name, screen_no, acc_no, order_type, code, qty, price, hoga_gb, org_order_no):
        order_number = str(self.order_counter)
        self.order_counter += 1
        self.pending_orders[order_number] = {
            "rq_name": rq_name,
            "code": code,
            "qty": qty,
            "price": price,
            "order_type": order_type,
        }
        QTimer.singleShot(50, lambda: self._simulate_tr_response(rq_name, order_number))
        QTimer.singleShot(100, lambda: self._simulate_fill(order_number))
        return order_number

    def send_nxt_order(self, order_type, rq_name, screen_no, acc_no, code, qty, price, hoga_gb, org_order_no=""):
        return self.send_order(rq_name, screen_no, acc_no, order_type, code, qty, price, hoga_gb, org_order_no)

    def get_comm_data(self, tr_code, rq_name, index, item_name):
        if item_name == "주문번호":
            return getattr(self, "_mock_tr_data", "")
        return ""

    def get_chejan_data(self, fid):
        return getattr(self, "_mock_chejan_data", {}).get(fid, "")

    def _simulate_tr_response(self, rq_name, order_number):
        self._mock_tr_data = order_number
        self.tr_data_received.emit("200", rq_name, "ORDER", "", "")

    def _simulate_fill(self, order_number):
        order_info = self.pending_orders.get(order_number)
        if not order_info:
            return
        self._mock_chejan_data = {
            9203: order_number,
            902: "0",  # 미체결수량
            910: str(order_info.get("price", 0) or 50000),
            911: str(order_info.get("qty", 0)),
            909: f"exec_{order_number}",
            913: "체결",
        }
        self.chejan_data_received.emit("0", 5, "9203;902;910;911;909;913")

    # ---- Replay control ----------------------------------------------------
    def start(self):
        self._timer.start(self._interval)

    def _emit_next(self):
        if self._index >= len(self._data):
            self._timer.stop()
            self.finished.emit()
            return

        row = self._data.iloc[self._index]
        self._index += 1

        code = str(row["raw_code"])
        if self._registered_codes and code not in self._registered_codes:
            return

        self._current_values[code] = {
            41: row.get("fid_41", ""),
            51: row.get("fid_51", ""),
            61: row.get("fid_61", ""),
            71: row.get("fid_71", ""),
        }
        self.real_data_received.emit(code, row.get("real_type", ""), "")


# ----------------------------------------------------------------------------

def load_data(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_excel(path)
    # Clean and sort
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(str).str.strip("'"))
        df = df.sort_values("timestamp")
    return df


def main():
    parser = ArgumentParser(description="Replay recorded market data")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA_FILE, help="Path to data file")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_MS, help="Delay between ticks (ms)")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS, help="Limit number of rows to replay")
    args = parser.parse_args()

    if not args.data.exists():
        print(f"Data file not found: {args.data}")
        return 1

    df = load_data(args.data)
    if args.max_rows is not None:
        df = df.head(args.max_rows)

    import os
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    setup_logging()
    app = QApplication(sys.argv)

    # Stub QAxWidget for headless environments
    import types, PyQt5
    from PyQt5.QtCore import QObject as _QObject
    QAxContainer = types.ModuleType("QAxContainer")

    class QAxWidget(_QObject):
        def __init__(self, *args, **kwargs):
            super().__init__()

    QAxContainer.QAxWidget = QAxWidget
    PyQt5.QAxContainer = QAxContainer
    sys.modules["PyQt5.QAxContainer"] = QAxContainer

    from src.core.market_data import MarketDataManager

    config = ConfigManager().load_config()

    kiwoom = MockKiwoomConnector(df, interval_ms=args.interval)
    session_state = SessionStateManager(config)
    session_state.manual_arm()
    mdm = MarketDataManager(kiwoom, config)
    spread_engine = SpreadEngine(mdm, session_state, config)
    router = Router(config)
    throttler = Throttler(config)
    execution = ExecutionGateway(kiwoom, throttler, config)
    pair_manager = PairManager(router, throttler, execution, session_state, config)


    symbols = sorted({str(code).replace("_NX", "") for code in df["raw_code"].unique()})
    mdm.load_symbol_universe(symbols)
    mdm.subscribe_real_time_data()

    def on_quote_updated(symbol: str, venue: str):
        q = mdm.get_quote(symbol)
        if venue == "KRX":
            print(f"{symbol} KRX bid={q.krx_bid} ask={q.krx_ask} size={q.krx_bid_size}/{q.krx_ask_size}")
        else:
            print(f"{symbol} NXT bid={q.nxt_bid} ask={q.nxt_ask} size={q.nxt_bid_size}/{q.nxt_ask_size}")

    mdm.quote_updated.connect(on_quote_updated)
    mdm.quote_updated.connect(lambda s, v: spread_engine.market_data.get_quote(s))
    spread_engine.signal_generated.connect(pair_manager.handle_signal)
    pair_manager.pair_state_changed.connect(lambda pid, st: print(f"PAIR {pid} {st}"))
    pair_manager.pair_completed.connect(lambda pair: print(f"PAIR DONE {pair.pair_id}"))
    kiwoom.finished.connect(app.quit)

    spread_engine.start()
    kiwoom.start()
    return app.exec_()


if __name__ == "__main__":
    sys.exit(main())
