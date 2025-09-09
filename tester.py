#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Replay recorded market data through MarketDataManager."""

import sys
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd
from PyQt5.QtCore import QObject, QTimer, pyqtSignal
from PyQt5.QtWidgets import QApplication

from src.core.config_manager import ConfigManager

# Default parameters; can be overridden via CLI
DEFAULT_DATA_FILE = Path("data/Real_data_for_simulation_sample_0909.xlsx")
DEFAULT_INTERVAL_MS = 0
DEFAULT_MAX_ROWS = None


class MockKiwoomConnector(QObject):
    """Minimal KiwoomConnector replacement for data replay."""

    real_data_received = pyqtSignal(str, str, str)  # code, real_type, real_data
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

    # Kiwoom API method stubs -------------------------------------------------
    def set_real_reg(self, screen_no: str, code_list: str, fid_list: str, real_type: str):
        """Simulate real-time registration by tracking requested codes."""
        self._registered_codes.update(code_list.split(";"))
        return 0

    def unregister_real(self, screen_no: str, code_list: str = "ALL"):
        return 0

    def get_comm_real_data(self, code: str, fid: int) -> str:
        return str(self._current_values.get(code, {}).get(fid, ""))

    # Replay control ----------------------------------------------------------
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
            return  # Skip unregistered codes

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
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA_FILE,
                        help="Path to parquet or Excel data file")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_MS,
                        help="Delay between ticks in milliseconds")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS,
                        help="Limit number of rows to replay")
    args = parser.parse_args()

    if not args.data.exists():
        print(f"Data file not found: {args.data}")
        return 1

    df = load_data(args.data)
    if args.max_rows is not None:
        df = df.head(args.max_rows)

    # Stub QAxWidget for non-Windows environments
    import types, sys, PyQt5
    from PyQt5.QtCore import QObject as _QObject
    QAxContainer = types.ModuleType('QAxContainer')
    class QAxWidget(_QObject):
        def __init__(self, *args, **kwargs):
            super().__init__()
    QAxContainer.QAxWidget = QAxWidget
    PyQt5.QAxContainer = QAxContainer
    sys.modules['PyQt5.QAxContainer'] = QAxContainer

    from src.core.market_data import MarketDataManager

    import os
    os.environ.setdefault('QT_QPA_PLATFORM', 'offscreen')
    app = QApplication(sys.argv)
    config = ConfigManager().load_config()

    kiwoom = MockKiwoomConnector(df, interval_ms=args.interval)
    mdm = MarketDataManager(kiwoom, config)

    # Derive symbol universe
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
    kiwoom.finished.connect(app.quit)

    kiwoom.start()
    return app.exec_()


if __name__ == "__main__":
    sys.exit(main())