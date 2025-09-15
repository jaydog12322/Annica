# -*- coding: utf-8 -*-
"""main_window.py
-----------------
PyQt GUI for the KRX–NXT arbitrage engine.

The window provides the operator controls specified in the blueprint:

* **API Log-In & PW Set-up** – triggers Kiwoom login and optional
  account‑password dialog.
* **Load Symbols** – loads the trading universe from an Excel file and
  subscribes to real‑time data.
* **System Log / Event Feed** – displays telemetry messages.
* **Active Symbols** – table showing per-symbol quote information.
* **Pair Monitor** – table placeholder for pair states.

Only basic interactions are implemented; the widgets mainly act as hooks for
future expansion.  The class exposes ``log_event``, ``update_session_state`` and
``update_quote`` methods which are called by the main application and core
modules.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import List

try:  # pragma: no cover - optional dependency for Excel loading
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas may not be installed in test env
    pd = None
from PyQt5.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QTextEdit,
    QTableWidget,
    QTableWidgetItem,
    QLabel,
    QFileDialog,
)

from PyQt5.QtCore import Qt, QTimer

logger = logging.getLogger(__name__)


class MainWindow(QMainWindow):
    """Main operator GUI."""

    def __init__(self, config, parent: QWidget | None = None):
        super().__init__(parent)

        self.config = config
        self.kiwoom = None  # populated by main.py
        self.market_data = None
        self.session_state = None
        self.spread_engine = None
        self.router = None
        self.throttler = None
        self.execution_gateway = None
        self.pair_manager = None
        self.vi_lister = None

        self.setWindowTitle("KRX-NXT Arbitrage System")
        self.resize(1000, 700)

        self._build_ui()

    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)

        layout = QVBoxLayout(central)

        # -- Control buttons -------------------------------------------------
        btn_row = QHBoxLayout()
        self.login_btn = QPushButton("API Log-In & PW Set-up")
        self.login_btn.clicked.connect(self._on_login_clicked)
        btn_row.addWidget(self.login_btn)

        self.load_btn = QPushButton("Load symbols")
        self.load_btn.clicked.connect(self._on_load_symbols)
        btn_row.addWidget(self.load_btn)

        self.vi_btn = QPushButton("Start VI Lister")
        self.vi_btn.clicked.connect(self._on_start_vi_lister)
        btn_row.addWidget(self.vi_btn)

        layout.addLayout(btn_row)

        # -- Status labels ---------------------------------------------------
        status_row = QHBoxLayout()
        self.session_label = QLabel("Session: DISARMED")
        status_row.addWidget(self.session_label)
        status_row.addStretch()
        layout.addLayout(status_row)

        # -- Active symbols table -------------------------------------------
        self.symbol_table = QTableWidget(0, 5)
        self.symbol_table.setHorizontalHeaderLabels(
            ["Symbol", "KRX Bid", "KRX Ask", "NXT Bid", "NXT Ask"]
        )
        layout.addWidget(self.symbol_table)

        # -- Pair monitor placeholder ---------------------------------------
        self.pair_table = QTableWidget(0, 4)
        self.pair_table.setHorizontalHeaderLabels(
            ["Pair", "State", "Leg1", "Leg2"]
        )
        layout.addWidget(self.pair_table)

        # -- VI monitor ------------------------------------------------------
        self.vi_table = QTableWidget(0, 1)
        self.vi_table.setHorizontalHeaderLabels(["VI Halted Symbols"])
        layout.addWidget(self.vi_table)

        # Timer for polling execution log for pair update
        self._pair_log_path = Path("logs") / f"execution_{datetime.now():%Y%m%d}.log"
        self._pair_log_pos = 0
        self._pair_timer = QTimer(self)
        self._pair_timer.setInterval(1000)
        self._pair_timer.timeout.connect(self._poll_pair_log)
        self._pair_timer.start()

        # -- Event feed / log -----------------------------------------------
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)

    # ------------------------------------------------------------------
    # Button handlers
    def _on_login_clicked(self) -> None:
        if not self.kiwoom:
            self.log_event("Kiwoom connector not available")
            return
        # Always show the account password window after login
        if self.kiwoom.login(show_account_pw=True):
            self.log_event("Kiwoom login successful")

        else:
            self.log_event("Kiwoom login failed")

    def _on_load_symbols(self) -> None:
        if not self.market_data:
            self.log_event("MarketDataManager not set")
            return
        if not self.kiwoom or not getattr(self.kiwoom, "logged_in", False):
            self.log_event("Please log in before loading symbols")
            return

        path, _ = QFileDialog.getOpenFileName(
            self, "Select symbol universe", str(Path.cwd()), "Excel Files (*.xlsx)"
        )
        if not path:
            return

        try:
            if pd is None:
                raise RuntimeError("pandas not installed")
            df = pd.read_excel(path)
            symbols: List[str] = df.iloc[:, 0].dropna().astype(str).tolist()
            self.market_data.load_symbol_universe(symbols)
            self.market_data.subscribe_real_time_data()
            if self.spread_engine and not self.spread_engine.batch_timer.isActive():
                self.spread_engine.start()
            self.log_event(f"Loaded {len(symbols)} symbols")
        except Exception as exc:  # pragma: no cover - dialog path issues
            self.log_event(f"Failed to load symbols: {exc}")
            logger.exception("Failed to load symbols")

    def _on_start_vi_lister(self) -> None:
        if not self.vi_lister:
            self.log_event("VI Lister not set")
            return
        if not self.kiwoom or not getattr(self.kiwoom, "logged_in", False):
            self.log_event("Please log in before starting VI Lister")
            return
        try:
            self.vi_lister.start()
            self.log_event("VI Lister started")
        except Exception as exc:  # pragma: no cover - GUI only
            self.log_event(f"Failed to start VI Lister: {exc}")
            logger.exception("Failed to start VI Lister")

    # ------------------------------------------------------------------
    # Methods called from core modules
    def log_event(self, message: str) -> None:
        """Append a message to the event feed."""
        self.log_view.append(message)

    def update_session_state(self, state: str) -> None:
        """Update the session status label."""
        self.session_label.setText(f"Session: {state}")

    def update_quote(self, symbol: str, venue: str) -> None:  # pragma: no cover - GUI only
        """Update quote information for a symbol.

        Parameters
        ----------
        symbol: str
            The symbol code.
        venue: str
            Either ``"KRX"`` or ``"NXT"``; determines which columns to update.
        """
        row = self._find_symbol_row(symbol)
        if row is None:
            row = self.symbol_table.rowCount()
            self.symbol_table.insertRow(row)
            self.symbol_table.setItem(row, 0, QTableWidgetItem(symbol))

        snapshot = self.market_data.quotes.get(symbol) if self.market_data else None
        if not snapshot:
            return

        if venue == "KRX":
            self.symbol_table.setItem(row, 1, QTableWidgetItem(str(snapshot.krx_bid)))
            self.symbol_table.setItem(row, 2, QTableWidgetItem(str(snapshot.krx_ask)))
        else:
            self.symbol_table.setItem(row, 3, QTableWidgetItem(str(snapshot.nxt_bid)))
            self.symbol_table.setItem(row, 4, QTableWidgetItem(str(snapshot.nxt_ask)))

    def update_vi_status(self, symbol: str, in_vi: bool) -> None:  # pragma: no cover - GUI only
        """Update the VI monitor table when a symbol's status changes."""
        row = self._find_vi_row(symbol)
        if in_vi:
            if row is None:
                row = self.vi_table.rowCount()
                self.vi_table.insertRow(row)
                self.vi_table.setItem(row, 0, QTableWidgetItem(symbol))
        else:
            if row is not None:
                self.vi_table.removeRow(row)

    # Pair monitor -----------------------------------------------------
    def _poll_pair_log(self) -> None:  # pragma: no cover - GUI only
        """Read new entries from execution log and update table."""
        if not self._pair_log_path.exists():
            return
        with self._pair_log_path.open() as fh:
            fh.seek(self._pair_log_pos)
            lines = fh.readlines()
            self._pair_log_pos = fh.tell()
        for line in lines:
            self._handle_pair_log_line(line.strip())

    def _handle_pair_log_line(self, line: str) -> None:
        try:
            if "|" not in line:
                return
            _, payload = line.split("|", 1)
            parts = [p.strip() for p in payload.split(",")]
            event = parts[0]
            if not event.startswith("PAIR"):
                return
            pair_id = parts[1] if len(parts) > 1 else ""
            leg1 = parts[3] if len(parts) > 3 else ""
            leg2 = parts[4] if len(parts) > 4 else ""
            self._update_pair_row(pair_id, event, leg1, leg2)
        except Exception as exc:  # pragma: no cover - parsing errors not critical
            logger.warning(f"Failed to parse execution log line '{line}': {exc}")

    def _update_pair_row(self, pair_id: str, state: str, leg1: str, leg2: str) -> None:
        row = self._find_pair_row(pair_id)
        if row is None:
            row = self.pair_table.rowCount()
            self.pair_table.insertRow(row)
            self.pair_table.setItem(row, 0, QTableWidgetItem(pair_id))
        self.pair_table.setItem(row, 1, QTableWidgetItem(state))
        if leg1:
            self.pair_table.setItem(row, 2, QTableWidgetItem(leg1))
        if leg2:
            self.pair_table.setItem(row, 3, QTableWidgetItem(leg2))

    # Utility -----------------------------------------------------------
    def _find_symbol_row(self, symbol: str) -> int | None:
        for row in range(self.symbol_table.rowCount()):
            item = self.symbol_table.item(row, 0)
            if item and item.text() == symbol:
                return row
        return None

    def _find_pair_row(self, pair_id: str) -> int | None:
        for row in range(self.pair_table.rowCount()):
            item = self.pair_table.item(row, 0)
            if item and item.text() == pair_id:
                return row

        return None

    def _find_vi_row(self, symbol: str) -> int | None:
        for row in range(self.vi_table.rowCount()):
            item = self.vi_table.item(row, 0)
            if item and item.text() == symbol:
                return row
        return None