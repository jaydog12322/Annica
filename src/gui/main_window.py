from PyQt5.QtWidgets import (
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QLabel,
    QTableWidget,
    QSplitter,
    QTextEdit,
    QTableWidgetItem,
)


class MainWindow(QMainWindow):
    """Main GUI window for the arbitrage system.

    Layout is inspired by the blueprint's ops view and provides:
    - Top status bar (session state, orders/sec, tokens free)
    - Active symbols table
    - Pair monitor table
    - Event feed log
    """

    def __init__(self, config):
        super().__init__()
        self.config = config

        # Core component references to keep them alive
        self.kiwoom = None
        self.market_data = None
        self.session_state = None
        self.spread_engine = None
        self.router = None
        self.throttler = None
        self.execution_gateway = None
        self.pair_manager = None
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("KRX-NXT Arbitrage System")
        self.setGeometry(100, 100, 1200, 800)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        root_layout = QVBoxLayout(central_widget)

        # Top status bar
        status_layout = QHBoxLayout()
        self.session_label = QLabel("Session: DISARMED")
        self.orders_label = QLabel("Orders/sec: 0")
        self.tokens_label = QLabel("Tokens free: 0")
        status_layout.addWidget(self.session_label)
        status_layout.addWidget(self.orders_label)
        status_layout.addWidget(self.tokens_label)
        status_layout.addStretch()
        root_layout.addLayout(status_layout)

        # Splitter separates symbol tables and event feed
        splitter = QSplitter()
        root_layout.addWidget(splitter)

        # Left side: Active symbols table
        symbols_widget = QWidget()
        symbols_layout = QVBoxLayout(symbols_widget)
        self.symbols_table = QTableWidget(0, 5)
        self.symbols_table.setHorizontalHeaderLabels(
            ["Symbol", "KRX Bid", "KRX Ask", "NXT Bid", "NXT Ask"]
        )
        symbols_layout.addWidget(self.symbols_table)
        splitter.addWidget(symbols_widget)

        # Right side: Pair monitor table and event feed
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        self.pair_table = QTableWidget(0, 4)
        self.pair_table.setHorizontalHeaderLabels(
            ["Symbol", "State", "Entry Time", "Notes"]
        )
        right_layout.addWidget(self.pair_table)

        self.event_feed = QTextEdit()
        self.event_feed.setReadOnly(True)
        self.event_feed.append("Event feed initialized...")
        right_layout.addWidget(self.event_feed)

        splitter.addWidget(right_widget)

        def update_session_state(self, state: str):
            """Update session state label."""
            self.session_label.setText(f"Session: {state}")
            self.log_event(f"Session state changed: {state}")

        def update_quote(self, symbol: str, venue: str):
            """Update quote table when market data changes."""
            if not self.market_data:
                return
            quote = self.market_data.get_quote(symbol)
            if not quote:
                return

            row = None
            for r in range(self.symbols_table.rowCount()):
                item = self.symbols_table.item(r, 0)
                if item and item.text() == symbol:
                    row = r
                    break

            if row is None:
                row = self.symbols_table.rowCount()
                self.symbols_table.insertRow(row)
                self.symbols_table.setItem(row, 0, QTableWidgetItem(symbol))

            self.symbols_table.setItem(row, 1, QTableWidgetItem(str(quote.krx_bid)))
            self.symbols_table.setItem(row, 2, QTableWidgetItem(str(quote.krx_ask)))
            self.symbols_table.setItem(row, 3, QTableWidgetItem(str(quote.nxt_bid)))
            self.symbols_table.setItem(row, 4, QTableWidgetItem(str(quote.nxt_ask)))

            self.log_event(f"Quote updated: {symbol} ({venue})")

        def log_event(self, message: str):
            """Append message to event feed."""
            self.event_feed.append(message)