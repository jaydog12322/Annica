from PyQt5.QtWidgets import QMainWindow, QVBoxLayout, QWidget, QLabel


class MainWindow(QMainWindow):
    """
    Main GUI Window

    Planned Features:
    - Connection status
    - Active symbols table
    - Live metrics
    - Configuration controls
    """

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle("KRX-NXT Arbitrage System")
        self.setGeometry(100, 100, 1200, 800)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout(central_widget)

        # Placeholder content
        status_label = QLabel("KRX-NXT Arbitrage System - Initializing...")
        layout.addWidget(status_label)

        connect_label = QLabel("Click 'Connect' to begin Kiwoom login process")
        layout.addWidget(connect_label)