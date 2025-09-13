#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
main.py
-------
KRX-NXT Arbitrage Trading System - Main Entry Point
-
This is the main application launcher that initializes the GUI and core components
according to the master architecture plan.

Author: Arbitrage Team
"""

import sys
import os
import logging
from pathlib import Path
import pandas as pd

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QThread, pyqtSignal

from src.gui.main_window import MainWindow
from src.core.config_manager import ConfigManager
from src.utils.logger import setup_logging
from src.kiwoom.kiwoom_connector import KiwoomConnector
from src.core.market_data import MarketDataManager
from src.core.session_state import SessionStateManager
from src.core.spread_engine import SpreadEngine
from src.core.router import Router
from src.core.throttler import Throttler
from src.kiwoom.execution_gateway import ExecutionGateway
from src.core.pair_manager import PairManager


def main():
    """Main application entry point"""

    # Setup logging first
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("=== KRX-NXT Arbitrage System Starting ===")

    try:
        # Load configuration
        config_manager = ConfigManager()
        config = config_manager.load_config()

        # Create Qt Application (required for PyQt5)
        app = QApplication(sys.argv)
        app.setApplicationName("KRX-NXT Arbitrage System")
        app.setApplicationVersion("1.0.0")

        # Initialize Kiwoom connection and login
        kiwoom = KiwoomConnector()
        if not kiwoom.login(show_account_pw=config.kiwoom.prompt_account_pw):
            logger.error("Unable to login to Kiwoom API")
            return 1

        # Show Kiwoom account password window if configured
        # (handled during login when prompt_account_pw is True)

        # Initialize core components
        throttler = Throttler(config)
        market_data = MarketDataManager(kiwoom, config)
        session_state = SessionStateManager(config)
        spread_engine = SpreadEngine(market_data, session_state, config)
        router = Router(config)
        execution_gateway = ExecutionGateway(kiwoom, throttler, config)
        pair_manager = PairManager(router, throttler, execution_gateway, session_state, config)

        # Create and show main window (keep reference to kiwoom)

        # Create and show main window
        main_window = MainWindow(config)
        main_window.kiwoom = kiwoom
        main_window.market_data = market_data
        main_window.session_state = session_state
        main_window.spread_engine = spread_engine
        main_window.router = router
        main_window.throttler = throttler
        main_window.execution_gateway = execution_gateway
        main_window.pair_manager = pair_manager
        main_window.show()

        # Load symbol universe and subscribe for quotes
        try:
            symbols_df = pd.read_excel(Path('data') / 'ticker_universe.xlsx')
            symbols = symbols_df.iloc[:, 0].dropna().astype(str).tolist()
        except Exception as e:
            logger.error(f"Failed to load symbol universe: {e}")
            symbols = []
        market_data.load_symbol_universe(symbols)
        market_data.subscribe_real_time_data()

        # Connect signals
        session_state.state_changed.connect(main_window.update_session_state)
        market_data.quote_updated.connect(main_window.update_quote)
        spread_engine.signal_generated.connect(pair_manager.handle_signal)
        spread_engine.batch_processed.connect(
            lambda stats: main_window.log_event(
                f"Batch {stats['batch_number']}: {stats['signals_generated']} signals"
            )
        )
        pair_manager.pair_state_changed.connect(
            lambda pid, state: main_window.log_event(f"Pair {pid} -> {state}")
        )

        # Start processing loops
        spread_engine.start()

        logger.info("Application initialized successfully")

        # Start the Qt event loop
        sys.exit(app.exec_())

    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
