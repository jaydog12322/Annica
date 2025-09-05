#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
main.py
-------
KRX-NXT Arbitrage Trading System - Main Entry Point

This is the main application launcher that initializes the GUI and core components
according to the master architecture plan.

Author: Arbitrage Team
"""

import sys
import os
import logging
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QThread, pyqtSignal

from src.gui.main_window import MainWindow
from src.core.config_manager import ConfigManager
from src.utils.logger import setup_logging


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

        # Create and show main window
        main_window = MainWindow(config)
        main_window.show()

        logger.info("Application initialized successfully")

        # Start the Qt event loop
        sys.exit(app.exec_())

    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()