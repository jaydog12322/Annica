# -*- coding: utf-8 -*-
"""
logger.py
---------
Logging utilities for the arbitrage system.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime


def setup_logging(level=logging.INFO, log_dir="logs"):
    """Setup application logging"""

    # Create logs directory
    Path(log_dir).mkdir(exist_ok=True)

    # Create main logger
    logger = logging.getLogger()
    logger.setLevel(level)

    # Clear any existing handlers
    logger.handlers.clear()

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler with rotation
    log_file = os.path.join(log_dir, f"arbitrage_{datetime.now().strftime('%Y%m%d')}.log")
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Execution log (separate file for trading events)
    exec_log_file = os.path.join(log_dir, f"execution_{datetime.now().strftime('%Y%m%d')}.log")
    exec_handler = logging.handlers.RotatingFileHandler(
        exec_log_file,
        maxBytes=50 * 1024 * 1024,  # 50MB
        backupCount=10,
        encoding='utf-8'
    )
    exec_formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    exec_handler.setFormatter(exec_formatter)

    # Create execution logger
    exec_logger = logging.getLogger('execution')
    exec_logger.addHandler(exec_handler)
    exec_logger.setLevel(logging.INFO)
    exec_logger.propagate = False  # Don't propagate to root logger

    logging.info("Logging system initialized")