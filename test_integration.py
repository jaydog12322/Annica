#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_integration.py
-------------------
Integration test for the complete KRX-NXT arbitrage trading system.

This test demonstrates the complete flow:
SpreadEngine → Router → Throttler → ExecutionGateway → PairManager

Can be run with mock data to verify the system works end-to-end.
"""

import sys
import os
import time
import logging
from typing import List

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer

# Import our modules
from src.core.config_manager import ConfigManager
from src.core.session_state import SessionStateManager
from src.core.market_data import MarketDataManager
from src.core.spread_engine import SpreadEngine, ArbitrageSignal
from src.core.router import Router
from src.core.throttler import Throttler
from src.kiwoom.kiwoom_connector import KiwoomConnector
from src.kiwoom.execution_gateway import ExecutionGateway
from src.core.pair_manager import PairManager
from src.utils.logger import setup_logging


class MockKiwoomConnector(KiwoomConnector):
    """Mock Kiwoom connector for testing without real API"""

    def __init__(self):
        # Skip parent init to avoid Qt control creation
        self.is_connected = True
        self.account = "1234567890"
        self.account_list = ["1234567890"]
        self.user_id = "testuser"

        # Mock order tracking
        self.mock_orders = {}
        self.next_order_number = 1000

    def send_order(self, rq_name, screen_no, acc_no, order_type, code, qty, price, hoga_gb, org_order_no):
        """Mock order sending"""
        order_number = str(self.next_order_number)
        self.next_order_number += 1

        print(f"MOCK ORDER: {rq_name} - {code} {order_type} {qty}@{price}")

        # Simulate success
        return order_number

    def send_nxt_order(self, order_type, rq_name, screen_no, acc_no, code, qty, price, hoga_gb, org_order_no=""):
        """Mock NXT order sending"""
        order_number = str(self.next_order_number)
        self.next_order_number += 1

        print(f"MOCK NXT ORDER: {rq_name} - {code} type:{order_type} {qty}@{price}")

        return order_number

    def get_comm_data(self, tr_code, rq_name, index, item_name):
        """Mock TR data"""
        if item_name == "주문번호":
            return str(self.next_order_number - 1)
        return ""


class IntegrationTestRunner:
    """Complete integration test runner"""

    def __init__(self):
        setup_logging(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        # Load config
        self.config = ConfigManager().load_config()

        # Initialize components with mock connector
        self.kiwoom = MockKiwoomConnector()
        self.session_state = SessionStateManager(self.config)
        self.market_data = MarketDataManager(self.kiwoom, self.config)
        self.spread_engine = SpreadEngine(self.market_data, self.session_state, self.config)
        self.router = Router(self.config)
        self.throttler = Throttler(self.config)
        self.execution = ExecutionGateway(self.kiwoom, self.throttler, self.config)
        self.pair_manager = PairManager(self.router, self.throttler, self.execution,
                                        self.session_state, self.config)

        # Connect signals for monitoring
        self.spread_engine.signal_generated.connect(self.on_signal_generated)
        self.pair_manager.pair_state_changed.connect(self.on_pair_state_changed)
        self.pair_manager.pair_completed.connect(self.on_pair_completed)
        self.execution.execution_event.connect(self.on_execution_event)

        self.logger.info("Integration test runner initialized")

    def setup_mock_data(self):
        """Setup mock market data"""
        # Add test symbols
        test_symbols = ["005930", "000660", "035420"]
        self.market_data.load_symbol_universe(test_symbols)

        # Manually create mock quotes with arbitrage opportunities
        for symbol in test_symbols:
            quote = self.market_data.quotes[symbol]

            # Set up a profitable KRX->NXT arbitrage
            quote.krx_bid = 72000
            quote.krx_ask = 72100
            quote.krx_bid_size = 100
            quote.krx_ask_size = 100
            quote.krx_last_update = time.time()

            quote.nxt_bid = 72200  # Higher NXT bid = arbitrage opportunity
            quote.nxt_ask = 72300
            quote.nxt_bid_size = 50
            quote.nxt_ask_size = 50
            quote.nxt_last_update = time.time()

            quote.is_dirty = True
            quote.both_venues_touched = True

            self.market_data.dirty_symbols.add(symbol)

        self.logger.info(f"Mock data setup for {len(test_symbols)} symbols")

    def run_test(self):
        """Run the complete integration test"""
        self.logger.info("=== STARTING INTEGRATION TEST ===")

        # 1. Setup
        self.setup_mock_data()

        # 2. Manually arm the session (skip time checks)
        self.session_state.manual_arm()
        self.logger.info("Session manually armed")

        # 3. Start spread engine
        self.spread_engine.start()
        self.logger.info("Spread engine started")

        # 4. Wait for signals to be generated and processed
        self.logger.info("Waiting for arbitrage signals...")

        # Let the system run for a few seconds
        return True

    def on_signal_generated(self, signal: ArbitrageSignal):
        """Handle arbitrage signal"""
        self.logger.info(f"SIGNAL GENERATED: {signal}")

        # Pass to pair manager
        accepted = self.pair_manager.handle_signal(signal)
        self.logger.info(f"Signal {'ACCEPTED' if accepted else 'REJECTED'}")

    def on_pair_state_changed(self, pair_id: str, new_state: str):
        """Handle pair state change"""
        self.logger.info(f"PAIR STATE: {pair_id} -> {new_state}")

    def on_pair_completed(self, pair_trade):
        """Handle pair completion"""
        self.logger.info(f"PAIR COMPLETED: {pair_trade.pair_id} - "
                         f"Edge: {pair_trade.realized_edge_krw:.2f} KRW, "
                         f"Profitable: {pair_trade.is_profitable}")

    def on_execution_event(self, event):
        """Handle execution event"""
        self.logger.info(f"EXECUTION: {event}")

        # Simulate order fills for mock testing
        if event.event_type == "ORDER_ACK":
            # Simulate immediate fill after small delay
            QTimer.singleShot(100, lambda: self.simulate_fill(event))

    def simulate_fill(self, ack_event):
        """Simulate order fill for mock testing"""
        from src.kiwoom.execution_gateway import ExecutionEvent

        # Create mock fill event
        fill_event = ExecutionEvent(
            event_type="TRADE_FILL",
            pair_id=ack_event.pair_id,
            leg=ack_event.leg,
            order_id=ack_event.order_id,
            timestamp=time.time(),
            data={
                "fill_price": 72100,  # Mock fill price
                "fill_qty": 1,
                "filled_qty": 1,
                "remaining_qty": 0,
                "avg_fill_price": 72100,
                "exec_id": f"MOCK_{int(time.time())}"
            }
        )

        self.logger.info(f"SIMULATING FILL: {fill_event}")
        self.execution.execution_event.emit(fill_event)

    def print_statistics(self):
        """Print system statistics"""
        self.logger.info("=== SYSTEM STATISTICS ===")

        spread_stats = self.spread_engine.get_statistics()
        self.logger.info(f"SpreadEngine: {spread_stats}")

        throttler_status = self.throttler.get_status()
        self.logger.info(f"Throttler: {throttler_status}")

        pair_stats = self.pair_manager.get_statistics()
        self.logger.info(f"PairManager: {pair_stats}")

        execution_stats = self.execution.get_statistics()
        self.logger.info(f"ExecutionGateway: {execution_stats}")


def main():
    """Main test function"""
    # Create Qt application for event loop
    app = QApplication(sys.argv)

    # Create and run test
    test_runner = IntegrationTestRunner()

    if test_runner.run_test():
        print("\n🚀 Integration test started successfully!")
        print("📊 Watch the logs for signal generation and processing...")
        print("⏹️  Press Ctrl+C to stop\n")

        # Set up a timer to print statistics periodically
        stats_timer = QTimer()
        stats_timer.timeout.connect(test_runner.print_statistics)
        stats_timer.start(5000)  # Every 5 seconds

        # Set up a timer to stop the test after 30 seconds
        stop_timer = QTimer()
        stop_timer.timeout.connect(lambda: (
            print("\n✅ Integration test completed!"),
            test_runner.print_statistics(),
            app.quit()
        ))
        stop_timer.start(30000)  # 30 seconds

        # Run the Qt event loop
        try:
            sys.exit(app.exec_())
        except KeyboardInterrupt:
            print("\n⏹️  Test stopped by user")
            test_runner.print_statistics()
    else:
        print("❌ Integration test failed to start")
        sys.exit(1)


if __name__ == "__main__":
    main()