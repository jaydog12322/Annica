#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
test_integration.py
-------------------
Integration test for the complete KRX-NXT arbitrage trading system.
"""

import sys
import os
import time
import logging

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer, QObject, pyqtSignal

# Import our modules
from src.core.config_manager import ConfigManager
from src.core.session_state import SessionStateManager
from src.core.market_data import MarketDataManager
from src.core.spread_engine import SpreadEngine, ArbitrageSignal
from src.core.router import Router
from src.core.throttler import Throttler
from src.kiwoom.execution_gateway import ExecutionGateway
from src.core.pair_manager import PairManager
from src.utils.logger import setup_logging


class MockKiwoomConnector(QObject):
    """Mock Kiwoom connector for testing without real API"""

    # Define the same signals as KiwoomConnector
    connected = pyqtSignal(int)
    tr_data_received = pyqtSignal(str, str, str, str, str)
    real_data_received = pyqtSignal(str, str, str)
    chejan_data_received = pyqtSignal(str, int, str)
    msg_received = pyqtSignal(str, str, str, str)

    def __init__(self):
        super().__init__()

        # Mock connection state
        self.is_connected = True
        self.account = "1234567890"
        self.account_list = ["1234567890"]
        self.user_id = "testuser"

        # Mock order counter
        self.order_counter = 1000
        self.pending_orders = {}

    def send_order(self, rq_name, screen_no, acc_no, order_type, code, qty, price, hoga_gb, org_order_no):
        """Mock order sending"""
        order_number = str(self.order_counter)
        self.order_counter += 1

        self.pending_orders[order_number] = {
            "rq_name": rq_name,
            "code": code,
            "qty": qty,
            "price": price,
            "order_type": order_type
        }

        print(f"MOCK: KRX Order sent - {rq_name}, code={code}, qty={qty}, price={price}")

        # Simulate responses
        QTimer.singleShot(50, lambda: self._simulate_tr_response(rq_name, order_number))
        QTimer.singleShot(100, lambda: self._simulate_fill(order_number))

        return order_number

    def send_nxt_order(self, order_type, rq_name, screen_no, acc_no, code, qty, price, hoga_gb, org_order_no=""):
        """Mock NXT order sending"""
        order_number = str(self.order_counter)
        self.order_counter += 1

        self.pending_orders[order_number] = {
            "rq_name": rq_name,
            "code": code,
            "qty": qty,
            "price": price,
            "order_type": order_type
        }

        print(f"MOCK: NXT Order sent - {rq_name}, code={code}, qty={qty}, price={price}")

        # Simulate responses
        QTimer.singleShot(50, lambda: self._simulate_tr_response(rq_name, order_number))
        QTimer.singleShot(100, lambda: self._simulate_fill(order_number))

        return order_number

    def _simulate_tr_response(self, rq_name, order_number):
        """Simulate TR response with order number"""
        try:
            self._mock_tr_data = order_number
            self.tr_data_received.emit("200", rq_name, "ORDER", "", "")
        except Exception as e:
            print(f"Mock TR response error: {e}")

    def _simulate_fill(self, order_number):
        """Simulate order fill via Chejan"""
        try:
            order_info = self.pending_orders.get(order_number)
            if not order_info:
                return

            # Mock Chejan data
            self._mock_chejan_data = {
                9203: order_number,
                902: "0",  # 미체결수량 (0 = fully filled)
                910: str(order_info["price"] or 50000),
                911: str(order_info["qty"]),
                909: f"exec_{order_number}",
                913: "체결"
            }

            self.chejan_data_received.emit("0", 5, "9203;902;910;911;909;913")

            print(
                f"MOCK: Order filled - {order_number}, qty={order_info['qty']}, price={order_info.get('price', 50000)}")

        except Exception as e:
            print(f"Mock fill error: {e}")

    def get_comm_data(self, tr_code, rq_name, index, item_name):
        """Mock TR data extraction"""
        if item_name == "주문번호":
            return getattr(self, '_mock_tr_data', "")
        return ""

    def get_chejan_data(self, fid):
        """Mock Chejan data extraction"""
        mock_data = getattr(self, '_mock_chejan_data', {})
        return mock_data.get(fid, "")

    def set_real_reg(self, screen_no, code_list, fid_list, real_type):
        """Mock real-time registration"""
        print(f"MOCK: Real-time registered - screen={screen_no}, codes={len(code_list.split(';'))}")
        return 0

    def unregister_real(self, screen_no, code_list="ALL"):
        """Mock real-time unregistration"""
        return 0


class IntegrationTest:
    """Integration test runner"""

    def __init__(self):
        # Setup logging
        setup_logging(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== Integration Test Starting ===")

        # Create a complete test configuration directly
        self.config = self._create_test_config()

        # Initialize components
        self.kiwoom = MockKiwoomConnector()
        self.session_state = SessionStateManager(self.config)
        self.session_state.manual_arm()  # Force arm for testing

        self.market_data = MarketDataManager(self.kiwoom, self.config)
        self.spread_engine = SpreadEngine(self.market_data, self.session_state, self.config)
        self.router = Router(self.config)
        self.throttler = Throttler(self.config)
        self.execution_gateway = ExecutionGateway(self.kiwoom, self.throttler, self.config)
        self.pair_manager = PairManager(self.router, self.throttler, self.execution_gateway,
                                        self.session_state, self.config)

        # Connect monitoring
        self.pair_manager.pair_state_changed.connect(self._on_pair_state_changed)
        self.pair_manager.pair_completed.connect(self._on_pair_completed)

        self.test_results = []
        self.logger.info("All components initialized successfully")

    def _create_test_config(self):
        """Create a complete test configuration"""
        from src.core.config_manager import (Config, AppConfig, KiwoomConfig, SessionConfig,
                                             RouterConfig, SpreadEngineConfig, ExecutionConfig,
                                             ThrottlingConfig, FeesConfig, TelemetryConfig, AlertsConfig)

        config = Config()

        # App config
        config.app = AppConfig(
            mode="real",
            timezone="Asia/Seoul"
        )

        # Kiwoom config
        config.kiwoom = KiwoomConfig(
            server="실서버",
            account="",
            screen_numbers={
                "marketdata": [101, 102, 103, 104],
                "orders": 200
            },
            rate_limits={
                "orders_per_sec": 5,
                "queries_per_sec": 5,
                "reserve_order_tokens": 2
            },
            features={
                "use_sor": False,
                "use_al_feed": False
            }
        )

        # Session config
        config.sessions = SessionConfig(
            arm_only_in_overlap=True,
            overlap_window={
                "start": "09:00:32",
                "end": "15:19:50"
            },
            nxt_main={
                "start": "09:00:30",
                "end": "15:20"
            },
            use_fid_215_signals=True
        )

        # Router config
        config.router = RouterConfig(
            entry_leg={"prefer": "ioc_or_market"},
            hedge_leg={
                "prefer": "limit_or_mid",
                "allow_nxt_mid_price": True,
                "fallback_after_ms": 1000
            }
        )

        # Spread engine config
        config.spread_engine = SpreadEngineConfig(
            batch_interval_ms=10,
            min_net_ticks_after_fees=1,
            also_require_min_visible_qty=1,
            cooldown_ms=100
        )

        # Execution config
        config.execution = ExecutionConfig(
            t_hedge_ms=1000,
            cancel_then_new_on_type_change=True,
            max_concurrent_symbols=2,
            max_outstanding_pairs_per_symbol=1
        )

        # Throttling config
        config.throttling = ThrottlingConfig(
            orders_bucket_per_sec=5,
            queries_bucket_per_sec=5,
            min_tokens_free_to_start_new_pair=4
        )

        # Fees config
        config.fees = FeesConfig(
            krx={"broker_bps": 1.5},
            nxt={"broker_bps": 1.45, "regulatory_bps": 0.31833}
        )

        # Telemetry config
        config.telemetry = TelemetryConfig(
            slo_targets_ms={
                "tick_to_signal_p95": 25,
                "signal_to_send_p95": 15,
                "send_to_ack_p95": 150
            },
            orders_utilization_autopause={
                "threshold": 0.80,
                "sustain_seconds": 5,
                "enabled": True
            }
        )

        # Alerts config
        config.alerts = AlertsConfig(
            slack={
                "webhook": "",
                "send_on": {
                    "buy_fill": True,
                    "sell_fill": True,
                    "pair_done": True,
                    "auto_pause_on": True,
                    "hedge_timeout": True,
                    "reject_spike": True
                }
            }
        )

        return config

    def _on_pair_state_changed(self, pair_id, new_state):
        """Monitor pair state changes"""
        print(f"PAIR STATE: {pair_id} -> {new_state}")

    def _on_pair_completed(self, pair_trade):
        """Monitor pair completions"""
        profit = pair_trade.realized_edge_krw
        profitable = pair_trade.is_profitable
        print(f"PAIR COMPLETED: {pair_trade.pair_id} - "
              f"Profit: {profit:.2f} KRW, Profitable: {profitable}")

        self.test_results.append({
            'pair_id': pair_trade.pair_id,
            'symbol': pair_trade.symbol,
            'profit': profit,
            'profitable': profitable
        })

    def create_test_signal(self, symbol="005930") -> ArbitrageSignal:
        """Create a test arbitrage signal"""
        return ArbitrageSignal(
            symbol=symbol,
            buy_venue="KRX",
            sell_venue="NXT",
            buy_price=50000,
            sell_price=50100,
            max_qty=1,
            edge_krw=100.0,
            edge_bps=20.0,
            total_fees_krw=30.0,
            net_edge_krw=70.0,
            timestamp=time.time()
        )

    def test_single_trade(self):
        """Test a single arbitrage trade"""
        self.logger.info("Testing single arbitrage trade...")

        signal = self.create_test_signal("005930")
        print(f"Created test signal: {signal}")

        success = self.pair_manager.handle_signal(signal)
        print(f"Signal {'accepted' if success else 'rejected'} by PairManager")

        return success

    def test_multiple_symbols(self):
        """Test multiple symbols"""
        self.logger.info("Testing multiple symbols...")

        symbols = ["005930", "000660", "035420"]
        success_count = 0

        for symbol in symbols:
            signal = self.create_test_signal(symbol)
            if self.pair_manager.handle_signal(signal):
                success_count += 1
                print(f"Signal accepted for {symbol}")
            else:
                print(f"Signal rejected for {symbol}")

        print(f"Accepted {success_count}/{len(symbols)} signals")
        return success_count

    def show_statistics(self):
        """Show final statistics"""
        print("\n=== Final Statistics ===")

        stats = self.pair_manager.get_statistics()
        print(f"Pair Manager Stats:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        throttler_status = self.throttler.get_status()
        print(f"\nThrottler Status:")
        print(f"  Orders available: {throttler_status['orders']['available']}")

        if self.test_results:
            print(f"\nCompleted Pairs:")
            for result in self.test_results:
                status = "✓" if result['profitable'] else "✗"
                print(f"  {result['pair_id']}: {result['profit']:.2f} KRW {status}")

    def run_tests(self):
        """Run all tests"""
        print("Starting integration tests...\n")

        # Test 1: Single trade
        test1_result = self.test_single_trade()
        time.sleep(0.5)  # Allow processing

        # Test 2: Multiple symbols
        test2_result = self.test_multiple_symbols()
        time.sleep(1.0)  # Allow processing

        # Show results
        self.show_statistics()

        # Summary
        print(f"\n=== Test Summary ===")
        print(f"Single trade test: {'PASS' if test1_result else 'FAIL'}")
        print(f"Multiple symbols test: {'PASS' if test2_result > 0 else 'FAIL'}")
        print(f"Completed pairs: {len(self.test_results)}")

        if self.test_results:
            profitable_count = sum(1 for r in self.test_results if r['profitable'])
            print(f"Profitable pairs: {profitable_count}/{len(self.test_results)}")

        return test1_result and test2_result > 0


def main():
    """Main test entry point"""
    app = QApplication([])
    app.setApplicationName("Arbitrage Integration Test")

    try:
        # Create and run test
        test = IntegrationTest()
        success = test.run_tests()

        if success:
            print("\n✅ Integration test completed successfully!")
            return 0
        else:
            print("\n❌ Integration test failed!")
            return 1

    except Exception as e:
        print(f"\n❌ Integration test crashed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        app.quit()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)