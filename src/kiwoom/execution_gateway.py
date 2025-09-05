# -*- coding: utf-8 -*-
"""
execution_gateway.py
--------------------
Execution Gateway - Order Send/Receive and Event Correlation

Handles the complete order lifecycle from OrderIntent to execution events.
Correlates TR responses with Chejan events and manages order state tracking.

Key Features:
- TR/Chejan event correlation
- Order state tracking with timeouts
- Cancel-then-new for order type changes
- Proper FID field extraction
- Execution event generation for PairManager
"""

import logging
import time
import uuid
from typing import Dict, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum
from PyQt5.QtCore import QObject, pyqtSignal, QTimer

from src.kiwoom.kiwoom_connector import KiwoomConnector
from src.core.router import OrderIntent, Venue, OrderSide
from src.core.throttler import Throttler, TokenType

logger = logging.getLogger(__name__)
exec_logger = logging.getLogger('execution')


class OrderState(Enum):
    """Order lifecycle states"""
    PENDING_SEND = "PENDING_SEND"
    SENT = "SENT"
    ACCEPTED = "ACCEPTED"  # 접수
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"  # 체결
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    TIMEOUT = "TIMEOUT"


@dataclass
class OrderRecord:
    """Complete order record with state tracking"""
    # From OrderIntent
    pair_id: str
    leg: str
    symbol: str
    venue: Venue
    side: OrderSide
    quantity: int
    price: int
    order_type: str

    # Kiwoom identifiers
    client_order_id: str  # Our generated ID
    kiwoom_order_number: str = ""  # 주문번호 from Kiwoom
    screen_no: str = ""

    # State tracking
    state: OrderState = OrderState.PENDING_SEND
    send_time: float = 0.0
    ack_time: float = 0.0

    # Execution tracking
    filled_quantity: int = 0
    remaining_quantity: int = 0
    average_fill_price: float = 0.0

    # Events
    fills: List[dict] = field(default_factory=list)

    # Timeouts
    tr_timeout_timer: Optional[QTimer] = None
    chejan_timeout_timer: Optional[QTimer] = None


@dataclass
class ExecutionEvent:
    """Execution events emitted to PairManager and other components"""
    event_type: str  # ORDER_ACK, TRADE_FILL, ORDER_REJECT, etc.
    pair_id: str
    leg: str
    order_id: str
    timestamp: float
    data: dict = field(default_factory=dict)

    def __str__(self):
        return f"ExecutionEvent({self.event_type}: {self.pair_id}.{self.leg} - {self.data})"


class ExecutionGateway(QObject):
    """
    Execution Gateway for order lifecycle management.

    Signals:
        execution_event: Emitted for all order lifecycle events
        order_state_changed: Emitted when order state changes
    """

    # Signals
    execution_event = pyqtSignal(object)  # ExecutionEvent
    order_state_changed = pyqtSignal(str, str)  # order_id, new_state

    # Important Kiwoom FID codes
    ORDER_NUMBER_FID = 9203  # 주문번호
    ORIGINAL_ORDER_FID = 904  # 원주문번호
    SYMBOL_CODE_FID = 9001  # 종목코드
    ORDER_STATUS_FID = 913  # 주문상태
    ORDER_TYPE_FID = 905  # 주문구분
    BUY_SELL_FID = 906  # 매매구분
    REMAINING_QTY_FID = 902  # 미체결수량
    FILL_PRICE_FID = 910  # 체결가
    FILL_QTY_FID = 911  # 체결량
    EXEC_ID_FID = 909  # 체결번호
    ORDER_TIME_FID = 908  # 주문/체결시간
    REJECT_REASON_FID = 919  # 거부사유

    def __init__(self, kiwoom_connector: KiwoomConnector, throttler: Throttler, config):
        super().__init__()

        self.kiwoom = kiwoom_connector
        self.throttler = throttler
        self.config = config

        # Order tracking
        self.active_orders: Dict[str, OrderRecord] = {}  # client_order_id -> OrderRecord
        self.kiwoom_to_client_id: Dict[str, str] = {}  # kiwoom_order_number -> client_order_id

        # Screen allocation
        self.order_screen = str(config.kiwoom.screen_numbers["orders"])

        # Timeouts
        self.tr_timeout_ms = 200  # TR ack timeout
        self.chejan_timeout_ms = 300  # Chejan 접수 timeout

        # Connect to Kiwoom events
        self.kiwoom.tr_data_received.connect(self._on_tr_data)
        self.kiwoom.chejan_data_received.connect(self._on_chejan_data)
        self.kiwoom.msg_received.connect(self._on_message)

        logger.info("ExecutionGateway initialized")

    def send_order_intent(self, intent: OrderIntent) -> str:
        """
        Send order intent to Kiwoom.

        Args:
            intent: Order intent from Router

        Returns:
            Client order ID for tracking
        """
        try:
            # Generate client order ID
            client_order_id = self._generate_client_order_id(intent)

            # Request tokens from throttler
            priority = 0 if intent.is_take_leg or intent.priority == 0 else 1
            token_response = self.throttler.request_order_tokens(
                count=1,
                requester_id=f"ExecutionGateway.{client_order_id}",
                priority=priority
            )

            if not token_response.granted:
                # Emit rejection event
                self._emit_execution_event("ORDER_REJECTED", intent.pair_id, intent.leg,
                                           client_order_id, {"reason": token_response.reason})
                return client_order_id

            # Create order record
            order_record = self._create_order_record(intent, client_order_id)
            self.active_orders[client_order_id] = order_record

            # Send to Kiwoom
            success = self._send_to_kiwoom(intent, order_record)

            if success:
                # Start TR timeout timer
                self._start_tr_timeout(client_order_id)

                exec_logger.info(f"ORDER_SENT,{client_order_id},{intent.pair_id},{intent.leg},"
                                 f"{intent.venue.value},{intent.side.name},{intent.quantity},"
                                 f"{intent.symbol},{intent.price},{intent.order_type.value}")
            else:
                # Send failed
                order_record.state = OrderState.REJECTED
                self._emit_execution_event("ORDER_REJECTED", intent.pair_id, intent.leg,
                                           client_order_id, {"reason": "send_failed"})

            return client_order_id

        except Exception as e:
            logger.error(f"Failed to send order intent: {e}")
            self._emit_execution_event("ORDER_REJECTED", intent.pair_id, intent.leg,
                                       "", {"reason": f"exception: {e}"})
            return ""

    def _generate_client_order_id(self, intent: OrderIntent) -> str:
        """Generate unique client order ID"""
        timestamp = int(time.time() * 1000) % 100000  # Last 5 digits
        uuid_short = str(uuid.uuid4())[:6]
        return f"{intent.symbol}_{intent.leg}_{timestamp}_{uuid_short}"

    def _create_order_record(self, intent: OrderIntent, client_order_id: str) -> OrderRecord:
        """Create order record from intent"""
        return OrderRecord(
            pair_id=intent.pair_id,
            leg=intent.leg,
            symbol=intent.symbol,
            venue=intent.venue,
            side=intent.side,
            quantity=intent.quantity,
            price=intent.price,
            order_type=intent.order_type.value,
            client_order_id=client_order_id,
            screen_no=self.order_screen,
            remaining_quantity=intent.quantity,
            send_time=time.time()
        )

    def _send_to_kiwoom(self, intent: OrderIntent, record: OrderRecord) -> bool:
        """Send order to Kiwoom API"""
        try:
            # Determine symbol code based on venue
            if intent.venue == Venue.KRX:
                symbol_code = intent.symbol
            else:  # NXT
                symbol_code = f"{intent.symbol}_NX"

            # Prepare parameters
            rq_name = f"ORDER_{record.client_order_id}"
            account = self.kiwoom.account  # Should be set after login

            # Send order based on venue
            if intent.venue == Venue.KRX:
                # Regular KRX order
                result = self.kiwoom.send_order(
                    rq_name=rq_name,
                    screen_no=record.screen_no,
                    acc_no=account,
                    order_type=intent.kiwoom_order_type,
                    code=symbol_code,
                    qty=intent.quantity,
                    price=intent.price,
                    hoga_gb=intent.hoga_gb,
                    org_order_no=""
                )
            else:
                # NXT order
                result = self.kiwoom.send_nxt_order(
                    order_type=intent.kiwoom_order_type,
                    rq_name=rq_name,
                    screen_no=record.screen_no,
                    acc_no=account,
                    code=symbol_code,
                    qty=intent.quantity,
                    price=intent.price,
                    hoga_gb=intent.hoga_gb
                )

            # Check if send was successful
            if result and result != "":
                record.state = OrderState.SENT
                logger.debug(f"Order sent successfully: {record.client_order_id}")
                return True
            else:
                logger.error(f"Order send failed: {record.client_order_id}")
                return False

        except Exception as e:
            logger.error(f"Exception sending order {record.client_order_id}: {e}")
            return False

    def _start_tr_timeout(self, client_order_id: str):
        """Start TR acknowledgement timeout timer"""
        record = self.active_orders.get(client_order_id)
        if not record:
            return

        # Create timeout timer
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self._handle_tr_timeout(client_order_id))
        timer.start(self.tr_timeout_ms)

        record.tr_timeout_timer = timer

    def _handle_tr_timeout(self, client_order_id: str):
        """Handle TR acknowledgement timeout"""
        record = self.active_orders.get(client_order_id)
        if not record or record.state != OrderState.SENT:
            return

        logger.warning(f"TR timeout for order {client_order_id}")

        record.state = OrderState.TIMEOUT
        self._emit_execution_event("ORDER_TIMEOUT", record.pair_id, record.leg,
                                   client_order_id, {"timeout_type": "tr_ack"})

        # Clean up
        self._cleanup_order(client_order_id)

    def _on_tr_data(self, screen_no: str, rq_name: str, tr_code: str, record_name: str, next: str):
        """Handle TR data response (order acknowledgement)"""
        try:
            # Extract client order ID from rq_name
            if not rq_name.startswith("ORDER_"):
                return

            client_order_id = rq_name[6:]  # Remove "ORDER_" prefix
            record = self.active_orders.get(client_order_id)

            if not record:
                logger.warning(f"Received TR data for unknown order: {client_order_id}")
                return

            # Cancel timeout timer
            if record.tr_timeout_timer:
                record.tr_timeout_timer.stop()
                record.tr_timeout_timer = None

            # Get Kiwoom order number from response
            order_number = self.kiwoom.get_comm_data(tr_code, rq_name, 0, "주문번호").strip()

            if order_number:
                # Order accepted
                record.kiwoom_order_number = order_number
                record.state = OrderState.ACCEPTED
                record.ack_time = time.time()

                # Map Kiwoom order number to client ID
                self.kiwoom_to_client_id[order_number] = client_order_id

                self._emit_execution_event("ORDER_ACK", record.pair_id, record.leg,
                                           client_order_id, {
                                               "kiwoom_order_number": order_number,
                                               "ack_latency_ms": (record.ack_time - record.send_time) * 1000
                                           })

                # Start Chejan timeout timer
                self._start_chejan_timeout(client_order_id)

                exec_logger.info(f"ORDER_ACK,{client_order_id},{order_number},"
                                 f"{record.ack_time - record.send_time:.3f}")
            else:
                # Order rejected at TR level
                record.state = OrderState.REJECTED
                self._emit_execution_event("ORDER_REJECTED", record.pair_id, record.leg,
                                           client_order_id, {"reason": "tr_empty_order_number"})

                exec_logger.info(f"ORDER_REJECTED,{client_order_id},tr_empty_order_number")

                # Clean up
                self._cleanup_order(client_order_id)

        except Exception as e:
            logger.error(f"Error processing TR data: {e}")

    def _start_chejan_timeout(self, client_order_id: str):
        """Start Chejan 접수 timeout timer"""
        record = self.active_orders.get(client_order_id)
        if not record:
            return

        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self._handle_chejan_timeout(client_order_id))
        timer.start(self.chejan_timeout_ms)

        record.chejan_timeout_timer = timer

    def _handle_chejan_timeout(self, client_order_id: str):
        """Handle Chejan 접수 timeout"""
        record = self.active_orders.get(client_order_id)
        if not record:
            return

        logger.warning(f"Chejan timeout for order {client_order_id}")

        self._emit_execution_event("ORDER_STUCK", record.pair_id, record.leg,
                                   client_order_id, {"timeout_type": "chejan_silence"})

    def _on_chejan_data(self, gubun: str, item_cnt: int, fid_list: str):
        """
        Handle Chejan data (order/fill events).

        gubun: "0"=order/fill, "1"=balance
        """
        try:
            if gubun != "0":  # Only process order/fill events
                return

            # Extract key fields
            order_number = self.kiwoom.get_chejan_data(self.ORDER_NUMBER_FID).strip()

            if not order_number or order_number not in self.kiwoom_to_client_id:
                return  # Not our order

            client_order_id = self.kiwoom_to_client_id[order_number]
            record = self.active_orders.get(client_order_id)

            if not record:
                logger.warning(f"Chejan for unknown client order: {client_order_id}")
                return

            # Cancel Chejan timeout timer
            if record.chejan_timeout_timer:
                record.chejan_timeout_timer.stop()
                record.chejan_timeout_timer = None

            # Extract Chejan fields
            order_status = self.kiwoom.get_chejan_data(self.ORDER_STATUS_FID).strip()
            remaining_qty = int(self.kiwoom.get_chejan_data(self.REMAINING_QTY_FID) or "0")
            fill_price_str = self.kiwoom.get_chejan_data(self.FILL_PRICE_FID).strip()
            fill_qty_str = self.kiwoom.get_chejan_data(self.FILL_QTY_FID).strip()
            exec_id = self.kiwoom.get_chejan_data(self.EXEC_ID_FID).strip()

            # Process based on order status
            if order_status == "접수":
                self._handle_order_accepted(record)
            elif fill_qty_str and int(fill_qty_str) > 0:
                # This is a fill
                fill_price = int(fill_price_str.replace(",", "")) if fill_price_str else 0
                fill_qty = int(fill_qty_str)
                self._handle_fill(record, fill_price, fill_qty, remaining_qty, exec_id)
            elif order_status == "확인":
                # Order cancelled/modified confirmation
                self._handle_order_cancelled(record)

        except Exception as e:
            logger.error(f"Error processing Chejan data: {e}")

    def _handle_order_accepted(self, record: OrderRecord):
        """Handle order accepted (접수) event"""
        if record.state == OrderState.ACCEPTED:
            # Already processed from TR response
            return

        record.state = OrderState.ACCEPTED

        self._emit_execution_event("ORDER_ACCEPTED", record.pair_id, record.leg,
                                   record.client_order_id, {
                                       "kiwoom_order_number": record.kiwoom_order_number
                                   })

        exec_logger.info(f"ORDER_ACCEPTED,{record.client_order_id},{record.kiwoom_order_number}")

    def _handle_fill(self, record: OrderRecord, fill_price: int, fill_qty: int,
                     remaining_qty: int, exec_id: str):
        """Handle trade fill event"""
        # Update quantities
        record.filled_quantity += fill_qty
        record.remaining_quantity = remaining_qty

        # Update average fill price
        if record.filled_quantity > 0:
            # Weighted average
            total_value = (record.average_fill_price * (record.filled_quantity - fill_qty) +
                           fill_price * fill_qty)
            record.average_fill_price = total_value / record.filled_quantity

        # Record this fill
        fill_record = {
            "exec_id": exec_id,
            "fill_price": fill_price,
            "fill_qty": fill_qty,
            "timestamp": time.time()
        }
        record.fills.append(fill_record)

        # Determine if partial or complete fill
        if remaining_qty > 0:
            record.state = OrderState.PARTIALLY_FILLED
            event_type = "TRADE_PARTIAL"
        else:
            record.state = OrderState.FILLED
            event_type = "TRADE_FILL"

        # Emit execution event
        self._emit_execution_event(event_type, record.pair_id, record.leg,
                                   record.client_order_id, {
                                       "fill_price": fill_price,
                                       "fill_qty": fill_qty,
                                       "filled_qty": record.filled_quantity,
                                       "remaining_qty": remaining_qty,
                                       "avg_fill_price": record.average_fill_price,
                                       "exec_id": exec_id
                                   })

        exec_logger.info(f"{event_type},{record.client_order_id},{exec_id},"
                         f"{fill_price},{fill_qty},{record.filled_quantity},{remaining_qty}")

        # Clean up if fully filled
        if remaining_qty == 0:
            self._cleanup_order(record.client_order_id, delay_ms=1000)  # Small delay for any late events

    def _handle_order_cancelled(self, record: OrderRecord):
        """Handle order cancelled event"""
        record.state = OrderState.CANCELLED

        self._emit_execution_event("ORDER_CANCELLED", record.pair_id, record.leg,
                                   record.client_order_id, {
                                       "remaining_qty": record.remaining_quantity
                                   })

        exec_logger.info(f"ORDER_CANCELLED,{record.client_order_id},{record.remaining_quantity}")

        # Clean up
        self._cleanup_order(record.client_order_id)

    def _on_message(self, screen_no: str, rq_name: str, tr_code: str, msg: str):
        """Handle OnReceiveMsg events"""
        if not rq_name.startswith("ORDER_"):
            return

        client_order_id = rq_name[6:]
        record = self.active_orders.get(client_order_id)

        if record:
            # This is likely an error message
            logger.warning(f"Order message for {client_order_id}: {msg}")

            if "거부" in msg or "오류" in msg or "실패" in msg:
                record.state = OrderState.REJECTED
                self._emit_execution_event("ORDER_REJECTED", record.pair_id, record.leg,
                                           client_order_id, {"reason": msg})
                self._cleanup_order(client_order_id)

    def _emit_execution_event(self, event_type: str, pair_id: str, leg: str,
                              order_id: str, data: dict = None):
        """Emit execution event to subscribers"""
        event = ExecutionEvent(
            event_type=event_type,
            pair_id=pair_id,
            leg=leg,
            order_id=order_id,
            timestamp=time.time(),
            data=data or {}
        )

        logger.debug(f"Execution event: {event}")
        self.execution_event.emit(event)

        # Also emit state change signal
        if order_id in self.active_orders:
            self.order_state_changed.emit(order_id, self.active_orders[order_id].state.value)

    def _cleanup_order(self, client_order_id: str, delay_ms: int = 0):
        """Clean up order record and timers"""
        if delay_ms > 0:
            # Delayed cleanup
            timer = QTimer()
            timer.setSingleShot(True)
            timer.timeout.connect(lambda: self._cleanup_order(client_order_id, 0))
            timer.start(delay_ms)
            return

        record = self.active_orders.get(client_order_id)
        if not record:
            return

        # Stop any active timers
        if record.tr_timeout_timer:
            record.tr_timeout_timer.stop()
        if record.chejan_timeout_timer:
            record.chejan_timeout_timer.stop()

        # Remove from mappings
        if record.kiwoom_order_number in self.kiwoom_to_client_id:
            del self.kiwoom_to_client_id[record.kiwoom_order_number]

        del self.active_orders[client_order_id]

        logger.debug(f"Cleaned up order: {client_order_id}")

    def cancel_order(self, client_order_id: str) -> bool:
        """
        Cancel an active order.

        Args:
            client_order_id: Client order ID to cancel

        Returns:
            True if cancel request sent successfully
        """
        record = self.active_orders.get(client_order_id)
        if not record or not record.kiwoom_order_number:
            logger.warning(f"Cannot cancel order {client_order_id}: not found or no Kiwoom order number")
            return False

        try:
            # Request cancel tokens
            token_response = self.throttler.request_order_tokens(
                count=1,
                requester_id=f"Cancel.{client_order_id}",
                priority=0  # High priority for cancels
            )

            if not token_response.granted:
                logger.warning(f"Cancel denied due to rate limiting: {client_order_id}")
                return False

            # Send cancel based on venue
            if record.venue == Venue.KRX:
                # KRX cancel
                result = self.kiwoom.send_order(
                    rq_name=f"CANCEL_{client_order_id}",
                    screen_no=record.screen_no,
                    acc_no=self.kiwoom.account,
                    order_type=3,  # Cancel
                    code=record.symbol,
                    qty=0,
                    price=0,
                    hoga_gb="00",
                    org_order_no=record.kiwoom_order_number
                )
            else:
                # NXT cancel
                result = self.kiwoom.send_nxt_order(
                    order_type=23,  # NXT cancel
                    rq_name=f"CANCEL_{client_order_id}",
                    screen_no=record.screen_no,
                    acc_no=self.kiwoom.account,
                    code=f"{record.symbol}_NX",
                    qty=0,
                    price=0,
                    hoga_gb="00",
                    org_order_no=record.kiwoom_order_number
                )

            if result:
                logger.info(f"Cancel request sent for order {client_order_id}")
                exec_logger.info(f"CANCEL_SENT,{client_order_id},{record.kiwoom_order_number}")
                return True
            else:
                logger.error(f"Failed to send cancel for order {client_order_id}")
                return False

        except Exception as e:
            logger.error(f"Exception cancelling order {client_order_id}: {e}")
            return False

    def get_order_status(self, client_order_id: str) -> Optional[dict]:
        """Get current status of an order"""
        record = self.active_orders.get(client_order_id)
        if not record:
            return None

        return {
            "state": record.state.value,
            "filled_qty": record.filled_quantity,
            "remaining_qty": record.remaining_quantity,
            "avg_fill_price": record.average_fill_price,
            "kiwoom_order_number": record.kiwoom_order_number,
            "fills_count": len(record.fills)
        }

    def get_active_orders(self) -> List[dict]:
        """Get list of all active orders"""
        active = []
        for client_id, record in self.active_orders.items():
            active.append({
                "client_order_id": client_id,
                "pair_id": record.pair_id,
                "leg": record.leg,
                "symbol": record.symbol,
                "venue": record.venue.value,
                "side": record.side.name,
                "state": record.state.value,
                "quantity": record.quantity,
                "filled_qty": record.filled_quantity,
                "remaining_qty": record.remaining_quantity
            })
        return active

    def get_statistics(self) -> dict:
        """Get execution gateway statistics"""
        return {
            "active_orders_count": len(self.active_orders),
            "kiwoom_mappings_count": len(self.kiwoom_to_client_id),
            "tr_timeout_ms": self.tr_timeout_ms,
            "chejan_timeout_ms": self.chejan_timeout_ms
        }