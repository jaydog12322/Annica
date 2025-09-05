class ExecutionGateway:
    """
    Execution Gateway - Order send/receive and event correlation

    Planned Features:
    - TR request/response correlation
    - Chejan event processing
    - Cancel-then-new for order type changes
    - Order state tracking
    """

    def __init__(self, kiwoom_connector, config):
        self.kiwoom = kiwoom_connector
        self.config = config
        pass

    def send_order_intent(self, order_intent):
        """Send order to Kiwoom"""
        pass