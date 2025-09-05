class Router:
    """
    Router Module - Determines order styles and venue routing

    Planned Features:
    - Direct venue routing (no SOR)
    - Order style selection (IOC/Market for take, Limit/Mid for post)
    - NXT mid-price order handling (hoga=29, price=0)
    """

    def __init__(self, config):
        self.config = config
        pass

    def route_signal(self, arbitrage_signal):
        """Convert arbitrage signal to order intents"""
        pass