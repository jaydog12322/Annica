class RiskManager:
    """
    Risk Manager - Position and exposure limits

    Planned Features:
    - Per-symbol position limits
    - Global exposure limits
    - Kill switches (deferred to V2)
    - Unhedged time monitoring
    """

    def __init__(self, config):
        self.config = config
        pass

    def check_risk_limits(self, signal):
        """Check if signal passes risk limits"""
        pass