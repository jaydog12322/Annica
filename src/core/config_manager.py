# -*- coding: utf-8 -*-
"""
config_manager.py
-----------------
Configuration Management Module

Handles loading and validation of config.yaml according to the V1 schema
defined in the master plan.
"""

import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    """Application configuration settings"""
    mode: str = "real"  # real | paper
    timezone: str = "Asia/Seoul"
    logging_level: str = "INFO"
    logging_file_rotation_size: int = 10  # MB
    logging_file_rotation_count: int = 5


@dataclass
class KiwoomConfig:
    """Kiwoom API configuration"""
    server: str = "실서버"  # 실서버 | 모의
    account: str = ""  # Set after login
    screen_numbers: Dict[str, Any] = field(default_factory=lambda: {
        "marketdata": [101, 102, 103, 104],
        "orders": 200
    })
    rate_limits: Dict[str, int] = field(default_factory=lambda: {
        "orders_per_sec": 5,
        "queries_per_sec": 5,
        "reserve_order_tokens": 2
    })
    features: Dict[str, bool] = field(default_factory=lambda: {
        "use_sor": False,
        "use_al_feed": False
    })


@dataclass
class SessionConfig:
    """Trading session configuration"""
    arm_only_in_overlap: bool = True
    overlap_window: Dict[str, str] = field(default_factory=lambda: {
        "start": "09:00:32",
        "end": "15:19:50"
    })
    nxt_main: Dict[str, str] = field(default_factory=lambda: {
        "start": "09:00:30",
        "end": "15:20"
    })
    use_fid_215_signals: bool = True


@dataclass
class SpreadEngineConfig:
    """Spread engine configuration"""
    batch_interval_ms: int = 10
    min_net_ticks_after_fees: int = 1
    also_require_min_visible_qty: int = 1
    cooldown_ms: int = 100


@dataclass
class ExecutionConfig:
    """Execution configuration"""
    t_hedge_ms: int = 1000
    cancel_then_new_on_type_change: bool = True
    max_concurrent_symbols: int = 2
    max_outstanding_pairs_per_symbol: int = 1

@dataclass
class RouterConfig:
    """Router configuration"""
    entry_leg: Dict[str, Any] = field(default_factory=lambda: {
        "prefer": "ioc_or_market"
    })
    hedge_leg: Dict[str, Any] = field(default_factory=lambda: {
        "prefer": "limit_or_mid",
        "allow_nxt_mid_price": True,
        "fallback_after_ms": 1000
    })


@dataclass
class ThrottlingConfig:
    """Throttling configuration"""
    orders_bucket_per_sec: int = 5
    queries_bucket_per_sec: int = 5
    min_tokens_free_to_start_new_pair: int = 4



@dataclass
class FeesConfig:
    """Fee configuration"""
    krx: Dict[str, float] = field(default_factory=lambda: {"broker_bps": 1.5})
    nxt: Dict[str, float] = field(default_factory=lambda: {"broker_bps": 1.45})
    # Trade tax applied on sell executions for any venue
    trade_tax_bps: float = 20.0


@dataclass
class TelemetryConfig:
    """Telemetry and SLO configuration"""
    slo_targets_ms: Dict[str, int] = field(default_factory=lambda: {
        "tick_to_signal_p95": 25,
        "signal_to_send_p95": 15,
        "send_to_ack_p95": 150
    })
    orders_utilization_autopause: Dict[str, Any] = field(default_factory=lambda: {
        "threshold": 0.80,
        "sustain_seconds": 5,
        "enabled": True
    })


@dataclass
class AlertsConfig:
    """Alerts configuration"""
    slack: Dict[str, Any] = field(default_factory=lambda: {
        "webhook": "",  # Set via env var
        "send_on": {
            "buy_fill": True,
            "sell_fill": True,
            "pair_done": True,
            "auto_pause_on": True,
            "hedge_timeout": True,
            "reject_spike": True
        }
    })


@dataclass
class Config:
    """Main configuration container"""
    app: AppConfig = field(default_factory=AppConfig)
    kiwoom: KiwoomConfig = field(default_factory=KiwoomConfig)
    sessions: SessionConfig = field(default_factory=SessionConfig)
    router: RouterConfig = field(default_factory=RouterConfig)
    spread_engine: SpreadEngineConfig = field(default_factory=SpreadEngineConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    throttling: ThrottlingConfig = field(default_factory=ThrottlingConfig)
    fees: FeesConfig = field(default_factory=FeesConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    alerts: AlertsConfig = field(default_factory=AlertsConfig)


class ConfigManager:
    """Configuration manager for loading and validating config files"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/config.yaml"
        self.config: Optional[Config] = None

    def load_config(self) -> Config:
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)

            if config_file.exists():
                logger.info(f"Loading configuration from {self.config_path}")
                with open(config_file, 'r', encoding='utf-8') as f:
                    config_data = yaml.safe_load(f)

                # Create config object from loaded data
                self.config = self._create_config_from_dict(config_data)
            else:
                logger.warning(f"Config file not found at {self.config_path}, using defaults")
                self.config = Config()
                self.save_default_config()

            return self.config

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            logger.info("Using default configuration")
            self.config = Config()
            return self.config

    def _create_config_from_dict(self, data: Dict[str, Any]) -> Config:
        """Create Config object from dictionary"""
        # This is a simplified version - in practice you'd want more robust parsing
        config = Config()

        if 'app' in data:
            app_data = data['app']
            config.app = AppConfig(**{k: v for k, v in app_data.items() if hasattr(AppConfig, k)})

        if 'kiwoom' in data:
            kiwoom_data = data['kiwoom']
            config.kiwoom = KiwoomConfig(**{k: v for k, v in kiwoom_data.items() if hasattr(KiwoomConfig, k)})

        # Continue for other sections...

        return config

    def save_default_config(self):
        """Save default configuration to file"""
        try:
            config_file = Path(self.config_path)
            config_file.parent.mkdir(parents=True, exist_ok=True)

            # Convert config to dict and save
            config_dict = self._config_to_dict(Config())

            with open(config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config_dict, f, default_flow_style=False, allow_unicode=True)

            logger.info(f"Default configuration saved to {self.config_path}")

        except Exception as e:
            logger.error(f"Failed to save default configuration: {e}")

    def _config_to_dict(self, config: Config) -> Dict[str, Any]:
        """Convert Config object to dictionary for YAML export"""
        return {
            "app": {
                "mode": config.app.mode,
                "timezone": config.app.timezone,
                "logging": {
                    "level": config.app.logging_level,
                    "file_rotation": {
                        "size": config.app.logging_file_rotation_size,
                        "count": config.app.logging_file_rotation_count
                    }
                }
            },
            "kiwoom": {
                "server": config.kiwoom.server,
                "account": config.kiwoom.account,
                "screen_numbers": config.kiwoom.screen_numbers,
                "rate_limits": config.kiwoom.rate_limits,
                "features": config.kiwoom.features
            },
            "sessions": {
                "arm_only_in_overlap": config.sessions.arm_only_in_overlap,
                "overlap_window": config.sessions.overlap_window,
                "nxt_main": config.sessions.nxt_main,
                "use_fid_215_signals": config.sessions.use_fid_215_signals
            },
            "router": {
                "entry_leg": config.router.entry_leg,
                "hedge_leg": config.router.hedge_leg
            },
            "spread_engine": {
                "batch_interval_ms": config.spread_engine.batch_interval_ms,
                "edge_rule": {
                    "min_net_ticks_after_fees": config.spread_engine.min_net_ticks_after_fees,
                    "also_require_min_visible_qty": config.spread_engine.also_require_min_visible_qty
                },
                "cooldown_ms": config.spread_engine.cooldown_ms
            },
            "execution": {
                "t_hedge_ms": config.execution.t_hedge_ms,
                "cancel_then_new_on_type_change": config.execution.cancel_then_new_on_type_change,
                "max_concurrent_symbols": config.execution.max_concurrent_symbols,
                "max_outstanding_pairs_per_symbol": config.execution.max_outstanding_pairs_per_symbol
            },
            "throttling": {
                "orders_bucket_per_sec": config.throttling.orders_bucket_per_sec,
                "queries_bucket_per_sec": config.throttling.queries_bucket_per_sec,
                "min_tokens_free_to_start_new_pair": config.throttling.min_tokens_free_to_start_new_pair
            },
            "fees": {
                "krx": config.fees.krx,
                "nxt": config.fees.nxt,
                "trade_tax_bps": config.fees.trade_tax_bps
            },
            "telemetry": {
                "slo_targets_ms": config.telemetry.slo_targets_ms,
                "orders_utilization_autopause": config.telemetry.orders_utilization_autopause
            },
            "alerts": {
                "slack": config.alerts.slack
            }
        }
