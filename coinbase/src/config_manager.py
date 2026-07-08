"""
Hot-reload YAML configuration with feature flags.
"""

import os
import yaml
import threading
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional, List, Callable
from pathlib import Path

log = logging.getLogger(__name__)

_CONFIG_DIR = Path("config")
_CONFIG_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class FeatureFlags:
    """Runtime feature toggles."""
    # Core features
    enable_live_trading: bool = False
    enable_paper_trading: bool = True
    enable_shorts: bool = True
    enable_leverage: bool = True
    enable_prediction_markets: bool = True
    enable_news_sentiment: bool = True
    enable_macro_risk: bool = True
    enable_pair_trading: bool = False
    
    # Scanning
    enable_minute_scan: bool = True
    enable_batch_scan: bool = True
    enable_full_scan: bool = True
    
    # Risk
    enable_portfolio_risk: bool = True
    enable_correlation_clustering: bool = True
    enable_dynamic_leverage: bool = True
    enable_kelly_sizing: bool = True
    
    # Execution
    enable_bracket_orders: bool = True
    enable_trailing_stops: bool = True
    enable_trailing_take_profit: bool = True
    enable_maker_orders: bool = True
    
    # WebSocket
    enable_public_ws: bool = True
    enable_advanced_ws: bool = True
    
    # Observability
    enable_metrics_server: bool = True
    enable_structured_logging: bool = True
    enable_approval_server: bool = True


@dataclass
class ScanConfig:
    """Scan scheduling configuration."""
    minute_scan_interval: int = 60
    minute_scan_top_n: int = 150
    minute_scan_min_top_n: int = 10
    minute_scan_max_top_n: int = 80
    minute_scan_use_hotset: bool = False
    minute_scan_hotset_size: int = 150
    
    batch_scan_interval: int = 300
    batch_scan_top_n: int = 50
    batch_scan_min_volume: float = 1000
    
    full_scan_interval: int = 900
    full_scan_granularity: int = 3600


@dataclass
class RiskConfig:
    """Risk management configuration."""
    # Portfolio limits
    max_portfolio_drawdown_pct: float = 15.0
    max_daily_loss_pct: float = 5.0
    max_sector_exposure_pct: float = 30.0
    max_single_asset_pct: float = 10.0
    max_correlated_positions: int = 3
    max_leverage: float = 1.5
    min_cash_buffer_pct: float = 5.0
    
    # Per-trade limits
    risk_per_trade_pct: float = 1.0
    min_risk_reward: float = 1.5
    max_notional_per_trade_usd: float = 5000.0
    
    # Sizing
    kelly_fraction: float = 0.25
    max_position_pct: float = 0.10
    min_trade_usd: float = 25.0
    
    # Cooldowns
    product_cooldown_seconds: int = 300
    max_new_positions: int = 50


@dataclass
class ThresholdConfig:
    """Entry/exit thresholds."""
    paper_min_confidence: float = 0.30
    paper_min_win_rate: float = 0.35
    paper_min_sharpe: float = 0.3
    paper_min_edge_bps: float = 0.5
    paper_maker_pct: float = 0.50
    min_change_pct: float = 0.05


@dataclass
class LiveConfig:
    """Live trading specific config."""
    require_approval: bool = True
    approval_base_url: str = "http://localhost:8080"
    dry_run: bool = True
    max_positions: int = 10
    max_position_pct: float = 0.05
    max_drawdown_pct: float = 10.0
    max_daily_loss_pct: float = 3.0
    max_notional_per_trade_usd: float = 100.0
    risk_per_trade_pct: float = 0.5
    min_risk_reward: float = 2.0


@dataclass
class AppConfig:
    """Complete application configuration."""
    mode: str = "paper"  # "paper", "live", "approval"
    environment: str = "development"
    
    feature_flags: FeatureFlags = field(default_factory=FeatureFlags)
    scan: ScanConfig = field(default_factory=ScanConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    live: LiveConfig = field(default_factory=LiveConfig)
    
    # Logging
    log_level: str = "INFO"
    log_file: str = "logs/trader-v4.log"
    
    # Health/metrics
    health_port: int = 9090
    metrics_port: int = 9091
    
    # WebSocket
    ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    adv_ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    
    # Data
    candle_cache_ttl: Dict[int, float] = field(default_factory=lambda: {
        60: 15.0, 300: 45.0, 900: 90.0, 3600: 180.0, 21600: 300.0, 86400: 600.0
    })


class ConfigManager:
    """Hot-reload YAML configuration manager with feature flags."""
    
    def __init__(self, config_path: str = "config/app.yaml"):
        self.config_path = Path(config_path)
        self._config: Optional[AppConfig] = None
        self._lock = threading.RLock()
        self._last_modified: float = 0
        self._callbacks: List[Callable[[AppConfig, AppConfig], None]] = []
        self._watcher_thread: Optional[threading.Thread] = None
        self._watching = False
        
        # Ensure config directory exists
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load initial config
        self._load()
        
        # Start watcher
        self.start_watcher()
    
    def _load(self) -> None:
        """Load config from YAML file."""
        with self._lock:
            old_config = self._config
            
            # Start with defaults
            config = AppConfig()
            
            # Override with YAML if exists
            if self.config_path.exists():
                with open(self.config_path, "r") as f:
                    data = yaml.safe_load(f) or {}
                
                if data:
                    config = self._merge_config(config, data)
            
            # Override with environment variables
            config = self._apply_env_overrides(config)
            
            self._config = config
            self._last_modified = self.config_path.stat().st_mtime if self.config_path.exists() else 0
            
            # Notify callbacks
            if old_config is not None:
                for cb in self._callbacks:
                    try:
                        cb(old_config, config)
                    except Exception as e:
                        log.error(f"Config callback error: {e}")
            
            log.info(f"Configuration loaded from {self.config_path}")
    
    def _merge_config(self, base: AppConfig, overrides: Dict[str, Any]) -> AppConfig:
        """Recursively merge override dict into base config."""
        result = AppConfig()
        
        # Copy all fields from base
        for field_name in base.__dataclass_fields__:
            setattr(result, field_name, getattr(base, field_name))
        
        # Apply overrides
        for key, value in overrides.items():
            if hasattr(result, key):
                attr = getattr(result, key)
                if hasattr(attr, '__dataclass_fields__'):
                    # Nested dataclass - merge recursively
                    merged = self._merge_dataclass(attr, value)
                    setattr(result, key, merged)
                elif isinstance(attr, dict) and isinstance(value, dict):
                    merged = {**attr, **value}
                    setattr(result, key, merged)
                else:
                    setattr(result, key, value)
            else:
                log.warning(f"Unknown config key: {key}")
        
        return result
    
    def _merge_dataclass(self, base_obj: Any, overrides: Dict[str, Any]) -> Any:
        """Merge dict into dataclass instance."""
        result = type(base_obj)()
        for field_name in base_obj.__dataclass_fields__:
            base_val = getattr(base_obj, field_name)
            if field_name in overrides:
                override_val = overrides[field_name]
                if hasattr(base_val, '__dataclass_fields__') and isinstance(override_val, dict):
                    setattr(result, field_name, self._merge_dataclass(base_val, override_val))
                elif isinstance(base_val, dict) and isinstance(override_val, dict):
                    setattr(result, field_name, {**base_val, **override_val})
                else:
                    setattr(result, field_name, override_val)
            else:
                setattr(result, field_name, base_val)
        return result
    
    def _apply_env_overrides(self, config: AppConfig) -> AppConfig:
        """Apply environment variable overrides."""
        env_mappings = {
            # Mode
            "TRADING_MODE": ("mode", str),
            "ENVIRONMENT": ("environment", str),
            
            # Feature flags
            "ENABLE_LIVE_TRADING": ("feature_flags.enable_live_trading", lambda x: x.lower() == "true"),
            "ENABLE_PAPER_TRADING": ("feature_flags.enable_paper_trading", lambda x: x.lower() == "true"),
            "ENABLE_SHORTS": ("feature_flags.enable_shorts", lambda x: x.lower() == "true"),
            "ENABLE_LEVERAGE": ("feature_flags.enable_leverage", lambda x: x.lower() == "true"),
            
            # Scan
            "MINUTE_SCAN_INTERVAL": ("scan.minute_scan_interval", int),
            "MINUTE_SCAN_TOP_N": ("scan.minute_scan_top_n", int),
            "FULL_SCAN_INTERVAL": ("scan.full_scan_interval", int),
            
            # Risk
            "MAX_PORTFOLIO_DRAWDOWN": ("risk.max_portfolio_drawdown_pct", float),
            "MAX_DAILY_LOSS": ("risk.max_daily_loss_pct", float),
            "MAX_POSITION_PCT": ("risk.max_single_asset_pct", float),
            "MAX_LEVERAGE": ("risk.max_leverage", float),
            "RISK_PER_TRADE": ("risk.risk_per_trade_pct", float),
            
            # Thresholds
            "PAPER_MIN_CONFIDENCE": ("thresholds.paper_min_confidence", float),
            "PAPER_MIN_WIN_RATE": ("thresholds.paper_min_win_rate", float),
            "PAPER_MIN_SHARPE": ("thresholds.paper_min_sharpe", float),
            "PAPER_MIN_EDGE_BPS": ("thresholds.paper_min_edge_bps", float),
            
            # Live
            "REQUIRE_APPROVAL": ("live.require_approval", lambda x: x.lower() == "true"),
            "DRY_RUN": ("live.dry_run", lambda x: x.lower() == "true"),
            
            # Ports
            "HEALTH_PORT": ("health_port", int),
            "METRICS_PORT": ("metrics_port", int),
            
            # Logging
            "LOG_LEVEL": ("log_level", str),
        }
        
        for env_key, (path, converter) in env_mappings.items():
            env_val = os.getenv(env_key)
            if env_val is not None:
                try:
                    value = converter(env_val)
                    self._set_nested(config, path, value)
                    log.info(f"Config override from env: {path} = {value}")
                except Exception as e:
                    log.warning(f"Failed to apply env override {env_key}: {e}")
        
        return config
    
    def _set_nested(self, obj: Any, path: str, value: Any) -> None:
        """Set nested attribute using dot notation."""
        parts = path.split(".")
        for part in parts[:-1]:
            obj = getattr(obj, part)
        setattr(obj, parts[-1], value)
    
    def get(self) -> AppConfig:
        """Get current config (thread-safe)."""
        with self._lock:
            return self._config
    
    def get_feature(self, name: str) -> Any:
        """Get a feature flag value."""
        with self._lock:
            return getattr(self._config.feature_flags, name, False)
    
    def set_feature(self, name: str, value: bool) -> None:
        """Set a feature flag at runtime."""
        with self._lock:
            if hasattr(self._config.feature_flags, name):
                setattr(self._config.feature_flags, name, value)
                log.info(f"Feature flag changed: {name} = {value}")
    
    def register_callback(self, callback: Callable[[AppConfig, AppConfig], None]) -> None:
        """Register a callback for config changes."""
        with self._lock:
            self._callbacks.append(callback)
    
    def start_watcher(self, interval: float = 5.0) -> None:
        """Start file watcher thread."""
        if self._watching:
            return
        
        self._watching = True
        self._watcher_thread = threading.Thread(
            target=self._watch_loop, 
            args=(interval,), 
            daemon=True,
            name="config_watcher"
        )
        self._watcher_thread.start()
        log.info("Config watcher started")
    
    def stop_watcher(self) -> None:
        """Stop file watcher."""
        self._watching = False
        if self._watcher_thread:
            self._watcher_thread.join(timeout=2.0)
    
    def _watch_loop(self, interval: float) -> None:
        while self._watching:
            try:
                if self.config_path.exists():
                    mtime = self.config_path.stat().st_mtime
                    if mtime > self._last_modified:
                        log.info("Config file changed, reloading...")
                        self._load()
                time.sleep(interval)
            except Exception as e:
                log.error(f"Config watcher error: {e}")
                time.sleep(interval)
    
    def save(self) -> None:
        """Save current config to YAML."""
        with self._lock:
            data = self._config_to_dict(self._config)
            with open(self.config_path, "w") as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            self._last_modified = self.config_path.stat().st_mtime
            log.info(f"Config saved to {self.config_path}")
    
    def _config_to_dict(self, config: AppConfig) -> Dict[str, Any]:
        """Convert config to dict for YAML serialization."""
        result = {}
        for field_name in config.__dataclass_fields__:
            value = getattr(config, field_name)
            if hasattr(value, '__dataclass_fields__'):
                result[field_name] = self._config_to_dict(value)
            elif isinstance(value, dict):
                result[field_name] = value
            else:
                result[field_name] = value
        return result


# Global config manager
_CONFIG_MANAGER: Optional[ConfigManager] = None
_CONFIG_LOCK = threading.Lock()


def get_config() -> AppConfig:
    """Get current application config."""
    global _CONFIG_MANAGER
    with _CONFIG_LOCK:
        if _CONFIG_MANAGER is None:
            _CONFIG_MANAGER = ConfigManager()
        return _CONFIG_MANAGER.get()


def get_config_manager() -> ConfigManager:
    """Get config manager instance."""
    global _CONFIG_MANAGER
    with _CONFIG_LOCK:
        if _CONFIG_MANAGER is None:
            _CONFIG_MANAGER = ConfigManager()
        return _CONFIG_MANAGER


def is_feature_enabled(name: str) -> bool:
    """Check if a feature flag is enabled."""
    return get_config_manager().get_feature(name)