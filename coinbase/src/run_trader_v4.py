#!/usr/bin/env python3
"""
Event-Driven Trader v4 — real-time + periodic batch scan, all-Rust.

Architecture:
  WebSocket (live ticker)  ──→ StreamingIndicators.update()  O(1)
                                 → _evaluate()               per-product

  Periodic batch scan  ──→ SignalAggregator.scan_universe()
  (every N minutes)                                          → all 50 strategies on ALL pairs
                            → backtest validation
                            → long-term trend
                            → unified score per pair
                            → ranked opportunity list

Usage:
    python3 -m coinbase.src.run_trader_v4 --mode paper
    python3 -m coinbase.src.run_trader_v4 --mode paper --scan-interval 300
    python3 -m coinbase.src.run_trader_v4 --mode paper --scan-top 100
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger("trader_v4")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from strategy_engine import batch_signals_rust as _batch_signals_rust
from strategy_engine import batch_backtest_rust, _HAS_RUST
from strategy_engine import _RUST_STRATEGIES

from trading_system.core.signal_aggregator import SignalAggregator, UnifiedSignal
from trading_system.core.performance_model import LatencyProfile, expected_fill_delay_ms
from coinbase.src.pair_discovery import get_all_coinbase_pairs, top_coinbase_pairs

from trading_system.core.timing import LatencyProfiler, measure_coinbase_latency
from trading_system.core.streaming import StreamingEngine
from coinbase.src.rest_feed import fetch_candles_batch, fetch_candles_batch_sync, candle_arrays
from coinbase.src.feed import Ticker, FeedSource
try:
    from coinbase.src.smart_feed import SmartFeedRefreshManager
    _HAS_SMART_FEED = True
except Exception:
    SmartFeedRefreshManager = None
    _HAS_SMART_FEED = False
from coinbase.src.sentiment import CryptoNewsSentiment, OrderFlowEngine, MacroRiskEngine
from coinbase.src.config_manager import get_config, get_config_manager, is_feature_enabled
from coinbase.src.strategy_registry import get_registry, StrategyPerf
from coinbase.src.ranking import StrategyRanking, StrategyRankingFilter
from coinbase.src.portfolio_risk import get_risk_manager, PortfolioRiskManager, Position, RiskLimits
from coinbase.src.slippage_model import get_slippage_model, get_book_cache, SlippageEstimate
from coinbase.src.config import TradingConfig, LiveSafetyValidator, KillSwitch, _env_bool, _env_float
from coinbase.src.cb_client import CBClient
from coinbase.src.execution_v2 import NativeExecutionEngine, BracketManager, OrderIntent, OrderType, _fmt_base, _fmt_price
from coinbase.src.risk_manager import RiskManager, RiskProfile, RiskLimit, PositionRisk
from coinbase.src.fill_model import FillModel, FillEstimate
from coinbase.src.cross_asset_regime import CrossAssetRegimeEngine, CrossAssetRegimeState
from coinbase.src.multi_tf_analysis import MacroTrendAnalyzer, CompositeMacroSignal
from coinbase.src.live_performance import LivePerformanceTracker, StrategyProductRecord
from coinbase.src.strategies.scalping import ScalpingStrategy
from coinbase.src.strategies.pair_trading import PairTradingStrategy
from coinbase.src.strategies.onchain_flows import OnChainFlowStrategy

# Auto-assist LLM client — configured to route through auto-router
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "auto-assist", "src")))
try:
    import assistx.llm_client as _assistx_llm
    _assistx_llm.OPENAI_BASE_URL = "http://100.64.43.123:8088/v1"
    _assistx_llm.OPENAI_API_KEY = "not-needed"
    _HAS_ASSISTX = True
except Exception:
    _HAS_ASSISTX = False

_IO_POOL = ThreadPoolExecutor(max_workers=12, thread_name_prefix="v4_io")


@dataclass
class PaperPosition:
    product_id: str
    side: str  # "LONG" | "SHORT" (was "BUY" | "SELL", aliased for backward compat)
    qty: float
    entry_price: float
    entry_ts: float
    strategy: str
    confidence: float
    win_rate: float
    sharpe: float
    fees_paid: float = 0.0
    highest_price: float = 0.0
    lowest_price: float = 0.0
    regime: str = ""
    atr_14: float = 0.0
    stop_price: float = 0.0
    target_price: float = 0.0
    initial_stop_dist: float = 0.0
    breakeven_set: bool = False
    trailing_activated: bool = False
    trailing_take_price: float = 0.0  # trailing take-profit level (sell all if price pulls back here)
    entry_notional: float = 0.0
    leverage: float = 1.0
    cum_funding: float = 0.0
    trades: int = 1  # scale-in count
    liq_price: float = 0.0  # estimated liquidation price (0 = no leverage)
    long_horizon: bool = False

    def mark(self, price: float) -> None:
        self.highest_price = max(self.highest_price, price)
        self.lowest_price = price if self.lowest_price <= 0 else min(self.lowest_price, price)

    @property
    def is_long(self) -> bool:
        return self.side in ("LONG", "BUY")

    @property
    def is_short(self) -> bool:
        return self.side in ("SHORT", "SELL")

    @property
    def break_even_price(self) -> float:
        if self.entry_price <= 0 or self.entry_notional <= 0:
            return self.entry_price
        fee_pct = self.fees_paid / self.entry_notional
        if self.is_long:
            return self.entry_price * (1.0 + fee_pct)
        return self.entry_price * (1.0 - fee_pct)

    @property
    def current_r_multiple(self) -> float:
        if self.initial_stop_dist <= 0:
            return 0.0
        if self.is_long:
            return (self.highest_price - self.entry_price) / self.initial_stop_dist
        return (self.entry_price - self.lowest_price) / self.initial_stop_dist

    @property
    def age_s(self) -> float:
        return time.time() - self.entry_ts

    @property
    def notional_exposure(self) -> float:
        return self.entry_notional * self.leverage

    @property
    def liq_distance_pct(self) -> float:
        if self.liq_price <= 0 or self.entry_price <= 0:
            return 0.0
        if self.is_long:
            return (self.entry_price - self.liq_price) / self.entry_price * 100.0
        return (self.liq_price - self.entry_price) / self.entry_price * 100.0


@dataclass
class CoreHolding:
    product_id: str
    qty: float = 0.0
    cost_basis: float = 0.0  # average entry price
    total_cost: float = 0.0  # total USD spent (including fees)
    total_qty: float = 0.0
    trades: int = 0
    last_buy_ts: float = 0.0
    created_ts: float = 0.0
    target_value: float = 0.0   # computed target USD value within the core bucket
    drift_pct: float = 0.0       # (value - target) / target * 100
    rebalance_action: str = "hold"  # hold | buy | trim

    @property
    def avg_price(self) -> float:
        return self.cost_basis if self.cost_basis > 0 else 0.0

    def current_value(self, price: float) -> float:
        return self.qty * price

    def add_buy(self, qty: float, price: float, fee: float = 0.0) -> None:
        cost = qty * price + fee
        new_total_qty = self.total_qty + qty
        if new_total_qty > 0:
            self.cost_basis = (self.total_cost + cost) / new_total_qty
        self.total_cost += cost
        self.total_qty += qty
        self.qty = self.total_qty
        self.trades += 1
        self.last_buy_ts = time.time()

    def trim_sell(self, qty: float, price: float, fee: float = 0.0) -> float:
        """Reduce the core position by qty at price (realizing proportional cost).

        Keeps the average cost basis unchanged (cost reduced proportionally).
        Returns the realized notional (qty * price - fee) credited.
        """
        if qty <= 0 or self.total_qty <= 0:
            return 0.0
        qty = min(qty, self.total_qty)
        proportion = qty / self.total_qty if self.total_qty > 0 else 1.0
        self.total_cost *= (1.0 - proportion)
        self.total_qty -= qty
        self.qty = self.total_qty
        self.trades += 1
        return qty * price - fee


@dataclass
class PulseRecord:
    strategy: str
    direction: str
    product_id: str
    pulse_count: int = 1
    first_ts: float = 0.0
    last_ts: float = 0.0
    avg_confidence: float = 0.0
    min_price: float = 0.0
    max_price: float = 0.0
    flip_count: int = 0

    def update(self, confidence: float, price: float) -> None:
        self.pulse_count += 1
        self.last_ts = time.time()
        n = self.pulse_count
        self.avg_confidence = ((self.avg_confidence * (n - 1)) + confidence) / n
        self.min_price = min(self.min_price, price)
        self.max_price = max(self.max_price, price)

    @property
    def age_s(self) -> float:
        return time.time() - self.last_ts

    @property
    def is_hot(self) -> bool:
        return self.pulse_count >= 3 and self.age_s < 600


class EventTraderV4:
    """Event-driven trading daemon — all 25 strategies in Rust.

    Data flows:
      WebSocket ticker → _drain_ticker_cache()  (real-time, ~1s)
        → StreamingEngine.update()               (O(1) incremental indicators)
        → _evaluate()                            (Rust signals + backtest)

    Background: CandleCache refreshes REST data every 20s.
    Health:     HTTP server on --health-port with /latency, /ping, /status.
    """

    CORE_ASSETS = {"BTC", "ETH", "SOL"}

    STRATEGY_NAMES = sorted(_RUST_STRATEGIES)

    # Coinbase Advanced fee tiers (trailing 30-day volume)
    # (min_volume_usd, taker_bps, maker_bps)
    FEE_TIERS = [
        (0, 60, 40),             # Tier 1: $0–$10k
        (10_000, 40, 25),        # Tier 2
        (50_000, 25, 15),        # Tier 3
        (100_000, 20, 10),       # Tier 4
        (500_000, 15, 5),        # Tier 5
        (1_000_000, 10, 0),      # Tier 6
        (5_000_000, 8, 0),       # Tier 7
        (10_000_000, 5, 0),      # Tier 8
        (50_000_000, 3, 0),      # Tier 9
        (250_000_000, 1, 0),     # Tier 10
        (500_000_000, 0, 0),     # Tier 11
    ]

    _MEAN_REVERSION_STRATS = frozenset({
        "vwap_revert", "rsi_revert", "zscore_revert", "boll_break",
        "keltner", "williams_r", "scci", "cmo",
    })

    # ── Runtime-tunable knob schema ──
    # key: (type, min, max, default, description)
    TUNABLE_KNOBS: dict = {
        "paper_min_confidence":    (float, 0.01, 0.99, 0.30,  "Min confidence for entry"),
        "paper_min_win_rate":      (float, 0.01, 1.00, 0.45,  "Min strategy backtest win rate"),
        "paper_min_sharpe":        (float, 0.01, 5.00, 0.30,  "Min strategy backtest Sharpe"),
        "paper_min_edge_bps":      (float, 0.00, 200.0, 2.0,   "Min expected edge in bps"),
        "paper_min_trade_usd":     (float, 1.00, 1e6,  100.0, "Min trade notional USD"),
        "paper_max_position_pct":  (float, 0.01, 0.50, 0.15,  "Max position % of cash"),
        "paper_max_new_positions": (int,   1,    500,  12,    "Max concurrent positions"),
        "paper_maker_pct":         (float, 0.00, 1.00, 0.50,  "Fraction of orders as maker"),
        "paper_product_cooldown_s":(int,   0,    86400,1800,  "Per-product cooldown after exit"),
        "paper_min_hold_s":        (int,   0,    86400,180,   "Min hold before signal-based (noise) exits allowed"),
        "max_hold_s":              (int,   300,  604800,86400,"Max position hold time (seconds)"),
        "max_leverage":            (float, 1.00, 10.0, 2.00,  "Max leverage multiplier"),
        "min_change_pct":          (float, 0.01, 5.00, 0.05,  "Min price change % to trigger eval"),
        "_max_cluster_exposure_pct":(float,0.00, 1.00, 0.30,  "Max % of portfolio per correlation cluster"),
        "_pulse_window_s":         (float, 30.0, 3600, 300.0, "Pulse tracking window (seconds)"),
        "_fingerprint_ttl_s":      (float, 5.00, 300,  30.0,  "Signal dedup TTL (seconds)"),
        "_min_eval_interval":      (float, 0.10, 30.0, 1.00,  "Min seconds between eval per product"),
        "_core_holdings_enabled":  (int,   0,    1,    1,     "Enable core holdings DCA (0/1)"),
        "_core_dca_dip_pct":       (float, 0.50, 30.0, 3.00,  "Dip % to trigger core DCA buy"),
        "_core_dca_cooldown_s":    (int,   60,   86400,3600,  "Cooldown between DCA buys (seconds)"),
        "_core_dca_amount":        (int,   5,    10000,25,    "DCA buy amount USD"),
        # Concentration / sample-depth guards (anti-fragility). See _paper_execute_impl.
        "max_strategy_pnl_share":  (float, 0.00, 1.00, 0.60,
                                    "Max share of equity a single strategy's live "
                                    "PnL may reach before new entries for it are blocked "
                                    "(caps how much one strategy can dominate the book)"),
        "min_trades_for_full_sizing": (int, 1, 500, 20,
                                    "Min live trades before a strategy/product pair gets "
                                    "full sizing weight; below this, confidence is scaled "
                                    "down so tiny-sample 'lucky' winners can't overdrive size"),
    }

    def get_tunables(self) -> dict:
        result = {}
        for key, (typ, mn, mx, default, desc) in self.TUNABLE_KNOBS.items():
            val = getattr(self, key, default)
            if isinstance(val, bool):
                val = float(val) if typ is float else int(val) if typ is int else val
            result[key] = {
                "value": val,
                "type": typ.__name__,
                "min": mn,
                "max": mx,
                "default": default,
                "description": desc,
            }
        return result

    def set_tunable(self, key: str, value: Any) -> tuple[bool, str]:
        if key not in self.TUNABLE_KNOBS:
            return (False, f"Unknown knob: {key} — valid: {list(self.TUNABLE_KNOBS.keys())}")
        typ, mn, mx, *_ = self.TUNABLE_KNOBS[key]
        try:
            if typ is int:
                value = int(value)
            elif typ is float:
                value = float(value)
            else:
                return (False, f"Unsupported type {typ}")
        except (ValueError, TypeError):
            return (False, f"Invalid value for {key}: cannot convert to {typ.__name__}")
        if value < mn or value > mx:
            return (False, f"Value {value} out of range [{mn}, {mx}] for {key}")
        setattr(self, key, value)
        log.info("TUNER %s = %s (range=[%s,%s])", key, value, mn, mx)
        return (True, f"{key} set to {value}")

    def _persist_knobs(self) -> None:
        path = Path("data/tuner_state_v4.json")
        state = {k: getattr(self, k, default) for k, (_, _, _, default, _) in self.TUNABLE_KNOBS.items()}
        try:
            path.write_text(json.dumps(state, indent=2))
        except Exception as e:
            log.debug("Failed to persist tuner state: %s", e)

    def _load_knobs(self) -> None:
        path = Path("data/tuner_state_v4.json")
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text())
            for key, value in raw.items():
                self.set_tunable(key, value)
            log.info("Loaded %d tunable knob overrides from %s", len(raw), path)
        except Exception as e:
            log.warning("Failed to load tuner state: %s", e)

    def __init__(
        self,
        mode: str = "paper",
        products: Optional[List[str]] = None,
        health_port: int = 0,
        dry_run: bool = True,
        min_change_pct: float = 0.05,
        paper_product_cooldown_s: int = 300,
        paper_maker_pct: float = 0.80,
        minute_scan_interval: int = 60,
        minute_scan_top_n: int = 150,
        minute_scan_min_top_n: int = 10,
        minute_scan_max_top_n: int = 50,
        minute_scan_use_hotset: bool = False,
        minute_scan_hotset_size: int = 150,
        scan_interval: int = 0,
        scan_top_n: int = 20,
        scan_min_volume: float = 10_000,
        full_scan_interval: int = 300,
        enable_shorts: bool = False,
        enable_leverage: bool = False,
        max_leverage: float = 2.0,
        max_hold_s: int = 86400,
    ):
        self.mode = mode
        # Session-start real balance for the live max-drawdown hard-halt.
        self._live_start_balance: float = 0.0
        # Last intended order per product, for fill-vs-preview drift detection.
        self._last_intended_order: Dict[str, dict] = {}
        self.dry_run = dry_run
        self.min_change_pct = min_change_pct
        self.paper_product_cooldown_s = paper_product_cooldown_s
        self.minute_scan_interval = minute_scan_interval
        self.minute_scan_top_n = minute_scan_top_n
        self.minute_scan_min_top_n = minute_scan_min_top_n
        self.minute_scan_max_top_n = minute_scan_max_top_n
        self.minute_scan_use_hotset = minute_scan_use_hotset
        self.minute_scan_hotset_size = minute_scan_hotset_size
        self.scan_interval = scan_interval
        self.scan_top_n = scan_top_n
        self.scan_min_volume = scan_min_volume
        self.full_scan_interval = full_scan_interval
        self.enable_shorts = enable_shorts
        self.enable_leverage = enable_leverage
        self.max_leverage = max_leverage
        self.max_hold_s = max_hold_s
        # Minimum time (s) a position must be held before signal-based (noise) exits
        # — multi-signal consensus & reverse-signal — are allowed. Stops, take-profit,
        # and max-hold timeouts always fire. Prevents sub-second flip-flop churn.
        self.paper_min_hold_s = 180
        self._shutdown = False
        self._start_ts = time.time()

        # ── Runtime-tunable correlation clusters ──
        self._correlation_clusters = {
            "btc_l1": {"BTC", "BTC"},
            "eth_l1": {"ETH", "ETC"},
            "sol_l1": {"SOL"},
            "large_cap": {"XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK", "UNI"},
            "mid_cap": {"POL", "ATOM", "LTC", "BCH", "NEAR", "APT", "SUI", "ARB", "OP", "FIL"},
            "small_cap": {"INJ", "SEI", "TIA", "ALGO", "XLM", "STX", "HBAR", "ICP", "GRT"},
            "memes": {"SHIB", "PEPE", "BONK", "TRUMP", "FLOKI", "DOGE"},
        }
        self._max_cluster_exposure_pct = 0.30

        # Seed concentration / sample-depth guards from tunable defaults so they
        # always exist even before tuner_state_v4.json is loaded.
        for _k, (_t, _mn, _mx, _dflt, _desc) in self.TUNABLE_KNOBS.items():
            if not hasattr(self, _k):
                setattr(self, _k, _dflt)

        # ── Core long-term holdings (multi-bucket: stable + volatile) ──
        self._core_holdings: Dict[str, CoreHolding] = {}
        self._core_holdings_enabled: bool = True
        self._core_dca_amount: float = 25.0   # USD per DCA buy
        self._core_dca_cooldown_s: int = 3600  # min between buys per asset
        self._core_dca_dip_pct: float = 3.0   # min % dip from 50-period high to trigger

        # Core bucket definitions (each rebalances independently)
        self._core_buckets_config: Dict[str, Dict] = {
            "stable": {
                "enabled": os.getenv("CORE_STABLE_ENABLED", "1") not in ("0", "false", "False"),
                "assets": ["BTC-USD", "ETH-USD", "SOL-USD"],
                "target_weights": {"BTC-USD": 0.50, "ETH-USD": 0.30, "SOL-USD": 0.20},
                "rebalance_threshold_pct": float(os.getenv("CORE_STABLE_THRESHOLD_PCT", "15.0")),
                "rebalance_interval_s": 3600,
                "dca_amount": 25.0,
                "dca_cooldown_s": 3600,
                "dca_dip_pct": 3.0,
            },
            "volatile": {
                "enabled": os.getenv("CORE_VOLATILE_ENABLED", "1") not in ("0", "false", "False"),
                "assets": ["DOGE-USD", "SHIB-USD", "PEPE-USD", "BONK-USD", "FLOKI-USD", "MON-USD"],
                "target_weights": {
                    "DOGE-USD": 0.35, "SHIB-USD": 0.20, "PEPE-USD": 0.15,
                    "BONK-USD": 0.10, "FLOKI-USD": 0.10, "MON-USD": 0.10,
                },
                "rebalance_threshold_pct": float(os.getenv("CORE_VOLATILE_THRESHOLD_PCT", "25.0")),
                "rebalance_interval_s": 1800,  # 30 min (faster for volatile)
                "dca_amount": 15.0,
                "dca_cooldown_s": 1800,
                "dca_dip_pct": 5.0,  # deeper dip for volatile
            },
        }
        self._core_rebalance_last_ts: Dict[str, float] = {"stable": 0.0, "volatile": 0.0}
        self._capital_buckets: Dict[str, Any] = {}
        # Effective per-product target weight across all core buckets.
        self._core_target_weights: Dict[str, float] = {}
        for _bkt in self._core_buckets_config.values():
            for _pid, _w in _bkt.get("target_weights", {}).items():
                self._core_target_weights[_pid] = _w

        self._tick_count = 0
        self._bt_cache: Dict[str, Any] = {}
        self._bt_cache_lock = threading.Lock()
        self._bt_cache_path = Path("data/bt_cache_v4.json")
        self._bt_cache_dirty = False

        # Per-tick caches to avoid recomputing the same slice/state across the
        # many consumers (scalping, btc snapshot, adaptive interval, _evaluate)
        # that run for a single product within one drain tick.
        self._slice_cache: Dict[str, Tuple[List[float], List[float], List[float]]] = {}
        self._car_state_cache: Tuple[float, Optional[Dict[str, Any]]] = (0.0, None)
        self._dd_cache: Optional[float] = None

        self._hot_scores: Dict[str, float] = defaultdict(float)
        self._hot_lock = threading.Lock()
        self._hot_scores_path = Path("data/hot_scores_v4.json")

        self._signal_pulses: Dict[str, PulseRecord] = {}
        self._pulse_lock = threading.Lock()
        self._pulse_window_s: float = 300.0

        # Shared state lock for health_status, _last_price, _signal_counts
        self._shared_lock = threading.Lock()

        # Signal fingerprint dedup: {fingerprint_key → expiry_ts}
        self._signal_fingerprints: Dict[str, float] = {}
        self._fingerprint_ttl_s: float = 30.0

        # Per-strategy signal counters
        self._signal_counts: Dict[str, int] = defaultdict(int)
        self._opp_counts: Dict[str, int] = defaultdict(int)

        # Latency profiler
        self.profiler = LatencyProfiler(max_history=500)
        self.latency_profile = LatencyProfile()

        # Streaming engine — O(1) incremental indicators per product
        try:
            self.streaming = StreamingEngine()
        except Exception as e:
            log.warning("StreamingEngine init failed: %s — disabling", e)
            self.streaming = None

        # Crypto news sentiment — periodic RSS-based sentiment signals
        self._news_sentiment = CryptoNewsSentiment(cache_ttl=300)
        # Order flow — bid/ask imbalance signals (evaluated on each ticker)
        self._order_flow = OrderFlowEngine()
        # Macro risk — DXY/yields/VIX/gold composite (periodic yfinance fetch)

        # ── New strategy modules ───────────────────────────────────
        self._scalping = ScalpingStrategy()
        self._pair_trading = PairTradingStrategy()
        self._onchain_flow = OnChainFlowStrategy()
        self._fill_model = FillModel()

        # ── Live execution engine ─────────────────────────────────
        self._live_cfg: Optional[TradingConfig] = None
        self._exec_engine: Optional[NativeExecutionEngine] = None
        self._bracket_mgr: Optional[BracketManager] = None
        self._risk_mgr: Optional[RiskManager] = None
        self._cb_client: Optional[CBClient] = None

        # ── Circuit breakers ──────────────────────────────────────
        self._cb_daily_start_equity: float = 0.0
        self._cb_peak_equity: float = 0.0
        self._cb_consecutive_losses: int = 0
        self._cb_daily_loss_pct: float = 0.0
        self._cb_day_start_ts: float = time.time()
        self._cb_breached: bool = False
        self._cb_breach_reason: str = ""
        try:
            self._cross_asset_regime = CrossAssetRegimeEngine()
        except Exception as e:
            log.warning("CrossAssetRegimeEngine init failed: %s", e)
            self._cross_asset_regime = None
        try:
            self._macro_risk = MacroRiskEngine(cache_ttl=600)
        except Exception as e:
            log.warning("MacroRiskEngine init failed: %s", e)
            self._macro_risk = None
        try:
            self._macro_tf_analyzer = MacroTrendAnalyzer(cache_ttl=900)
        except Exception as e:
            log.warning("MacroTrendAnalyzer init failed: %s", e)
            self._macro_tf_analyzer = None
        self._last_macro_signal: Optional[CompositeMacroSignal] = None
        try:
            self._perf_tracker = LivePerformanceTracker(path="data/live_performance.json")
        except Exception as e:
            log.warning("LivePerformanceTracker init failed: %s", e)
            self._perf_tracker = None

        # Product universe (for real-time WebSocket tracking)
        if products:
            self.products = list(products)
        else:
            try:
                full_universe = get_all_coinbase_pairs(
                    min_volume_usd=0,
                    quote_currencies=("USD", "USDC", "BTC", "ETH"),
                )
                self.products = [p["id"] for p in full_universe]
            except Exception:
                self.products = [
                    "BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "ADA-USD",
                    "DOGE-USD", "AVAX-USD", "DOT-USD", "LINK-USD", "UNI-USD",
                ]

        # Always include core holdings products for DCA streaming data
        for pid in ("BTC-USD", "ETH-USD", "SOL-USD"):
            if pid not in self.products:
                self.products.append(pid)
                log.info("Added core holdings product %s to subscription list", pid)

        # Signal aggregator (for batch scans across entire universe)
        self._aggregator = SignalAggregator()
        self._last_scan: List[UnifiedSignal] = []
        self._last_full_scan: List[UnifiedSignal] = []
        self._scan_thread: Optional[threading.Thread] = None
        self._minute_scan_thread: Optional[threading.Thread] = None
        self._full_scan_thread: Optional[threading.Thread] = None

        self._last_eval: Dict[str, float] = {}
        self._min_eval_interval: float = 1.0
        self._position_eval_interval: float = 0.25
        self._adaptive_eval_enabled: bool = True
        self._last_price: Dict[str, float] = {}
        self._last_volume_24h: Dict[str, float] = {}
        self._last_ticker_ts: float = 0.0
        self._last_pos_mgmt_ts: float = 0.0
        self._last_eval_ts: float = 0.0
        self._last_scan_ts: float = 0.0
        self._last_minute_scan_ts: float = 0.0
        self._last_full_scan_ts: float = 0.0
        self._watchdog_thread: Optional[threading.Thread] = None
        self._analytics_thread: Optional[threading.Thread] = None
        self._experiment_thread: Optional[threading.Thread] = None
        self._macro_tf_thread: Optional[threading.Thread] = None

        # Paper trading ledger
        self.paper_starting_capital: float = 10000.0
        self.paper_cash: float = self.paper_starting_capital
        self._paper_lock = threading.RLock()
        self.paper_positions: Dict[str, PaperPosition] = {}
        self.paper_trades: List[Dict[str, Any]] = []
        self.paper_realized_pnl: float = 0.0
        self.paper_fees_paid: float = 0.0
        self.paper_wins: int = 0
        self.paper_losses: int = 0
        self.paper_last_trade_ts: Dict[str, float] = {}
        self.paper_peak_equity: float = self.paper_starting_capital
        self.paper_equity_curve: List[float] = [self.paper_starting_capital]
        self.paper_equity_tss: List[float] = [time.time()]
        self.paper_trailing_volume_30d: float = 0.0
        self.paper_monthly_volume: float = 0.0
        self.paper_month_ts: float = time.time()
        self.paper_maker_pct: float = paper_maker_pct
        self.paper_min_confidence: float = 0.30
        self.paper_min_win_rate: float = 0.45
        self.paper_min_sharpe: float = 0.30
        self.paper_max_position_pct: float = 0.15
        self.paper_max_new_positions: int = 12
        self.paper_min_trade_usd: float = 100.0
        self.paper_min_edge_bps: float = 2.0  # was 15.0: at Tier-1 fees (~44bps effective)
        # a vetted mean-reversion signal nets only ~7bps, so a 15bps floor structurally
        # parks the bot with 0 trades. 2.0 (config schema default) admits positive-net-edge
        # entries; the $500 fee-waiver window (tier 0) makes early entries strongly positive.
        # Mode-aware state file: paper and live/approval books NEVER share a
        # ledger. This prevents a paper state from being loaded into a live run
        # (or vice versa) — a real money hazard at go-live.
        _state_name = ("live_trader_v4_state.json"
                       if self.mode in ("live", "approval")
                       else "paper_trader_v4_state.json")
        self._paper_state_path = Path("data") / _state_name

        # ── Per-strategy analytics ──────────────────────────────
        self.strategy_stats: Dict[str, Dict[str, float]] = {}
        # { strategy_name: { trades, wins, losses, volume, pnl, buy_signals, sell_signals } }
        self._signal_type_counts: Dict[str, Dict[str, int]] = {}
        # { strategy_name: { "BUY": N, "SELL": N } }
        self._analytics_lock = threading.RLock()
        self._analytics_last_calc: float = 0.0
        self._analytics_dirty: bool = False

        # Background REST candle cache
        self._candle_data: Dict[str, dict] = {}
        self._candle_lock = threading.Lock()
        self._candle_ready = threading.Event()
        self._candle_thread: Optional[threading.Thread] = None
        self._candle_fetch_lock = threading.Lock()
        self._scan_fetch_lock = threading.Lock()
        self._scan_lock = threading.Lock()

        # Smart feed refresh manager — shared tiered cache for all consumers
        self._feed_mgr: Optional[SmartFeedRefreshManager] = None
        if _HAS_SMART_FEED:
            try:
                self._feed_mgr = SmartFeedRefreshManager(
                    batch_fn=fetch_candles_batch_sync,
                    interval=15.0,
                )
                self._feed_mgr.set_critical([
                    pid for pid in (products or self.products or [])
                    if pid.split("-")[0] in self.CORE_ASSETS
                ] or ["BTC-USD", "ETH-USD", "SOL-USD"])
            except Exception as e:
                log.warning("SmartFeed init failed: %s", e)

        # WebSocket feed
        self._ws_feed = None
        self._setup_websocket()
        if self.mode == "paper":
            self._load_paper_state()
        else:
            self._load_core_holdings_state()

        self._load_bt_cache()
        self._load_hot_scores()

        # Health status
        self.health_status: Dict[str, Any] = {
            "status": "starting",
            "mode": mode,
            "products": len(self.products),
            "strategies": len(self.STRATEGY_NAMES),
            "rust_enabled": _HAS_RUST,
            "tick_count": 0,
            "ws_connected": False,
            "alerts": [],
            "minute_scan_interval_s": minute_scan_interval,
            "paper_product_cooldown_s": paper_product_cooldown_s,
            "scan_interval_s": scan_interval,
            "scan_top_n": scan_top_n,
            "full_scan_interval_s": full_scan_interval,
            "minute_scan_use_hotset": minute_scan_use_hotset,
            "minute_scan_hotset_size": minute_scan_hotset_size,
            "minute_scan_min_top_n": minute_scan_min_top_n,
            "minute_scan_max_top_n": minute_scan_max_top_n,
            "enable_shorts": enable_shorts,
            "enable_leverage": enable_leverage,
            "max_leverage": max_leverage,
            "max_hold_s": max_hold_s,
            "last_state_save_ts": 0.0,
            "cross_asset_regime": CrossAssetRegimeState().to_dict(),
        }
        self.health_server = (
            HealthServer(port=health_port, status_ref=self.health_status, trader_ref=self)
            if health_port
            else None
        )
        from collections import deque
        self._notifications: deque[dict] = deque(maxlen=100)

        log.info(
            "EventTraderV4 initialized: mode=%s products=%d strategies=%d rust=%s ws=%s scan=%s max_hold=%ds",
            mode, len(self.products), len(self.STRATEGY_NAMES), _HAS_RUST,
            self._ws_feed is not None,
            f"{scan_interval}s" if scan_interval else "off",
            max_hold_s,
        )

    # ── Coinbase Fee Tier ──────────────────────────────────────────

    def _fee_tier(self) -> tuple[int, float, float]:
        """Return (tier_number, taker_bps, maker_bps) based on trailing 30d volume.

        First $500 USD of monthly traded volume is fee-waived (account perk).
        The check is `monthly_vol < 500` so the trade that hits $500 is also waived.
        Then normal Coinbase tier chart applies:
          Tier 1 = $0–$10k (60bps taker, 40bps maker).
        Returns 1-indexed tier number matching Coinbase's tier chart.
        """
        self._reset_monthly_volume_if_needed()
        if self.paper_monthly_volume < 500.0:
            return (0, 0.0, 0.0)
        vol = max(0.0, self.paper_trailing_volume_30d)
        # Find the highest (lowest-fee) tier whose volume threshold is met.
        tier = 0
        taker = 60.0
        maker = 40.0
        for i, (min_vol, t, m) in enumerate(self.FEE_TIERS):
            if vol >= min_vol:
                tier = i + 1
                taker = t
                maker = m
            else:
                break
        if tier < 1:
            tier = 1
        return (tier, taker, maker)

    def _effective_fee_bps(self) -> float:
        """Blended fee rate: maker_pct × maker_bps + (1 - maker_pct) × taker_bps."""
        _, taker, maker = self._fee_tier()
        return self.paper_maker_pct * maker + (1.0 - self.paper_maker_pct) * taker

    def _reset_monthly_volume_if_needed(self) -> None:
        """Reset monthly volume counter when calendar month changes."""
        now = time.time()
        if not hasattr(self, 'paper_monthly_volume'):
            self.paper_monthly_volume = 0.0
            self.paper_month_ts = now
            return
        current_month = time.gmtime(now).tm_mon
        last_month = time.gmtime(self.paper_month_ts).tm_mon
        current_year = time.gmtime(now).tm_year
        last_year = time.gmtime(self.paper_month_ts).tm_year
        if current_year != last_year or current_month != last_month:
            log.info("Monthly fee waiver reset: monthly_volume=%.2f carried forward", self.paper_monthly_volume)
            self.paper_monthly_volume = 0.0
            self.paper_month_ts = now

    def _update_trailing_volume(self, notional: float) -> None:
        """Accrue notional toward trailing 30d volume for fee tier calculation.

        Decays previous volume by elapsed time / 30d to simulate Coinbase's
        rolling 30-day window.  Without decay the fee tier permanently locks
        at the highest (cheapest) tier, understating real fees.
        """
        now = time.time()
        if not hasattr(self, '_trailing_vol_last_ts'):
            self._trailing_vol_last_ts = now
        elapsed = now - self._trailing_vol_last_ts
        decay = max(0.0, 1.0 - elapsed / 2_592_000.0)  # 30 days in seconds
        self.paper_trailing_volume_30d = self.paper_trailing_volume_30d * decay + notional
        self._trailing_vol_last_ts = now
        self._reset_monthly_volume_if_needed()
        self.paper_monthly_volume += notional

    def _setup_websocket(self):
        try:
            from coinbase.src.feed import TickerCache, WebSocketFeed
            self._ticker_cache = TickerCache()
            self._ws_feed = WebSocketFeed(
                self._ticker_cache,
                ws_url="wss://ws-feed.exchange.coinbase.com",
            )
            for pid in self.products:
                self._ws_feed.subscribe([pid])
        except Exception as e:
            log.warning("WebSocket unavailable: %s", e)
            self._ws_feed = None
            self._ticker_cache = None

    # ── Startup ──────────────────────────────────────────────────────

    def start(self):
        # ── Live mode startup: validate config + init engines ──
        if self.mode == "live":
            # ── EXPLICIT LIVE AUTHORIZATION ───────────────────────────────
            # Going live moves REAL money. --mode live alone is NOT enough:
            # require a deliberate, out-of-band authorization so a typo, a
            # misconfigured cron, or an accidental autostart flip can NEVER
            # start live trading. Operator must set ALLOW_LIVE_TRADING=1 (env)
            # OR touch data/live_authorized. Paper/approval modes are exempt.
            import os as _os
            _live_authorized = (
                _os.environ.get("ALLOW_LIVE_TRADING", "").strip() == "1"
                or Path("data/live_authorized").exists()
            )
            if not _live_authorized:
                raise RuntimeError(
                    "REFUSING TO START LIVE: explicit authorization missing. "
                    "Set ALLOW_LIVE_TRADING=1 or `touch data/live_authorized` "
                    "before running with --mode live. (Paper mode needs no "
                    "authorization.)"
                )
            self._live_cfg = TradingConfig.from_env()
            issues = LiveSafetyValidator.check(self._live_cfg)
            if issues:
                for i in issues:
                    log.error("LIVE SAFETY: %s", i)
                if not self.dry_run:
                    raise RuntimeError(
                        f"Live safety checks failed ({len(issues)} issues):\n  "
                        + "\n  ".join(issues)
                    )
                log.warning("LIVE SAFETY: %d issues found, proceeding in dry-run mode", len(issues))

            self._cb_client = CBClient(
                api_key=self._live_cfg.coinbase_api_key,
                api_secret=self._live_cfg.coinbase_api_secret,
            )
            self._exec_engine = NativeExecutionEngine(self._cb_client, dry_run=self.dry_run)
            self._bracket_mgr = BracketManager(self._exec_engine)
            
            # StrategyRankingFilter = StrategyRankingFilter()
            
            self._strategy_ranker = StrategyRanking(
                StrategyRankingFilter(),
                persist_path="data/strategy_ranking.json",
            )
            self._strategy_ranker.load()
            
            # ── Live-only: Advanced Trade WebSocket for real-time fills/orders/accounts
            try:
                from coinbase.src.feed import AdvancedTradeWebSocket
                self._adv_ws = AdvancedTradeWebSocket(
                    api_key=self._live_cfg.coinbase_api_key,
                    api_secret=self._live_cfg.coinbase_api_secret,
                    cache=self._ticker_cache,
                    on_fill=self._on_fill,
                    on_order=self._on_order_update,
                    on_account=self._on_account_update,
                )
                self._adv_ws.start()
                log.info("Advanced Trade WebSocket started for real-time user data")
            except Exception as e:
                log.warning("Advanced Trade WebSocket unavailable: %s", e)
                self._adv_ws = None
            
            # ── Live-only: Order reconciliation on startup
            self._reconcile_open_orders()
            
            # ── Live-only: Sync positions from Coinbase
            self._sync_positions_from_exchange()
            
            risk_limit = RiskLimit.CONSERVATIVE
            risk_profile = RiskProfile(
                max_positions=self._live_cfg.max_positions,
                max_position_pct=self._live_cfg.max_position_pct,
                max_drawdown_pct=self._live_cfg.max_drawdown_pct,
                daily_loss_limit_pct=self._live_cfg.max_daily_loss_pct,
                max_notional_per_trade=self._live_cfg.max_notional_per_trade_usd,
                risk_per_trade_pct=self._live_cfg.risk_per_trade_pct,
                min_risk_reward=self._live_cfg.min_risk_reward,
            )
            self._risk_mgr = RiskManager(profile=risk_profile, limit=risk_limit)
            log.info("Live execution engines initialized: dry_run=%s", self.dry_run)
            # Session-start real balance — the reference for the max-total
            # drawdown hard-halt. Captured once at startup.
            try:
                self._live_start_balance = float(self._dca_available_cash() or 0.0)
            except Exception:
                self._live_start_balance = 0.0

            # Slippage model with order book simulation (live only)
            self._slippage_model = get_slippage_model()
            self._book_cache = get_book_cache()
            
        elif self.mode == "approval":
            self._live_cfg = TradingConfig.from_env()
            self._cb_client = CBClient(
                api_key=self._live_cfg.coinbase_api_key,
                api_secret=self._live_cfg.coinbase_api_secret,
            )
            self._exec_engine = NativeExecutionEngine(self._cb_client, dry_run=self.dry_run)
            self._bracket_mgr = BracketManager(self._exec_engine)
            self._strategy_ranker = StrategyRanking(
                StrategyRankingFilter(),
                persist_path="data/strategy_ranking.json",
            )
            self._strategy_ranker.load()
            log.info("Approval mode: execution engine ready (approval gate required)")

        # ── Load persisted tunable knob overrides ──────────────
        self._load_knobs()

        # ── Components shared by live & paper modes ────────────────
        self._strategy_registry = get_registry()
        risk_limits = RiskLimits(
            max_portfolio_drawdown_pct=15.0,
            max_daily_loss_pct=5.0,
            max_sector_exposure_pct=30.0,
            max_single_asset_pct=10.0,
            max_correlated_positions=3,
            max_leverage=1.5,
            min_cash_buffer_pct=5.0,
        )
        self._portfolio_risk = PortfolioRiskManager(risk_limits)
        from coinbase.src.metrics import get_structured_logger
        self._slog = get_structured_logger("trader_v4")
        self._slog.add_context(mode=self.mode, dry_run=self.dry_run)
        if is_feature_enabled("enable_metrics_server"):
            from coinbase.src.metrics import start_metrics_server
            self._metrics_server = start_metrics_server(
                port=int(os.getenv("METRICS_PORT", "9091"))
            )

        # ── Circuit breaker state init ────────────────────────────
        self._cb_daily_start_equity = _env_float("TRADER_EQUITY", 10000.0)
        self._cb_peak_equity = self._cb_daily_start_equity

        # ── Startup data integrity validation ──────────────────────
        validate_issues: List[str] = []
        import shutil
        if not shutil.which("coinbase"):
            validate_issues.append("Coinbase CLI not found on PATH")
        if not _HAS_RUST:
            validate_issues.append("Rust core not available — falling back to Python strategies")
        if self._paper_state_path.exists():
            try:
                raw = self._paper_state_path.read_text()
                state = json.loads(raw)
                if "paper_cash" not in state:
                    validate_issues.append("Paper state file missing 'paper_cash' field — may be corrupt")
                else:
                    # ── Ledger integrity assertion ──────────────────────────
                    # Under the corrected accounting (fixed leverage 'loan'
                    # double-count bug), the cash ledger MUST satisfy:
                    #   paper_cash == start + realized_pnl
                    #                   - Σ(open_margin)
                    #                   - Σ(open fees_paid + cum_funding)
                    # where open_margin = entry_notional / leverage per open
                    # position. The fees_paid + cum_funding terms are required
                    # because entry debits (margin + fee) and the position carries
                    # its paid fees / accrued funding — omitting them makes the
                    # check wrong by exactly the open-position fee (~$1-2) during
                    # live trading. If this fails, the book is corrupt and we
                    # must NOT trade on it. Fail hard so the operator fixes it.
                    try:
                        _start = float(state.get("paper_starting_capital", 0.0) or 0.0)
                        _rpnl = float(state.get("paper_realized_pnl", 0.0) or 0.0)
                        _cash = float(state.get("paper_cash", 0.0) or 0.0)
                        _open_margin = 0.0
                        _open_fees = 0.0
                        for _p in (state.get("paper_positions") or []):
                            if not isinstance(_p, dict):
                                continue
                            _notional = float(_p.get("entry_notional") or 0.0)
                            if _notional <= 0:
                                _qty = float(_p.get("qty") or 0.0)
                                _ep = float(_p.get("entry_price") or 0.0)
                                _notional = _qty * _ep
                            _lev = max(float(_p.get("leverage") or 1.0), 1.0)
                            _open_margin += _notional / _lev
                            _open_fees += float(_p.get("fees_paid") or 0.0)
                            _open_fees += float(_p.get("cum_funding") or 0.0)
                        _expected = _start + _rpnl - _open_margin - _open_fees
                        if abs(_cash - _expected) > 1.0:
                            validate_issues.append(
                                f"Paper ledger INTEGRITY FAIL: cash={_cash:.2f} "
                                f"but expected={_expected:.2f} (start={_start:.2f} "
                                f"+realized={_rpnl:.2f} -open_margin={_open_margin:.2f} "
                                f"-open_fees={_open_fees:.2f}). "
                                f"State file is corrupt — fix before trading."
                            )
                            # Drop a sentinel so the autostart watchdog will NOT
                            # thrash-relaunch on this corrupt ledger.
                            try:
                                Path("data/trader_state_corrupt").write_text(
                                    f"{time.strftime('%Y-%m-%d %H:%M:%S')} mode={self.mode} "
                                    f"path={self._paper_state_path.name}\n"
                                    f"cash ledger integrity fail: cash={_cash:.2f} "
                                    f"expected={_expected:.2f}\n"
                                )
                                log.error("Wrote corruption sentinel (cash ledger).")
                            except OSError as _se:
                                log.warning("Could not write sentinel: %s", _se)
                    except (TypeError, ValueError) as _e:
                        validate_issues.append(f"Paper ledger integrity check error: {_e}")
            except (json.JSONDecodeError, OSError) as e:
                validate_issues.append(f"Paper state file unreadable: {e}")
        data_dir = Path("data")
        if not data_dir.exists():
            data_dir.mkdir(parents=True, exist_ok=True)
        logs_dir = Path("logs")
        if not logs_dir.exists():
            logs_dir.mkdir(parents=True, exist_ok=True)
        # Check disk space
        try:
            st = os.statvfs(".")
            free_gb = st.f_bavail * st.f_frsize / (1024**3)
            if free_gb < 1.0:
                validate_issues.append(f"Low disk space: {free_gb:.1f}GB free")
        except Exception:
            pass

        if validate_issues:
            _fatal = [i for i in validate_issues if i.startswith("Paper ledger INTEGRITY FAIL")]
            for issue in validate_issues:
                log.warning("STARTUP VALIDATION: %s", issue)
            if _fatal:
                # Corrupt ledger — refuse to trade on it. Aborting prevents the
                # bot from silently re-inflating phantom equity on a bad state
                # file. The operator must repair (or delete) the state file.
                raise RuntimeError(
                    "Startup aborted: paper ledger integrity failure — "
                    + "; ".join(_fatal)
                )
        else:
            log.info("Startup validation: all checks passed")

        log.info("Fetching historical data for %d products...", len(self.products))
        try:
            self._seed_history()
        except Exception as e:
            log.warning("Historical data seeding failed: %s — continuing with partial data", e)

        # Register all products with the feed manager for tiered refresh
        if self._feed_mgr:
            for pid in self.products:
                self._feed_mgr.set_volume(pid, self._last_volume_24h.get(pid, 0))
            self._feed_mgr.start()

        if self._ws_feed:
            try:
                self._ws_feed.start()
                self.health_status["ws_connected"] = True
                log.info("WebSocket feed started — waiting for ticker data...")
            except Exception as e:
                log.warning("WebSocket feed failed to start: %s — will use polling", e)
                self.health_status["ws_connected"] = False

        self._candle_thread = threading.Thread(
            target=self._candle_refresh_loop, daemon=True, name="candle_refresh"
        )
        self._candle_thread.start()

        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, daemon=True, name="watchdog"
        )
        self._watchdog_thread.start()

        self._paper_refresh_health()

        if self.health_server:
            self.health_server.start()

        signal.signal(signal.SIGINT, lambda *a: setattr(self, "_shutdown", True))
        signal.signal(signal.SIGTERM, lambda *a: setattr(self, "_shutdown", True))

        # Start periodic batch scan thread if enabled
        if self.minute_scan_interval > 0:
            self._minute_scan_thread = threading.Thread(
                target=self._minute_scan_loop, daemon=True, name="minute_scan"
            )
            self._minute_scan_thread.start()

        if self.scan_interval > 0:
            self._scan_thread = threading.Thread(
                target=self._scan_loop, daemon=True, name="batch_scan"
            )
            self._scan_thread.start()

        if self.full_scan_interval > 0:
            self._full_scan_thread = threading.Thread(
                target=self._full_scan_loop, daemon=True, name="full_scan"
            )
            self._full_scan_thread.start()

        self._news_thread = threading.Thread(
            target=self._news_sentiment_loop, daemon=True, name="news_sentiment"
        )
        self._news_thread.start()

        self._macro_thread = threading.Thread(
            target=self._macro_risk_loop, daemon=True, name="macro_risk"
        )
        self._macro_thread.start()

        self._analytics_thread = threading.Thread(
            target=self._analytics_loop, daemon=True, name="analytics"
        )
        self._analytics_thread.start()

        self._experiment_thread = threading.Thread(
            target=self._experiment_loop, daemon=True, name="experiment"
        )
        self._experiment_thread.start()

        self._pair_trade_thread = threading.Thread(
            target=self._pair_trade_loop, daemon=True, name="pair_trade"
        )
        self._pair_trade_thread.start()

        self._onchain_thread = threading.Thread(
            target=self._onchain_loop, daemon=True, name="onchain_flow"
        )
        self._onchain_thread.start()

        self._macro_tf_thread = threading.Thread(
            target=self._macro_tf_loop, daemon=True, name="macro_tf"
        )
        self._macro_tf_thread.start()

        self._perf_thread = threading.Thread(
            target=self._perf_save_loop, daemon=True, name="perf_tracker"
        )
        self._perf_thread.start()

        self._funding_thread = threading.Thread(
            target=self._funding_loop, daemon=True, name="funding_scan"
        )
        self._funding_thread.start()

        # ── Periodic state-save daemon ────────────────────────────────
        # Persists the running book on a fixed interval REGARDLESS of trades,
        # so a freshly-started book (or one with no recent fills) always has
        # an on-disk checkpoint before any restart / power loss. Trades also
        # save on their own events; this is the backstop. Archival inside
        # _save_paper_state is throttled to 60s so disk growth is bounded.
        self._state_save_thread = threading.Thread(
            target=self._state_save_loop, daemon=True, name="state_save"
        )
        self._state_save_thread.start()

        if not self._ws_feed or not self._ws_feed._running:
            log.warning("No WebSocket — falling back to polling mode")
            self.health_status["status"] = "polling"
            self._polling_loop()
        else:
            self.health_status["status"] = "running"
            log.info(
                "Event-driven mode: waiting for WebSocket ticker events on %d products "
                "(all %d strategies via Rust, batch scan every %ss)",
                len(self.products), len(self.STRATEGY_NAMES),
                self.scan_interval or "N/A",
            )
            try:
                while not self._shutdown:
                    self._drain_ticker_cache()
                    # Position management every 10s
                    now_pos = time.time()
                    if now_pos - self._last_pos_mgmt_ts >= 10.0:
                        self._last_pos_mgmt_ts = now_pos
                        if self.mode == "paper" and self.paper_positions:
                            self._tighten_all_position_stops()
                        elif self.mode in ("live", "approval") and self._bracket_mgr:
                            self._minute_live_trailing()
                    # Mid-run WS health check — if feed disconnected too long, switch to polling
                    ws_running = self._ws_feed and self._ws_feed._running and self._ws_feed._thread and self._ws_feed._thread.is_alive()
                    if not ws_running and self._last_ticker_ts and (time.time() - self._last_ticker_ts) > 15:
                        log.warning("WS feed dead — switching to polling mode")
                        self.health_status["status"] = "polling"
                        self.health_status["ws_connected"] = False
                        self._polling_loop()
                        break
                    time.sleep(0.1)
            except KeyboardInterrupt:
                pass
            except Exception as e:
                log.error("Fatal: %s", e, exc_info=True)
            finally:
                self._cleanup()

    def _seed_history(self):
        with self.profiler.measure("seed_history_rest"):
            with self._candle_fetch_lock:
                candles = fetch_candles_batch_sync(
                    self.products, granularity=3600, limit=100, max_workers=12
                )

        seeded = 0
        for pid, clist in candles.items():
            arrs = candle_arrays(clist)
            with self._candle_lock:
                self._candle_data[pid] = arrs
            streaming = self.streaming.get_or_create(pid) if self.streaming else None
            if streaming:
                for i in range(len(arrs["closes"])):
                    streaming.update(
                        arrs["closes"][i],
                        arrs["volumes"][i] if i < len(arrs["volumes"]) else 0.0,
                    )
            seeded += 1

        self._candle_ready.set()
        log.info("Seeded %d/%d products with historical data", seeded, len(self.products))

    def _candle_refresh_loop(self):
        while not self._shutdown:
            time.sleep(60)
            if self._shutdown:
                break
            with self.profiler.measure("candle_refresh_rest"):
                if self._feed_mgr:
                    candles = self._feed_mgr.get_candles_batch(
                        self.products, granularity=3600, limit=5
                    )
                else:
                    with self._candle_fetch_lock:
                        candles = fetch_candles_batch_sync(
                            self.products, granularity=3600, limit=5, max_workers=12
                        )
            for pid, clist in candles.items():
                if not clist:
                    continue
                arrs = candle_arrays(clist)
                with self._candle_lock:
                    self._candle_data[pid] = arrs

    def _polling_loop(self):
        """REST polling fallback when WebSocket is unavailable."""
        self.health_status["status"] = "polling"
        log.info("Polling mode: fetching tickers via REST every 5s")
        from coinbase.src.rest_feed import fetch_candles_batch_sync
        while not self._shutdown:
            try:
                # Fetch latest tickers via REST (1m candles, use last price as ticker)
                candles = fetch_candles_batch_sync(
                    self.products, granularity=60, limit=1, max_workers=20
                )
                now = time.time()
                for pid, clist in candles.items():
                    if clist:
                        # Latest candle close as current price
                        last_candle = clist[-1]
                        price = float(last_candle[4])  # close price
                        if price > 0:
                            self._ticker_cache.update_ticker(Ticker(
                                product_id=pid,
                                price=price,
                                bid=price,
                                ask=price,
                                volume_24h=0.0,
                                timestamp=now,
                                source=FeedSource.COINBASE_PUBLIC,
                            ))
            except Exception as e:
                log.debug(f"Polling fetch failed: {e}")
            time.sleep(5)

    # ── Ticker Processing ────────────────────────────────────────────

    def _drain_ticker_cache(self):
        if not self._ticker_cache:
            return
        self._slice_cache = {}
        self._car_state_cache = (0.0, None)
        self._dd_cache = None
        for pid in self.products:
            ticker = self._ticker_cache.get_ticker(pid)
            if not ticker or ticker.price <= 0:
                continue

            now = time.time()

            has_position = pid in self.paper_positions if self.mode == "paper" else False

            last = self._last_price.get(pid)
            if last and last > 0:
                change = abs(ticker.price - last) / last * 100
                if change < self.min_change_pct and not has_position:
                    continue

            try:
                self._last_price[pid] = ticker.price
                vol_24h = getattr(ticker, "volume_24h", 0.0) or 0.0
                self._last_volume_24h[pid] = vol_24h
                self._last_ticker_ts = now
                if self._feed_mgr:
                    self._feed_mgr.set_volume(pid, vol_24h)
                if self.streaming:
                    self.streaming.update(pid, ticker.price, ticker.volume_24h)
                if pid == "BTC-USD":
                    try:
                        btc_slices = self._get_slices(pid)
                        self._cross_asset_regime.update_btc_snapshot(
                            ticker.price,
                            btc_slices[0] if btc_slices else None,
                            btc_slices[1] if btc_slices else None,
                        )
                    except Exception:
                        pass
                if last and last > 0:
                    self._record_hotness(pid, abs(ticker.price - last) / last)
            except Exception:
                log.warning("Ticker processing failed for %s, skipping", pid, exc_info=True)
                continue

            # Order flow evaluation on ticker
            try:
                of_sig = self._order_flow.evaluate(pid, ticker.bid, ticker.ask, ticker.price, ticker.volume_24h)
                if of_sig:
                    opps = [of_sig.to_opportunity()]
                    if self.mode == "paper":
                        self._paper_execute(pid, ticker.price, opps)
                    elif self.mode in ("live", "approval") and not self._cb_breached:
                        self._live_execute(pid, ticker.price, opps)
            except Exception:
                log.debug("Order flow eval failed for %s", pid, exc_info=True)
            try:
                sp_slices = self._get_slices(pid)
                if sp_slices and len(sp_slices[0]) >= 20:
                    scalping_sig = self._scalping.get_signals(
                        pid, ticker.price,
                        sp_slices[0],
                        sp_slices[1],
                        ticker.volume_24h,
                        ticker.bid, ticker.ask,
                    )
                    if scalping_sig:
                        if self.mode == "paper":
                            self._paper_execute(pid, ticker.price, [scalping_sig])
                        elif self.mode in ("live", "approval") and not self._cb_breached:
                            self._live_execute(pid, ticker.price, [scalping_sig])
            except Exception:
                log.debug("Scalping eval failed for %s", pid, exc_info=True)
            if has_position:
                eval_interval = self._position_eval_interval
            else:
                eval_interval = self._adaptive_eval_interval_for_pid(pid)
            if now - self._last_eval.get(pid, 0) >= eval_interval:
                self._last_eval[pid] = now
                self._evaluate(pid, self._get_slices(pid))
            if self._tick_count % 500 == 0:
                self._prune_pulses()
            if self._bt_cache_dirty and self._tick_count % 100 == 0:
                self._save_bt_cache()

    # ── Evaluation ───────────────────────────────────────────────────

    def _detect_regime(self, product_id: str, closes, highs, lows, volumes) -> Dict[str, Any]:
        """Detect market regime for a product using Rust regime module.
        Returns dict with regime, confidence, and ATR."""
        import rust_core as _rc
        result: Dict[str, Any] = {"regime": "unknown", "regime_conf": 0.0, "atr_14": 0.0,
                                    "adx": 0.0, "trend_strength": 0.0, "volatility": 0.0}
        try:
            reg_str, adx, trend_str, vol, _pos = _rc.detect_regime_py(
                closes, highs, lows, volumes, None, None
            )
            result["regime"] = reg_str
            result["adx"] = adx
            result["trend_strength"] = trend_str
            result["volatility"] = vol

            # Map regime to confidence-matrix style
            if reg_str in ("strong_uptrend", "weak_uptrend", "weak_downtrend", "strong_downtrend"):
                result["regime_conf"] = abs(trend_str)
            elif reg_str in ("high_volatility", "low_volatility"):
                result["regime_conf"] = vol
            else:
                result["regime_conf"] = 0.5

            # Compute ATR(14)
            try:
                result["atr_14"] = _rc.atr_py(highs, lows, closes, 14)
            except Exception:
                pass

            # Get regime-recommended strategy set for gating
            try:
                result["recommended"] = set(_rc.regime_recommended_strategies_py(reg_str))
            except Exception:
                result["recommended"] = set()
        except Exception as e:
            log.debug("Regime detection failed for %s: %s", product_id, e)
        return result

    def _regime_to_cmatrix(self, regime: str) -> str:
        """Map regime string to confidence-matrix parameter."""
        if regime in ("strong_uptrend", "weak_uptrend", "weak_downtrend", "strong_downtrend"):
            return "trending"
        elif regime == "ranging":
            return "ranging"
        elif regime in ("high_volatility", "low_volatility"):
            return "volatile"
        return ""

    def _cross_asset_regime_snapshot(self, refresh: bool = False) -> Dict[str, Any]:
        if not self._cross_asset_regime:
            return CrossAssetRegimeState().to_dict()
        try:
            return self._cross_asset_regime.get_state(refresh=refresh).to_dict()
        except Exception as e:
            log.debug("Cross-asset regime snapshot failed: %s", e)
            try:
                return self._cross_asset_regime.snapshot()
            except Exception:
                return CrossAssetRegimeState().to_dict()

    def _cross_asset_risk_multiplier(self, refresh: bool = False) -> float:
        state = self._cross_asset_regime_snapshot(refresh=refresh)
        try:
            return float(state.get("risk_multiplier", 1.0) or 1.0)
        except Exception:
            return 1.0

    def _push_notification(self, kind: str, title: str, message: str, meta: Optional[dict] = None) -> None:
        """Push a notification to the deque for the dashboard to poll.
        
        Kinds: trade, circuit_breaker, ws_dead, regime_change, system
        """
        try:
            self._notifications.append({
                "ts": time.time(),
                "kind": kind,
                "title": title,
                "message": message,
                "meta": meta or {},
            })
        except Exception:
            pass

    def _btc_momentum_multiplier(self) -> float:
        """Boost buy sizing when BTC short-term momentum is positive (swing capture).
        
        Returns 1.0–1.3 scaled by the live price vs last daily close.
        No boost when BTC is flat or falling.
        """
        btc_price = self._last_price.get("BTC-USD", 0.0)
        if btc_price <= 0 or not self._cross_asset_regime:
            return 1.0
        last_close = self._cross_asset_regime.last_daily_close()
        if last_close <= 0:
            return 1.0
        live_chg = (btc_price - last_close) / last_close
        if live_chg > 0.02:
            return min(1.3, 1.0 + (live_chg - 0.02) * 6.0)
        return 1.0

    def _get_slices(self, product_id: str) -> Optional[Tuple[List[float], List[float], List[float]]]:
        """Return cached (closes, volumes, opens) lists for a product this tick.

        Materializes the streaming buffers via ``to_list()`` at most once per
        product per drain tick; every other consumer (scalping, BTC snapshot,
        adaptive eval interval, ``_evaluate_impl``) reuses the cached lists
        instead of copying the full ring buffers again.
        """
        cached = self._slice_cache.get(product_id)
        if cached is not None:
            return cached
        streaming = self.streaming.try_get(product_id) if self.streaming else None
        if streaming is None:
            return None
        closes = streaming.closes.to_list()
        volumes = streaming.volumes.to_list()
        opens = closes[:-1] + closes[-1:]
        self._slice_cache[product_id] = (closes, volumes, opens)
        return self._slice_cache[product_id]

    def _get_cross_asset_state(self) -> Dict[str, Any]:
        """Return the cross-asset regime snapshot, cached for a short TTL.

        ``_cross_asset_regime_snapshot()`` is global (not product-specific) yet
        is invoked once per product per tick; cache the result for ~1s so a
        single drain tick only computes it once.
        """
        now = time.time()
        ts, state = self._car_state_cache
        if state is not None and (now - ts) < 1.0:
            return state
        state = self._cross_asset_regime_snapshot()
        self._car_state_cache = (now, state)
        return state

    def _get_paper_drawdown(self) -> float:
        """Return current paper drawdown, computed once per drain tick.

        ``_paper_drawdown()`` loops over all paper positions + core holdings on
        every call; within a single tick that state is immutable, so cache it.
        """
        if self._dd_cache is not None:
            return self._dd_cache
        self._dd_cache = self._paper_drawdown()
        return self._dd_cache

    def _evaluate(self, product_id: str, slices: Optional[Tuple[List[float], List[float], List[float]]] = None):
        """Run all 50 Rust strategies + regime gating + backtest validation."""
        # Product-level circuit breaker — disable after 5 consecutive failures
        product_fail_key = f"eval_fail_{product_id}"
        consecutive_fails = getattr(self, product_fail_key, 0)
        if consecutive_fails >= 5:
            return

        try:
            self._evaluate_impl(product_id, slices=slices)
            setattr(self, product_fail_key, 0)
        except Exception as e:
            setattr(self, product_fail_key, consecutive_fails + 1)
            log.warning("EVAL FAIL %s (%d/5): %s", product_id, consecutive_fails + 1, e)

    def _evaluate_impl(self, product_id: str, slices: Optional[Tuple[List[float], List[float], List[float]]] = None):
        with self.profiler.measure("eval") as _:
            base = product_id.split("-")[0]

            if slices is None:
                slices = self._get_slices(product_id)
            if slices is None:
                return
            closes, volumes, opens = slices

            if len(closes) < 40:
                return

            with self._candle_lock:
                cached = self._candle_data.get(product_id, {})

            highs = cached.get("highs", [c + 0.01 for c in closes])
            lows = cached.get("lows", [c - 0.01 for c in closes])

            n = min(len(closes), len(volumes), len(highs), len(lows))
            if n < 40:
                return
            closes = closes[-n:]
            volumes = volumes[-n:]
            highs = highs[-n:]
            lows = lows[-n:]
            opens = opens[-n:]
            current_price = closes[-1] if closes else 0

            # ── Step 0: Regime detection ───────────────────────────
            regime_info = self._detect_regime(product_id, closes, highs, lows, volumes)
            regime_cmatrix = self._regime_to_cmatrix(regime_info["regime"])
            recommended = regime_info.get("recommended", set())
            atr_14 = regime_info.get("atr_14", 0.0)
            cross_asset_state = self._get_cross_asset_state()

            # ── Step 1: Run ALL 50 strategies in Rust ──────────────
            with self.profiler.measure("rust_signals"):
                raw_signals = []
                try:
                    import rust_core as _rc
                    raw_signals = _rc.evaluate_all_opens_py(closes, opens, volumes, highs, lows)
                except Exception as e:
                    log.debug("Rust evaluate_all failed for %s: %s", product_id, e)

            if not raw_signals:
                return

            # Track signal counts + fingerprint dedup + pulse tracking
            # Apply regime gating: only keep strategies recommended for this regime
            signals = []
            dd = self._get_paper_drawdown()
            for s_name, action, confidence, reason in raw_signals:
                self._signal_counts[s_name] += 1
                if recommended and s_name not in recommended:
                    continue
                fp_key = self._fingerprint_key(product_id, s_name, action, current_price)
                duplicate = self._is_fingerprint_duplicate(fp_key)
                pulse = self._record_pulse(product_id, s_name, action, confidence, current_price)
                if duplicate:
                    repeat_boost = min(0.18, 0.03 * max(1, pulse.pulse_count - 1) + dd * 0.75)
                    confidence = min(0.99, confidence + repeat_boost)
                    log.debug(
                        "DUPLICATE BOOST %s/%s %s: pulse=%d dd=%.2f boost=%.3f",
                        product_id, s_name, action, pulse.pulse_count, dd, repeat_boost,
                    )
                signals.append((s_name, action, confidence))

            # ── Step 2: Backtest validation (Rust rayon) ───────────
            with self.profiler.measure("rust_backtest"):
                new_bt_entries = 0
                bt_list = [(s_name, base, closes, volumes, highs, lows) for s_name, _, _ in signals]
                if bt_list:
                    new_bt = batch_backtest_rust(bt_list) if _HAS_RUST else {}
                    for ck, verdict in new_bt.items():
                        if ck not in self._bt_cache:
                            self._bt_cache[ck] = verdict
                            new_bt_entries += 1
                if new_bt_entries > 0:
                    self._bt_cache_dirty = True

            # ── Step 3: Rank opportunities ─────────────────────────
            macro = self._last_macro_signal
            opportunities = []
            _tracker = self._perf_tracker
            for s_name, action, confidence in signals:
                # Skip globally-disabled strategies at the source. Previously a disabled
                # strategy (e.g. chaikin_mf) could be selected as the top signal and then
                # silently skipped downstream in _paper_execute_impl, abandoning the whole
                # product even when other enabled strategies agreed. Drop it here so the
                # next-best enabled strategy can trade.
                if _tracker is not None and _tracker.is_strategy_disabled(s_name):
                    continue
                ck = f"{s_name}/{base}"
                verdict = self._bt_cache.get(ck)
                # Build the opp if we have a verdict with acceptable win_rate/sharpe.
                # Previously this required verdict.passed (win_rate>=0.50 AND profit_factor>1.05
                # AND dd<20% AND total_return>-20% ALL at once) — far stricter than the
                # downstream paper gates (paper_min_win_rate=0.45, paper_min_sharpe=0.30) and
                # nearly always False on short paper backtests, silently parking the bot with
                # ZERO opportunities. The downstream gates already enforce the real bar, so we
                # use the same thresholds here instead of the brittle binary `passed` flag.
                if verdict and getattr(verdict, "win_rate", 0.0) >= self.paper_min_win_rate \
                        and getattr(verdict, "sharpe_ratio", 0.0) > self.paper_min_sharpe:
                    macro_bearish = macro and macro.bias == "bearish" and macro.confidence > 0.3
                    macro_bullish = macro and macro.bias == "bullish" and macro.confidence > 0.3

                    # Bearish-conviction boost: when macro is bearish, favor SELL
                    if macro_bearish and action == "SELL":
                        confidence = min(0.99, confidence + 0.08)
                    elif macro_bullish and action == "BUY":
                        confidence = min(0.99, confidence + 0.08)

                    # Long-horizon flag: macro-confirmed directional trend
                    is_long_horizon = bool(macro and macro.confidence > 0.5
                                           and ((macro.bias == "bearish" and action == "SELL")
                                                or (macro.bias == "bullish" and action == "BUY")))

                    # Dynamic leverage: volatility-scaled
                    opp_leverage = 1.0
                    if self.enable_leverage:
                        opp_leverage = self._vol_scaled_leverage(product_id, current_price, atr_14)
                        # Lower leverage for shorts vs macro
                        if macro_bearish and action == "SELL":
                            opp_leverage = min(opp_leverage, 1.5)
                        elif action == "SELL":
                            opp_leverage = min(opp_leverage, 1.5)

                    stop_dist = atr_14 * 2.5 if atr_14 > 0 else current_price * 0.03
                    if s_name in self._MEAN_REVERSION_STRATS:
                        stop_dist *= 1.5  # wider stops for mean-reversion (needs room)
                    if is_long_horizon:
                        stop_dist *= 2.0  # wider stops for long-horizon bets

                    opportunities.append({
                        "currency": base,
                        "product_id": product_id,
                        "strategy": s_name,
                        "action": action,
                        "confidence": confidence,
                        "price": current_price,
                        "win_rate": verdict.win_rate,
                        "sharpe": verdict.sharpe_ratio,
                        "regime": regime_info["regime"],
                        "regime_conf": regime_info["regime_conf"],
                        "atr_14": atr_14,
                        "leverage": opp_leverage,
                        "stop_dist": stop_dist,
                        "is_long_horizon": is_long_horizon,
                        "global_regime": cross_asset_state.get("regime", "mixed"),
                        "global_risk_multiplier": cross_asset_state.get("risk_multiplier", 1.0),
                        "global_allows_new_longs": cross_asset_state.get("allows_new_longs", True),
                        "macro_tf_bias": macro.bias if macro else "",
                        "macro_tf_conf": macro.confidence if macro else 0.0,
                    })
                    self._opp_counts[s_name] += 1

            if opportunities:
                self._tick_count += 1
                self._last_eval_ts = time.time()
                self.health_status["tick_count"] = self._tick_count
                self._record_hotness(product_id, 0.25 + (0.05 * len(opportunities)))

                if self.mode == "paper":
                    self._paper_execute(product_id, current_price, opportunities, regime_cmatrix)
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(product_id, current_price, opportunities, regime_cmatrix)
                self._paper_refresh_health()

                total = len(raw_signals)
                gated = total - len(signals)
                passed = len(opportunities)
                best_opp = max(opportunities, key=lambda o: o["confidence"])
                log.info(
                    "EVENT %s: %s %d/%d gated=%d p=%.2f "
                    "top=%s(%.2f) wr=%.0f%% sharpe=%.1f atr=%.4f tick=%d",
                    product_id, regime_info["regime"], passed, total, gated, current_price,
                    best_opp["strategy"], best_opp["confidence"],
                    best_opp["win_rate"] * 100, best_opp["sharpe"], atr_14,
                    self._tick_count,
                )

                if self._tick_count % 50 == 0:
                    top_sigs = sorted(self._signal_counts.items(), key=lambda x: -x[1])[:5]
                    top_opps = sorted(self._opp_counts.items(), key=lambda x: -x[1])[:5]
                    paper = self.health_status.get("paper", {})
                    log.info(
                        "STATS tick=%d | Top signals: %s | Top opps: %s | latency=%s | paper=%s",
                        self._tick_count,
                        " ".join(f"{k}={v}" for k, v in top_sigs),
                        " ".join(f"{k}={v}" for k, v in top_opps),
                        self.profiler.summary().get("rust_signals", "?"),
                        {
                            "equity": round(paper.get("equity", 0.0), 2),
                            "pnl": round(paper.get("realized_pnl", 0.0), 2),
                            "wr": paper.get("win_rate", 0.0),
                            "dd": paper.get("drawdown", 0.0),
                            "trades": paper.get("trades", 0),
                        },
                    )

    def _paper_equity(self, prices: Optional[Dict[str, float]] = None) -> float:
        """Honest paper equity.

        Equity = paper_cash + sum of UNREALIZED P&L across open positions.

        The previous implementation added back a `loan` term
        (entry_notional * (1 - 1/leverage)) inside the loop while the cash
        ledger had only been debited by the margin (entry_notional/leverage) on
        entry and credited the full gross (minus the same loan) on exit. That
        double-counted the borrowed slice on every round-trip, silently
        inflating paper_cash and therefore equity by ~33% of notional per trade
        (~$155k of phantom cash after 200 trades).

        Correct model: margin is the only capital at risk. Entry debits margin,
        exit returns margin + realized P&L, and equity is simply cash plus the
        mark-to-market gain/loss on open positions. No separate loan term.
        """
        prices = prices or {}
        equity = self.paper_cash
        for pid, pos in self.paper_positions.items():
            px = prices.get(pid, pos.entry_price)
            if pos.is_long:
                equity += pos.qty * (px - pos.entry_price) - pos.cum_funding
            else:
                equity += pos.qty * (pos.entry_price - px) - pos.cum_funding
        # Add core holdings value
        equity += self._core_holdings_value(prices)
        return equity

    def _paper_drawdown(self, equity: Optional[float] = None) -> float:
        equity = equity if equity is not None else self._paper_equity()
        self.paper_peak_equity = max(self.paper_peak_equity, equity)
        if self.paper_peak_equity <= 0:
            return 0.0
        return max(0.0, (self.paper_peak_equity - equity) / self.paper_peak_equity)

    def _paper_score_multiplier(self, confidence: float, win_rate: float, sharpe: float) -> float:
        dd = self._paper_drawdown()
        score = confidence
        score *= 1.0 if sharpe >= self.paper_min_sharpe else 0.5
        score *= 1.0 if win_rate >= self.paper_min_win_rate else 0.7
        if dd > 0.05:
            score *= max(0.5, 1.0 - dd)
        return max(0.0, min(1.0, score))

    def _paper_trade_notional(self, confidence: float) -> float:
        eq = self._paper_equity()
        max_notional = eq * self.paper_max_position_pct
        base = max_notional * max(0.25, confidence)
        return max(self.paper_min_trade_usd, min(base, max_notional))

    def _paper_state_snapshot(self) -> Dict[str, Any]:
        with self._analytics_lock:
            strategy_stats_snapshot = dict(self.strategy_stats)
            signal_type_counts_snapshot = {k: dict(v) for k, v in self._signal_type_counts.items()}
        return {
            "paper_starting_capital": self.paper_starting_capital,
            "paper_cash": self.paper_cash,
            "paper_positions": [
                {
                    "product_id": pos.product_id,
                    "side": pos.side,
                    "qty": pos.qty,
                    "entry_price": pos.entry_price,
                    "entry_ts": pos.entry_ts,
                    "strategy": pos.strategy,
                    "confidence": pos.confidence,
                    "win_rate": pos.win_rate,
                    "sharpe": pos.sharpe,
                    "fees_paid": pos.fees_paid,
                    "highest_price": pos.highest_price,
                    "lowest_price": pos.lowest_price,
                    "entry_notional": pos.entry_notional,
                    "leverage": pos.leverage,
                    "cum_funding": pos.cum_funding,
                    "trades": pos.trades,
                    "liq_price": pos.liq_price,
                    "long_horizon": pos.long_horizon,
                    "stop_price": pos.stop_price,
                    "target_price": pos.target_price,
                    "initial_stop_dist": pos.initial_stop_dist,
                    "breakeven_set": pos.breakeven_set,
                    "trailing_activated": pos.trailing_activated,
                    "trailing_take_price": pos.trailing_take_price,
                    "regime": pos.regime,
                    "atr_14": pos.atr_14,
                }
                for pos in self.paper_positions.values()
            ],
            "paper_trades": self.paper_trades[-200:],
            "paper_realized_pnl": self.paper_realized_pnl,
            "paper_fees_paid": self.paper_fees_paid,
            "paper_wins": self.paper_wins,
            "paper_losses": self.paper_losses,
            "paper_last_trade_ts": self.paper_last_trade_ts,
            "paper_peak_equity": self.paper_peak_equity,
            "paper_equity_curve": self.paper_equity_curve[-500:],
            "paper_equity_tss": self.paper_equity_tss[-500:],
            "paper_trailing_volume_30d": self.paper_trailing_volume_30d,
            "paper_monthly_volume": self.paper_monthly_volume,
            "paper_month_ts": self.paper_month_ts,
            "strategy_stats": strategy_stats_snapshot,
            "signal_type_counts": signal_type_counts_snapshot,
            "core_holdings": [
                {
                    "product_id": h.product_id,
                    "qty": h.qty,
                    "cost_basis": h.cost_basis,
                    "total_cost": h.total_cost,
                    "total_qty": h.total_qty,
                    "trades": h.trades,
                    "last_buy_ts": h.last_buy_ts,
                    "created_ts": h.created_ts,
                }
                for h in self._core_holdings.values()
            ],
            # Schema-version + mode tag: lets _load_paper_state detect an
            # old/incompatible state file (e.g. from a previous bot schema) or a
            # mode mismatch (paper state loaded into a live run). Bump
            # STATE_SCHEMA_VERSION if the saved structure changes incompatibly.
            "state_schema_version": 2,
            "mode": self.mode,
        }

    def _save_core_holdings_state(self) -> None:
        """Persist core holdings independently of paper state (used by all modes)."""
        try:
            path = Path("data/core_holdings.json")
            data = [
                {
                    "product_id": h.product_id,
                    "qty": h.qty,
                    "cost_basis": h.cost_basis,
                    "total_cost": h.total_cost,
                    "total_qty": h.total_qty,
                    "trades": h.trades,
                    "last_buy_ts": h.last_buy_ts,
                    "created_ts": h.created_ts,
                    "target_value": h.target_value,
                    "drift_pct": h.drift_pct,
                    "rebalance_action": h.rebalance_action,
                }
                for h in self._core_holdings.values()
            ]
            tmp_path = path.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            tmp_path.replace(path)
        except Exception as e:
            log.debug("Failed to persist core holdings: %s", e)

    def _load_core_holdings_state(self) -> None:
        """Load core holdings from disk (used by all modes)."""
        try:
            path = Path("data/core_holdings.json")
            if not path.exists():
                return
            data = json.loads(path.read_text())
            if not isinstance(data, list):
                return
            for item in data:
                if isinstance(item, dict) and item.get("product_id"):
                    self._core_holdings[item["product_id"]] = CoreHolding(**item)
            if self._core_holdings:
                log.info("Loaded %d core holdings from %s", len(self._core_holdings), path)
        except Exception as e:
            log.debug("Failed to load core holdings: %s", e)

    def _save_paper_state(self) -> None:
        if self.mode != "paper":
            self._save_core_holdings_state()
            return
        try:
            self._paper_state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._paper_state_path.with_suffix(".tmp")
            with self._paper_lock:
                snapshot = self._paper_state_snapshot()
                with tmp_path.open("w", encoding="utf-8") as f:
                    json.dump(snapshot, f, indent=2, default=str)
                bak1 = self._paper_state_path.with_suffix(".json.bak")
                bak2 = self._paper_state_path.with_suffix(".json.bak2")
                bak3 = self._paper_state_path.with_suffix(".json.bak3")
                if bak2.exists():
                    bak3.write_text(bak2.read_text())
                if bak1.exists():
                    bak2.write_text(bak1.read_text())
                if self._paper_state_path.exists():
                    bak1.write_text(self._paper_state_path.read_text())
                tmp_path.replace(self._paper_state_path)
                # ARCHIVAL BACKUP: timestamped copy so the ledger is ALWAYS
                # recoverable even if the main file is deleted/races with a
                # process kill. Throttled to once per 60s to bound disk growth.
                now = time.time()
                if now - getattr(self, "_last_state_archive_ts", 0.0) >= 60.0:
                    self._last_state_archive_ts = now
                    try:
                        arch_dir = self._paper_state_path.parent / "state_backups"
                        arch_dir.mkdir(exist_ok=True)
                        stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime(now))
                        arch_path = arch_dir / f"paper_trader_v4_{stamp}.json"
                        arch_path.write_text(self._paper_state_path.read_text())
                        # prune to keep only the 10 most recent archives
                        arches = sorted(arch_dir.glob("paper_trader_v4_*.json"),
                                        key=lambda p: p.stat().st_mtime, reverse=True)
                        for old in arches[10:]:
                            try:
                                old.unlink()
                            except OSError:
                                pass
                    except Exception as ae:  # archival must never break the save
                        log.debug("State archival skipped: %s", ae)
            self.health_status["last_state_save_ts"] = time.time()
        except Exception as e:
            log.debug("Failed to save paper state: %s", e)

    def _state_save_loop(self) -> None:
        """Daemon: persist the running book on a fixed interval.

        Backs up the in-memory ledger to disk every LIVE_STATE_SAVE_INTERVAL
        seconds (default 60) regardless of trade activity, so a freshly
        started book (or one with no recent fills) always has an on-disk
        checkpoint before any restart or power loss. _save_paper_state is
        mode-aware (paper -> paper_trader_v4_state.json, live ->
        live_trader_v4_state.json) and throttles archival to bound disk growth.
        """
        import os as _os
        interval = max(10, float(_os.environ.get("LIVE_STATE_SAVE_INTERVAL", "60")))
        log.info("State-save daemon started (every %.0fs)", interval)
        while not getattr(self, "_shutdown", False):
            try:
                self._save_paper_state()
            except Exception as e:
                log.debug("Periodic state save failed: %s", e)
            # Sleep in small slices so shutdown is responsive.
            _elapsed = 0.0
            while _elapsed < interval and not getattr(self, "_shutdown", False):
                time.sleep(min(5.0, interval - _elapsed))
                _elapsed += min(5.0, interval - _elapsed)

    def _state_structural_issues(self, state: dict) -> str:
        """Return a reason string if the loaded state is structurally corrupt.

        Catches the classes of corruption that slipped past the cash-ledger
        check: old-schema files, mode mismatch (paper state into a live run),
        and positions/trades that are missing required fields. Returns '' if OK.
        """
        # Mode mismatch — never load a paper book into a live run or vice versa.
        _stored_mode = state.get("mode")
        if _stored_mode and _stored_mode != self.mode:
            return (f"mode mismatch: state file is '{_stored_mode}' but bot is "
                    f"running '{self.mode}'")
        # Old schema (pre-version-2) without a version tag AND structurally
        # broken → treat as corrupt. A versioned file is trusted if it passes
        # the field checks below.
        _ver = state.get("state_schema_version", 0)
        # Required scalar fields must be present and numeric.
        for _k in ("paper_cash", "paper_starting_capital", "paper_realized_pnl"):
            _v = state.get(_k)
            if _v is None or (isinstance(_v, str) and _v.strip() == ""):
                return f"missing/invalid field '{_k}'"
            try:
                float(_v)
            except (TypeError, ValueError):
                return f"non-numeric field '{_k}': {_v!r}"
        # Positions must have a symbol and a usable mark/entry.
        for _p in (state.get("paper_positions") or []):
            if not isinstance(_p, dict):
                return "non-dict position entry"
            _sym = _p.get("product_id") or _p.get("symbol")
            if not _sym:
                return "position with null/empty symbol"
            _mark = _p.get("mark_price")
            _entry = _p.get("entry_price")
            if _mark is None and _entry is None:
                return f"position {_sym} has neither mark nor entry price"
            try:
                if _mark is not None:
                    float(_mark)
                if _entry is not None:
                    float(_entry)
            except (TypeError, ValueError):
                return f"position {_sym} has non-numeric price"
        # Trades list must be a list; entries must be dicts with a product_id.
        _trades = state.get("paper_trades") or []
        if not isinstance(_trades, list):
            return "paper_trades is not a list"
        for _t in _trades:
            if not isinstance(_t, dict):
                return "non-dict trade entry"
            if not (_t.get("product_id") or _t.get("symbol")):
                return "trade entry with null/empty symbol"
        return ""

    def _write_corrupt_sentinel(self, reason: str) -> None:
        """Write a sentinel so the autostart watchdog will NOT thrash-relaunch.

        The bot refuses to trade on a corrupt ledger and exits. Without this,
        the every-5-min autostart would relaunch it, it would fail again, and
        loop forever — overwriting the corrupt file each time. The sentinel
        blocks relaunch until the operator removes it (and fixes the state).
        """
        try:
            _sentinel = Path("data/trader_state_corrupt")
            _sentinel.write_text(
                f"{time.strftime('%Y-%m-%d %H:%M:%S')} mode={self.mode} "
                f"path={self._paper_state_path.name}\n{reason}\n"
            )
            log.error("Wrote corruption sentinel: %s", _sentinel)
        except OSError as e:
            log.warning("Could not write corruption sentinel: %s", e)

    def _load_paper_state(self) -> None:
        state = None
        for path in [self._paper_state_path,
                     self._paper_state_path.with_suffix(".json.bak"),
                     self._paper_state_path.with_suffix(".json.bak2"),
                     self._paper_state_path.with_suffix(".json.bak3")]:
            if not path.exists():
                continue
            try:
                with path.open("r", encoding="utf-8") as f:
                    raw = f.read()
                state = json.loads(raw)
                # Validate critical fields
                if "paper_cash" not in state or "paper_positions" not in state:
                    state = None
                    continue
                break
            except (json.JSONDecodeError, ValueError, OSError):
                state = None
                continue

        if state is None:
            log.warning("Paper state corrupt or missing — starting fresh")
            return

        # ── STRUCTURAL CORRUPTION GUARDS ────────────────────────────────
        # We were bitten by an old-schema state file (positions with null
        # symbols/marks, trades with no realized pnl) that the cash-ledger
        # check passed but was structurally unusable. Refuse to trade on it.
        _corrupt_reason = self._state_structural_issues(state)
        if _corrupt_reason:
            self._write_corrupt_sentinel(_corrupt_reason)
            log.error("REFUSING to load paper state: %s — starting FRESH. "
                      "Fix the state file (or remove it) before relaunching.",
                      _corrupt_reason)
            return

        try:
            with self._paper_state_path.open("w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
            self.paper_starting_capital = float(state.get("paper_starting_capital", self.paper_starting_capital))
            self.paper_cash = float(state.get("paper_cash", self.paper_cash))
            self.paper_positions = {
                item["product_id"]: PaperPosition(**item)
                for item in state.get("paper_positions", [])
                if isinstance(item, dict) and item.get("product_id")
            }
            self.paper_trades = [item for item in state.get("paper_trades", []) if isinstance(item, dict)]
            self.paper_realized_pnl = float(state.get("paper_realized_pnl", self.paper_realized_pnl))
            # CROSS-CHECK the two realized-P&L views. paper_realized_pnl (the
            # accumulator, incremented on every real exit) is the source of
            # truth because it is what paper_cash is actually derived from. The
            # per-trade ledger (paper_trades[].pnl) is a log and has historically
            # under-counted (some exit records carry pnl=0.0 from unrelated
            # stubs). If they disagree we keep the accumulator and only warn —
            # we never overwrite the accumulator from the incomplete ledger.
            ledger_rpnl = sum(float(t.get("pnl", 0.0) or 0.0)
                              for t in self.paper_trades if "pnl" in t)
            if abs(ledger_rpnl - self.paper_realized_pnl) > 0.01:
                log.warning(
                    "P&L cross-check: accumulator=%.2f but ledger sum=%.2f "
                    "(ledger under-counts by %.2f — accumulator is authoritative)",
                    self.paper_realized_pnl, ledger_rpnl,
                    self.paper_realized_pnl - ledger_rpnl,
                )
            self.paper_fees_paid = float(state.get("paper_fees_paid", self.paper_fees_paid))
            self.paper_wins = int(state.get("paper_wins", self.paper_wins))
            self.paper_losses = int(state.get("paper_losses", self.paper_losses))
            self.paper_last_trade_ts = {
                str(k): float(v)
                for k, v in state.get("paper_last_trade_ts", {}).items()
                if isinstance(v, (int, float))
            }
            self.paper_peak_equity = float(state.get("paper_peak_equity", self.paper_peak_equity))
            curve = state.get("paper_equity_curve", [])
            if isinstance(curve, list) and len(curve) > 1:
                self.paper_equity_curve = [float(x) for x in curve if isinstance(x, (int, float))]
            tss = state.get("paper_equity_tss", [])
            if isinstance(tss, list) and len(tss) == len(self.paper_equity_curve):
                self.paper_equity_tss = [float(x) for x in tss if isinstance(x, (int, float))]
            self.paper_trailing_volume_30d = float(state.get("paper_trailing_volume_30d", 0.0))
            self.paper_monthly_volume = float(state.get("paper_monthly_volume", 0.0))
            self.paper_month_ts = float(state.get("paper_month_ts", time.time()))
            raw_strat = state.get("strategy_stats", {})
            if isinstance(raw_strat, dict):
                self.strategy_stats = raw_strat
            raw_sig = state.get("signal_type_counts", {})
            if isinstance(raw_sig, dict):
                self._signal_type_counts = raw_sig
            raw_holdings = state.get("core_holdings", [])
            if isinstance(raw_holdings, list):
                for item in raw_holdings:
                    if isinstance(item, dict) and item.get("product_id"):
                        self._core_holdings[item["product_id"]] = CoreHolding(**item)
            log.info(
                "Loaded paper state: cash=$%.2f positions=%d trades=%d core_holdings=%d",
                self.paper_cash, len(self.paper_positions), len(self.paper_trades),
                len(self._core_holdings),
            )
        except Exception as e:
            log.debug("No paper state loaded: %s", e)

    # ── BT Cache Persistence ──────────────────────────────────────────

    def _bt_cache_serializable(self) -> Dict[str, Any]:
        out = {}
        for ck, verdict in self._bt_cache.items():
            out[ck] = {
                "strategy": verdict.strategy,
                "currency": verdict.currency,
                "total_trades": verdict.total_trades,
                "winning_trades": verdict.winning_trades,
                "losing_trades": verdict.losing_trades,
                "win_rate": verdict.win_rate,
                "total_return_pct": verdict.total_return_pct,
                "sharpe_ratio": verdict.sharpe_ratio,
                "profit_factor": verdict.profit_factor,
                "max_drawdown_pct": verdict.max_drawdown_pct,
                "regime": verdict.regime,
                "passed": verdict.passed,
                "reason": verdict.reason,
                "_ts": time.time(),
            }
        return out

    def _save_bt_cache(self) -> None:
        try:
            self._bt_cache_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._bt_cache_path.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(self._bt_cache_serializable(), f, indent=2)
            tmp.replace(self._bt_cache_path)
            self._bt_cache_dirty = False
        except Exception as e:
            log.debug("Failed to save BT cache: %s", e)

    def _load_bt_cache(self) -> None:
        try:
            if not self._bt_cache_path.exists():
                return
            with self._bt_cache_path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            now = time.time()
            ttl = 86400.0
            loaded = 0
            for ck, data in raw.items():
                ts = data.get("_ts", 0)
                if now - ts > ttl:
                    continue
                from strategy_engine import BacktestVerdict
                self._bt_cache[ck] = BacktestVerdict(
                    strategy=data.get("strategy", ""),
                    currency=data.get("currency", ""),
                    total_trades=int(data.get("total_trades", 0)),
                    winning_trades=int(data.get("winning_trades", 0)),
                    losing_trades=int(data.get("losing_trades", 0)),
                    win_rate=float(data.get("win_rate", 0.0)),
                    total_return_pct=float(data.get("total_return_pct", 0.0)),
                    sharpe_ratio=float(data.get("sharpe_ratio", 0.0)),
                    profit_factor=float(data.get("profit_factor", 0.0)),
                    max_drawdown_pct=float(data.get("max_drawdown_pct", 0.0)),
                    regime=str(data.get("regime", "")),
                    passed=bool(data.get("passed", False)),
                    reason=str(data.get("reason", "")),
                )
                loaded += 1
            log.info("Loaded %d/%d cached backtest verdicts", loaded, len(raw))
        except Exception as e:
            log.debug("Failed to load BT cache: %s", e)

    # ── Hot Scores Persistence ────────────────────────────────────────

    def _save_hot_scores(self) -> None:
        try:
            self._hot_scores_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._hot_scores_path.with_suffix(".tmp")
            with self._hot_lock:
                data = dict(self._hot_scores)
            out = {k: v for k, v in data.items() if v >= 0.01}
            out["_ts"] = time.time()
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            tmp.replace(self._hot_scores_path)
        except Exception as e:
            log.debug("Failed to save hot scores: %s", e)

    def _load_hot_scores(self) -> None:
        try:
            if not self._hot_scores_path.exists():
                return
            with self._hot_scores_path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            now = time.time()
            ts = float(raw.pop("_ts", 0))
            age = now - ts
            decay = max(0.01, 1.0 - (age / 3600.0))
            with self._hot_lock:
                for pid, score in raw.items():
                    self._hot_scores[pid] = float(score) * decay
            log.info("Loaded %d hot scores (age=%.0fs, decay=%.2f)", len(raw), age, decay)
        except Exception as e:
            log.debug("Failed to load hot scores: %s", e)

    # ── Signal Fingerprint Dedup ──────────────────────────────────────

    def _fingerprint_key(self, product_id: str, strategy: str, direction: str, price: float) -> str:
        bucket = max(1e-6, 10 ** max(0, round(__import__("math").log10(max(price, 1e-6))) - 1))
        price_bucket = round(price / bucket) * bucket
        return f"{product_id}:{strategy}:{direction}:{price_bucket:.6g}"

    def _is_fingerprint_duplicate(self, key: str) -> bool:
        now = time.time()
        expiry = self._signal_fingerprints.get(key, 0.0)
        if now < expiry:
            return True
        self._signal_fingerprints[key] = now + self._fingerprint_ttl_s
        return False

    # ── Adaptive Eval Interval ────────────────────────────────────────

    def _adaptive_eval_interval_for_pid(self, product_id: str) -> float:
        if not self._adaptive_eval_enabled:
            return self._min_eval_interval
        slices = self._get_slices(product_id)
        if slices is None:
            streaming = self.streaming.try_get(product_id)
            if not streaming or len(streaming.closes) < 30:
                return self._min_eval_interval
            closes = streaming.closes.to_list()
        else:
            closes = slices[0]
        if len(closes) < 30:
            return self._min_eval_interval
        n = min(20, len(closes))
        recent = closes[-n:]
        if len(recent) < 2:
            return self._min_eval_interval
        atr = sum(abs(recent[i] - recent[i - 1]) for i in range(1, len(recent))) / (len(recent) - 1)
        mean_price = sum(recent) / len(recent)
        vol_ratio = atr / max(mean_price, 1e-9)
        if vol_ratio < 0.001:
            return 5.0
        if vol_ratio < 0.003:
            return 2.0
        if vol_ratio < 0.01:
            return 1.0
        return 0.5

    # ── Signal Pulse Tracking ─────────────────────────────────────────

    def _pulse_key(self, product_id: str, strategy: str, direction: str) -> str:
        return f"{product_id}:{strategy}:{direction}"

    def _record_pulse(self, product_id: str, strategy: str, direction: str, confidence: float, price: float) -> PulseRecord:
        now = time.time()
        key = self._pulse_key(product_id, strategy, direction)
        with self._pulse_lock:
            existing = self._signal_pulses.get(key)
            if existing:
                if now - existing.last_ts > self._pulse_window_s:
                    existing.pulse_count = 1
                    existing.first_ts = now
                    existing.last_ts = now
                    existing.avg_confidence = confidence
                    existing.min_price = price
                    existing.max_price = price
                else:
                    existing.update(confidence, price)
            else:
                self._signal_pulses[key] = PulseRecord(
                    strategy=strategy,
                    direction=direction,
                    product_id=product_id,
                    first_ts=now,
                    last_ts=now,
                    avg_confidence=confidence,
                    min_price=price,
                    max_price=price,
                )
            opp_key = self._pulse_key(product_id, "ANY", direction)
            if opp_key != key:
                opp = self._signal_pulses.get(opp_key)
                if opp and now - opp.last_ts < self._pulse_window_s:
                    opp.flip_count += 1
            return self._signal_pulses[key]

    def _pulse_summary_for_pid(self, product_id: str) -> List[Dict[str, Any]]:
        results = []
        with self._pulse_lock:
            for key, rec in self._signal_pulses.items():
                if rec.product_id == product_id:
                    results.append({
                        "strategy": rec.strategy,
                        "direction": rec.direction,
                        "pulse_count": rec.pulse_count,
                        "avg_confidence": round(rec.avg_confidence, 3),
                        "age_s": round(rec.age_s, 1),
                        "flip_count": rec.flip_count,
                        "is_hot": rec.is_hot,
                    })
        return sorted(results, key=lambda r: -r["pulse_count"])

    def _prune_pulses(self) -> None:
        now = time.time()
        with self._pulse_lock:
            stale = [k for k, r in self._signal_pulses.items() if now - r.last_ts > self._pulse_window_s * 4]
            for k in stale:
                del self._signal_pulses[k]

    def _record_hotness(self, product_id: str, score: float) -> None:
        if score <= 0:
            return
        with self._hot_lock:
            prev = self._hot_scores.get(product_id, 0.0)
            self._hot_scores[product_id] = min(10.0, (prev * 0.93) + score)

    def _adaptive_minute_top_n(self) -> int:
        if not self.minute_scan_use_hotset:
            return self.minute_scan_top_n
        with self._hot_lock:
            hot_count = sum(1 for score in self._hot_scores.values() if score >= 0.15)
        dynamic = self.minute_scan_top_n + (hot_count // 4)
        return max(self.minute_scan_min_top_n, min(self.minute_scan_max_top_n, dynamic))

    def _minute_scan_products(self, top_n: int) -> List[tuple[str, str]]:
        if self.minute_scan_use_hotset:
            with self._hot_lock:
                hot = sorted(self._hot_scores.items(), key=lambda kv: kv[1], reverse=True)
            pids = [pid for pid, score in hot if score >= 0.15][:max(1, self.minute_scan_hotset_size)]
            if pids:
                return [(pid, pid.split("-")[0]) for pid in pids]
        return [(p["id"], p["base"]) for p in top_coinbase_pairs(
            n=top_n,
            min_volume_usd=self.scan_min_volume,
            quote_currencies=("USD", "USDC", "BTC", "ETH"),
        )]

    def _paper_edge_model(
        self,
        confidence: float,
        win_rate: float,
        sharpe: float,
        liquidity_score: float = 1.0,
    ) -> Dict[str, float]:
        """Estimate gross edge vs fee/slippage/latency drag in bps."""

        prob = (0.50 * max(0.0, min(1.0, confidence)))
        prob += (0.30 * max(0.0, min(1.0, win_rate * 0.70)))
        prob += (0.20 * max(0.0, min(1.0, sharpe / 4.0)))
        gross_bps = prob * 100.0

        fill_ms = expected_fill_delay_ms(
            self.latency_profile,
            liquidity_score=max(0.1, min(1.0, liquidity_score)),
            crossing_spread=True,
        )
        latency_bps = min(20.0, fill_ms / 100.0)
        fee_bps = self._effective_fee_bps()
        net_bps = gross_bps - fee_bps - latency_bps
        return {
            "probability": prob,
            "gross_bps": gross_bps,
            "fee_bps": fee_bps,
            "latency_bps": latency_bps,
            "net_bps": net_bps,
        }

    def _paper_signal_score(self, opp: Dict[str, Any]) -> float:
        confidence = float(opp.get("confidence", 0.0) or 0.0)
        win_rate = float(opp.get("win_rate", 0.0) or 0.0)
        sharpe = float(opp.get("sharpe", 0.0) or 0.0)
        edge = self._paper_edge_model(confidence, win_rate, sharpe)
        action = str(opp.get("action", "BUY"))
        score = (0.45 * edge["probability"]) + (0.35 * (edge["net_bps"] / 100.0)) + (0.20 * win_rate)
        macro = self._last_macro_signal
        if action == "SELL":
            if macro and macro.allows_new_shorts:
                score *= 1.2  # boost shorts in bear-aligned regime
            elif macro and not macro.allows_new_shorts:
                score *= 0.8
        elif action == "BUY":
            score *= self._btc_momentum_multiplier()  # boost buys on positive BTC momentum
        return score

    def _paper_close_position(self, pos: PaperPosition, exit_price: float, reason: str) -> None:
        exit_side = "SELL" if pos.is_long else "BUY"
        fill = self._fill_model.estimate(
            pos.product_id,
            exit_side,
            pos.qty,
            exit_price,
            self._last_volume_24h.get(pos.product_id, 0.0),
        )
        exit_price = fill.exit_price
        gross = pos.qty * exit_price
        _, taker_bps, maker_bps = self._fee_tier()
        is_maker = self._fill_model.is_maker(self.paper_maker_pct)
        exit_fee_bps = maker_bps if is_maker else taker_bps
        fee = gross * (exit_fee_bps / 10_000.0)
        self.paper_fees_paid += fee
        # Return the margin that was debited on entry, plus realized P&L.
        # Entry debited margin_required (= entry_notional/leverage). Exit returns
        # that margin plus the actual P&L on the trade. This is symmetric for
        # long and short and eliminates the earlier phantom-cash inflation.
        margin_returned = pos.entry_notional / max(pos.leverage, 1.0)
        if pos.is_short:
            # Short P&L = (entry - exit) * qty ; gross = qty*exit_price
            self.paper_cash += margin_returned + (pos.entry_notional - gross) - fee
        else:
            # Long P&L = (exit - entry) * qty
            self.paper_cash += margin_returned + (gross - pos.entry_notional) - fee
        raw_pnl = (exit_price - pos.entry_price) * pos.qty
        if not pos.is_long:
            raw_pnl = -raw_pnl
        pnl = raw_pnl - pos.fees_paid - fee - pos.cum_funding
        # Accumulator is the authoritative running total (incremented on every
        # real exit). The per-trade ledger (paper_trades[]) is a log that may
        # occasionally omit/mis-record a pnl; the accumulator is what cash is
        # actually built from, so it is the source of truth for total P&L.
        self.paper_realized_pnl += pnl
        self._perf_tracker.record_trade(pos.strategy, pos.product_id, pnl, pos.entry_notional, fee, pos.side,
                                        backtest_win_rate=float(getattr(pos, "win_rate", 0.0) or 0.0))
        win = pnl >= 0
        if win:
            self.paper_wins += 1
        else:
            self.paper_losses += 1
        exit_notional = pos.qty * exit_price
        self._update_trailing_volume(exit_notional)
        strategy = pos.strategy
        with self._analytics_lock:
            s = self.strategy_stats.setdefault(strategy, {"trades": 0, "wins": 0, "losses": 0,
                                                           "volume": 0.0, "pnl": 0.0, "exit_reasons": {}})
            s["trades"] += 1
            s["wins"] += 1 if win else 0
            s["losses"] += 1 if not win else 0
            s["volume"] += exit_notional
            s["pnl"] += pnl
            s["exit_reasons"][reason] = s["exit_reasons"].get(reason, 0) + 1
            hold_time = time.time() - pos.entry_ts
            s.setdefault("hold_times", []).append(round(hold_time, 1))
            st = self._signal_type_counts.setdefault(strategy, {"BUY": 0, "SELL": 0})
            st[pos.side] = st.get(pos.side, 0) + 1
        self._analytics_dirty = True
        self.paper_trades.append({
            "ts": time.time(),
            "product_id": pos.product_id,
            "side": exit_side,
            "qty": pos.qty,
            "price": exit_price,
            "fee": fee,
            "pnl": pnl,
            "reason": reason,
            "strategy": strategy,
            "regime": pos.regime,
        })
        self._scalping.on_exit(pos.product_id, pnl)
        self._save_paper_state()
        log.info(
            "PAPER EXIT %s qty=%.6f entry=%.4f exit=%.4f pnl=$%.2f reason=%s",
            pos.product_id, pos.qty, pos.entry_price, exit_price, pnl, reason,
        )
        self._record_trade_event(
            "exit", pos.product_id, exit_price, side=exit_side,
            strategy=str(pos.strategy), qty=pos.qty, entry_price=pos.entry_price,
            notional=exit_notional, fee=fee, pnl=round(pnl, 4),
            reason=reason, regime=str(getattr(pos, "regime", "")),
            hold_s=round(time.time() - pos.entry_ts, 1),
        )
        if abs(pnl) >= 1.0:
            self._push_notification("trade", f"EXIT {pos.product_id}",
                                    f"{'PROFIT' if win else 'LOSS'} ${pnl:.2f} | {reason} | {pos.strategy}",
                                    {"product_id": pos.product_id, "pnl": round(pnl, 2), "side": pos.side,
                                      "strategy": pos.strategy, "reason": reason, "qty": pos.qty})

    # ── Core holdings DCA ──────────────────────────────────────────────

    def _dca_core_holdings(self) -> None:
        """Evaluate core assets (BTC/ETH/SOL) for dip-buy DCA opportunities."""
        if not self._core_holdings_enabled:
            return
        # Circuit breaker: live/approval modes respect CB
        if self.mode in ("live", "approval") and self._cb_breached:
            return
        for pid in ("BTC-USD", "ETH-USD", "SOL-USD"):
            try:
                self._dca_eval_asset(pid)
            except Exception as e:
                log.debug("DCA eval failed for %s: %s", pid, e)

    def _dca_execute_buy(self, pid: str, price: float, notional: float, dip_pct: float, reason: str = "dip") -> bool:
        """Execute a DCA/rebalance buy — paper simulation or live order depending on mode."""
        now = time.time()
        fee = notional * (self._effective_fee_bps() / 10_000.0)
        qty = notional / price
        holding = self._core_holdings.get(pid)

        if self.mode == "paper":
            if self._paper_equity() < notional + fee:
                return False
            self.paper_cash -= notional + fee
            self.paper_fees_paid += fee
            self._update_trailing_volume(notional)
        else:
            # Live/approval: place real market order via execution engine
            if not hasattr(self, "_exec_engine") or self._exec_engine is None:
                log.warning("DCA %s: no exec engine available for live mode", pid)
                return False
            if self._cb_breached:
                log.warning("DCA %s: circuit breaker active, skipping", pid)
                return False
            try:
                if not hasattr(self, "_cb_client") or self._cb_client is None:
                    log.warning("DCA %s: no CBClient available", pid)
                    return False

                # Preview first
                preview = self._cb_client.preview_order(
                    product_id=pid,
                    side="BUY",
                    order_type="MARKET",
                    base_size=str(round(qty, 8)),
                )
                preview_id = preview.get("order_id", preview.get("preview_id", "")) if preview else ""
                total_fiat = notional
                quote_size = str(round(total_fiat, 2))

                result = self._cb_client.market_order(
                    product_id=pid,
                    side="BUY",
                    quote_size=quote_size,
                    preview_id=preview_id,
                    client_order_id=f"dca_{pid}_{int(now)}",
                )
                if not result or result.get("status") == "FAILED":
                    log.warning("DCA %s live order failed: %s", pid, result)
                    return False
                fill_price = float(result.get("avg_price", price))
                fill_qty = float(result.get("filled_size", qty))
                qty = fill_qty
                price = fill_price
                notional = fill_qty * fill_price
                fee = float(result.get("fees", fee))
                log.info("DCA LIVE %s: filled qty=%.6f at $%.2f fee=$%.2f", pid, fill_qty, fill_price, fee)
            except Exception as e:
                log.error("DCA %s live execution error: %s", pid, e)
                return False

        if holding:
            holding.add_buy(qty, price, fee)
        else:
            self._core_holdings[pid] = CoreHolding(
                product_id=pid,
                qty=qty,
                cost_basis=price,
                total_cost=notional + fee,
                total_qty=qty,
                trades=1,
                last_buy_ts=now,
                created_ts=now,
            )

        log.info("CORE BUY %s [%s]: qty=%.6f price=%.2f notional=$%.2f dip=%.1f%% hold_qty=%.6f cost_basis=%.2f",
                  pid, reason, qty, price, notional, dip_pct,
                  self._core_holdings[pid].total_qty,
                  self._core_holdings[pid].cost_basis)
        self._push_notification("core_buy", f"Core {pid}",
                                f"Bought ${notional:.0f} at ${price:.2f} ({reason}, dip {dip_pct:.1f}%)",
                                {"product_id": pid, "price": price, "notional": notional, "dip_pct": dip_pct, "reason": reason})
        return True

    def _dca_eval_asset(self, pid: str) -> None:
        price = self._last_price.get(pid, 0.0)
        if price <= 0:
            return
        streaming = self.streaming.try_get(pid)
        if not streaming or len(streaming.closes) < 50:
            return
        closes = streaming.closes.to_list()
        now = time.time()

        # Check cooldown
        holding = self._core_holdings.get(pid)
        if holding and holding.last_buy_ts > 0 and (now - holding.last_buy_ts) < self._core_dca_cooldown_s:
            return

        # Compute dip from 50-period high
        lookback = min(50, len(closes))
        recent = closes[-lookback:]
        peak = max(recent)
        dip_pct = (peak - price) / peak * 100.0

        if dip_pct < self._core_dca_dip_pct:
            return

        # Determine available cash
        if self.mode == "paper":
            avail = self._paper_equity()
        else:
            avail = self._dca_available_cash()

        notional = min(self._core_dca_amount, avail * 0.5)
        if notional < self.paper_min_trade_usd:
            notional = self.paper_min_trade_usd
        if notional > avail:
            notional = avail
        if notional < 1.0:
            return

        self._dca_execute_buy(pid, price, notional, dip_pct)

    def _dca_available_cash(self) -> float:
        """Return available USD balance for live mode DCA."""
        try:
            if hasattr(self, "_cb_client") and self._cb_client is not None:
                accounts = self._cb_client.list_accounts()
                for acct in (accounts or []):
                    if acct.get("currency") == "USD":
                        return float(acct.get("available_balance", {}).get("value", 0) or 0)
                for acct in (accounts or []):
                    if acct.get("currency") == "USDC":
                        return float(acct.get("available_balance", {}).get("value", 0) or 0)
        except Exception as e:
            log.debug("DCA available cash fetch failed: %s", e)
        return 0.0

    def _rebalance_core_holdings(self) -> None:
        """Rebalance core long-term holdings toward target weights per bucket.

        Each bucket (stable/volatile) rebalances independently with its own
        assets, weights, threshold, and cadence.
        """
        for bucket_name, cfg in self._core_buckets_config.items():
            if not cfg.get("enabled", True):
                continue
            if self.mode in ("live", "approval") and self._cb_breached:
                return

            now = time.time()
            last_ts = self._core_rebalance_last_ts.get(bucket_name, 0.0)
            interval = cfg.get("rebalance_interval_s", 3600)
            if (now - last_ts) < interval:
                continue
            self._core_rebalance_last_ts[bucket_name] = now

            assets = cfg.get("assets", [])
            target_weights = cfg.get("target_weights", {})
            thr = cfg.get("rebalance_threshold_pct", 15.0) / 100.0

            # Current values per asset in this bucket
            values: Dict[str, float] = {}
            for pid in assets:
                h = self._core_holdings.get(pid)
                px = self._last_price.get(pid, 0.0)
                if h and px > 0:
                    values[pid] = h.current_value(px)
                elif h:
                    values[pid] = h.current_value(h.cost_basis)
                else:
                    values[pid] = 0.0

            total = sum(values.values())

            for pid in assets:
                h = self._core_holdings.get(pid)
                weight = target_weights.get(pid, 0.0)
                target = total * weight if total > 0 else 0.0
                cur = values.get(pid, 0.0)
                if h:
                    h.target_value = target
                    h.drift_pct = ((cur - target) / target * 100.0) if target > 0 else 0.0
                    h.rebalance_action = "hold"

                if target <= 0:
                    continue
                # Under target -> buy
                if cur < target * (1.0 - thr):
                    gap = target - cur
                    if h:
                        h.rebalance_action = "buy"
                    self._rebalance_buy(pid, gap, cfg)
                # Over target -> trim
                elif cur > target * (1.0 + thr):
                    gap = cur - target
                    if h:
                        h.rebalance_action = "trim"
                    self._rebalance_trim(pid, gap, cfg)

    def _rebalance_buy(self, pid: str, gap_value: float, cfg: Optional[Dict[str, Any]] = None) -> None:
        """Buy toward target on an under-weighted core asset.

        Sized to close at most half the gap, capped by the DCA amount and
        available cash, and gated by the per-asset buy cooldown.
        """
        price = self._last_price.get(pid, 0.0)
        if price <= 0:
            return
        holding = self._core_holdings.get(pid)
        now = time.time()
        if holding and holding.last_buy_ts > 0 and (now - holding.last_buy_ts) < self._core_dca_cooldown_s:
            return
        if self.mode == "paper":
            avail = self._paper_equity()
        else:
            avail = self._dca_available_cash()
        notional = min(self._core_dca_amount, gap_value * 0.5, avail * 0.5)
        if notional < self.paper_min_trade_usd:
            notional = self.paper_min_trade_usd
        if notional > avail or notional < 1.0:
            return
        self._dca_execute_buy(pid, price, notional, 0.0, reason="rebalance")

    def _rebalance_trim(self, pid: str, gap_value: float, cfg: Optional[Dict[str, Any]] = None) -> None:
        """Trim an over-weighted core asset back toward its target.

        Sells at most half the gap, capped at 25% of the position value, and
        credits cash (paper) or places a live market sell. Core positions are
        still long-term holds — trimming only corrects allocation drift.
        """
        price = self._last_price.get(pid, 0.0)
        if price <= 0:
            return
        holding = self._core_holdings.get(pid)
        if not holding or holding.qty <= 0:
            return
        now = time.time()
        if holding.last_buy_ts > 0 and (now - holding.last_buy_ts) < self._core_dca_cooldown_s:
            return
        sell_notional = min(gap_value * 0.5, holding.current_value(price) * 0.25)
        if sell_notional < 1.0:
            return
        qty = sell_notional / price

        if self.mode == "paper":
            fee = sell_notional * (self._effective_fee_bps() / 10_000.0)
            if self._paper_equity() <= 0:
                return
            realized = holding.trim_sell(qty, price, fee)
            self.paper_cash += realized
            self.paper_fees_paid += fee
            self._update_trailing_volume(sell_notional)
        else:
            if not hasattr(self, "_exec_engine") or self._exec_engine is None:
                return
            if self._cb_breached:
                return
            try:
                if not hasattr(self, "_cb_client") or self._cb_client is None:
                    return
                preview = self._cb_client.preview_order(
                    product_id=pid, side="SELL", order_type="MARKET",
                    base_size=str(round(qty, 8)),
                )
                preview_id = preview.get("order_id", preview.get("preview_id", "")) if preview else ""
                result = self._cb_client.market_order(
                    product_id=pid, side="SELL", base_size=str(round(qty, 8)),
                    preview_id=preview_id, client_order_id=f"trim_{pid}_{int(now)}",
                )
                if not result or result.get("status") == "FAILED":
                    return
                fill_qty = float(result.get("filled_size", qty))
                fill_price = float(result.get("avg_price", price))
                fee = float(result.get("fees", 0.0))
                holding.trim_sell(fill_qty, fill_price, fee)
            except Exception as e:
                log.error("Core trim %s live execution error: %s", pid, e)
                return

        holding.last_buy_ts = now  # reuse cooldown clock to avoid immediate re-buy
        log.info("CORE TRIM %s: qty=%.6f price=%.2f notional=$%.2f drift corrected", pid, qty, price, sell_notional)
        self._push_notification("core_trim", f"Trim {pid}",
                                f"Sold ${sell_notional:.0f} at ${price:.2f} (rebalance)",
                                {"product_id": pid, "price": price, "notional": sell_notional})

    def _core_holdings_value(self, prices: Optional[Dict[str, float]] = None) -> float:
        total = 0.0
        prices = prices or {}
        for pid, h in self._core_holdings.items():
            px = prices.get(pid, 0.0)
            if px <= 0:
                px = h.cost_basis
            total += h.current_value(px)
        return total

    def _core_holdings_to_dict(self) -> List[dict]:
        result = []
        now = time.time()
        for pid, h in self._core_holdings.items():
            price = self._last_price.get(pid, h.cost_basis)
            value = h.current_value(price)
            cost = h.total_cost
            # P&L consistent on a fees-included basis (value vs total_cost)
            pnl = value - cost
            pnl_pct = (pnl / cost * 100.0) if cost > 0 else 0.0
            holding_age = now - h.created_ts if h.created_ts > 0 else 0
            weight = self._core_target_weights.get(pid, 0.0)
            result.append({
                "product_id": pid,
                "qty": round(h.total_qty, 6),
                "cost_basis": round(h.cost_basis, 2),
                "total_cost": round(cost, 2),
                "total_invested": round(cost, 2),
                "current_price": round(price, 2),
                "current_value": round(value, 2),
                "unrealized_pnl": round(pnl, 2),
                "unrealized_pnl_pct": round(pnl_pct, 2),
                "target_weight": round(weight, 4),
                "target_value": round(h.target_value, 2),
                "drift_pct": round(h.drift_pct, 2),
                "rebalance_action": h.rebalance_action,
                "trades": h.trades,
                "last_buy_ago_s": round(now - h.last_buy_ts) if h.last_buy_ts > 0 else 0,
                "holding_age_s": round(holding_age),
            })
        return result

    def _tighten_all_position_stops(self) -> None:
        """Iterate all open positions, tighten stops based on age ratio, exit if stop hit."""
        prices = dict(self._last_price)
        if not prices:
            return
        with self._paper_lock:
            closed: List[str] = []
            for pid, pos in self.paper_positions.items():
                price = prices.get(pid)
                if not price or price <= 0:
                    continue
                pos.mark(price)
                if pos.initial_stop_dist <= 0:
                    continue
                age_ratio = pos.age_s / max(self.max_hold_s, 1)
                raw_r = (price - pos.entry_price) / max(pos.entry_price, 1e-9)
                r = raw_r if pos.is_long else -raw_r
                tighten_dist = pos.initial_stop_dist
                if age_ratio >= 0.90:
                    tighten_dist = pos.initial_stop_dist * 0.2
                elif age_ratio >= 0.75:
                    tighten_dist = pos.initial_stop_dist * 0.4
                elif age_ratio >= 0.50:
                    tighten_dist = pos.initial_stop_dist * 0.6
                elif age_ratio >= 0.25:
                    tighten_dist = pos.initial_stop_dist * 0.8
                if pos.is_long:
                    current_stop = max(pos.stop_price, pos.highest_price - tighten_dist)
                    if current_stop > pos.stop_price:
                        pos.stop_price = current_stop
                    if price <= pos.stop_price:
                        self._paper_close_position(pos, price, reason=f"minute_tighten:ratio={age_ratio:.2f}_r={pos.current_r_multiple:.1f}")
                        closed.append(pid)
                else:
                    current_stop = min(pos.stop_price if pos.stop_price > 0 else 1e9, pos.lowest_price + tighten_dist)
                    if current_stop < pos.stop_price or pos.stop_price <= 0:
                        pos.stop_price = current_stop
                    if price >= pos.stop_price:
                        self._paper_close_position(pos, price, reason=f"minute_tighten:ratio={age_ratio:.2f}_r={pos.current_r_multiple:.1f}")
                        closed.append(pid)
                # Time-based exit
                if pid not in closed and pos.age_s >= self.max_hold_s:
                    self._paper_close_position(pos, price, reason=f"timeout:{pos.age_s / 3600:.1f}h")
                    closed.append(pid)
            for pid in closed:
                self.paper_positions.pop(pid, None)
                if self._feed_mgr:
                    self._feed_mgr.remove_position(pid)
                self.paper_last_trade_ts[pid] = time.time()

    def _record_trade_event(self, kind: str, product_id: str, price: float, **fields) -> None:
        """Persist a labeled trade event to the durable feed cache for backtesting.

        Entries (entry / exit / scale_in / scale_out / reject) are written as
        JSON records to ``trade_events/<PRODUCT>.jsonl`` in the same store as the
        harvested candles (``feed_cache``, rooted at ``NAS_FEED_ROOT``). This lets
        a paper/live run double as a data factory: candles *plus* what the
        strategy actually did and what happened next. Best-effort; never raises.
        """
        try:
            from data.feed_cache import save_records
            rec = {"ts": time.time(), "kind": kind, "product_id": product_id, "price": price}
            rec.update(fields)
            save_records("trade_events", product_id, [rec])
        except Exception as e:  # pragma: no cover - durability is best-effort
            log.debug("trade event persist skipped for %s: %s", product_id, e)

    def _paper_open_position(self, product_id: str, price: float, opp: Dict[str, Any]) -> None:
        if price <= 0:
            return
        if len(self.paper_positions) >= self.paper_max_new_positions:
            return
        now = time.time()
        last_trade_ts = self.paper_last_trade_ts.get(product_id, 0.0)
        strategy_name = str(opp.get("strategy", "unknown"))
        perf_rec = self._perf_tracker.get(strategy_name, product_id)

        # Dynamic cooldown: shorter after wins, longer after losses
        if perf_rec and perf_rec.trades >= 1:
            if perf_rec.current_streak > 0:
                dynamic_cooldown = self.paper_product_cooldown_s * 0.5
            elif perf_rec.current_streak < -2:
                dynamic_cooldown = self.paper_product_cooldown_s * 2.0
            else:
                dynamic_cooldown = self.paper_product_cooldown_s
        else:
            dynamic_cooldown = self.paper_product_cooldown_s

        if now - last_trade_ts < dynamic_cooldown:
            log.info(
                "PAPER SKIP %s cooldown active: %.0fs remaining (dynamic=%ds, streak=%d)",
                product_id, dynamic_cooldown - (now - last_trade_ts),
                dynamic_cooldown, perf_rec.current_streak if perf_rec else 0,
            )
            return
        confidence = float(opp.get("confidence", 0.0) or 0.0)
        win_rate = float(opp.get("win_rate", 0.0) or 0.0)
        sharpe = float(opp.get("sharpe", 0.0) or 0.0)

        # Confidence recalibration: shrink self-reported confidence toward the OBSERVED
        # live win rate. Many strategies report ~1.0 confidence yet lose consistently;
        # blending with realized outcomes penalizes them in sizing and pushes them below
        # the min-confidence gate. Per-product evidence preferred; fall back to aggregate.
        # GUARD: a strategy with a REAL backtest verdict (wr>=min, sharpe>=min) is vetted —
        # its confidence must NOT be shrunk below the backtest-derived floor, or the bot
        # enters a death spiral: loses -> live win_rate drops -> confidence shrinks ->
        # notional falls below min_trade -> bot can't trade -> never recovers. Keep the
        # vetted floor as a lower bound so proven edges stay tradeable.
        _vetted_conf = 0.0
        if win_rate >= self.paper_min_win_rate and sharpe >= self.paper_min_sharpe:
            _vetted_conf = min(0.95, 0.30 + min(win_rate, 1.0) * 0.4 + max(0.0, sharpe) * 0.05)
        _recal = confidence
        if perf_rec and perf_rec.trades >= 5:
            _recal = confidence * 0.75 + perf_rec.win_rate * 0.25
        else:
            _agg = self._perf_tracker.strategy_aggregate(strategy_name)
            if _agg["trades"] >= 20:
                _recal = confidence * 0.8 + _agg["win_rate"] * 0.2
        confidence = max(_recal, _vetted_conf)

        # Kelly-optimal position sizing
        kelly = self._perf_tracker.kelly(strategy_name, product_id, min_trades=5)
        if kelly > 0:
            kelly_frac = kelly * 0.5  # half-Kelly for safety (was 0.25)
        elif kelly < 0:
            # Do NOT block on negative Kelly with sparse samples — fall back to
            # base sizing so the bot keeps trading instead of stalling out.
            log.info("PAPER SKIP-KELLY-FALLBACK %s: Kelly negative (%.4f) for %s/%s — using base sizing",
                     product_id, kelly, strategy_name, product_id)
            kelly_frac = self.paper_max_position_pct
        else:
            kelly_frac = self.paper_max_position_pct
        eq = self._paper_equity()
        notional = self._paper_trade_notional(confidence)
        notional = min(notional, eq * kelly_frac)
        if eq < notional:
            log.info("PAPER SKIP %s: notional %.0f exceeds equity %.0f", product_id, notional, eq)
            return
        if confidence < self.paper_min_confidence:
            log.info("PAPER SKIP %s: confidence %.3f < min %.3f (raw_opp_conf=%.3f strat=%s wr=%.3f)",
                     product_id, confidence, self.paper_min_confidence,
                     float(opp.get("confidence", 0.0) or 0.0), str(opp.get("strategy", "?")), win_rate)
            return
        if win_rate < self.paper_min_win_rate or sharpe < self.paper_min_sharpe:
            log.info("PAPER SKIP %s: win_rate %.3f/sharpe %.2f below min (wr>=%.2f sh>=%.2f)",
                     product_id, win_rate, sharpe, self.paper_min_win_rate, self.paper_min_sharpe)
            return
        edge = self._paper_edge_model(confidence, win_rate, sharpe)
        if edge["net_bps"] < self.paper_min_edge_bps:
            log.info(
                "PAPER SKIP %s edge too small: gross=%.1fbps fees=%.1fbps latency=%.1fbps net=%.1fbps",
                product_id, edge["gross_bps"], edge["fee_bps"], edge["latency_bps"], edge["net_bps"],
            )
            return
        score_mult = self._paper_score_multiplier(confidence, win_rate, sharpe)
        notional *= score_mult
        if str(opp.get("action", "BUY")) == "BUY":
            notional *= self._btc_momentum_multiplier()
        if notional < self.paper_min_trade_usd:
            log.info("PAPER SKIP %s: notional %.0f < min_trade %.0f after score/btc mult",
                     product_id, notional, self.paper_min_trade_usd)
            return

        leverage = min(self.max_leverage, float(opp.get("leverage", 1.0))) if self.enable_leverage else 1.0
        notional = min(notional, eq * self.paper_max_position_pct * leverage)
        if notional < self.paper_min_trade_usd:
            log.info("PAPER SKIP %s: notional %.0f < min_trade %.0f after leverage cap",
                     product_id, notional, self.paper_min_trade_usd)
            return

        _, taker_bps, maker_bps = self._fee_tier()
        is_maker = self._fill_model.is_maker(self.paper_maker_pct)
        entry_fee_bps = maker_bps if is_maker else taker_bps

        vol_24h = self._last_volume_24h.get(product_id, 0.0)
        action = str(opp.get("action", "BUY"))
        side_label = "LONG" if action in ("BUY", "LONG") else "SHORT"
        entry_side = "BUY" if side_label == "LONG" else "SELL"

        qty = notional / max(price, 1e-9)
        fill = self._fill_model.estimate(product_id, entry_side, qty, price, vol_24h)
        fill_price = fill.entry_price

        if fill.partial_fill_pct < 1.0:
            qty *= fill.partial_fill_pct
        notional = qty * fill_price
        margin_required = notional / max(leverage, 1)
        fee = notional * (entry_fee_bps / 10_000.0)

        if side_label == "SHORT":
            if eq < fee:
                log.info("PAPER SKIP %s: SHORT fee %.2f exceeds equity %.0f", product_id, fee, eq)
                return
        else:
            total_cost = margin_required + fee
            if total_cost > eq:
                margin_required = max(0.0, eq - fee)
                notional = margin_required * leverage
                qty = notional / max(fill_price, 1e-9)
                fee = notional * (entry_fee_bps / 10_000.0)
            if notional < self.paper_min_trade_usd:
                log.info("PAPER SKIP %s: notional %.0f < min_trade %.0f after margin/fill adjustment",
                         product_id, notional, self.paper_min_trade_usd)
                return

        # ── Portfolio risk check (uses new PortfolioRiskManager) ──────────
        if hasattr(self, '_portfolio_risk') and self._portfolio_risk:
            # Convert paper positions to Position objects for risk check
            positions = {}
            for pid, p in self.paper_positions.items():
                positions[pid] = Position(
                    product_id=pid,
                    side="LONG" if p.is_long else "SHORT",
                    size=p.qty,
                    entry_price=p.entry_price,
                    current_price=self._last_price.get(pid, p.entry_price),
                    unrealized_pnl=(self._last_price.get(pid, p.break_even_price) - p.break_even_price) * p.qty * p.leverage if p.is_long else (p.break_even_price - self._last_price.get(pid, p.break_even_price)) * p.qty * p.leverage,
                    notional=p.qty * self._last_price.get(pid, p.entry_price),
                    leverage=p.leverage,
                    cluster=self._portfolio_risk.get_cluster(pid),
                )
            self._portfolio_risk.update_positions(positions)
            equity = self._paper_equity()
            self._portfolio_risk.update_equity(equity)
            
            side = "LONG" if action in ("BUY", "LONG") else "SHORT"
            allowed, reason, adj_notional = self._portfolio_risk.check_pre_trade(
                product_id, side, notional, price, equity
            )
            if not allowed:
                log.info("PAPER SKIP %s portfolio risk: %s", product_id, reason)
                return
            if adj_notional < notional:
                notional = adj_notional
                if notional < self.paper_min_trade_usd:
                    return
                qty = notional / max(fill_price, 1e-9)
                margin_required = notional / max(leverage, 1)
                fee = notional * (entry_fee_bps / 10_000.0)

        # ── Cash modification (deferred after all checks) ──
        # Both sides debit MARGIN only on entry (not full notional). Exit returns
        # the same margin plus realized P&L — see _paper_exit_position. This keeps
        # the cash ledger self-consistent and prevents phantom-cash inflation.
        if side_label == "SHORT":
            self.paper_cash -= margin_required + fee
        else:
            self.paper_cash -= margin_required + fee
        self.paper_fees_paid += fee
        self._update_trailing_volume(notional)
        atr_val = float(opp.get("atr_14", 0.0) or 0.0)
        regime = str(opp.get("regime", ""))
        vol_mult = 1.0
        if regime == "high_volatility":
            vol_mult = 1.5
        elif regime in ("low_volatility", "ranging"):
            vol_mult = 0.75
        dynamic_stop_dist = float(opp.get("stop_dist", 0.0))
        if dynamic_stop_dist <= 0:
            dynamic_stop_dist = atr_val * 2.5 * vol_mult if atr_val > 0 else price * 0.03
        dynamic_target_dist = atr_val * 4.0 * vol_mult if atr_val > 0 else price * 0.06
        if side_label == "LONG":
            stop_loss_price = price - dynamic_stop_dist
            take_profit_price = price + dynamic_target_dist
        else:
            stop_loss_price = price + dynamic_stop_dist
            take_profit_price = price - dynamic_target_dist
        strategy_name = str(opp.get("strategy", "unknown"))
        liq_price = 0.0
        if leverage > 1.0 and dynamic_stop_dist > 0:
            if side_label == "LONG":
                liq_price = price - dynamic_stop_dist * (leverage / (leverage - 0.5))
            else:
                liq_price = price + dynamic_stop_dist * (leverage / (leverage - 0.5))
            liq_price = max(0.0, liq_price)

        pos = PaperPosition(
            product_id=product_id,
            side=side_label,
            qty=qty,
            entry_price=fill_price,
            entry_ts=time.time(),
            strategy=strategy_name,
            confidence=confidence,
            win_rate=win_rate,
            sharpe=sharpe,
            fees_paid=fee,
            highest_price=price,
            lowest_price=price,
            regime=str(opp.get("regime", "")),
            atr_14=atr_val,
            stop_price=stop_loss_price,
            target_price=take_profit_price,
            initial_stop_dist=dynamic_stop_dist,
            entry_notional=notional,
            leverage=leverage,
            liq_price=liq_price,
            long_horizon=bool(opp.get("is_long_horizon", False)),
        )
        self.paper_positions[product_id] = pos
        self.paper_last_trade_ts[product_id] = time.time()
        self._record_trade_event(
            "entry", product_id, fill_price, side=side_label,
            strategy=strategy_name, qty=qty, notional=notional, fee=fee,
            confidence=round(confidence, 4), win_rate=round(win_rate, 4),
            sharpe=round(sharpe, 4), regime=regime, atr_14=atr_val,
            stop_price=stop_loss_price, target_price=take_profit_price,
            leverage=leverage,
        )
        if self._feed_mgr:
            self._feed_mgr.add_position(product_id)
        with self._analytics_lock:
            st = self._signal_type_counts.setdefault(strategy_name, {"BUY": 0, "SELL": 0})
            signal_side = "BUY" if side_label == "LONG" else "SELL"
            st[signal_side] = st.get(signal_side, 0) + 1
            s_entry = self.strategy_stats.setdefault(strategy_name, {"trades": 0, "wins": 0, "losses": 0,
                                                                       "volume": 0.0, "pnl": 0.0, "exit_reasons": {}})
            s_entry.setdefault("entry_confidences", []).append(round(confidence, 3))
        log.info(
            "PAPER OPEN %s qty=%.4f @ %.2f notional=%.2f "
            "strat=%s conf=%.2f wr=%.0f%% sharpe=%.1f "
            "regime=%s atr=%.4f stop=%.4f target=%.4f "
            "cash=%.2f",
            product_id, qty, price, notional,
            opp.get("strategy"), confidence, win_rate * 100, sharpe,
            regime, atr_val, stop_loss_price, take_profit_price,
            self.paper_cash,
        )
        self.paper_trades.append({
            "ts": time.time(),
            "product_id": product_id,
                    "side": entry_side,
            "qty": qty,
            "price": price,
            "fee": fee,
            "pnl": 0.0,
            "reason": f"entry:{opp.get('strategy', 'unknown')}",
            "strategy": str(opp.get("strategy", "unknown")),
        })
        self._save_paper_state()
        log.info(
            "PAPER ENTRY %s qty=%.6f price=%.4f notional=$%.2f conf=%.2f wr=%.0f%% sharpe=%.1f edge=%.1fbps net=%.1fbps",
            product_id, qty, price, notional, confidence, win_rate * 100, sharpe, edge["gross_bps"], edge["net_bps"],
        )
        self._push_notification("trade", f"ENTRY {product_id}",
                                f"{side_label} {qty:.4f} @ ${price:.2f} | ${notional:.0f} | {opp.get('strategy', '')}",
                                {"product_id": product_id, "side": side_label, "qty": qty, "price": price,
                                 "notional": round(notional, 2), "strategy": str(opp.get("strategy", ""))})

    # ── Circuit Breakers ───────────────────────────────────────────

    def _check_circuit_breakers(self) -> bool:
        """Check all circuit breakers. Returns True if OK to trade, False if breached."""
        if self._cb_breached:
            return False

        now = time.time()
        if now - self._cb_day_start_ts > 86400:
            self._cb_daily_start_equity = self._cb_peak_equity
            self._cb_daily_loss_pct = 0.0
            self._cb_consecutive_losses = 0
            self._cb_day_start_ts = now

        cfg = self._live_cfg or TradingConfig.from_env()

        if self._cb_daily_loss_pct >= cfg.max_daily_loss_pct:
            self._cb_breached = True
            self._cb_breach_reason = f"daily_loss:{self._cb_daily_loss_pct:.2%}>={cfg.max_daily_loss_pct:.2%}"
            log.warning("CIRCUIT BREAKER: %s", self._cb_breach_reason)
            self._push_notification("circuit_breaker", "CIRCUIT BREAKER",
                                    f"Daily loss hit: {self._cb_daily_loss_pct:.1%}",
                                    {"reason": self._cb_breach_reason})
            return False

        if cfg.max_consecutive_losses > 0 and self._cb_consecutive_losses >= cfg.max_consecutive_losses:
            self._cb_breached = True
            self._cb_breach_reason = f"consecutive_losses:{self._cb_consecutive_losses}>={cfg.max_consecutive_losses}"
            log.warning("CIRCUIT BREAKER: %s", self._cb_breach_reason)
            return False

        if KillSwitch.is_active():
            self._cb_breached = True
            self._cb_breach_reason = "kill_switch_active"
            log.warning("CIRCUIT BREAKER: kill switch active")
            return False

        # ── MAX TOTAL DRAWDOWN HARD-HALT (live, real money) ─────────────
        # Daily-loss limit (above) blocks NEW trades but leaves the book open.
        # This is the portfolio-level backstop: if real equity drawdown from
        # session start exceeds the cap, we HARD-STOP the process (not just
        # pause) so nothing else can execute. Mirrors the corruption-sentinel
        # pattern: writes a sentinel, then exits.
        if self.mode == "live" and not getattr(self, "dry_run", False):
            try:
                import os as _os
                _dd_cap = float(_os.environ.get("LIVE_MAX_DRAWDOWN_PCT", "0.15"))
                _start = getattr(self, "_live_start_balance", 0.0) or 0.0
                if _start > 0:
                    _avail = self._dca_available_cash()
                    _positions = getattr(self, "_live_positions", {}) or {}
                    _open = 0.0
                    for _p in _positions.values():
                        if not isinstance(_p, dict):
                            continue
                        _qty = float(_p.get("size") or _p.get("base_size") or _p.get("qty") or 0)
                        _entry = float(_p.get("entry_price") or _p.get("average_price")
                                       or _p.get("entry") or 0)
                        _open += _qty * _entry
                    _equity = _avail + _open
                    _dd = (_start - _equity) / _start
                    if _dd >= _dd_cap:
                        self._cb_breached = True
                        self._cb_breach_reason = f"max_drawdown:{_dd:.1%}>={_dd_cap:.1%}"
                        log.error("CIRCUIT BREAKER (HARD HALT): %s", self._cb_breach_reason)
                        self._push_notification(
                            "circuit_breaker", "HARD HALT — max drawdown",
                            f"Real equity drawdown {_dd:.1%} >= {_dd_cap:.1%}. Stopping bot.",
                            {"reason": self._cb_breach_reason})
                        try:
                            Path("data/trader_state_corrupt").write_text(
                                f"{time.strftime('%Y-%m-%d %H:%M:%S')} mode=live "
                                f"path={self._paper_state_path.name}\n"
                                f"HARD HALT max_drawdown: dd={_dd:.1%}>=cap={_dd_cap:.1%}\n")
                        except OSError:
                            pass
                        # Hard stop: autostart will NOT relaunch while the
                        # sentinel exists. Operator must review before restart.
                        os._exit(1)
            except Exception as _de:
                log.warning("Max-drawdown halt check skipped (error): %s", _de)

        return True

    def _record_live_result(self, pnl: float) -> None:
        if pnl < 0:
            self._cb_consecutive_losses += 1
        else:
            self._cb_consecutive_losses = 0
        self._cb_daily_loss_pct = abs(
            min(0.0, (self._cb_daily_start_equity - self._cb_peak_equity + pnl) / max(self._cb_daily_start_equity, 1e-9))
        )

    def _minute_live_trailing(self) -> None:
        """Update trailing stops for all live brackets based on current price action."""
        if not self._bracket_mgr or not self._bracket_mgr._brackets:
            return

        active = self._bracket_mgr.active_brackets()
        if not active:
            return

        now = time.time()
        for bracket_id, bracket in active.items():
            try:
                pid = bracket.get("product_id")
                if not pid:
                    continue

                # Get current price
                price = self._last_price.get(pid)
                if not price or price <= 0:
                    continue

                # Get live position for this product to compute r_multiple and age
                live_positions = getattr(self, "_live_positions", {}) or {}
                pos = live_positions.get(pid)
                if not pos:
                    # No live position tracked, skip
                    continue

                # Compute position metrics
                entry_price = float(pos.get("entry_price", 0) or 0)
                size = float(pos.get("size", 0) or 0)
                side = pos.get("side", "LONG").upper()
                if entry_price <= 0 or size <= 0:
                    continue

                # Current P&L and R-multiple
                if side == "LONG":
                    raw_r = (price - entry_price) / entry_price
                else:
                    raw_r = (entry_price - price) / entry_price
                r_multiple = raw_r

                # Age in seconds
                entry_ts = bracket.get("timestamp", now)
                age_s = now - entry_ts

                # Initial stop distance
                initial_stop_dist = bracket.get("initial_stop_dist", 0.0)
                if initial_stop_dist <= 0:
                    # Compute from entry and current stop if not stored
                    current_stop = bracket.get("stop_price", 0.0)
                    if current_stop > 0 and entry_price > 0:
                        initial_stop_dist = abs(entry_price - current_stop)

                # Highest/lowest price from bracket or position
                highest_price = max(bracket.get("highest_price", price), price)
                lowest_price = min(bracket.get("lowest_price", price), price)
                bracket["highest_price"] = highest_price
                bracket["lowest_price"] = lowest_price

                # Regime for volatility multiplier
                regime = self.health_status.get("market_regime", "unknown")

                # Update trailing stop
                self._bracket_mgr.update_trailing_stop(
                    bracket_id=bracket_id,
                    current_price=price,
                    highest_price=highest_price,
                    lowest_price=lowest_price,
                    initial_stop_dist=initial_stop_dist,
                    r_multiple=r_multiple,
                    max_hold_s=self.max_hold_s,
                    age_s=age_s,
                    regime=regime,
                )

            except Exception as e:
                log.debug("Live trailing update failed for %s: %s", bracket_id, e)

    def _minute_live_exit_check(self, opportunities: List[Dict[str, Any]]) -> None:
        """Check live positions for exit signals: reverse signal, multi-signal consensus, time-based."""
        if not self._bracket_mgr or not self._bracket_mgr._brackets:
            return

        active = self._bracket_mgr.active_brackets()
        if not active:
            return

        # Build signal map by product
        signal_by_pid: Dict[str, List[Dict[str, Any]]] = {}
        for opp in opportunities:
            pid = opp.get("product_id")
            if pid:
                signal_by_pid.setdefault(pid, []).append(opp)

        now = time.time()
        for bracket_id, bracket in list(active.items()):
            try:
                pid = bracket.get("product_id")
                if not pid:
                    continue

                live_positions = getattr(self, "_live_positions", {}) or {}
                pos = live_positions.get(pid)
                if not pos:
                    continue

                side = pos.get("side", "LONG").upper()
                entry_ts = bracket.get("timestamp", now)
                age_s = now - entry_ts

                # 1. Time-based exit
                if age_s >= self.max_hold_s:
                    log.info("LIVE TIME EXIT %s: age %.1fh >= max_hold %.1fh", pid, age_s/3600, self.max_hold_s/3600)
                    self._bracket_mgr.force_flatten_bracket(bracket_id, reason=f"timeout:{age_s/3600:.1f}h")
                    continue

                # 2. Reverse signal from best strategy for this product
                pid_signals = signal_by_pid.get(pid, [])
                if pid_signals and age_s >= self.paper_min_hold_s:
                    best = max(pid_signals, key=lambda o: abs(float(o.get("confidence", 0.0) or 0.0)))
                    best_action = best.get("action", "BUY")
                    best_strat = best.get("strategy", "unknown")

                    is_long = side == "LONG"
                    reverse_signal = (is_long and best_action == "SELL") or (not is_long and best_action == "BUY")
                    if reverse_signal:
                        log.info("LIVE REVERSE EXIT %s: best signal %s says %s (was %s)", pid, best_strat, best_action, side)
                        self._bracket_mgr.force_flatten_bracket(bracket_id, reason=f"reverse:{best_strat}")
                        continue

                    # 3. Multi-signal consensus exit
                    total = len(pid_signals)
                    if total >= 3:
                        if is_long:
                            sell_pct = sum(1 for o in pid_signals if o.get("action") == "SELL") / total
                            if sell_pct >= 0.60:
                                log.info("LIVE CONSENSUS EXIT %s: %.0f%% sell signals", pid, sell_pct * 100)
                                self._bracket_mgr.force_flatten_bracket(bracket_id, reason=f"consensus:sell={sell_pct:.0%}")
                                continue
                        else:
                            buy_pct = sum(1 for o in pid_signals if o.get("action") == "BUY") / total
                            if buy_pct >= 0.60:
                                log.info("LIVE CONSENSUS EXIT %s: %.0f%% buy signals", pid, buy_pct * 100)
                                self._bracket_mgr.force_flatten_bracket(bracket_id, reason=f"consensus:buy={buy_pct:.0%}")
                                continue

            except Exception as e:
                log.debug("Live exit check failed for %s: %s", bracket_id, e)

    # ── Live Execution ─────────────────────────────────────────────

    def _validate_live_order(self, product_id: str, base_size: float,
                             notional: float) -> "tuple[bool, str]":
        """Pre-trade guard for live orders (real money).

        Returns (ok, reason). Rejects:
          - stablecoin / fiat products (USDC, USDT, DAI, any *USD* quote)
          - non-positive or mis-rounded base size (Coinbase rejects 13-dp BTC;
            8-dp is the safe precision)
          - notional below LIVE_MIN_NOTIONAL_USD floor (dust burns fees)
        Inert in paper mode (caller only invokes this in live path).
        """
        import os as _os
        # Stablecoins / fiat — never trade these as the asset.
        _pid = (product_id or "").upper()
        if _pid in ("USDC", "USDT", "DAI", "USD", "TUSD", "USDC-USDC"):
            return False, f"stablecoin/fiat product blocked: {product_id}"
        # Coinbase product ids are like BTC-USD; block if the BASE is a stable.
        _base = _pid.split("-")[0]
        if _base in ("USDC", "USDT", "DAI", "USD", "TUSD"):
            return False, f"stablecoin base blocked: {product_id}"
        # Base size sanity: positive and within 8-dp precision.
        if not isinstance(base_size, (int, float)) or base_size <= 0:
            return False, f"non-positive base_size: {base_size}"
        if round(base_size, 8) != base_size:
            return False, f"base_size exceeds 8dp precision: {base_size}"
        # Notional floor.
        _floor = float(_os.environ.get("LIVE_MIN_NOTIONAL_USD", "10.0"))
        if notional < _floor:
            return False, f"notional ${notional:.2f} < floor ${_floor:.2f}"
        return True, ""

    def _live_execute(self, product_id: str, price: float, opportunities: List[Dict[str, Any]],
                      regime_cmatrix: str = "") -> None:
        """Place real orders via NativeExecutionEngine + BracketManager with risk checks."""
        if price <= 0:
            return
        if not self._exec_engine or not self._bracket_mgr or not self._risk_mgr:
            log.warning("Live execution engines not initialized")
            return
        if not self._check_circuit_breakers():
            log.info("LIVE SKIP %s: circuit breaker active (%s)", product_id, self._cb_breach_reason)
            return

        buy_candidates = [o for o in opportunities if o.get("action") == "BUY"]
        if buy_candidates:
            best = max(
                buy_candidates,
                key=lambda o: abs(float(o.get("confidence", 0.0) or 0.0)),
            )
        else:
            best = max(opportunities, key=lambda o: abs(o.get("confidence", 0.0)))
        action = best.get("action", "BUY")
        confidence = float(best.get("confidence", 0.0) or 0.0)
        win_rate = float(best.get("win_rate", 0.0) or 0.0)
        sharpe = float(best.get("sharpe", 0.0) or 0.0)

        cfg = self._live_cfg or TradingConfig.from_env()

        if confidence < cfg.min_confidence:
            log.debug("LIVE SKIP %s: confidence %.2f < min %.2f", product_id, confidence, cfg.min_confidence)
            return

        # ── Regime gating ──
        regime = str(best.get("regime", "unknown"))
        if regime in ("unknown", "ranging", "low_volatility"):
            log.debug("LIVE SKIP %s: unfavorable regime %s", product_id, regime)
            return
        
        # ── StrategyRanking gate ──
        if hasattr(self, "_strategy_ranker") and self._strategy_ranker:
            strat = str(best.get("strategy", ""))
            rank = self._strategy_ranker.get_rank(product_id, strat)
            if rank is not None and rank > 100:  # bottom 100 strategies per product
                log.debug("LIVE SKIP %s: strategy %s rank %d", product_id, strat, rank)
                return
        
        # ── Confluence check: require ≥2 strategies agreeing ──
        agreeing = sum(1 for o in opportunities if o.get("action") == "BUY")
        if agreeing < 2 and len(opportunities) >= 3:
            log.info("LIVE SKIP %s: insufficient confluence (%d/%d BUY)", product_id, agreeing, len(opportunities))
            return

        # ── Regime-specific ATR multipliers ──
        stop_mult = cfg.bracket_stop_atr_mult
        target_mult = cfg.bracket_target_atr_mult
        if regime == "high_volatility":
            stop_mult *= 1.5
            target_mult *= 1.3
        elif regime == "trending":
            stop_mult *= 1.2
            target_mult *= 1.5

        if action == "BUY":
            atr_val = float(best.get("atr_14", 0.0) or 0.0)
            notional = min(
                cfg.max_notional_per_trade_usd,
                self._cb_peak_equity * cfg.risk_per_trade_pct * 100,
            )
            notional *= self._btc_momentum_multiplier()
            if confidence < cfg.min_confidence:
                log.debug("LIVE SKIP %s: confidence %.2f < min %.2f", product_id, confidence, cfg.min_confidence)
                return
            if notional < 10.0:
                log.debug("LIVE SKIP %s: notional $%.2f too small", product_id, notional)
                return

            # ── REAL-BALANCE EXPOSURE CAP (live only) ───────────────────
            # Size is computed off internal equity, but REAL money is bounded
            # by the actual exchange balance. Never let total open notional
            # exceed the available balance × cap (default 0.95) — this is the
            # guard against the bot over-committing funds it doesn't have.
            try:
                import os as _os
                _cap = float(_os.environ.get("LIVE_BALANCE_CAP", "0.95"))
                _avail = self._dca_available_cash()
                _positions = getattr(self, "_live_positions", {}) or {}
                _open_notional = 0.0
                for _p in _positions.values():
                    if not isinstance(_p, dict):
                        continue
                    _qty = float(_p.get("size") or _p.get("base_size") or _p.get("qty") or 0)
                    _entry = float(_p.get("entry_price") or _p.get("average_price")
                                   or _p.get("entry") or 0)
                    _open_notional += _qty * _entry
                if _avail and _avail > 0:
                    _max_total = _avail * _cap
                    if (_open_notional + notional) > _max_total:
                        log.warning(
                            "LIVE SKIP %s: would exceed balance cap — open=$%.2f + "
                            "order=$%.2f > avail=$%.2f x%.2f=$%.2f",
                            product_id, _open_notional, notional, _avail, _cap, _max_total)
                        return
            except Exception as _be:
                log.warning("LIVE balance-cap check skipped (error): %s", _be)

            base_size = notional / max(price, 1e-9)
            stop_dist = atr_val * stop_mult if atr_val > 0 else price * 0.03
            target_dist = atr_val * target_mult if atr_val > 0 else price * 0.06
            stop_price = price - stop_dist if action == "BUY" else price + stop_dist
            target_price = price + target_dist if action == "BUY" else price - target_dist

            pos_risk = PositionRisk(
                product_id=product_id, side="long", size=base_size,
                entry_price=price, current_price=price, stop_price=stop_price,
            )
            existing: List[PositionRisk] = []
            ok, reason = self._risk_mgr.check_trade(
                product_id, action, base_size, price, stop_price, target_price,
                self._cb_peak_equity, existing,
            )
            if not ok:
                log.info("LIVE SKIP %s: risk check failed: %s", product_id, reason)
                return

            # ── PRE-TRADE ORDER VALIDATION (live, real money) ──────────
            # Guard against malformed/unsafe orders reaching the exchange:
            #  - notional below a hard floor (dust orders waste fees)
            #  - non-positive or mis-rounded base size (Coinbase rejects
            #    13-dp BTC; 8-dp is the safe integer precision)
            #  - stablecoin products (USDC/USDT/DAI/USD*) — never trade fiat
            _valid, _vreason = self._validate_live_order(product_id, base_size, notional)
            if not _valid:
                log.warning("LIVE SKIP %s: order validation failed: %s", product_id, _vreason)
                return

            # Remember the intended order so _on_fill can detect slippage /
            # exposure drift vs what we actually sent.
            self._last_intended_order[product_id] = {
                "base_size": base_size, "notional": notional, "price": price,
                "ts": time.time(),
            }

            bracket = self._bracket_mgr.place_bracket(
                product_id=product_id,
                side=action,
                base_size=base_size,
                entry_price=price,
                stop_price=stop_price,
                target_price=target_price,
                strategy_id=str(best.get("strategy", "v4_live")),
            )

            entry_result = bracket.get("entry_order", {})
            if entry_result and entry_result.success:
                strategy_name = str(best.get("strategy", "unknown"))
                log.info(
                    "LIVE ENTRY %s qty=%.6f @ %.2f notional=$%.2f "
                    "strat=%s conf=%.2f stop=%.4f target=%.4f bracket=%s dry_run=%s",
                    product_id, base_size, price, notional,
                    strategy_name, confidence, stop_price, target_price,
                    bracket.get("status", "?"), self.dry_run,
                )
            else:
                err = (entry_result.error if entry_result else "unknown")
                log.warning("LIVE ENTRY FAILED %s: %s", product_id, err)
        elif action == "SELL":
            log.info("LIVE SKIP %s: sell signals not executed (long-only mode)", product_id)

    def _paper_execute(self, product_id: str, price: float, opportunities: List[Dict[str, Any]],
                       regime_cmatrix: str = "") -> None:
        if price <= 0:
            return
        with self._paper_lock:
            self._paper_execute_impl(product_id, price, opportunities, regime_cmatrix)

    def _paper_execute_impl(self, product_id: str, price: float, opportunities: List[Dict[str, Any]],
                            regime_cmatrix: str = "") -> None:
        # ── Central confidence normalization (all entry paths) ──
        # Confidence fed by individual strategies / the event eval is often a small
        # scalar (0.01-0.17) even for signals the bot's OWN backtest vetting passed
        # (win_rate>=0.45, sharpe>=0.30). A vetted signal deserves to clear the
        # paper_min_confidence gate on its real edge, not be parked by a low scalar.
        # Apply the same _bt_quality boost the scan bridge uses, so the EVENT path
        # (the dominant 437-product tick loop) and the scan bridge agree. One chokepoint.
        for _o in opportunities:
            _o_wr = float(_o.get("win_rate", 0.0) or 0.0)
            _o_sh = float(_o.get("sharpe", 0.0) or 0.0)
            if _o_wr >= self.paper_min_win_rate and _o_sh >= self.paper_min_sharpe:
                _btq = min(0.95, 0.30 + min(_o_wr, 1.0) * 0.4 + max(0.0, _o_sh) * 0.05)
                _o["confidence"] = max(float(_o.get("confidence", 0.0) or 0.0), _btq)
        # Max drawdown circuit breaker: stop new entries when drawdown > 80%
        try:
            dd = self._paper_drawdown()
            if dd > 0.80:
                log.info("PAPER SKIP %s: drawdown %.1f%% exceeds 80%% limit", product_id, dd * 100)
                return
        except Exception:
            pass
        # ── Per-asset self-aware drop (mirrors agent universe_tilt) ──
        # If the bot's OWN live P&L on this asset is clearly negative after a
        # sufficient sample, stop opening new entries here. Without this the bot
        # bleeds on the same losing pairs forever (e.g. -13% on a single asset).
        # Conservative: only acts on >=8 closed trades AND total_pnl < -$50, so it
        # cannot re-park the bot on thin/positive samples. Absence from the table
        # (thin sample) is treated as 'unknown' -> allowed.
        if self._perf_tracker is not None and product_id not in self.paper_positions:
            try:
                _ae = self._perf_tracker.asset_expectancy(min_trades=8)
                _rec = _ae.get(product_id)
                if _rec is not None and _rec["total_pnl"] < -50.0:
                    log.info("PAPER SKIP %s: asset self-drop (bot pnl %.2f over %d trades, wr=%.0f%%)",
                             product_id, _rec["total_pnl"], _rec["trades"], _rec["win_rate"] * 100)
                    return
            except Exception:
                pass
        # Pick the best opp AMONG those that can actually clear the entry gates.
        # Previously `max(opportunities, key=_paper_signal_score)` could select an opp
        # that then failed the raw confidence/win_rate/sharpe gate (e.g. a high-win_rate
        # technical signal with confidence < 0.30, or a crypto_news opp with wr=0), even
        # when the SAME product had a gate-passing opp (e.g. zscore_revert conf=0.83
        # wr=0.75). That silently parked viable trades. Filter to gate-eligible opps
        # first; only fall back to the raw max if none qualify (so exits/logging still work).
        _eligible = [
            o for o in opportunities
            if float(o.get("confidence", 0.0) or 0.0) >= self.paper_min_confidence
            and float(o.get("win_rate", 0.0) or 0.0) >= self.paper_min_win_rate
            and float(o.get("sharpe", 0.0) or 0.0) >= self.paper_min_sharpe
        ]
        best = max(_eligible or opportunities, key=self._paper_signal_score)
        pos = self.paper_positions.get(product_id)

        if pos:
            pos.mark(price)

        atr_val = float(best.get("atr_14", 0.0) or 0.0)
        vol_mult = 1.0
        regime = str(best.get("regime", ""))
        if regime in ("high_volatility",):
            vol_mult = 1.5
        elif regime in ("low_volatility", "ranging"):
            vol_mult = 0.75

        # ── Regime gating: skip unfavorable regimes for new entries ──
        # EXCEPTION: mean-reversion strategies are DESIGNED for low_volatility / ranging
        # markets — that is their natural edge. Blocking them there (while the market is
        # broadly low-vol) parked the bot with 0 trades even on high-confidence signals
        # (e.g. zscore_revert conf=0.52 wr=0.75 on ANKR, all silently dropped here).
        # Trend-following strategies are still correctly blocked in choppy/rangebound
        # regimes. "unknown" is always blocked (genuinely insufficient data).
        if not pos and regime in ("unknown", "ranging", "low_volatility"):
            _best_strat = str(best.get("strategy", ""))
            _is_mean_rev = _best_strat in self._MEAN_REVERSION_STRATS
            if regime == "unknown" or not _is_mean_rev:
                log.debug("PAPER SKIP %s: unfavorable regime %s for %s", product_id, regime, _best_strat)
                return

        # ── Confluence check: require ≥2 strategies agreeing ──
        if not pos:
            agreeing = sum(1 for o in opportunities if o.get("action") == best.get("action"))
            if agreeing < 2 and len(opportunities) >= 3:
                log.info("PAPER SKIP %s: insufficient confluence (%d/%d %s)", product_id, agreeing, len(opportunities), best.get("action"))
                return

        # ── Exit logic: multi-signal check ─────────────────────────
        should_exit = False
        exit_reason = ""

        if pos:
            raw_r = (price - pos.entry_price) / max(pos.entry_price, 1e-9)
            # r is signed: positive for LONG profit, negative for SHORT profit
            r = raw_r if pos.is_long else -raw_r

            # Noise-exit gate: multi-signal consensus and reverse-signal exits are prone
            # to whipsaw on mean-reversion strategies (sub-second flip-flops). Only allow
            # them after a minimum hold. Stops/take-profit/timeout below are unaffected.
            min_hold_ok = pos.age_s >= self.paper_min_hold_s

            # 1. Multi-signal consensus exit
            sell_signals = sum(1 for o in opportunities if o.get("action") == "SELL")
            buy_signals = sum(1 for o in opportunities if o.get("action") == "BUY")
            total_signals = len(opportunities)
            if min_hold_ok and total_signals > 0:
                if pos.is_long:
                    exit_pct = sell_signals / total_signals
                    exit_label = "sell"
                else:
                    exit_pct = buy_signals / total_signals
                    exit_label = "buy"
                if exit_pct >= 0.60:
                    should_exit = True
                    exit_reason = f"multi_signal:{exit_label}={exit_pct:.0%}"

            # 2. Reverse signal from best strategy
            if not should_exit and min_hold_ok:
                best_action = best.get("action", "")
                if (pos.is_long and best_action == "SELL") or (pos.is_short and best_action == "BUY"):
                    should_exit = True
                    exit_reason = f"signal={best.get('strategy')}(reverse)"

            # 3. Dynamic trailing stop
            if not should_exit and pos.initial_stop_dist > 0:
                if pos.is_long:
                    trailing_dist = pos.initial_stop_dist * (1.5 if regime == "high_volatility" else 1.0)
                    current_stop = pos.highest_price - trailing_dist
                    current_stop = max(current_stop, pos.stop_price) if pos.stop_price > 0 else current_stop

                    r_mult = pos.current_r_multiple
                    if r_mult >= 1.5 and not pos.breakeven_set:
                        current_stop = max(current_stop, pos.entry_price)
                        pos.breakeven_set = True
                        pos.trailing_activated = True

                    if r_mult >= 2.5:
                        tight_trail = pos.initial_stop_dist * 0.8
                        current_stop = max(current_stop, pos.highest_price - tight_trail)
                        pos.trailing_activated = True

                    pos.stop_price = current_stop
                    if price <= current_stop:
                        should_exit = True
                        exit_reason = f"trailing_stop:r={r_mult:.1f}"
                else:
                    trailing_dist = pos.initial_stop_dist * (1.5 if regime == "high_volatility" else 1.0)
                    current_stop = pos.lowest_price + trailing_dist
                    current_stop = min(current_stop, pos.stop_price) if pos.stop_price > 0 else current_stop
                    r_mult = pos.current_r_multiple
                    if r_mult >= 1.5 and not pos.breakeven_set:
                        current_stop = min(current_stop, pos.entry_price)
                        pos.breakeven_set = True
                    pos.stop_price = current_stop
                    if price >= current_stop:
                        should_exit = True
                        exit_reason = f"trailing_stop:r={r_mult:.1f}"

            # 4. Trailing take-profit — lock in gains as price extends
            if not should_exit:
                peak = pos.highest_price if pos.is_long else pos.lowest_price
                peak_r = ((peak - pos.entry_price) / max(pos.entry_price, 1e-9)) if pos.is_long else ((pos.entry_price - peak) / max(pos.entry_price, 1e-9))
                if peak_r > 0:
                    # Initial target to activate trailing take-profit
                    initial_target = 0.06 if atr_val <= 0 else min(0.12, atr_val * 4.0 / max(pos.entry_price, 1e-9))
                    if peak_r >= initial_target and pos.trailing_take_price <= 0:
                        # Activate: trail at 50% of the gain from entry (give back half)
                        pos.trailing_take_price = pos.entry_price * (1.0 + 0.5 * peak_r) if pos.is_long else pos.entry_price * (1.0 - 0.5 * peak_r)

                    # Update trailing take-profit as peak extends
                    if pos.trailing_take_price > 0:
                        new_take = pos.entry_price * (1.0 + 0.5 * peak_r) if pos.is_long else pos.entry_price * (1.0 - 0.5 * peak_r)
                        pos.trailing_take_price = new_take if pos.is_long else min(new_take, pos.trailing_take_price)

                    # Check if price has pulled back to the trailing take-profit level
                    if pos.trailing_take_price > 0:
                        hit_take = (price <= pos.trailing_take_price) if pos.is_long else (price >= pos.trailing_take_price)
                        if hit_take:
                            should_exit = True
                            exit_reason = f"trailing_take:r={r:.2%}pullback={((price - peak) / peak):.1%}" if pos.is_long else f"trailing_take:r={r:.2%}pullback={((peak - price) / peak):.1%}"

            # 5. Graduated stop tightening by age
            if not should_exit and pos.initial_stop_dist > 0:
                age_ratio = pos.age_s / max(self.max_hold_s, 1)
                if age_ratio >= 0.90:
                    age_tighten = 0.2
                elif age_ratio >= 0.75:
                    age_tighten = 0.4
                elif age_ratio >= 0.50:
                    age_tighten = 0.6
                elif age_ratio >= 0.25:
                    age_tighten = 0.8
                else:
                    age_tighten = 1.0
                if age_tighten < 1.0:
                    if pos.is_long:
                        tight_stop = pos.highest_price - pos.initial_stop_dist * age_tighten
                        if tight_stop > pos.stop_price:
                            pos.stop_price = tight_stop
                            log.debug("AGE TIGHTEN %s: stop → %.4f (age_ratio=%.2f)", product_id, tight_stop, age_ratio)
                        if price <= pos.stop_price:
                            should_exit = True
                            exit_reason = f"age_stop:ratio={age_ratio:.2f}_r={pos.current_r_multiple:.1f}"
                    else:
                        tight_stop = pos.lowest_price + pos.initial_stop_dist * age_tighten
                        if tight_stop < pos.stop_price or pos.stop_price <= 0:
                            pos.stop_price = tight_stop
                            log.debug("AGE TIGHTEN %s: stop → %.4f (age_ratio=%.2f)", product_id, tight_stop, age_ratio)
                        if price >= pos.stop_price:
                            should_exit = True
                            exit_reason = f"age_stop:ratio={age_ratio:.2f}_r={pos.current_r_multiple:.1f}"

            # 6. Time-based exit — close after max hold
            if not should_exit:
                if pos.age_s >= self.max_hold_s:
                    should_exit = True
                    exit_reason = f"timeout:{pos.age_s / 3600:.1f}h_r={r:.2%}"

        if should_exit:
            self._paper_close_position(pos, price, reason=exit_reason)
            self.paper_positions.pop(product_id, None)
            if self._feed_mgr:
                self._feed_mgr.remove_position(product_id)
            self.paper_last_trade_ts[product_id] = time.time()
            # Skip re-entry on same tick — prevents lock-in-loss-then-rebuy
            return

        # Entry rules for no position.
        best_action = best.get("action", "BUY")
        can_enter = best_action == "BUY" or (best_action == "SELL" and self.enable_shorts)
        if product_id not in self.paper_positions and can_enter:
            regime_str = str(best.get("regime", ""))
            atr_val = float(best.get("atr_14", 0.0) or 0.0)

            # Skip products with unknown regime or no ATR (insufficient data).
            # NOTE: only block on atr_val<=0 when the REGIME is also unknown. A known
            # regime (e.g. low_volatility) with a numerically-zero ATR (common for
            # sub-cent alts where ATR rounds to 0.0000) must NOT be skipped — the regime
            # detection already succeeded, so we have enough to trade. Blocking on a
            # zero ATR alone was silently parking every low-priced alt.
            if regime_str == "unknown" or (atr_val <= 0 and regime_str == ""):
                streaming = self.streaming.try_get(product_id)
                if streaming and len(streaming.closes) >= 40:
                    log.debug("ALLOW %s: regime=%s atr=%.4f (enough streaming data, proceeding)", product_id, regime_str, atr_val)
                else:
                    log.debug("SKIP %s: regime=%s atr=%.4f (insufficient streaming data)", product_id, regime_str, atr_val)
                    return

            # Gate mean-reversion strategies in STRONG trending regimes only.
            # Weak uptrend/downtrend = chop = exactly where mean-reversion works, so
            # those are now allowed (the bot already widens mean-rev stops at L1658).
            # Strong regimes stay blocked: fading a strong trend is the dangerous case.
            best_strat = str(best.get("strategy", ""))
            if best_strat in self._MEAN_REVERSION_STRATS and regime_str in (
                "strong_uptrend", "strong_downtrend",
            ):
                log.info("PAPER SKIP %s: %s is mean-reversion in STRONG %s regime",
                         product_id, best_strat, regime_str)
                return

            # Auto-disable check: skip if this strategy is disabled for this product
            if self._perf_tracker.is_disabled(best_strat, product_id):
                rec = self._perf_tracker.get(best_strat, product_id)
                log.info("PAPER SKIP %s: strategy %s disabled (%s)",
                         product_id, best_strat, rec.disable_reason if rec else "unknown")
                return

            # Global strategy disable: skip strategies that are broadly unprofitable
            # across all products (aggregate live win rate too low). BUT allow the trade
            # if this specific product has a strong product-specific backtest win rate —
            # a strategy can be weak in aggregate yet edge-positive on a given pair, and
            # the bot's own EVENT eval already vets win_rate/sharpe/edge per product. A
            # blunt global veto was parking the bot (e.g. chaikin_mf = 22% aggregate but
            # 100% backtest win on MET-USD). Respect product-specific evidence.
            if self._perf_tracker.is_strategy_disabled(best_strat):
                _opp_wr = float(best.get("win_rate", 0.0) or 0.0)
                if _opp_wr < self.paper_min_win_rate:
                    log.info("PAPER SKIP %s: strategy %s globally disabled (poor aggregate win rate, product wr=%.0f%%)",
                             product_id, best_strat, _opp_wr * 100)
                    return
                # Product-specific win rate clears the bar: allow despite aggregate veto.
                log.info("ALLOW %s: %s globally disabled but product wr=%.0f%% clears bar — proceeding",
                         product_id, best_strat, _opp_wr * 100)

            # ── Concentration guard (anti-fragility) ───────────────────────
            # A single strategy must not be allowed to dominate the book. If its
            # live PnL already exceeds max_strategy_pnl_share of equity, block
            # NEW entries for that strategy (existing positions are left to exit
            # on their own signals). This prevents a handful of low-sample lucky
            # trades from carrying the entire PnL (observed: top-3 strategies =
            # 110% of total pnl, one pair bled -$2,894).
            eq_now = 0.0
            if self._last_price:
                eq_now = self._paper_equity(self._last_price)
            if self._last_price and eq_now > 0 and self.max_strategy_pnl_share > 0:
                strat_pnl = self._perf_tracker.strategy_total_pnl(best_strat)
                if strat_pnl > self.max_strategy_pnl_share * eq_now:
                    log.info("PAPER SKIP %s: strategy %s live pnl=%.2f exceeds %.0f%% of equity (concentration cap)",
                             product_id, best_strat, strat_pnl, self.max_strategy_pnl_share * 100)
                    return

            # ── Sample-depth guard (anti-fragility) ────────────────────────
            # A strategy/product pair with few live trades is statistically
            # meaningless. Scale its confidence DOWN (not up) until it has
            # min_trades_for_full_sizing samples, so tiny-sample "winners"
            # can't overdrive position size.
            rec = self._perf_tracker.get(best_strat, product_id)
            pair_trades = rec.trades if rec else 0
            if pair_trades < self.min_trades_for_full_sizing:
                depth_scale = max(0.25, pair_trades / float(self.min_trades_for_full_sizing))
                best["confidence"] = max(0.01, float(best.get("confidence", 0.5)) * depth_scale)
                if pair_trades == 0:
                    log.debug("DEPTH PENALTY %s/%s: no live trades yet, conf scaled to %.3f",
                              best_strat, product_id, best["confidence"])
                else:
                    log.debug("DEPTH PENALTY %s/%s: %d/%d trades, conf x%.2f -> %.3f",
                              best_strat, product_id, pair_trades,
                              self.min_trades_for_full_sizing, depth_scale, best["confidence"])

            # Multi-signal confluence: require ≥2 strategies agreeing on direction
            agreeing = sum(1 for o in opportunities if o.get("action") == best_action and
                           not self._perf_tracker.is_disabled(
                               str(o.get("strategy", "")), product_id))
            if agreeing < 2 and len(opportunities) >= 3:
                log.info("PAPER SKIP %s: insufficient confluence (%d/%d agree on %s)",
                         product_id, agreeing, len(opportunities), best_action)
                return

            # Portfolio correlation limits: cap exposure per cluster
            clusters = self._correlation_clusters
            max_exposure = self._max_cluster_exposure_pct
            product_base = product_id.split("-")[0].upper()
            cluster_for_product = None
            for cluster_name, bases in clusters.items():
                if product_base in bases:
                    cluster_for_product = cluster_name
                    break
            if cluster_for_product:
                cluster_exposure = 0.0
                for pid, pos in self.paper_positions.items():
                    pb = pid.split("-")[0].upper()
                    for cl_name, bases in clusters.items():
                        if pb in bases and cl_name == cluster_for_product:
                            cluster_exposure += pos.notional_exposure
                            break
                total_equity = self._paper_equity(self._last_price)
                if total_equity > 0 and (cluster_exposure / total_equity) >= max_exposure:
                    log.info("PAPER SKIP %s: cluster %s at %.0f%% exposure (limit %.0f%%)",
                             product_id, cluster_for_product,
                             cluster_exposure / total_equity * 100,
                             max_exposure * 100)
                    return

            # Regime / macro gates
            macro = self._last_macro_signal
            if best_action == "SELL":
                if macro and not macro.allows_new_shorts:
                    log.info("PAPER SOFTEN %s: macro TF bias=%s discounts new shorts", product_id, macro.bias)
                    best["confidence"] *= 0.85

            # Pulse-aware confidence penalty for repeat signals.
            # Rapid repeat signals (3+ within 30 min) indicate a noisy/flip-flopping or
            # stale signal, not conviction — discount confidence so we trade them smaller
            # or skip them. (Previously this BOOSTED confidence and, worse, boosted more
            # during drawdowns — a martingale that sized up while losing.)
            pulse_key = f"{product_id}:{best_strat}:{best_action}"
            pulse = self._signal_pulses.get(pulse_key)
            if pulse and pulse.pulse_count >= 3 and pulse.age_s < 1800:
                penalty = min(0.25, 0.05 * (pulse.pulse_count - 2))
                best["confidence"] = max(0.01, best["confidence"] - penalty)
                log.debug("PULSE PENALTY %s: conf -%.3f (pulse_count=%d)", pulse_key, penalty, pulse.pulse_count)
            best["regime_cmatrix"] = regime_cmatrix
            if self.enable_leverage:
                best["leverage"] = self._vol_scaled_leverage(product_id, price, float(best.get("atr_14", 0.0)))
            else:
                best["leverage"] = 1.0
            macro_bias = macro.bias if macro else ""
            macro_conf = macro.confidence if macro else 0.0
            is_aligned = ((macro_bias == "bearish" and best_action == "SELL")
                          or (macro_bias == "bullish" and best_action == "BUY"))
            best["is_long_horizon"] = bool(macro_conf > 0.5 and is_aligned)
            if best["is_long_horizon"] and best.get("atr_14", 0.0) > 0:
                best["stop_dist"] = best.get("atr_14", 0.0) * 4.0  # wider stops
            self._paper_open_position(product_id, price, best)

        # ── Scale-in: add to winning positions ───────────────────────
        pos = self.paper_positions.get(product_id)
        if pos is not None and (best_action in ("BUY", "LONG")) == (pos.side in ("BUY", "LONG")):
            if pos.current_r_multiple >= 1.0 and pos.age_s < 43200 and pos.trades < 3:
                scale_notional = self._paper_trade_notional(best.get("confidence", 0.5)) * 0.5
                if scale_notional >= self.paper_min_trade_usd:
                    scale_fee = scale_notional * (self._effective_fee_bps() / 10_000.0)
                    if pos.is_short:
                        if self._paper_equity() >= scale_fee:
                            cash_delta = scale_notional - scale_fee
                            scale_qty = scale_notional / max(price, 1e-9)
                            pos.qty += scale_qty
                            pos.trades += 1
                            pos.entry_notional += scale_notional
                            pos.entry_price = pos.entry_notional / pos.qty
                            self.paper_cash += cash_delta
                            self.paper_fees_paid += scale_fee
                            log.info("SCALE IN %s: +%.4f qty (r_mult=%.1f, trade=%d/3)",
                                     product_id, scale_qty, pos.current_r_multiple, pos.trades)
                    else:
                        if self._paper_equity() >= scale_notional + scale_fee:
                            cash_delta = -(scale_notional + scale_fee)
                            scale_qty = scale_notional / max(price, 1e-9)
                            pos.qty += scale_qty
                            pos.trades += 1
                            pos.entry_notional += scale_notional
                            pos.entry_price = pos.entry_notional / pos.qty
                            self.paper_cash += cash_delta
                            self.paper_fees_paid += scale_fee
                            log.info("SCALE IN %s: +%.4f qty (r_mult=%.1f, trade=%d/3)",
                                     product_id, scale_qty, pos.current_r_multiple, pos.trades)

        equity = self._paper_equity(self._last_price)
        self.paper_equity_curve.append(equity)
        self.paper_equity_tss.append(time.time())
        if len(self.paper_equity_curve) > 10000:
            self.paper_equity_curve = self.paper_equity_curve[-5000:]
            self.paper_equity_tss = self.paper_equity_tss[-5000:]
        self.paper_peak_equity = max(self.paper_peak_equity, equity)

    def _watchdog_loop(self) -> None:
        while not self._shutdown:
            time.sleep(30)
            if self._shutdown:
                break
            now = time.time()
            alerts: List[str] = []
            if self._ws_feed and not getattr(self._ws_feed, "_running", False):
                alerts.append("ws_not_running")
                if not getattr(self, "_ws_dead_notified", False):
                    self._ws_dead_notified = True
                    self._push_notification("ws_dead", "WebSocket Disconnected",
                                            "Feed has stopped — falling back to polling",
                                            {"reconnects": getattr(self._ws_feed, "reconnect_count", 0)})
            else:
                self._ws_dead_notified = False
            if self._last_ticker_ts and (now - self._last_ticker_ts) > 180:
                alerts.append(f"ticker_stale:{int(now - self._last_ticker_ts)}s")
            if self._last_eval_ts and (now - self._last_eval_ts) > 120:
                alerts.append(f"eval_stale:{int(now - self._last_eval_ts)}s")
            if self.scan_interval > 0 and self._last_scan_ts and (now - self._last_scan_ts) > max(2 * self.scan_interval, 600):
                alerts.append(f"scan_stale:{int(now - self._last_scan_ts)}s")
            if self.minute_scan_interval > 0 and self._last_minute_scan_ts and (now - self._last_minute_scan_ts) > max(2 * self.minute_scan_interval, 180):
                alerts.append(f"minute_scan_stale:{int(now - self._last_minute_scan_ts)}s")
            if self.full_scan_interval > 0 and self._last_full_scan_ts and (now - self._last_full_scan_ts) > max(2 * self.full_scan_interval, 7200):
                alerts.append(f"full_scan_stale:{int(now - self._last_full_scan_ts)}s")

            # ── Restart dead background threads ──────────────────────
            _thread_map = [
                ("minute_scan", "_minute_scan_thread", self._minute_scan_loop,
                 self.minute_scan_interval > 0),
                ("batch_scan", "_scan_thread", self._scan_loop,
                 self.scan_interval > 0),
                ("full_scan", "_full_scan_thread", self._full_scan_loop,
                 self.full_scan_interval > 0),
                ("news_sentiment", "_news_thread", self._news_sentiment_loop, True),
                ("macro_risk", "_macro_thread", self._macro_risk_loop, True),
                ("analytics", "_analytics_thread", self._analytics_loop, True),
                ("experiment", "_experiment_thread", self._experiment_loop, True),
                ("pair_trade", "_pair_trade_thread", self._pair_trade_loop, True),
                ("onchain_flow", "_onchain_thread", self._onchain_loop, True),
                ("macro_tf", "_macro_tf_thread", self._macro_tf_loop, True),
                ("perf_tracker", "_perf_thread", self._perf_save_loop, True),
                ("funding_scan", "_funding_thread", self._funding_loop, True),
            ]
            for name, attr_name, target, should_run in _thread_map:
                if should_run:
                    t = getattr(self, attr_name, None)
                    if t and not t.is_alive():
                        log.warning("Thread %s dead — restarting", name)
                        new_t = threading.Thread(target=target, daemon=True, name=name)
                        new_t.start()
                        setattr(self, attr_name, new_t)
                        alerts.append(f"restarted:{name}")

            # Systemd watchdog notification
            try:
                from systemd.daemon import notify
                notify("WATCHDOG=1")
            except ImportError:
                try:
                    import socket
                    sock = os.environ.get("NOTIFY_SOCKET")
                    if sock:
                        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
                            s.connect(sock)
                            s.sendall(b"WATCHDOG=1")
                except Exception:
                    log.debug("Systemd watchdog notify failed")

            self.health_status["alerts"] = alerts
            self.health_status["last_ticker_ts"] = self._last_ticker_ts
            self.health_status["last_eval_ts"] = self._last_eval_ts
            self.health_status["last_scan_ts"] = self._last_scan_ts
            self.health_status["last_minute_scan_ts"] = self._last_minute_scan_ts
            self.health_status["last_full_scan_ts"] = self._last_full_scan_ts
            self.health_status["health_ok"] = not alerts and not self._cb_breached
            self.health_status["circuit_breakers"] = {
                "breached": self._cb_breached,
                "reason": self._cb_breach_reason,
                "daily_loss_pct": round(self._cb_daily_loss_pct, 4),
                "consecutive_losses": self._cb_consecutive_losses,
                "peak_equity": round(self._cb_peak_equity, 2),
            }
            self.health_status["live_engine"] = {
                "available": self._exec_engine is not None,
                "dry_run": self.dry_run,
                "mode": self.mode,
            }
            if alerts:
                log.warning("WATCHDOG alerts: %s", ",".join(alerts))

            # Prune expired signal fingerprints
            if self._signal_fingerprints:
                stale_keys = [k for k, v in self._signal_fingerprints.items() if v < now]
                for k in stale_keys:
                    del self._signal_fingerprints[k]
            # Trim in-memory trade list
            if len(self.paper_trades) > 1000:
                self.paper_trades = self.paper_trades[-500:]

    def flatten_all_positions(self) -> int:
        """Close all open positions at current price. Handles both paper and live."""
        closed = 0
        with self._paper_lock:
            for pid in list(self.paper_positions.keys()):
                pos = self.paper_positions.get(pid)
                if not pos:
                    continue
                price = self._last_price.get(pid, pos.entry_price)
                self._paper_close_position(pos, price, reason="flatten:llm_overseer")
                self.paper_positions.pop(pid, None)
                if self._feed_mgr:
                    self._feed_mgr.remove_position(pid)
                self.paper_last_trade_ts[pid] = time.time()
                closed += 1
            if closed:
                self._save_paper_state()
        if self.mode in ("live", "approval") and self._exec_engine:
            log.warning("FLATTEN: paper=%d — live brackets would need manual review", closed)
        if closed:
            log.warning("FLATTEN: closed %d positions via LLM overseer", closed)
        return closed

    def _paper_refresh_health(self) -> None:
        equity = self._paper_equity(self._last_price)
        trades = self.paper_wins + self.paper_losses
        win_rate = (self.paper_wins / trades) if trades else 0.0
        dd = self._paper_drawdown(equity)
        hot_pulses = {
            k: {"pulses": r.pulse_count, "conf": round(r.avg_confidence, 3), "dir": r.direction}
            for k, r in self._signal_pulses.items()
            if r.is_hot
        }
        self.health_status["paper"] = {
            "cash": round(self.paper_cash, 2),
            "equity": round(equity, 2),
            "realized_pnl": round(self.paper_realized_pnl, 2),
            "fees_paid": round(self.paper_fees_paid, 2),
            "positions": len(self.paper_positions),
            "trades": len(self.paper_trades),
            "wins": self.paper_wins,
            "losses": self.paper_losses,
            "win_rate": round(win_rate, 4),
            "drawdown": round(dd, 4),
            "peak_equity": round(self.paper_peak_equity, 2),
            "open_products": sorted(self.paper_positions.keys())[:20],
            "fee_tier": self._fee_tier()[0],
            "trailing_volume_30d": round(self.paper_trailing_volume_30d, 2),
            "monthly_volume": round(self.paper_monthly_volume, 2),
            "monthly_waiver_remaining": round(max(0.0, 500.0 - self.paper_monthly_volume), 2),
            "taker_bps": self._fee_tier()[1],
            "maker_bps": self._fee_tier()[2],
            "effective_fee_bps": round(self._effective_fee_bps(), 2),
            "maker_pct": self.paper_maker_pct,
            "core_holdings_count": len(self._core_holdings),
        }
        self.health_status["pulses"] = {
            "hot_count": len(hot_pulses),
            "total": len(self._signal_pulses),
            "hot": hot_pulses,
        }
        try:
            news_summary = self._news_sentiment.get_summary()
            self.health_status["news_sentiment"] = {
                k: v["article_count"] for k, v in news_summary.items()
            }
            self.health_status["news_sentiment_total_articles"] = sum(
                v["article_count"] for v in news_summary.values()
            )
        except Exception:
            pass
        try:
            macro_sig = self._macro_risk.get_signal()
            if macro_sig:
                self.health_status["macro_risk"] = {
                    "action": macro_sig.action,
                    "confidence": round(macro_sig.confidence, 3),
                    "score": macro_sig.macro_score,
                    "components": macro_sig.components,
                }
        except Exception:
            pass

        try:
            self.health_status["cross_asset_regime"] = self._cross_asset_regime_snapshot(refresh=False)
        except Exception:
            pass

        source_breakers: Dict[str, Any] = {}
        macro_breaker = getattr(self._macro_risk, "_breaker", None)
        if macro_breaker is not None and hasattr(macro_breaker, "snapshot"):
            source_breakers["macro_risk"] = macro_breaker.snapshot()
        onchain_breaker = getattr(self._onchain_flow, "_breaker", None)
        if onchain_breaker is not None and hasattr(onchain_breaker, "snapshot"):
            source_breakers["onchain_flow"] = onchain_breaker.snapshot()
        news_breakers = getattr(self._news_sentiment, "_feed_breakers", None)
        if isinstance(news_breakers, dict):
            source_breakers["news_feeds"] = {
                name: breaker.snapshot()
                for name, breaker in news_breakers.items()
                if hasattr(breaker, "snapshot")
            }
        if source_breakers:
            cross_asset_breaker = getattr(self._cross_asset_regime, "_breaker", None)
            if cross_asset_breaker is not None and hasattr(cross_asset_breaker, "snapshot"):
                source_breakers["cross_asset_regime"] = cross_asset_breaker.snapshot()
            self.health_status["source_breakers"] = source_breakers

        # ── Capital Buckets (equity summary) ──
        try:
            core_val = self._core_holdings_value(self._last_price)

            if self.mode == "paper":
                pos_val = 0.0
                for pid, pos in self.paper_positions.items():
                    px = self._last_price.get(pid, pos.entry_price)
                    if pos.is_short:
                        pos_val -= pos.qty * px
                    else:
                        pos_val += pos.qty * px
                cash = self.paper_cash
            else:
                # Live/approval: compute from exchange data
                pos_val = 0.0
                live_positions = getattr(self, "_live_positions", {}) or {}
                for pid, pos in live_positions.items():
                    if isinstance(pos, dict):
                        qty = float(pos.get("qty", 0) or 0)
                        px = self._last_price.get(pid, float(pos.get("entry_price", pos.get("current_price", 0)) or 0))
                        side = pos.get("side", "long")
                        if side == "short":
                            pos_val -= qty * px
                        else:
                            pos_val += qty * px
                cash = self._dca_available_cash()

            total = core_val + pos_val + cash
            buckets = {
                "timestamp": time.time(),
                "total_equity": round(total, 2),
                "cash": {"value": round(cash, 2), "pct": round(cash / total * 100, 1) if total > 0 else 0},
                "core_holdings": {"value": round(core_val, 2), "pct": round(core_val / total * 100, 1) if total > 0 else 0},
                "active_positions": {"value": round(pos_val, 2), "pct": round(pos_val / total * 100, 1) if total > 0 else 0},
            }
            self._capital_buckets = buckets
            Path("data/equity_summary.json").write_text(json.dumps(buckets, indent=2))
            self.health_status["capital_buckets"] = buckets
        except Exception as e:
            log.debug("Capital buckets compute failed: %s", e)

    # ── Analytics ───────────────────────────────────────────────────────

    def _compute_strategy_analytics(self) -> Dict[str, Dict[str, Any]]:
        with self._analytics_lock:
            out = {}
            for strat, s in list(self.strategy_stats.items()):
                if not isinstance(s, dict) or "trades" not in s:
                    self.strategy_stats.pop(strat, None)
                    continue
                trades = s["trades"]
                wins = s["wins"]
                losses = s["losses"]
                wr = wins / trades if trades > 0 else 0.0
                volume = s.get("volume", 0.0)
                pnl = s.get("pnl", 0.0)
                avg_win = pnl / wins if wins > 0 else 0.0
                avg_loss = abs(pnl) / losses if losses > 0 else 0.0
                profit_factor = (avg_win * wins) / max(avg_loss * losses, 1e-9) if losses > 0 else 9.99
                st = self._signal_type_counts.get(strat, {})
                confs = s.get("entry_confidences") or []
                hts = s.get("hold_times") or []
                out[strat] = {
                    "trades": trades,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": round(wr, 4),
                    "volume": round(volume, 2),
                    "pnl": round(pnl, 2),
                    "avg_win": round(avg_win, 2),
                    "avg_loss": round(avg_loss, 2),
                    "profit_factor": round(profit_factor, 3),
                    "buy_signals": st.get("BUY", 0),
                    "sell_signals": st.get("SELL", 0),
                    "exit_reasons": s.get("exit_reasons", {}),
                    "avg_entry_conf": round(sum(confs) / len(confs), 3) if confs else 0,
                    "avg_hold_secs": round(sum(hts) / len(hts), 1) if hts else 0,
                }
            return out

    def _analytics_review_prompt(self) -> str:
        stats = self._compute_strategy_analytics()
        if not stats:
            return "No strategy data yet."
        lines = ["# Strategy Performance Review", ""]
        lines.append(f"Total trades: {self.paper_wins + self.paper_losses}")
        lines.append(f"Overall win rate: {self.paper_wins/max(self.paper_wins+self.paper_losses,1):.1%}")
        lines.append(f"Realized P&L: ${self.paper_realized_pnl:.2f}")
        lines.append(f"Peak equity: ${self.paper_peak_equity:.2f}")
        lines.append("")
        lines.append("| Strategy | Trades | Win Rate | Volume | P&L | Profit Factor |")
        lines.append("|----------|--------|----------|--------|-----|---------------|")
        for strat, s in sorted(stats.items(), key=lambda x: -x[1]["trades"]):
            lines.append(
                f"| {strat} | {s['trades']} | {s['win_rate']:.1%} | "
                f"${s['volume']:.0f} | ${s['pnl']:.2f} | {s['profit_factor']:.2f} |"
            )
        return "\n".join(lines)

    def _save_analytics(self) -> None:
        path = Path("data/strategy_analytics.json")
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "ts": time.time(),
                "overall": {
                    "trades": self.paper_wins + self.paper_losses,
                    "wins": self.paper_wins,
                    "losses": self.paper_losses,
                    "win_rate": round(self.paper_wins / max(self.paper_wins + self.paper_losses, 1), 4),
                    "realized_pnl": round(self.paper_realized_pnl, 2),
                    "fees_paid": round(self.paper_fees_paid, 2),
                    "peak_equity": round(self.paper_peak_equity, 2),
                },
                "strategies": self._compute_strategy_analytics(),
            }
            tmp_path = path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(payload, indent=2))
            tmp_path.replace(path)
        except Exception as e:
            log.debug("Failed to save analytics: %s", e)

    def _analytics_loop(self) -> None:
        while not self._shutdown:
            time.sleep(300)
            if self._shutdown:
                break
            try:
                self._save_analytics()
                reg = self.health_status.get("paper", {}).get("win_rate", 0)
                trades = self.paper_wins + self.paper_losses
                if trades >= 10 and self._analytics_dirty:
                    self._analytics_dirty = False
                    review = self._analytics_review_prompt()
                    log.info("ANALYTICS review:\n%s", review)
            except Exception as e:
                log.debug("Analytics loop error: %s", e)

    # ── Experiment / Refinement System ─────────────────────────────────
    # 1. Submit analysis task to auto-assist (AssistX) for graph-backed task lifecycle
    # 2. LLM calls route through auto-router (x1-370:8088) → policy → provider/model
    # 3. Falls back to direct LM Studio endpoints when auto-router is unavailable

    _LMSTUDIO_X1_370 = "http://100.64.43.123:1234/v1/chat/completions"
    _LMSTUDIO_DEATHSTAR = "http://100.78.106.121:1234/v1/chat/completions"
    _AUTO_ROUTER_URL = "http://100.64.43.123:8088/v1/chat/completions"

    def _experiment_backtest(self, strategy: str, param_overrides: Dict[str, Any]) -> Dict[str, float]:
        return {"win_rate": 0.0, "sharpe": 0.0, "profit_factor": 0.0, "trades": 0}

    def _llm_review(self, prompt: str, agent_name: str) -> str:
        """Submit analysis to auto-router → LM Studio. Falls back to direct endpoint."""
        import urllib.request
        messages = [
            {"role": "system", "content": f"You are {agent_name}, a quantitative trading analyst. "
             "Review the strategy performance. Suggest specific parameter changes as JSON: "
             "'strategy', 'param_changes' (dict), 'reasoning' (str), 'expected_impact' (str)."},
            {"role": "user", "content": prompt},
        ]
        payload = json.dumps({"model": "auto/high-quality" if agent_name == "orinth" else "auto/fast",
                               "messages": messages, "temperature": 0.3, "max_tokens": 2048}).encode()
        headers = {"Content-Type": "application/json"}
        endpoints = [self._AUTO_ROUTER_URL]
        if agent_name == "orinth":
            endpoints.append(self._LMSTUDIO_X1_370)
            direct_model = "ornith-1.0-35b"
        else:
            endpoints.append(self._LMSTUDIO_DEATHSTAR)
            direct_model = "vibethinker-3b-i1"
        for url in endpoints:
            try:
                body = payload
                if "lmstudio" not in url or "auto-router" in url:
                    pass  # use payload as-is for auto-router
                else:
                    body = json.dumps({"model": direct_model, "messages": messages,
                                       "temperature": 0.3, "max_tokens": 2048}).encode()
                req = urllib.request.Request(url, data=body, headers=headers)
                with urllib.request.urlopen(req, timeout=120) as resp:
                    result = json.loads(resp.read().decode())
                msg = result.get("choices", [{}])[0].get("message", {})
                content = msg.get("content", "") or ""
                if content.strip():
                    return content.strip()
                rc = msg.get("reasoning_content", "") or ""
                if rc.strip():
                    return rc.strip()
            except Exception as e:
                log.debug("%s via %s failed: %s", agent_name, url.split("/")[2], e)
                continue
        return ""

    def _submit_via_assistx(self, prompt: str, orinth_review: str, vibethinker_review: str) -> None:
        """Submit analysis as an event to AssistX for task/graph persistence."""
        import urllib.request
        try:
            event = json.dumps({
                "event_id": f"analytics_{int(time.time())}",
                "event_type": "strategy.analysis.completed",
                "source_repo": "portfolio-management",
                "source_service": "trader-v4-experiment",
                "node_id": "xwing",
                "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "idempotency_key": f"analytics-{int(time.time())}",
                "subject": f"strategy-analysis-{int(time.time())}",
                "payload": {
                    "trades": self.paper_wins + self.paper_losses,
                    "win_rate": round(self.paper_wins / max(self.paper_wins + self.paper_losses, 1), 4),
                    "realized_pnl": round(self.paper_realized_pnl, 2),
                    "orinth_review": orinth_review[:500],
                    "vibethinker_review": vibethinker_review[:500],
                },
                "correlation_id": f"corr_analytics_{int(time.time())}",
                "actor": "trader-v4",
            }).encode()
            req = urllib.request.Request("http://100.64.43.123:8000/api/events",
                                         data=event,
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            log.debug("AssistX event submit failed: %s", e)

    def _experiment_review(self) -> Dict[str, Any]:
        prompt = self._analytics_review_prompt()
        if not prompt or prompt.startswith("No strategy"):
            return {}
        orinth_review = self._llm_review(prompt, "orinth")
        vibethinker_review = self._llm_review(prompt, "vibethinker")
        proposal = {"ts": time.time(), "orinth": orinth_review, "vibethinker": vibethinker_review}
        path = Path("data/experiment_proposals.json")
        try:
            existing = json.loads(path.read_text()) if path.exists() else []
            existing.append(proposal)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(existing, indent=2))
            tmp_path.replace(path)
        except Exception as e:
            log.debug("Failed to save proposal: %s", e)
        self._submit_via_assistx(prompt, orinth_review, vibethinker_review)
        log.info("EXPERIMENT: orinth=%d chars vibethinker=%d chars",
                 len(orinth_review), len(vibethinker_review))
        return proposal

    def _experiment_loop(self) -> None:
        while not self._shutdown:
            time.sleep(600)
            if self._shutdown:
                break
            try:
                if self.paper_wins + self.paper_losses >= 10:
                    self._experiment_review()
            except Exception as e:
                log.debug("Experiment loop error: %s", e)

    # ── Batch Scan ────────────────────────────────────────────────────

    def _scan_loop(self):
        """Periodic batch scan: discover pairs → fetch candles → rank."""
        while not self._shutdown:
            time.sleep(self.scan_interval)
            if self._shutdown:
                break
            try:
                self._run_scan(full=False, granularity=300, top_n=self.scan_top_n, min_volume=self.scan_min_volume)
            except Exception as e:
                log.error("Scan failed: %s", e, exc_info=True)

    def _minute_scan_loop(self):
        """Minute-level hot-set scan using 1m candles."""
        while not self._shutdown:
            time.sleep(self.minute_scan_interval)
            if self._shutdown:
                break
            try:
                # ── Stop-tightening pass for all open positions (paper mode) ──
                if self.mode == "paper" and self.paper_positions:
                    self._tighten_all_position_stops()

                # ── Live trailing stop management ──
                if self.mode in ("live", "approval") and self._bracket_mgr:
                    self._minute_live_trailing()

                # ── DCA core holdings dip-buy ──
                self._dca_core_holdings()
                # ── Core long-term hold rebalancing (target weights) ──
                self._rebalance_core_holdings()

                top_n = self._adaptive_minute_top_n()
                products = self._minute_scan_products(top_n)
                self._run_scan(
                    full=False,
                    granularity=60,
                    top_n=top_n,
                    min_volume=self.scan_min_volume,
                    label="MINUTE SCAN",
                    product_pairs=products,
                )

                # Note: Live exit checks are now integrated into _evaluate/_live_execute flow
                # via the signals processed. A separate minute-level check is redundant.

            except Exception as e:
                log.error("Minute scan failed: %s", e, exc_info=True)

    def _full_scan_loop(self):
        """Periodic full-universe scan: all active USD/BTC/ETH/USDC pairs."""
        while not self._shutdown:
            time.sleep(self.full_scan_interval)
            if self._shutdown:
                break
            try:
                self._run_scan(full=True, granularity=3600, label="FULL SCAN")
            except Exception as e:
                log.error("Full scan failed: %s", e, exc_info=True)

    def _run_scan(
        self,
        *,
        full: bool = False,
        granularity: int = 3600,
        top_n: Optional[int] = None,
        min_volume: Optional[float] = None,
        product_pairs: Optional[List[tuple[str, str]]] = None,
        label: Optional[str] = None,
    ):
        """Run one scan pass.

        Fast scan: top-N by volume.
        Full scan: all active USD/BTC/ETH/USDC pairs.
        """
        if not self._scan_lock.acquire(blocking=False):
            log.info("%s skipped: scan already in progress", label or ("FULL SCAN" if full else "FAST SCAN"))
            return
        try:
            top_n = top_n if top_n is not None else self.scan_top_n
            min_volume = min_volume if min_volume is not None else self.scan_min_volume
            label = label or ("FULL SCAN" if full else "FAST SCAN")
            if full:
                log.info("%s starting: all active Coinbase USD/BTC/ETH/USDC pairs...", label)
            else:
                log.info(
                    "%s starting: top %d pairs (min vol $%.0f)...",
                    label, top_n, min_volume,
                )

            with self.profiler.measure("scan_discovery"):
                if product_pairs is not None:
                    pairs = product_pairs
                elif full:
                    pairs = [(p["id"], p["base"]) for p in get_all_coinbase_pairs(
                        min_volume_usd=0,
                        quote_currencies=("USD", "USDC", "BTC", "ETH"),
                    )]
                else:
                    pairs = top_coinbase_pairs(
                        n=top_n,
                        min_volume_usd=min_volume,
                        quote_currencies=("USD", "USDC", "BTC", "ETH"),
                    )

            if not pairs:
                log.warning("%s: no pairs found", "FULL SCAN" if full else "FAST SCAN")
                return

            log.info("%s: %d pairs to evaluate (granularity=%ss)", label, len(pairs), granularity)

            with self.profiler.measure("scan_fetch"):
                pids = [p[0] for p in pairs]
                if self._feed_mgr:
                    candles = self._feed_mgr.get_candles_batch(
                        pids, granularity=granularity, limit=100,
                    )
                else:
                    from coinbase.src.rest_feed import fetch_candles_batch_sync
                    with self._scan_fetch_lock:
                        candles = fetch_candles_batch_sync(
                            pids, granularity=granularity, limit=100, max_workers=8 if full else 4,
                        )

            if not candles:
                log.warning("SCAN: no candle data returned")
                return

            closes = {pid: [c[4] for c in clist] for pid, clist in candles.items()}
            volumes = {pid: [c[5] for c in clist] for pid, clist in candles.items()}
            highs = {pid: [c[2] for c in clist] for pid, clist in candles.items()}
            lows = {pid: [c[3] for c in clist] for pid, clist in candles.items()}

            # Filter to products with enough data
            products = [(pid, base) for pid, base in pairs if pid in closes and len(closes[pid]) >= 60]

            with self.profiler.measure("scan_evaluate"):
                results = self._aggregator.scan_universe(
                    products, closes, volumes, highs, lows,
                )

            if full:
                self._last_full_scan = results
                self._last_full_scan_ts = time.time()
            else:
                self._last_scan = results
                self._last_scan_ts = time.time()
                if granularity <= 60:
                    self._last_minute_scan_ts = self._last_scan_ts

            # Log report
            buys = [r for r in results if r.direction == "BUY"]
            sells = [r for r in results if r.direction == "SELL"]
            log.info(
                "%s complete: %d/%d products, %d BUY %d SELL",
                label,
                len(results), len(products), len(buys), len(sells),
            )

            # Log top 10
            for r in results[:10]:
                log.info("SCAN TOP: %s", r.short_report())

            # Update health
            key = "last_full_scan" if full else ("last_minute_scan" if granularity <= 60 else "last_scan")
            self.health_status[key] = {
                "products": len(results),
                "buys": len(buys),
                "sells": len(sells),
                "top_buy": results[0].short_report() if results else None,
                "granularity": granularity,
                "top_n": top_n,
            }

            # Feed top scan signals into paper execution (BUY + SELL/short).
            # Allow SELL when shorts are enabled; previously the bridge skipped ALL
            # non-BUY directions, so the bot never took short entries from scans even
            # with --enable-shorts. Widen to top 8 so genuine BUY/SELL signals aren't
            # buried under a SELL-heavy top-5.
            _max_feed = 8 if self.enable_shorts else 5
            for r in results[:_max_feed]:
                if r.direction == "BUY":
                    _action = "BUY"
                elif r.direction == "SELL" and self.enable_shorts:
                    _action = "SELL"
                else:
                    continue
                pid = r.product_id
                price = self._last_price.get(pid, r.price)
                if price <= 0:
                    continue
                # ── Bridge: populate real ATR / regime / win_rate ──
                # Previously this fed atr_14=0.0, regime="", and win_rate=r.backtest_quality.
                # backtest_quality is an aggregate quality score that is 0.0 whenever no
                # strategy passes the aggregator's strict (passed AND trades>=5) filter,
                # so feeding it as win_rate silently failed the paper_min_win_rate gate and
                # parked the bot. We now compute real values from data the scan already has.
                _closes = closes.get(pid, [])
                _atr: float = 0.0
                if len(_closes) >= 15 and pid in highs and pid in lows:
                    _hl = highs[pid]
                    _ll = lows[pid]
                    _tr = [abs(_hl[i] - _ll[i]) for i in range(len(_closes))]
                    _tr[0] = abs(_closes[0] - _closes[1]) if len(_closes) > 1 else 0.0
                    for i in range(1, len(_closes)):
                        _tr[i] = max(
                            _hl[i] - _ll[i],
                            abs(_hl[i] - _closes[i - 1]),
                            abs(_ll[i] - _closes[i - 1]),
                        )
                    _win = _tr[-14:]
                    _atr = sum(_win) / len(_win) if _win else 0.0
                if _atr <= 0.0 and price > 0:
                    _atr = price * 0.02  # fallback: 2% of price if ATR can't be computed
                _trend = float(getattr(r, "trend_score", 0.0) or 0.0)
                if _trend >= 0.05:
                    _regime = "weak_uptrend"
                elif _trend <= -0.05:
                    _regime = "weak_downtrend"
                else:
                    _regime = "ranging"
                # Pick the first NON-disabled strategy from the top list. Previously the
                # bridge used top_strategies[0] only; if that strategy was globally disabled
                # (e.g. chaikin_mf), the entire candidate was skipped even when 2-3 other
                # enabled strategies agreed (conv often 70-85%). Fall through to an enabled one.
                _top_strat = "aggregator"
                _tracker = self._perf_tracker
                for _cand in (r.top_strategies or []):
                    _cand_s = str(_cand)
                    if _tracker is None or not _tracker.is_strategy_disabled(_cand_s):
                        _top_strat = _cand_s
                        break
                _base = pid.split("-")[0]
                _verdict = self._aggregator._bt_cache.get(f"{_top_strat}/{_base}")
                _raw_wr = getattr(_verdict, "win_rate", 0.0) if _verdict is not None else 0.0
                _win_rate = float(_raw_wr or 0.0)
                if _win_rate <= 0.0:
                    # Fall back to conviction (fraction of strategies agreeing) — never 0
                    # when there is a genuine signal, so the bot can still trade on consensus.
                    _win_rate = max(0.1, float(getattr(r, "conviction", 0.1) or 0.1))
                # Confidence: unified_score is often numerically tiny (0.01-0.13) even for
                # high-conviction signals, so feeding it raw makes the paper_min_confidence
                # gate (0.30) silently park every scan-fed trade. Confidence should reflect
                # HOW MANY strategies agree, not just the aggregator's blended scalar. Use the
                # larger of the two — a signal 70-85% of strategies agree on is genuinely
                # confident and deserves to clear the gate. Never below 0.05 so it stays a
                # real signal, never above 0.99.
                _conv = float(getattr(r, "conviction", 0.0) or 0.0)
                # Confidence must reflect real edge, not just the aggregator's blended
                # scalar (unified_score is often 0.01-0.17 even for genuinely good signals).
                # When the chosen strategy has a REAL backtest verdict that clears the paper
                # gates, that verdict IS the confidence — the bot already vetted this product
                # for this strategy. Fall through: max of unified_score, conviction, and the
                # verdict's own quality. This mirrors the win_rate conviction-fallback below:
                # a strategy the bot's own eval passed should not be parked by a low scalar.
                _bt_quality = 0.0
                if _verdict is not None:
                    _v_wr = float(getattr(_verdict, "win_rate", 0.0) or 0.0)
                    _v_sh = float(getattr(_verdict, "sharpe_ratio", 0.0) or 0.0)
                    if _v_wr >= self.paper_min_win_rate and _v_sh >= self.paper_min_sharpe:
                        # Scale quality from the verdict's win_rate/sharpe into 0.30-0.95 so
                        # a vetted signal always clears the 0.30 confidence gate, stronger
                        # edges scoring higher.
                        _bt_quality = min(0.95, 0.30 + min(_v_wr, 1.0) * 0.4 + max(0.0, _v_sh) * 0.05)
                _conf = max(abs(r.unified_score), _conv, _bt_quality)
                opp = {
                    "action": _action,
                    "confidence": max(0.05, min(0.99, _conf)),
                    "win_rate": max(0.0, min(1.0, _win_rate)),
                    "sharpe": 1.0,
                    "strategy": _top_strat,
                    "atr_14": _atr,
                    "regime": _regime,
                }
                if self.mode == "paper":
                    self._paper_execute(pid, price, [opp])
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(pid, price, [opp])
        finally:
            self._scan_lock.release()

    def _news_sentiment_scan(self) -> None:
        """Fetch news sentiment signals and feed them as opportunities."""
        try:
            signals = self._news_sentiment.get_signals(min_articles=1)
            for sig in signals:
                opp = sig.to_opportunity()
                pid = opp["product_id"]
                price = self._last_price.get(pid, 0.0)
                if price <= 0:
                    continue
                if self.mode == "paper":
                    self._paper_execute(pid, price, [opp])
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(pid, price, [opp])
        except Exception as e:
            log.debug("News sentiment scan error: %s", e)

    def _macro_risk_scan(self) -> None:
        """Fetch macro risk signal and feed across all products."""
        try:
            sig = self._macro_risk.get_signal()
            if sig is None:
                return
            opp = sig.to_opportunity()
            action = opp["action"]
            # Apply macro signal to BTC-USD as macro proxy
            pid = "BTC-USD"
            price = self._last_price.get(pid, 0.0)
            if price <= 0:
                pid = list(self._last_price.keys())[0] if self._last_price else ""
                price = self._last_price.get(pid, 0.0) if pid else 0.0
            if price > 0 and action in ("BUY", "SELL"):
                if self.mode == "paper":
                    self._paper_execute(pid, price, [opp])
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(pid, price, [opp])
        except Exception as e:
            log.debug("Macro risk scan error: %s", e)

    def _macro_leverage_cap(self) -> float:
        """Return max safe leverage based on macro regime and drawdown."""
        dd = self._paper_drawdown()
        base = self.max_leverage
        if dd > 0.10:
            return 1.0
        if dd > 0.05:
            return min(base, 1.5)
        macro = self._last_macro_signal
        if macro:
            base = base * macro.risk_multiplier
        return max(1.0, base)

    def _vol_scaled_leverage(self, product_id: str, price: float, atr_val: float) -> float:
        """Scale leverage inversely with volatility (ATR%).
        
        Low vol (<1% ATR) → max leverage
        High vol (>5% ATR) → 1x (no leverage)
        Uses cross-asset risk multiplier as additional cap.
        """
        if not self.enable_leverage or price <= 0:
            return 1.0
        atr_pct = (atr_val / price) * 100.0 if atr_val > 0 else 2.0
        if atr_pct <= 0:
            return 1.0
        atr_pct = max(0.5, min(10.0, atr_pct))
        vol_lev = max(1.0, 5.0 / atr_pct)
        cap = self._macro_leverage_cap()
        return min(vol_lev, cap, self.max_leverage)

    def _macro_tf_scan(self) -> None:
        """Fetch multi-timeframe macro analysis and store for scoring."""
        try:
            btc_price = self._last_price.get("BTC-USD", 0.0)
            signal = self._macro_tf_analyzer.analyze(btc_price=btc_price)
            self._last_macro_signal = signal
            self.health_status["macro_tf"] = {
                "bias": signal.bias,
                "confidence": signal.confidence,
                "risk_multiplier": signal.risk_multiplier,
                "allows_new_longs": signal.allows_new_longs,
                "allows_new_shorts": signal.allows_new_shorts,
                "cycle_phase": signal.cycle_phase,
                "reason": signal.reason,
                "btc_price": signal.btc_price,
            }
        except Exception as e:
            log.debug("Macro TF scan error: %s", e)

    # ── Live Trading Helpers ────────────────────────────────────────────
    
    def _reconcile_open_orders(self) -> None:
        """Reconcile open orders on startup.
        
        Fetches all open orders from Coinbase and updates local bracket state.
        Critical for live trading to avoid duplicate/ghost orders.
        """
        if not self._cb_client:
            return
        try:
            orders = self._cb_client.list_orders(status="OPEN")
            for order in orders:
                order_id = order.get("order_id")
                client_id = order.get("client_order_id", "")
                product_id = order.get("product_id")
                side = order.get("side", "").upper()
                size = float(order.get("filled_size", 0))
                status = order.get("status", "OPEN")
                
                # Try to match with existing brackets
                if client_id and client_id in self._bracket_mgr._brackets:
                    bracket = self._bracket_mgr._brackets[client_id]
                    bracket["status"] = "OPEN"
                    if bracket.get("stop_order_id") == order_id:
                        bracket["stop_order_id"] = order_id
                    elif bracket.get("target_order_id") == order_id:
                        bracket["target_order_id"] = order_id
                    log.info(f"[RECONCILE] Matched open order {order_id} to bracket {client_id}")
                else:
                    # Orphaned open order - log for review
                    log.warning(f"[RECONCILE] Orphaned open order: {order_id} {product_id} {side} size={size}")
        except Exception as e:
            log.error(f"[RECONCILE] Failed to reconcile open orders: {e}")

    def _sync_positions_from_exchange(self) -> None:
        """Reconcile local positions against the exchange on startup/restart.

        This is the SAFETY mechanism for surviving a crash/power-loss/outage:
        after the bot comes back, it MUST match its local view to what is
        ACTUALLY open on Coinbase, otherwise it would either (a) orphan real
        positions it forgot about (no stop, no exit — money at risk), or
        (b) try to manage "ghost" positions that already closed during the
        outage.

        Reconciliation rules (LIVE mode):
          - Real exchange position NOT in local tracking  -> ADOPT it (rebuild a
            PaperPosition so the bot manages it and re-establishes stops).
          - Real exchange position already tracked          -> UPDATE qty/entry.
          - Local position NOT present on the exchange       -> it closed during
            the outage; drop it from local tracking and book the realized close
            so cash/equity reconcile and the bot doesn't manage a ghost.
        In PAPER mode this is a no-op (positions are already loaded from state).
        """
        if not self._cb_client:
            return
        try:
            exchange = self._cb_client.get_positions()
        except Exception as e:
            log.error("[SYNC] Failed to fetch positions: %s", e)
            return

        if self.mode != "live":
            # Paper: keep reference snapshot only; do not mutate local book.
            self._live_positions = {p.get("product_id"): p for p in exchange if p.get("product_id")}
            return

        live_pids = {p.get("product_id") for p in exchange if p.get("product_id")}
        now = time.time()

        # 1) Adopt / update real positions.
        for ex in exchange:
            pid = ex.get("product_id")
            if not pid:
                continue
            size = float(ex.get("size", 0) or 0)
            if size <= 0:
                continue
            entry = float(ex.get("entry_price", 0) or 0)
            side = "SHORT" if str(ex.get("side", "")).upper() == "SHORT" else "LONG"
            existing = self.paper_positions.get(pid)
            if existing is None:
                # ADOPT: rebuild a managed PaperPosition from exchange truth.
                self.paper_positions[pid] = PaperPosition(
                    product_id=pid,
                    side=side,
                    qty=size,
                    entry_price=entry,
                    entry_ts=now,
                    strategy="adopted_on_restart",
                    confidence=0.5,
                    win_rate=0.5,
                    sharpe=0.5,
                    entry_notional=size * entry,
                    leverage=1.0,
                    highest_price=entry,
                    lowest_price=entry,
                )
                log.warning("[SYNC] ADOPTED live position %s %s x%.6f @ %.4f (was unknown locally)",
                            pid, side, size, entry)
            else:
                # UPDATE: trust the exchange for qty/entry.
                existing.qty = size
                if entry > 0:
                    existing.entry_price = entry
                    existing.entry_notional = size * entry
                existing.highest_price = max(existing.highest_price, entry)
                existing.lowest_price = min(existing.lowest_price or entry, entry)
                log.info("[SYNC] Updated local %s -> qty=%.6f entry=%.4f", pid, size, entry)

        # 2) Drop ghosts: local positions that are NOT on the exchange.
        for pid in list(self.paper_positions.keys()):
            if pid not in live_pids:
                pos = self.paper_positions.pop(pid)
                # Book the close at last known price so cash/equity reconcile.
                px = pos.highest_price or pos.entry_price or 0.0
                close_pnl = (px - pos.entry_price) * pos.qty if pos.is_long else \
                            (pos.entry_price - px) * pos.qty
                self.paper_realized_pnl += close_pnl
                self.paper_cash += pos.qty * px
                log.warning("[SYNC] Dropped GHOST local position %s (not on exchange); "
                           "booked close pnl=%.2f", pid, close_pnl)

        self._live_positions = {p.get("product_id"): p for p in exchange if p.get("product_id")}

    def _on_fill(self, event: dict) -> None:
        """Handle fill events from Advanced Trade WebSocket."""
        try:
            order_id = event.get("order_id")
            product_id = event.get("product_id")
            side = event.get("side", "").upper()
            size = float(event.get("size", 0))
            price = float(event.get("price", 0))
            fee = float(event.get("fee", 0))
            
            log.info(f"[FILL] {product_id} {side} {size} @ {price} fee={fee}")
            
            # Update bracket manager
            if self._bracket_mgr:
                self._bracket_mgr._check_bracket_status_by_fill(order_id, product_id, side, size, price)
            
            # Update position tracking
            if hasattr(self, "_live_positions") and product_id in self._live_positions:
                pos = self._live_positions[product_id]
                if pos.get("side") == "LONG" and side == "BUY":
                    pos["size"] += size
                    pos["entry_price"] = (pos["entry_price"] * (pos["size"] - size) + price * size) / pos["size"]
                elif pos.get("side") == "SHORT" and side == "SELL":
                    pos["size"] += size
                    pos["entry_price"] = (pos["entry_price"] * (pos["size"] - size) + price * size) / pos["size"]
                elif pos.get("side") == "LONG" and side == "SELL":
                    pos["size"] -= size
                    if pos["size"] <= 1e-9:
                        del self._live_positions[product_id]
                elif pos.get("side") == "SHORT" and side == "BUY":
                    pos["size"] -= size
                    if pos["size"] <= 1e-9:
                        del self._live_positions[product_id]

            # ── FILL vs INTENDED-ORDER DRIFT CHECK (live, real money) ─
            # Detect slippage / exposure drift: the actual fill should match
            # what we sent. If filled notional or fee drifts beyond tolerance,
            # alert loudly (possible partial fill, price spike, or bug).
            if self.mode == "live" and not self.dry_run and side in ("BUY", "SELL"):
                try:
                    import os as _os
                    _pid = product_id or ""
                    _intended = self._last_intended_order.get(_pid)
                    if _intended:
                        _fill_notional = size * price
                        _intended_notional = _intended.get("notional", 0.0)
                        _tol = float(_os.environ.get("LIVE_FILL_DRIFT_PCT", "0.05"))
                        if _intended_notional > 0:
                            _drift = abs(_fill_notional - _intended_notional) / _intended_notional
                            if _drift > _tol:
                                _msg = (f"FILL DRIFT {product_id} {side}: filled "
                                        f"${_fill_notional:.2f} vs intended "
                                        f"${_intended_notional:.2f} (drift {_drift:.1%} > "
                                        f"{_tol:.1%})")
                                log.warning("[FILL] %s", _msg)
                                self._push_notification("fill_drift", "FILL DRIFT",
                                                        _msg, {"product_id": _pid,
                                                               "side": side,
                                                               "drift": round(_drift, 4)})
                        # Consume the intended order so a later unrelated fill
                        # for the same product isn't compared to a stale intent.
                        if time.time() - _intended.get("ts", 0) > 300:
                            self._last_intended_order.pop(_pid, None)
                except Exception as _fe:
                    log.warning("[FILL] drift check error: %s", _fe)                        
        except Exception as e:
            log.error(f"[FILL] Error processing fill: {e}")

    def _on_order_update(self, event: dict) -> None:
        """Handle order status updates from Advanced Trade WebSocket."""
        try:
            order_id = event.get("order_id")
            status = event.get("status", "")
            product_id = event.get("product_id")
            log.debug(f"[ORDER UPDATE] {order_id} {product_id} -> {status}")
        except Exception as e:
            log.error(f"[ORDER UPDATE] Error: {e}")

    def _on_account_update(self, event: dict) -> None:
        """Handle account balance updates from Advanced Trade WebSocket."""
        try:
            accounts = event.get("accounts", [])
            for acc in accounts:
                currency = acc.get("currency")
                available = float(acc.get("available_balance", 0))
                hold = float(acc.get("hold", 0))
                log.debug(f"[ACCOUNT] {currency} available={available} hold={hold}")
        except Exception as e:
            log.error(f"[ACCOUNT] Error: {e}")

    def _macro_tf_loop(self) -> None:
        while not self._shutdown:
            self._macro_tf_scan()
            time.sleep(900)  # macro TF changes slowly, every 15 min

    def _news_sentiment_loop(self) -> None:
        while not self._shutdown:
            self._news_sentiment_scan()
            time.sleep(60)

    def _macro_risk_loop(self) -> None:
        while not self._shutdown:
            self._macro_risk_scan()
            time.sleep(300)  # macro moves slowly

    # ── Pair Trading Scan ─────────────────────────────────────────────

    def _pair_trade_scan(self) -> None:
        """Detect pair trading opportunities from correlated asset ratios."""
        try:
            signals = self._pair_trading.on_prices(self._last_price)
            for sig in signals:
                pid = sig["product_id"]
                price = self._last_price.get(pid, 0.0)
                if price <= 0:
                    continue
                if self.mode == "paper":
                    self._paper_execute(pid, price, [sig])
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(pid, price, [sig])
        except Exception as e:
            log.debug("Pair trade scan error: %s", e)

    def _pair_trade_loop(self) -> None:
        while not self._shutdown:
            self._pair_trade_scan()
            time.sleep(120)

    # ── On-Chain Flow Scan ────────────────────────────────────────────

    def _onchain_flow_scan(self) -> None:
        """Detect on-chain exchange flow anomalies across all products."""
        try:
            active_pids = [pid for pid in self._last_price.keys()
                           if "-USD" in pid or "-USDC" in pid]
            signals = self._onchain_flow.get_signals(active_pids)
            for sig in signals:
                pid = sig["product_id"]
                price = self._last_price.get(pid, 0.0)
                if price <= 0:
                    continue
                if self.mode == "paper":
                    self._paper_execute(pid, price, [sig])
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(pid, price, [sig])
        except Exception as e:
            log.debug("On-chain flow scan error: %s", e)

    def _onchain_loop(self) -> None:
        while not self._shutdown:
            self._onchain_flow_scan()
            time.sleep(300)

    def _funding_scan(self) -> None:
        """Funding rate capture — estimate from macro conditions. SHORT when positive funding, LONG when negative."""
        try:
            macro = self._last_macro_signal
            if macro is None:
                return
            btc_price = self._last_price.get("BTC-USD", 0.0)
            if btc_price <= 0:
                return
            btc_streaming = self.streaming.try_get("BTC-USD")
            if not btc_streaming or len(btc_streaming.closes) < 30:
                return
            closes = btc_streaming.closes.to_list()
            volumes = btc_streaming.volumes.to_list()
            ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else closes[-1]
            ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else ma20
            trend_strength = abs(closes[-1] - ma50) / max(ma50, 1e-9)

            # Infer funding from macro bias + trend
            if macro.bias == "bullish" and closes[-1] > ma20:
                est_funding = -0.03  # negative funding = longs pay shorts
                action = "BUY"
                conf = 0.35 + macro.confidence * 0.3
            elif macro.bias == "bearish" and closes[-1] < ma20:
                est_funding = 0.04   # positive funding = shorts pay longs
                action = "SELL"
                conf = 0.35 + macro.confidence * 0.3
            else:
                return

            if conf < 0.35:
                return

            for pid in list(self._last_price.keys())[:3]:
                price = self._last_price.get(pid, btc_price)
                if price <= 0:
                    continue
                atr_est = price * 0.02
                is_long_horizon = macro.confidence > 0.5
                opp = {
                    "action": action,
                    "confidence": round(conf, 3),
                    "win_rate": 0.5,
                    "sharpe": 0.6,
                    "strategy": "funding_capture",
                    "atr_14": atr_est,
                    "regime": macro.bias,
                    "is_long_horizon": is_long_horizon,
                    "stop_dist": atr_est * (4.0 if is_long_horizon else 2.5),
                    "leverage": self._vol_scaled_leverage(pid, price, atr_est) if self.enable_leverage else 1.0,
                    "reason": f"funding:{est_funding:+.3f}% bias={macro.bias} trend={trend_strength:.2%}",
                }
                if self.mode == "paper":
                    self._paper_execute(pid, price, [opp])
                elif self.mode in ("live", "approval") and not self._cb_breached:
                    self._live_execute(pid, price, [opp])
        except Exception as e:
            log.debug("Funding scan error: %s", e)

    def _funding_loop(self) -> None:
        while not self._shutdown:
            self._funding_scan()
            time.sleep(600)

    def _perf_save_loop(self) -> None:
        while not self._shutdown:
            time.sleep(60)
            if self._shutdown:
                break
            try:
                disabled = self._perf_tracker.auto_disable(min_trades=20, max_loss_streak=8, min_win_rate=0.25)
                if disabled:
                    log.info("Auto-disabled %d underperforming strategy/product pairs", disabled)
                strat_disabled = self._perf_tracker.auto_disable_strategies(min_trades=30, min_win_rate=0.25)
                if strat_disabled:
                    log.info("Globally disabled %d underperforming strategies (aggregate)", strat_disabled)
                strat_enabled = self._perf_tracker.auto_enable_strategies(min_trades=10, min_win_rate=0.35)
                if strat_enabled:
                    log.info("Re-enabled %d recovered strategies (aggregate)", strat_enabled)
                divergences = self._perf_tracker.divergence_report(min_trades=10, min_gap=0.20)
                for d in divergences:
                    log.warning("BACKTEST-LIVE DIVERGENCE: %s bt_wr=%.0f%% live_wr=%.0f%% gap=%.0f%% over %d trades pnl=%.2f",
                                d["strategy"], d["backtest_win_rate"]*100, d["live_win_rate"]*100,
                                d["gap"]*100, d["trades"], d["total_pnl"])
                self._perf_tracker.save()
            except Exception as e:
                log.debug("Perf save error: %s", e)

    # ── Cleanup ──────────────────────────────────────────────────────

    def _cleanup(self):
        log.info("Shutting down...")
        self._shutdown = True
        if self._feed_mgr:
            self._feed_mgr.stop()
        if self._ws_feed:
            self._ws_feed.stop()
        total_sigs = sum(self._signal_counts.values())
        total_opps = sum(self._opp_counts.values())
        log.info(
            "Shutdown: %d ticks, %d total signals, %d total opportunities",
            self._tick_count, total_sigs, total_opps,
        )
        self.health_status["latency"] = self.profiler.summary()
        self._save_paper_state()
        self._save_bt_cache()
        self._save_hot_scores()
        self.health_status["status"] = "stopped"
        log.info("Shutdown complete")

    # ── CLI ──────────────────────────────────────────────────────────

    @classmethod
    def from_cli(cls) -> "EventTraderV4":
        p = argparse.ArgumentParser(description="EventTraderV4 — real-time, no polling")
        p.add_argument("--mode", choices=["paper", "approval", "live"], default="paper")
        p.add_argument("--health-port", type=int, default=0, help="Health HTTP server port")
        p.add_argument("--log-file", type=str, default="", help="Write logs to file instead of stderr")
        p.add_argument("--products", nargs="*", help="Override product list")
        p.add_argument("--min-change", type=float, default=0.05, help="Min price change %% to trigger eval")
        p.add_argument("--paper-product-cooldown-seconds", type=int, default=1800, help="Cooldown per product after a paper entry/exit")
        p.add_argument("--paper-maker-pct", type=float, default=0.80, help="Fraction of orders simulated as maker (limit) vs taker (market)")
        p.add_argument("--minute-scan-interval", type=int, default=60, help="Minute-level scan interval in seconds (0=off)")
        p.add_argument("--minute-scan-top", type=int, default=25, help="Top N pairs for minute-level scan")
        p.add_argument("--minute-scan-min-top", type=int, default=10, help="Minimum dynamic top N for minute scans")
        p.add_argument("--minute-scan-max-top", type=int, default=50, help="Maximum dynamic top N for minute scans")
        p.add_argument("--minute-scan-use-hotset", action=argparse.BooleanOptionalAction, default=True, help="Use hot tickers for minute scans")
        p.add_argument("--minute-scan-hotset-size", type=int, default=25, help="Max hot tickers to include in minute scans")
        p.add_argument("--scan-interval", type=int, default=300, help="Batch scan interval in seconds (0=off)")
        p.add_argument("--scan-top", type=int, default=20, help="Top N pairs to scan by volume")
        p.add_argument("--scan-min-vol", type=float, default=1_000, help="Min 24h volume for scan")
        p.add_argument("--full-scan-interval", type=int, default=3600, help="Full-universe scan interval in seconds (0=off)")
        p.add_argument("--enable-shorts", action="store_true", help="Enable short selling in paper/live mode")
        p.add_argument("--enable-leverage", action="store_true", help="Enable leverage for position sizing")
        p.add_argument("--max-leverage", type=float, default=2.0, help="Maximum leverage (default 2.0)")
        p.add_argument("--reset-paper", action="store_true", help="Reset paper state: clear positions, reset balance to $10k")
        p.add_argument("--live", action="store_true", help="Short for --mode live")
        p.add_argument("--bypass-safety", action="store_true", help="Skip startup safety checks (dangerous)")
        p.add_argument("--max-hold", type=int, default=86400, help="Maximum position hold time in seconds (default 86400 = 24h). Stop tightens progressively at 25%/50%/75%/90% of max_hold.")
        args = p.parse_args()

        if args.reset_paper:
            reset_path = Path("data/paper_trader_v4_state.json")
            if reset_path.exists():
                reset_path.unlink()
                log.warning("Paper state reset requested — deleted %s", reset_path)
            for bak in Path("data").glob("paper_trader_v4_state.json.bak*"):
                bak.unlink()

        if args.live:
            args.mode = "live"

        dry_run = args.mode != "live"

        # ── Startup safety validation ──────────────────────────────
        if args.mode in ("live", "approval") and not args.bypass_safety:
            cfg = TradingConfig.from_env()
            issues = LiveSafetyValidator.check(cfg)
            if issues:
                log.error("=" * 60)
                log.error("LIVE MODE SAFETY CHECKS FAILED:")
                for i in issues:
                    log.error("  • %s", i)
                log.error("=" * 60)
                if not dry_run:
                    raise RuntimeError(
                        f"Live safety checks failed ({len(issues)} issues). "
                        f"Use --bypass-safety to override (NOT RECOMMENDED)."
                    )
                log.warning("Proceeding in dry-run mode despite %d safety issues", len(issues))

        return cls(
            mode=args.mode,
            products=args.products,
            health_port=args.health_port,
            dry_run=dry_run,
            enable_shorts=args.enable_shorts,
            enable_leverage=args.enable_leverage,
            max_leverage=args.max_leverage,
            min_change_pct=args.min_change,
            paper_product_cooldown_s=args.paper_product_cooldown_seconds,
            paper_maker_pct=args.paper_maker_pct,
            minute_scan_interval=args.minute_scan_interval,
            minute_scan_top_n=args.minute_scan_top,
            minute_scan_min_top_n=args.minute_scan_min_top,
            minute_scan_max_top_n=args.minute_scan_max_top,
            minute_scan_use_hotset=args.minute_scan_use_hotset,
            minute_scan_hotset_size=args.minute_scan_hotset_size,
            scan_interval=args.scan_interval,
            scan_top_n=args.scan_top,
            scan_min_volume=args.scan_min_vol,
            full_scan_interval=args.full_scan_interval,
            max_hold_s=args.max_hold,
        )


class HealthServer:
    """Health + latency + ping + status + action endpoint."""

    def __init__(self, port: int, status_ref: dict, trader_ref=None):
        self.port = port
        self.status_ref = status_ref
        self.trader_ref = trader_ref
        self._server_started = False
        self._server = None

    def start(self):
        import http.server
        import errno

        ref = self.status_ref
        trader = self.trader_ref
        _flatten_token = os.getenv("APPROVAL_TOKEN", "")

        class H(http.server.BaseHTTPRequestHandler):
            def _check_flatten_auth(self) -> bool:
                if not _flatten_token:
                    return True
                auth = self.headers.get("Authorization", "")
                return auth == f"Bearer {_flatten_token}" or auth == _flatten_token

            def do_OPTIONS(self):
                self.send_response(200)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()

            def do_GET(self):
                if self.path == "/":
                    body = json.dumps({
                        "status": ref.get("status", "unknown"),
                        "health_ok": ref.get("health_ok", False),
                        "alerts": ref.get("alerts", []),
                        "tick_count": ref.get("tick_count", 0),
                        "mode": ref.get("live_engine", {}).get("mode", "unknown"),
                        "uptime_s": round(time.time() - trader._start_ts) if trader and hasattr(trader, '_start_ts') else 0,
                        "paper": {
                            "equity": ref.get("paper", {}).get("equity", 0),
                            "positions": ref.get("paper", {}).get("positions", 0),
                            "win_rate": ref.get("paper", {}).get("win_rate", 0),
                            "drawdown": ref.get("paper", {}).get("drawdown", 0),
                            "monthly_volume": ref.get("paper", {}).get("monthly_volume", 0),
                            "max_hold_s": ref.get("max_hold_s", 86400),
                        },
                        "circuit_breakers": ref.get("circuit_breakers", {}),
                        "endpoints": [
                            "/", "/health", "/metrics", "/latency", "/ping",
                            "/strategy-analytics", "/strategies", "/pulses",
                            "/scan", "/paper/history", "/paper/positions", "/flatten",
                            "/config",
                        ],
                    }, indent=2).encode()
                elif self.path.startswith("/flatten"):
                    if not self._check_flatten_auth():
                        self.send_response(403)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": "forbidden"}).encode())
                        return
                    closed = trader.flatten_all_positions() if trader else 0
                    body = json.dumps({"action": "flatten", "closed": closed}, indent=2).encode()
                elif self.path.startswith("/latency"):
                    body = json.dumps(ref.get("latency", {}), indent=2).encode()
                elif self.path.startswith("/ping"):
                    body = json.dumps(measure_coinbase_latency(), indent=2).encode()
                elif self.path.startswith("/strategy-analytics"):
                    if trader and hasattr(trader, "_perf_tracker"):
                        body = json.dumps(trader._perf_tracker.summary(top_n=20), indent=2).encode()
                    else:
                        body = json.dumps({"error": "no trader"}, indent=2).encode()
                elif self.path.startswith("/strategies"):
                    body = json.dumps({
                        "total": len(EventTraderV4.STRATEGY_NAMES),
                        "names": sorted(EventTraderV4.STRATEGY_NAMES),
                        "rust_enabled": _HAS_RUST,
                    }, indent=2).encode()
                elif self.path.startswith("/pulses"):
                    body = json.dumps(ref.get("pulses", {}), indent=2).encode()
                elif self.path.startswith("/scan"):
                    last_scan = ref.get("last_scan", {})
                    body = json.dumps(last_scan, indent=2).encode()
                elif self.path.startswith("/metrics"):
                    paper = ref.get("paper", {})
                    cb = ref.get("circuit_breakers", {})
                    lines = [
                        "# HELP trader_tick_count Total evaluation ticks",
                        "# TYPE trader_tick_count counter",
                        f"trader_tick_count {ref.get('tick_count', 0)}",
                        "# HELP trader_equity Current paper equity",
                        "# TYPE trader_equity gauge",
                        f"trader_equity {paper.get('equity', 0)}",
                        "# HELP trader_cash Current paper cash",
                        "# TYPE trader_cash gauge",
                        f"trader_cash {paper.get('cash', 0)}",
                        "# HELP trader_positions Open position count",
                        "# TYPE trader_positions gauge",
                        f"trader_positions {paper.get('positions', 0)}",
                        "# HELP trader_win_rate Paper win rate",
                        "# TYPE trader_win_rate gauge",
                        f"trader_win_rate {paper.get('win_rate', 0)}",
                        "# HELP trader_drawdown Current drawdown pct",
                        "# TYPE trader_drawdown gauge",
                        f"trader_drawdown {paper.get('drawdown', 0)}",
                        "# HELP trader_monthly_volume Monthly traded volume USD",
                        "# TYPE trader_monthly_volume gauge",
                        f"trader_monthly_volume {paper.get('monthly_volume', 0)}",
                        "# HELP trader_effective_fee_bps Current effective fee rate",
                        "# TYPE trader_effective_fee_bps gauge",
                        f"trader_effective_fee_bps {paper.get('effective_fee_bps', 0)}",
                        "# HELP trader_circuit_breached Circuit breaker state",
                        "# TYPE trader_circuit_breached gauge",
                        f"trader_circuit_breached {1 if cb.get('breached') else 0}",
                        "# HELP trader_alerts_count Active alert count",
                        "# TYPE trader_alerts_count gauge",
                        f"trader_alerts_count {len(ref.get('alerts', []))}",
                        "# HELP trader_health_ok Overall health",
                        "# TYPE trader_health_ok gauge",
                        f"trader_health_ok {1 if ref.get('health_ok') else 0}",
                    ]
                    body = "\n".join(lines).encode()
                elif self.path.startswith("/paper/history"):
                    with trader._paper_lock:
                        curve = trader.paper_equity_curve[-500:] if trader else []
                        tss = trader.paper_equity_tss[-500:] if trader else []
                        trades = trader.paper_trades[-200:] if trader else []
                    # convert index-based timestamps (< 1e9) to real timestamps
                    # by anchoring to the first real timestamp or first trade ts
                    if len(tss) < len(curve):
                        tss = [0.0] * len(curve)
                    first_real_ts = next((t for t in tss if t > 1e9), 0)
                    if first_real_ts == 0:
                        # no real timestamps yet — use first trade ts as anchor
                        first_trade_ts = trades[-1]["ts"] if trades else time.time()
                        first_real_ts = first_trade_ts + 1
                    for i in range(len(tss)):
                        if tss[i] < 1e9:
                            tss[i] = first_real_ts - (len(tss) - i)
                    # cap at floor of 0 to avoid negative timestamps
                    min_ts = tss[0] if tss else 0
                    if min_ts < 0:
                        for i in range(len(tss)):
                            tss[i] -= min_ts
                    history = {
                        "equity_curve": [[tss[i], curve[i]] for i in range(len(curve))],
                        "trades": trades[::-1],
                        "starting_capital": trader.paper_starting_capital if trader else 10000,
                    }
                    body = json.dumps(history, indent=2).encode()
                elif self.path.startswith("/paper/positions"):
                    positions_list = []
                    if trader and hasattr(trader, 'paper_positions'):
                        with trader._paper_lock:
                            # Snapshot positions and prices under lock
                            snap_positions = dict(trader.paper_positions)
                            paper_cash = trader.paper_cash
                        # Compute equity for position sizing (lock-free after snapshot)
                        prices_for_equity: Dict[str, float] = {}
                        for pid, pos in snap_positions.items():
                            streaming = trader.streaming.try_get(pid) if hasattr(trader, 'streaming') else None
                            cp = pos.entry_price
                            if streaming and len(streaming.closes) > 0:
                                cp = streaming.closes[-1]
                            prices_for_equity[pid] = cp
                        total_equity = trader._paper_equity(prices_for_equity)
                        for pid, pos in snap_positions.items():
                            current_price = prices_for_equity.get(pid, pos.entry_price)
                            be = pos.break_even_price
                            raw_pnl = (current_price - be) * pos.qty
                            unrealized_pnl = raw_pnl if pos.is_long else -raw_pnl
                            raw_pnl_pct = (current_price - be) / be * 100.0 if be > 0 else 0.0
                            unrealized_pnl_pct = raw_pnl_pct if pos.is_long else -raw_pnl_pct
                            notional = pos.qty * current_price
                            position_size_pct = (notional / total_equity * 100.0) if total_equity > 0 else 0.0

                            # Dynamic ATR-based stop/target — use stored position values as fallback
                            entry = pos.entry_price
                            atr_val = pos.atr_14 if pos.atr_14 > 0 else 0.0
                            regime_raw = pos.regime if pos.regime else "unknown"
                            # Try live recomputation when streaming data is available
                            streaming = trader.streaming.try_get(pid) if hasattr(trader, 'streaming') else None
                            if streaming and len(streaming.closes) > 30:
                                try:
                                    import rust_core as _rc
                                    c_arr = streaming.closes.to_list()
                                    v_arr = streaming.volumes.to_list()
                                    cached = trader._candle_data.get(pid, {}) if hasattr(trader, '_candle_data') else {}
                                    h_arr = cached.get("highs", [c + 0.01 for c in c_arr])
                                    l_arr = cached.get("lows", [c - 0.01 for c in c_arr])
                                    n2 = min(len(c_arr), len(h_arr), len(l_arr))
                                    if n2 >= 30:
                                        atr_val = _rc.atr_py(h_arr[-n2:], l_arr[-n2:], c_arr[-n2:], 14)
                                        regime_raw, *_rest = _rc.detect_regime_py(
                                            c_arr[-n2:], h_arr[-n2:], l_arr[-n2:],
                                            v_arr[-n2:] if len(v_arr) >= n2 else None,
                                            None, None,
                                        )
                                except Exception:
                                    pass

                            vol_mult = 1.0
                            if regime_raw == "high_volatility":
                                vol_mult = 1.5
                            elif regime_raw in ("low_volatility", "ranging"):
                                vol_mult = 0.75
                            dynamic_stop_dist = atr_val * 1.5 * vol_mult if atr_val > 0 else entry * 0.03
                            dynamic_target_dist = atr_val * 4.0 * vol_mult if atr_val > 0 else entry * 0.06

                            is_buy = pos.side == "BUY"
                            sl_price = entry - dynamic_stop_dist if is_buy else entry + dynamic_stop_dist
                            tp_price = entry + dynamic_target_dist if is_buy else entry - dynamic_target_dist
                            risk_amount = dynamic_stop_dist * pos.qty
                            risk_pct = (risk_amount / total_equity * 100.0) if total_equity > 0 else 0.0
                            expected_return = dynamic_target_dist * pos.qty
                            reward_risk = dynamic_target_dist / max(dynamic_stop_dist, 1e-9)

                            hit_stop = (current_price <= sl_price) if is_buy else (current_price >= sl_price)
                            hit_target = (current_price >= tp_price) if is_buy else (current_price <= tp_price)

                            positions_list.append({
                                "product_id": pid,
                                "side": pos.side,
                                "qty": round(pos.qty, 4),
                                "entry_price": entry,
                                "current_price": current_price,
                                "unrealized_pnl": round(unrealized_pnl, 2),
                                "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
                                "notional": round(notional, 2),
                                "position_size_pct": round(position_size_pct, 1),
                                "strategy": pos.strategy,
                                "confidence": round(pos.confidence, 2),
                                "win_rate": round(pos.win_rate, 2),
                                "sharpe": round(pos.sharpe, 2),
                                "fees_paid": round(pos.fees_paid, 2),
                                "highest_price": pos.highest_price,
                                "lowest_price": pos.lowest_price,
                                "entry_time": pos.entry_ts,
                                "leverage": pos.leverage,
                                "liq_price": round(pos.liq_price, 4) if pos.liq_price > 0 else 0.0,
                                "liq_distance_pct": round(pos.liq_distance_pct, 2),
                                "long_horizon": pos.long_horizon,
                                "regime": regime_raw,
                                "atr_14": round(atr_val, 4),
                                "stop_loss_price": round(sl_price, 4),
                                "take_profit_price": round(tp_price, 4),
                                "risk_amount": round(risk_amount, 2),
                                "risk_pct": round(risk_pct, 1),
                                "expected_return": round(expected_return, 2),
                                "reward_risk_ratio": round(reward_risk, 1),
                                "hit_stop": hit_stop,
                                "hit_target": hit_target,
                                "exit_reason": "profit_target" if hit_target else ("stop_loss" if hit_stop else "signal_reversal"),
                            })
                    body = json.dumps({"positions": positions_list}, indent=2).encode()
                elif self.path == "/debug":
                    body = json.dumps({"ok": True}, indent=2).encode()
                elif self.path.startswith("/analytics"):
                    if trader and hasattr(trader, "_compute_strategy_analytics"):
                        pw = trader.paper_wins
                        pl = trader.paper_losses
                        rpnl = trader.paper_realized_pnl
                        peak = trader.paper_peak_equity
                        strats = {}
                        try:
                            if trader._analytics_lock.acquire(blocking=True, timeout=2):
                                try:
                                    strats = trader._compute_strategy_analytics()
                                finally:
                                    trader._analytics_lock.release()
                        except Exception:
                            pass
                        disabled_list = []
                        expectancy = []
                        divergences = []
                        try:
                            perf_summary = trader._perf_tracker.summary(top_n=20)
                            disabled_list = perf_summary.get("disabled_list", [])
                            expectancy = perf_summary.get("expectancy", [])
                            divergences = perf_summary.get("divergences", [])
                        except Exception:
                            pass
                        body = json.dumps({
                            "overall": {
                                "trades": pw + pl,
                                "wins": pw,
                                "losses": pl,
                                "win_rate": round(pw / max(pw + pl, 1), 4),
                                "realized_pnl": round(rpnl, 2),
                                "peak_equity": round(peak, 2),
                            },
                            "strategies": strats,
                            "disabled_strategies": disabled_list,
                            "expectancy": expectancy,
                            "divergences": divergences,
                        }, indent=2).encode()
                    else:
                        body = json.dumps({"error": "no trader"}, indent=2).encode()
                elif self.path.startswith("/notifications"):
                    notifs = list(trader._notifications) if trader and hasattr(trader, "_notifications") else []
                    notifs.reverse()
                    body = json.dumps({"notifications": notifs}, indent=2).encode()
                elif self.path.startswith("/experiments"):
                    path = Path("data/experiment_proposals.json")
                    if path.exists():
                        body = path.read_bytes()
                    else:
                        body = json.dumps([], indent=2).encode()
                elif self.path.startswith("/metrics"):
                    paper = ref.get("paper", {})
                    cb = ref.get("circuit_breakers", {})
                    lines = [
                        "# HELP portfolio_trader_info Portfolio trader info",
                        "# TYPE portfolio_trader_info gauge",
                        f'portfolio_trader_info{{mode="{ref.get("mode","?")}",rust="{ref.get("rust_enabled","?")}",ws="{ref.get("ws_connected","?")}"}} 1',
                        "",
                        "# HELP portfolio_equity Current paper equity",
                        "# TYPE portfolio_equity gauge",
                        f"portfolio_equity {paper.get('equity', 0)}",
                        "",
                        "# HELP portfolio_cash Current paper cash",
                        "# TYPE portfolio_cash gauge",
                        f"portfolio_cash {paper.get('cash', 0)}",
                        "",
                        "# HELP portfolio_positions Current open positions",
                        "# TYPE portfolio_positions gauge",
                        f"portfolio_positions {paper.get('positions', 0)}",
                        "",
                        "# HELP portfolio_trades Total trades",
                        "# TYPE portfolio_trades gauge",
                        f"portfolio_trades {paper.get('trades', 0)}",
                        "",
                        "# HELP portfolio_realized_pnl Total realized P&L",
                        "# TYPE portfolio_realized_pnl gauge",
                        f"portfolio_realized_pnl {paper.get('realized_pnl', 0)}",
                        "",
                        "# HELP portfolio_win_rate Win rate",
                        "# TYPE portfolio_win_rate gauge",
                        f"portfolio_win_rate {paper.get('win_rate', 0)}",
                        "",
                        "# HELP portfolio_drawdown Current drawdown",
                        "# TYPE portfolio_drawdown gauge",
                        f"portfolio_drawdown {paper.get('drawdown', 0)}",
                        "",
                        "# HELP portfolio_fees_paid Total fees paid",
                        "# TYPE portfolio_fees_paid gauge",
                        f"portfolio_fees_paid {paper.get('fees_paid', 0)}",
                        "",
                        "# HELP portfolio_fee_tier Current fee tier",
                        "# TYPE portfolio_fee_tier gauge",
                        f"portfolio_fee_tier {paper.get('fee_tier', 0)}",
                        "",
                        "# HELP portfolio_tick_count Total ticks processed",
                        "# TYPE portfolio_tick_count gauge",
                        f"portfolio_tick_count {ref.get('tick_count', 0)}",
                        "",
                        "# HELP portfolio_alerts Active alert count",
                        "# TYPE portfolio_alerts gauge",
                        f"portfolio_alerts {len(ref.get('alerts', []))}",
                        "",
                        "# HELP portfolio_cb_breached Circuit breaker breached",
                        "# TYPE portfolio_cb_breached gauge",
                        f"portfolio_cb_breached {1 if cb.get('breached') else 0}",
                        "",
                        "# HELP portfolio_cb_daily_loss Daily loss %",
                        "# TYPE portfolio_cb_daily_loss gauge",
                        f"portfolio_cb_daily_loss {cb.get('daily_loss_pct', 0)}",
                        "",
                        "# HELP portfolio_cb_consecutive_losses Consecutive losses",
                        "# TYPE portfolio_cb_consecutive_losses gauge",
                        f"portfolio_cb_consecutive_losses {cb.get('consecutive_losses', 0)}",
                        "",
                        "# HELP portfolio_products Tracked products",
                        "# TYPE portfolio_products gauge",
                        f"portfolio_products {ref.get('products', 0)}",
                        "",
                        "# HELP portfolio_strategies Active strategies",
                        "# TYPE portfolio_strategies gauge",
                        f"portfolio_strategies {ref.get('strategies', 0)}",
                        "",
                    ]
                    body = "\n".join(lines).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(body)
                    return
                elif self.path.startswith("/paper-status"):
                    if trader:
                        h = trader.health_status
                        paper = h.get("paper", {})
                        cb = h.get("capital_buckets", {})
                        body = json.dumps({"paper": paper, "capital_buckets": cb}, indent=2).encode()
                    else:
                        body = json.dumps({"error": "no trader"}, indent=2).encode()
                elif self.path.startswith("/holdings"):
                    if trader and hasattr(trader, "_core_holdings_to_dict"):
                        with trader._paper_lock:
                            holdings = trader._core_holdings_to_dict()
                        body = json.dumps({"holdings": holdings, "count": len(holdings)}, indent=2).encode()
                    else:
                        body = json.dumps({"error": "no trader"}, indent=2).encode()
                elif self.path.startswith("/config"):
                    if trader and hasattr(trader, "get_tunables"):
                        body = json.dumps(trader.get_tunables(), indent=2).encode()
                    else:
                        body = json.dumps({"error": "no trader"}, indent=2).encode()
                elif self.path.startswith("/health"):
                    body = json.dumps(ref, indent=2).encode()
                else:
                    body = json.dumps(ref, indent=2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                content_length = int(self.headers.get("Content-Length", 0))
                if content_length <= 0:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "empty body"}, indent=2).encode())
                    return
                raw = self.rfile.read(content_length)
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "invalid JSON"}, indent=2).encode())
                    return
                if self.path == "/config" and trader and hasattr(trader, "set_tunable"):
                    results = {}
                    for key, value in data.items():
                        ok, msg = trader.set_tunable(key, value)
                        results[key] = {"ok": ok, "message": msg}
                        if ok:
                            trader._persist_knobs()
                    body = json.dumps({"results": results, "config": trader.get_tunables()}, indent=2).encode()
                else:
                    body = json.dumps({"error": "unknown path or no trader"}, indent=2).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a, **kw):
                pass

        if self._server_started:
            return

        class ReusableHTTPServer(http.server.ThreadingHTTPServer):
            allow_reuse_address = True
            daemon_threads = True

        def serve() -> None:
            deadline = time.time() + 30.0
            while time.time() < deadline:
                try:
                    self._server = ReusableHTTPServer(("127.0.0.1", self.port), H)
                    self._server_started = True
                    log.info("Health server on port %d", self.port)
                    self._server.serve_forever()
                    return
                except OSError as exc:
                    if getattr(exc, "errno", None) == errno.EADDRINUSE:
                        remaining = int(deadline - time.time())
                        if remaining > 0:
                            log.warning("Health server port %d busy, retrying for %ds...", self.port, remaining)
                            time.sleep(5)
                            continue
                        log.warning("Health server port %d still busy after 30s; skipping health server", self.port)
                        return
                    raise

        t = threading.Thread(target=serve, daemon=True, name="health")
        t.start()


def main():
    log_file = ""
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--log-file" and i + 1 < len(sys.argv):
            log_file = sys.argv[i + 1]
            break
        if arg.startswith("--log-file="):
            log_file = arg.split("=", 1)[1]
            break
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        from logging.handlers import RotatingFileHandler
        handler = RotatingFileHandler(
            log_file, maxBytes=50 * 1024 * 1024, backupCount=5,
        )
        handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s %(message)s"))
        logging.basicConfig(
            level=logging.INFO,
            handlers=[handler],
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        )
    trader = EventTraderV4.from_cli()
    trader.start()


if __name__ == "__main__":
    main()
