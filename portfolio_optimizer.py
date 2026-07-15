#!/usr/bin/env python3
"""
Portfolio Optimizer Daemon

Continuously monitors the Coinbase portfolio and executes trades that improve
the user's position across four dimensions:
  1. Tax-loss harvesting     – sell positions at a loss for tax savings
  2. Fee-tier optimization   – generate volume to reach lower Coinbase fees
  3. Rebalancing             – maintain target allocation (safe/growth/spec)
  4. Strategy signals        – execute alpha-generating trades from 40+ strategies

Usage:
    python3 portfolio_optimizer.py                    # Dry-run mode (no real trades)
    python3 portfolio_optimizer.py --live             # Live execution
    python3 portfolio_optimizer.py --interval 300     # Check every 5 minutes
    python3 portfolio_optimizer.py --min-value 100    # Ignore positions under $100
"""

import argparse
import fcntl
import json
import logging
import math
import os
import random
import sys
import time
import uuid
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("optimizer")

_IO_EXECUTOR = ThreadPoolExecutor(max_workers=12, thread_name_prefix="opt_io")
_BT_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="opt_bt")

# Strategy engine
from strategy_engine import run_strategies as _run_strategies
from strategy_engine import batch_signals_fast as _batch_signals_fast
from strategy_engine import Signal as StrategySignal
from strategy_engine import backtest_strategy as _backtest_strategy
from strategy_engine import BacktestVerdict
from strategy_engine import batch_backtest_rust as _batch_backtest_rust
from strategy_engine import FundingRateContrarian as _FundingRateContrarian
from strategy_engine import OrderFlowCVD as _OrderFlowCVD
from strategy_engine import WickPressureFlow as _WickPressureFlow
from strategy_engine import ExchangeNetflowSignal as _ExchangeNetflowSignal
from strategy_engine import StablecoinFlowSignal as _StablecoinFlowSignal

# State store
from state_store import StateStore
try:
    from neo4j_store import Neo4jStore
except ImportError:
    Neo4jStore = None  # type: ignore

# Notification (optional)
from notification import TradeNotifier

# Event market connectors (optional)
try:
    from event_markets.comparison_engine import ComparisonEngine as _CE, format_signal as _fs
    ComparisonEngine = _CE
    format_signal = _fs
    _HAS_PM_COMPARISON = True
except ImportError:
    ComparisonEngine = None
    format_signal = None
    _HAS_PM_COMPARISON = False

try:
    from event_markets.arbitrage import EventArbitrageScanner as _EAS, format_arbitrage as _fa
    EventArbitrageScanner = _EAS
    format_arbitrage = _fa
    _HAS_PM_ARBITRAGE = True
except ImportError:
    EventArbitrageScanner = None
    format_arbitrage = None
    _HAS_PM_ARBITRAGE = False

# Confidence matrix (strategy signal aggregation)
from confidence_matrix import ConfidenceMatrix, format_aggregated

# Confidence engine (signal modifiers: liquidity, spread, regime, sentiment, consensus)
try:
    from trading_system.signal_confidence import ConfidenceEngine
    _HAS_CONFIDENCE_ENGINE = True
except ImportError:
    ConfidenceEngine = None
    _HAS_CONFIDENCE_ENGINE = False
    logger.warning("ConfidenceEngine not available (trading_system.signal_confidence)")

try:
    from trading_system.core.performance_model import latency_tuned_priority as _latency_tuned_priority
except Exception:
    def _latency_tuned_priority(base_priority: float, **_: Any) -> float:
        return base_priority

try:
    from coinbase.src.multi_hop import (
        RouteContext as MultiHopContext,
        RoutePlan as MultiHopRoutePlan,
        RouteStep as MultiHopRouteStep,
        find_best_decision as _find_best_route_decision,
    )
    _HAS_MULTI_HOP = True
except Exception:
    MultiHopContext = None  # type: ignore
    MultiHopRoutePlan = None  # type: ignore
    MultiHopRouteStep = None  # type: ignore
    _find_best_route_decision = None  # type: ignore
    _HAS_MULTI_HOP = False

try:
    from coinbase.src.graph.neo4j_graph import CryptoGraphStore
    from coinbase.src.graph.models import GraphAssetSignal
    _HAS_COINBASE_GRAPH = True
except Exception:
    CryptoGraphStore = None  # type: ignore
    GraphAssetSignal = None  # type: ignore
    _HAS_COINBASE_GRAPH = False

# Regime detection for confidence modifiers
try:
    from multi_strategy_paper_trading import detect_regime as _detect_regime
except ImportError:
    def _detect_regime(data): return "neutral"

# Cross-asset macro regime engine (DXY, yields, VIX, gold → risk gate)
try:
    from coinbase.src.cross_asset_regime import CrossAssetRegimeEngine as _CrossAssetRegimeEngine
    _HAS_CROSS_ASSET_REGIME = True
except ImportError:
    _CrossAssetRegimeEngine = None  # type: ignore
    _HAS_CROSS_ASSET_REGIME = False

# Macro risk engine (composite macro risk score)
try:
    from coinbase.src.sentiment import MacroRiskEngine as _MacroRiskEngine
    _HAS_MACRO_RISK = True
except ImportError:
    _MacroRiskEngine = None  # type: ignore
    _HAS_MACRO_RISK = False

# Bayesian signal ensemble / meta learning
try:
    from coinbase.src.ensemble import BayesianSignalBlender as _BayesianSignalBlender
    _HAS_BAYESIAN_ENSEMBLE = True
except ImportError:
    _BayesianSignalBlender = None  # type: ignore
    _HAS_BAYESIAN_ENSEMBLE = False

# Order flow / microstructure engine
try:
    from coinbase.src.sentiment.order_flow import OrderFlowEngine as _OrderFlowEngine
    _HAS_ORDER_FLOW = True
except ImportError:
    _OrderFlowEngine = None  # type: ignore
    _HAS_ORDER_FLOW = False

# Smart money flow / CVD microstructure strategy
try:
    from coinbase.src.strat_orderflow import SmartMoneyFlowStrategy as _SmartMoneyFlowStrategy
    _HAS_SMART_MONEY_FLOW = True
except ImportError:
    _SmartMoneyFlowStrategy = None  # type: ignore
    _HAS_SMART_MONEY_FLOW = False

# Walk-forward parameter optimization
try:
    from archive.coinbase_src.walk_forward import WalkForwardOptimizer as _WalkForwardOptimizer
    from archive.coinbase_src.walk_forward import ParamRange as _ParamRange
    _HAS_WALK_FORWARD = True
except ImportError:
    _WalkForwardOptimizer = None  # type: ignore
    _ParamRange = None  # type: ignore
    _HAS_WALK_FORWARD = False

# Execution engine — bracket orders with stop-loss / take-profit
try:
    from coinbase.src.execution_v2 import (
        NativeExecutionEngine as _NativeExecutionEngine,
        BracketManager as _BracketManager,
        OrderIntent as _OrderIntent,
        OrderType as _OrderType,
        OrderResult as _OrderResult,
        OrderStatus as _OrderStatus,
    )
    from coinbase.src.cb_client import CBClient as _CBClient
    _HAS_EXECUTION_ENGINE = True
except Exception:
    _NativeExecutionEngine = None  # type: ignore
    _BracketManager = None  # type: ignore
    _OrderIntent = None  # type: ignore
    _OrderType = None  # type: ignore
    _OrderResult = None  # type: ignore
    _OrderStatus = None  # type: ignore
    _CBClient = None  # type: ignore
    _HAS_EXECUTION_ENGINE = False


def _compute_adx(highs: List[float], lows: List[float], closes: List[float], period: int = 14) -> float:
    """Compute ADX (Average Directional Index) for regime detection.
    Returns ADX value (0-100). >25 = trending, <20 = ranging."""
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return 20.0  # default to neutral
    
    try:
        plus_dm = []
        minus_dm = []
        tr_list = []
        
        for i in range(1, len(highs)):
            high_diff = highs[i] - highs[i-1]
            low_diff = lows[i-1] - lows[i]
            
            plus_dm.append(high_diff if high_diff > low_diff and high_diff > 0 else 0.0)
            minus_dm.append(low_diff if low_diff > high_diff and low_diff > 0 else 0.0)
            
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            tr_list.append(tr)
        
        # Wilder's smoothing
        def wilder_smooth(values, period):
            if len(values) < period:
                return sum(values) / len(values) if values else 0.0
            result = sum(values[:period]) / period
            for v in values[period:]:
                result = result + (v - result) / period
            return result
        
        plus_di = 100 * wilder_smooth(plus_dm, period) / wilder_smooth(tr_list, period) if wilder_smooth(tr_list, period) > 0 else 0
        minus_di = 100 * wilder_smooth(minus_dm, period) / wilder_smooth(tr_list, period) if wilder_smooth(tr_list, period) > 0 else 0
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) > 0 else 0
        
        # ADX is smoothed DX - simplified here
        return dx
    except Exception:
        return 20.0


def _detect_market_regime(highs: List[float], lows: List[float], closes: List[float]) -> str:
    """Detect market regime using ADX and volatility.
    Returns: 'trending', 'ranging', 'volatile', or 'quiet'"""
    if len(closes) < 30:
        return "neutral"
    
    # Compute ADX
    adx = _compute_adx(highs, lows, closes, 14)
    
    # Compute recent volatility (20-period)
    recent_closes = closes[-20:]
    returns = [(recent_closes[i] - recent_closes[i-1]) / recent_closes[i-1] for i in range(1, len(recent_closes))]
    volatility = (sum(r*r for r in returns) / len(returns)) ** 0.5 if returns else 0
    
    # Classify regime
    if adx > 25:
        return "trending"
    elif adx < 20:
        if volatility > 0.03:  # 3% daily vol
            return "volatile"
        return "ranging"
    else:
        return "neutral"


# Strategy groups by regime suitability
TREND_STRATEGIES = {
    "ema_cross", "macd", "adx", "trix", "psar", "hma", "aroon", 
    "ichimoku", "dmi_cross", "supertrend", "vortex", "coppock",
    "kama", "dmi_cross", "supertrend", "fisher"
}

MEAN_REVERSION_STRATEGIES = {
    "rsi_revert", "boll_break", "zscore_revert", "vwap_revert", 
    "williams_r", "cmo", "stoch", "rsi_fail", "mean_reversion",
    "gap_revert", "de_marker", "ultimate_osc", "fisher"
}

VOLATILITY_STRATEGIES = {
    "keltner", "donchian", "bb_squeeze", "atr_channel", "std_channel",
    "vol_prof", "liq_vac", "vcp", "choppiness", "mass_idx", "range_exp_idx"
}

EXTERNAL_STRATEGIES = {
    "kalman_mr", "hp_trend", "funding_contrarian", "exchange_flow", "btc_dxy_corr",
    "kalshi", "polymarket"
}


try:
    from market_universe import DEFAULT_STOCK_WATCHLIST as _DEFAULT_STOCK_WATCHLIST
except Exception:
    _DEFAULT_STOCK_WATCHLIST = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "SPY", "QQQ", "VTI", "IWM", "XLK", "XLF", "XLE"]

try:
    from data.fetch_unified import UnifiedMarketDataAdapter
except Exception:
    UnifiedMarketDataAdapter = None

# Unified signal accumulator (optional — merges news, PM, arb, divergence, strategy signals)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "graph-alpha-bot", "app", "strategies"))
_HAS_ACCUMULATOR = False
try:
    from unified_signal_accumulator import UnifiedSignalAccumulator
    _HAS_ACCUMULATOR = True
except Exception:
    UnifiedSignalAccumulator = None

# Signal aggregator (universe-wide cross-product ranking)
_HAS_AGGREGATOR = False
try:
    from trading_system.core.signal_aggregator import SignalAggregator, UnifiedSignal
    _HAS_AGGREGATOR = True
except Exception:
    SignalAggregator = None
    UnifiedSignal = None

# Smart feed refresh manager (tiered data freshness)
_HAS_SMART_FEED = False
try:
    from coinbase.src.smart_feed import SmartFeedRefreshManager
    from coinbase.src.rest_feed import fetch_candles_batch_sync
    _HAS_SMART_FEED = True
except Exception:
    SmartFeedRefreshManager = None
    fetch_candles_batch_sync = None

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

USDC_BENCHMARK_APY = 3.5

# Target capital buckets: keep half in USDC, keep a core long-term sleeve,
# and deploy the remaining capital into higher-risk opportunities.
PORTFOLIO_BUCKET_TARGETS = {
    "reserve": 0.50,
    "core": 0.20,
    "opportunity": 0.30,
}

USDC_YIELD_RESERVE_FRACTION = PORTFOLIO_BUCKET_TARGETS["reserve"]

CORE_LONG_TERM_ASSETS = {"BTC", "ETH", "SOL"}
CORE_BATCH_FRACTION = 0.05
OPPORTUNITY_BATCH_FRACTION = 0.03

DEFAULT_CAPITAL_POLICY = {
    "targets": dict(PORTFOLIO_BUCKET_TARGETS),
    "core_allowlist": ["BTC", "ETH", "SOL"],
    "static_holdings": ["BTC", "ETH"],
    "core_min_allocation_pct": 10.0,
    "core_batch_fraction": CORE_BATCH_FRACTION,
    "opportunity_batch_fraction": OPPORTUNITY_BATCH_FRACTION,
}

TARGET_ALLOCATION = {"safe": 0.75, "growth": 0.20, "speculative": 0.05}

SAFE_ASSETS = {"BTC", "ETH", "USDC", "USDT", "DAI"}
GROWTH_ASSETS = {
    "SOL", "XRP", "ADA", "DOGE", "AVAX", "DOT", "LINK", "UNI",
    "POL", "ATOM", "LTC", "BCH", "NEAR", "APT", "SUI", "ARB",
    "OP", "FIL", "INJ", "SEI", "TIA",
}
SPECULATIVE_ASSETS = {
    "ALGO", "XLM", "STX", "HBAR", "ICP", "GRT",
    "SHIB", "PEPE", "BONK", "TRUMP", "FLOKI",
}

COINBASE_FEE_TIERS = [
    (0, 0.0060, 0.0120),
    (1_000, 0.0035, 0.0075),
    (10_000, 0.0025, 0.0040),
    (50_000, 0.0015, 0.0025),
    (100_000, 0.0010, 0.0020),
    (1_000_000, 0.0008, 0.0018),
    (20_000_000, 0.0005, 0.0015),
]

# Minimum time between executions of the same type (seconds)
OP_COOLDOWN = {"tlh": 86400, "fee_tier": 3600, "rebalance": 43200, "rebalance_bot": 3600, "stairstep": 60, "strategy": 300, "cycle": 600, "accumulator": 120, "aggregator": 300, "funding_onchain": 600, "order_flow": 600}

# Fee tier volume cycling
CYCLE_MIN_PROFIT_PCT = 0.0   # we'll break even or small loss for volume
CYCLE_MAX_HOLD_HOURS = 168   # force-close after 7 days

# ---------------------------------------------------------------------------
# Support / Resistance detection
# ---------------------------------------------------------------------------

@dataclass
class _SwingPoint:
    index: int
    price: float
    kind: str  # "high" or "low"

@dataclass
class _SRLevel:
    price: float
    kind: str  # "support" or "resistance"
    strength: float = 1.0


def _detect_swing_points(highs: List[float], lows: List[float],
                          lookback: int = 10) -> List[_SwingPoint]:
    """Identify swing highs and lows in price series."""
    swings = []
    for i in range(lookback, min(len(highs), len(lows)) - lookback):
        if all(highs[i] > highs[j] for j in range(i - lookback, i)) and \
           all(highs[i] > highs[j] for j in range(i + 1, i + lookback + 1)):
            swings.append(_SwingPoint(i, highs[i], "high"))
        if all(lows[i] < lows[j] for j in range(i - lookback, i)) and \
           all(lows[i] < lows[j] for j in range(i + 1, i + lookback + 1)):
            swings.append(_SwingPoint(i, lows[i], "low"))
    return swings


def _oldest_first_candles(candles: List[Any]) -> List[Any]:
    """Normalize a raw candle list to oldest-first order regardless of source."""
    if not candles:
        return candles

    def _ts(c):
        if isinstance(c, dict):
            return to_float(c.get("start", 0))
        if isinstance(c, (list, tuple)) and len(c) >= 1:
            return to_float(c[0])
        return 0.0

    try:
        if _ts(candles[0]) > _ts(candles[-1]):
            return list(reversed(candles))
    except Exception:
        pass
    return list(candles)


def _rsi_14(closes: List[float]) -> float:
    """Standard 14-period RSI from a close series (50.0 when undefined)."""
    if len(closes) < 15:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    window = deltas[-14:]
    gains = [d for d in window if d > 0]
    losses = [-d for d in window if d < 0]
    avg_g = sum(gains) / 14.0
    avg_l = sum(losses) / 14.0
    if avg_l == 0:
        return 100.0 if avg_g > 0 else 50.0
    rs = avg_g / avg_l
    return 100.0 - 100.0 / (1.0 + rs)


def _build_sr_levels(highs: List[float], lows: List[float],
                      closes: List[float],
                      min_touches: int = 2) -> List[_SRLevel]:
    """Cluster swing points into support/resistance levels."""
    swings = _detect_swing_points(highs, lows)
    if not swings or len(closes) < 20:
        return []
    price_range = max(closes[-20:]) - min(closes[-20:])
    tolerance = price_range / max(max(closes[-20:]), 1e-9) * 0.02 if price_range > 0 else 0.01

    grouped: List[tuple] = []
    for sw in swings:
        merged = False
        for i, (price, kind, group) in enumerate(grouped):
            if kind == sw.kind and abs(price - sw.price) / max(price, 1e-9) < tolerance:
                group.append(sw)
                new_price = sum(s.price for s in group) / len(group)
                grouped[i] = (new_price, kind, group)
                merged = True
                break
        if not merged:
            grouped.append((sw.price, sw.kind, [sw]))

    levels = []
    for price, kind, group in grouped:
        touches = len(group)
        if touches >= min_touches:
            levels.append(_SRLevel(
                price=price,
                kind="support" if kind == "low" else "resistance",
                strength=touches,
            ))
    return levels


def _estimate_atr(closes: List[float], highs: List[float],
                   lows: List[float], period: int = 14) -> float:
    """Simple ATR estimate from OHLC data."""
    if len(closes) < period + 1:
        return 0.0
    tr_vals = []
    for i in range(1, min(period + 1, len(closes))):
        tr = max(highs[-i] - lows[-i],
                 abs(highs[-i] - closes[-i - 1]),
                 abs(lows[-i] - closes[-i - 1]))
        tr_vals.append(tr)
    return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class OpportunityType(Enum):
    TLH = "tlh"
    FEE_TIER_VOLUME = "fee_tier"
    REBALANCE = "rebalance"
    REBALANCE_BOT = "rebalance_bot"
    STAIRSTEP = "stairstep"
    STRATEGY_SIGNAL = "strategy"
    STOCK_SIGNAL = "stock_signal"
    NEW_LISTING_MOMENTUM = "new_listing_momentum"
    VOLUME_CYCLE = "cycle"
    EVENT_MARKET = "event_market"
    EVENT_ARBITRAGE = "event_arbitrage"
    ACCUMULATOR_SIGNAL = "accumulator"


@dataclass
class Opportunity:
    opp_type: OpportunityType
    currency: str
    side: str                           # "BUY" | "SELL"
    size_usd: float
    reason: str
    priority: float = 0.0               # 0-1, higher = more urgent
    product_id: str = ""
    expected_fee: float = 0.0
    preview_passed: bool = False
    executed: bool = False
    order_id: str = ""
    entry_price_est: float = 0.0        # Estimated entry price
    stop_loss_pct: float = 0.0          # Stop loss % from entry (positive = away from entry)
    take_profit_pct: float = 0.0        # Take profit % from entry
    holding_period_hours: float = 0.0   # Estimated holding period in hours
    expected_return_pct: float = 0.0    # Expected return %
    risk_pct: float = 0.0               # Risk % (distance to stop / entry)
    meta: Dict[str, Any] = field(default_factory=dict)  # Arbitrary extra data


@dataclass
class PortfolioState:
    holdings: Dict[str, Dict]           # currency -> {balance, held, price, value, classification, ...}
    total_value: float = 0.0
    usdc_balance: float = 0.0
    fee_volume_30d: float = 0.0
    fee_tier: Tuple = (0, 0.006, 0.012)
    volume_to_next_tier: float = 0.0
    timestamp: str = ""


# ---------------------------------------------------------------------------
# CLI Wrapper
# ---------------------------------------------------------------------------

class CoinbaseCLI:
    """Thin wrapper around the Coinbase CLI for all API interactions."""

    def __init__(self, environment: str = "live", timeout: int = 30):
        self.environment = environment
        self.timeout = timeout
        self._products: Dict[str, dict] = {}
        self._verify()

    def _verify(self):
        import subprocess
        subprocess.run(["coinbase", "--version"], capture_output=True, check=True, timeout=5)

    def _run(self, cmd, parse_json=True):
        import subprocess
        full = cmd + ["-e", self.environment]
        result = subprocess.run(full, capture_output=True, text=True, timeout=self.timeout)
        if result.returncode != 0:
            raise RuntimeError(result.stderr or result.stdout)
        if parse_json:
            return json.loads(result.stdout)
        return result.stdout.strip()

    def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None):
        url = f"https://api.exchange.coinbase.com{path}"
        if params:
            query = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items() if v is not None)
            if query:
                url = f"{url}?{query}"
        req = urllib.request.Request(url, headers={"User-Agent": "PortfolioOptimizer/1.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode())

    def get_products(self) -> Dict[str, dict]:
        if self._products:
            return self._products
        try:
            raw = self._run(["coinbase", "products", "list", "product_type==SPOT"])
        except Exception:
            try:
                raw = self._public_get("/products")
            except Exception as exc:
                logger.warning("Coinbase public product discovery failed: %s", exc)
                raw = []
        products_list = raw.get("products", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [raw])
        self._products = {p.get("product_id", ""): p for p in products_list if p.get("product_id")}
        return self._products

    def get_product(self, product_id: str) -> Optional[dict]:
        return self.get_products().get(product_id)

    def best_product(self, currency: str, side: str) -> Optional[str]:
        """Return the most appropriate product_id for a trade.

        BUY  → use USDC pair (we hold USDC)
        SELL → use USD pair (standard)
        """
        products = self.get_products()
        usdc_pair = f"{currency}-USDC"
        usd_pair = f"{currency}-USD"
        if side == "BUY" and usdc_pair in products:
            return usdc_pair
        if usd_pair in products:
            return usd_pair
        if usdc_pair in products:
            return usdc_pair
        return None

    def get_price(self, product_id):
        try:
            return self._run(["coinbase", "products", "get", product_id])
        except RuntimeError:
            return {}

    def get_balances(self):
        raw = self._run(["coinbase", "balance"])
        return raw if isinstance(raw, list) else raw.get("accounts", [])

    def get_fees(self):
        return self._run(["coinbase", "fees"])

    def get_fills(self, product_id=None):
        cmd = ["coinbase", "orders", "fills"]
        if product_id:
            cmd.insert(3, f"product_id=={product_id}")
        raw = self._run(cmd)
        if isinstance(raw, dict):
            return raw.get("fills", [])
        return raw if isinstance(raw, list) else [raw]

    def _round_quote(self, product_id: str, amount: float) -> float:
        prod = self.get_product(product_id)
        if prod:
            inc = to_float(prod.get("quote_increment", "0.01"))
            if inc > 0:
                return round(round(amount / inc) * inc, 12)
        return round(amount, 2)

    def _round_base(self, product_id: str, qty: float) -> float:
        prod = self.get_product(product_id)
        if prod:
            inc = to_float(prod.get("base_increment", "0.00000001"))
            if inc > 0:
                return round(qty / inc) * inc
        return qty

    def preview_order(self, product_id, side, amount, is_quote=True):
        try:
            cmd = [
                "coinbase", "orders", "preview",
                f"product_id={product_id}", f"side={side}",
                "type=market",
            ]
            if is_quote:
                cmd.append(f"quote_size={self._round_quote(product_id, amount)}")
            else:
                cmd.append(f"base_size={self._round_base(product_id, amount)}")
            raw = self._run(cmd)
            # Normalize field names from CLI response
            raw["total_fee"] = to_float(raw.get("commission_total", 0))
            raw["total_cost"] = to_float(raw.get("order_total", raw.get("quote_size", 0)))
            return raw
        except RuntimeError as e:
            logger.debug("Preview failed for %s %s $%.0f: %s", side, product_id, amount, e)
            return None

    def create_order(self, product_id, side, amount, is_quote=True):
        cid = str(uuid.uuid4())
        cmd = [
            "coinbase", "orders", "create",
            f"product_id={product_id}", f"side={side}",
            "type=market", f"client_order_id={cid}",
        ]
        if is_quote:
            cmd.append(f"quote_size={self._round_quote(product_id, amount)}")
        else:
            cmd.append(f"base_size={self._round_base(product_id, amount)}")
        try:
            return self._run(cmd)
        except RuntimeError as e:
            logger.error("Order failed: %s", e)
            return None

    def get_order(self, order_id):
        return self._run(["coinbase", "orders", "get", order_id])

    def get_candles(self, product_id: str, granularity: str = "1h", limit: int = 100) -> List[dict]:
        try:
            raw = self._run([
                "coinbase", "products", "candles", product_id,
                f"granularity=={granularity}",
            ])
            result = raw.get("candles", raw) if isinstance(raw, dict) else raw
            if isinstance(result, list):
                return result[:limit]
            return []
        except RuntimeError:
            try:
                from data.fetch_multi_source import MultiSourceDataFetcher
                fetcher = MultiSourceDataFetcher()
                start = datetime.now(timezone.utc) - timedelta(days=max(7, min(365, limit)))
                candles = fetcher.fetch_coinbase(product_id, granularity="hour" if granularity.endswith("h") else "day", start_date=start, end_date=datetime.now(timezone.utc))
                normalized = []
                for c in candles[-limit:]:
                    normalized.append({
                        "start": c.get("start", c.get("ts", 0)),
                        "open": c.get("open", 0),
                        "high": c.get("high", 0),
                        "low": c.get("low", 0),
                        "close": c.get("close", 0),
                        "volume": c.get("volume", 0),
                    })
                return normalized
            except Exception:
                return []


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _fmt_base(v: float) -> str:
    return f"{float(v):.8f}".rstrip("0").rstrip(".") or "0"

def _fmt_quote(v: float) -> str:
    return f"{float(v):.2f}".rstrip("0").rstrip(".") or "0"

def to_float(v) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v))
    except (ValueError, TypeError):
        return 0.0


def classify_asset(currency: str) -> str:
    c = currency.upper().replace("-USD", "")
    if c in SAFE_ASSETS:
        return "safe"
    if c in GROWTH_ASSETS:
        return "growth"
    if c in SPECULATIVE_ASSETS:
        return "speculative"
    return "speculative"


def current_fee_tier(volume_30d: float):
    for min_vol, maker, taker in reversed(COINBASE_FEE_TIERS):
        if volume_30d >= min_vol:
            return (min_vol, maker, taker)
    return COINBASE_FEE_TIERS[0]


def volume_to_next(volume_30d: float) -> float:
    current_min = 0
    for min_vol, _, _ in COINBASE_FEE_TIERS:
        if volume_30d >= min_vol:
            current_min = min_vol
        elif min_vol > current_min:
            return max(0.0, min_vol - volume_30d)
    return 0.0


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ---------------------------------------------------------------------------
# Portfolio Optimizer
# ---------------------------------------------------------------------------

class PortfolioOptimizer:
    """Continuously monitors and improves the Coinbase portfolio."""

    def __init__(
        self,
        environment: str = "live",
        interval: int = 60,
        min_value: float = 10.0,
        max_deployable_usd: Optional[float] = None,
        dry_run: bool = True,
        db_path: str = "optimizer_state.db",
        neo4j_uri: str = "",
        neo4j_user: str = "neo4j",
        neo4j_password: str = "",
        neo4j_db: str = "trading",
        require_approval: bool = False,
        smtp_user: str = "",
        smtp_password: str = "",
        from_addr: str = "",
        to_addr: str = "",
        approval_base_url: str = "http://localhost:8080",
        pending_file: str = "data/pending_approvals.json",
        enable_polymarket: bool = False,
        kalshi_email: str = "",
        kalshi_password: str = "",
    ):
        # Process exclusion lock — prevent duplicate optimizer instances
        os.makedirs("data", exist_ok=True)
        self._lock_fd = None
        try:
            self._lock_fd = os.open("data/optimizer.lock", os.O_CREAT | os.O_RDWR, 0o644)
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, BlockingIOError):
            logger.error("Another optimizer instance is running (data/optimizer.lock is held)")
            sys.exit(1)

        self.cli = CoinbaseCLI(environment)
        self.interval = interval
        self.min_value = min_value
        env_cap = os.getenv("MAX_DEPLOYABLE_USD", "").strip()
        if max_deployable_usd is not None:
            self._forced_max_deployable_usd = max(0.0, float(max_deployable_usd))
        elif env_cap:
            self._forced_max_deployable_usd = max(0.0, float(env_cap))
        else:
            self._forced_max_deployable_usd = 0.0
        self.dry_run = dry_run
        self.require_approval = require_approval
        self.pending_file = pending_file

        # State
        self.state: Optional[PortfolioState] = None
        self.cost_bases: Dict[str, float] = {}
        self.last_execution: Dict[str, float] = defaultdict(float)
        self.position_ages: Dict[str, float] = defaultdict(float)
        self.trade_log: List[dict] = []
        self.running = False
        self._bt_cache: Dict[str, BacktestVerdict] = {}
        self._bt_cache_ttl: float = 3600
        self._portfolio_peak_value: float = 0.0
        self._seen_products_meta_prefix = "coinbase_first_seen:"
        self.graph_store: Optional[Any] = None
        self._graph_signals: Dict[str, Any] = {}
        self._graph_cache_ts: float = 0.0
        self._graph_cache_ttl: float = 3600

        # Local SQLite store
        self.store = StateStore(db_path)
        self.capital_policy: Dict[str, Any] = dict(DEFAULT_CAPITAL_POLICY)

        # Optional Neo4j store (system of record)
        self.neo4j_store: Optional[Neo4jStore] = None
        if neo4j_uri:
            try:
                self.neo4j_store = Neo4jStore(
                    uri=neo4j_uri,
                    user=neo4j_user,
                    password=neo4j_password,
                    database=neo4j_db,
                )
                logger.info("Connected to Neo4j at %s (db=%s)", neo4j_uri, neo4j_db)
            except Exception as e:
                logger.warning("Neo4j connection failed: %s — falling back to SQLite only", e)

        if _HAS_COINBASE_GRAPH and neo4j_uri:
            try:
                self.graph_store = CryptoGraphStore(
                    uri=neo4j_uri,
                    user=neo4j_user,
                    password=neo4j_password,
                    database=neo4j_db,
                )
                logger.info("Connected to CoinGecko graph store at %s (db=%s)", neo4j_uri, neo4j_db)
            except Exception as e:
                logger.warning("CoinGecko graph store failed: %s", e)

        # Optional email notifier
        self.notifier: Optional[TradeNotifier] = None
        if require_approval and smtp_user and smtp_password:
            try:
                self.notifier = TradeNotifier(
                    smtp_user=smtp_user,
                    smtp_password=smtp_password,
                    from_addr=from_addr or smtp_user,
                    to_addr=to_addr or smtp_user,
                    approval_base_url=approval_base_url,
                )
                logger.info("Email notifications enabled (to: %s)", to_addr or smtp_user)
            except Exception as e:
                logger.warning("Notification setup failed: %s", e)

        # Unified prediction market client (primary source)
        self._pm_client: Optional[Any] = None
        try:
            from event_markets.unified_client import UnifiedPredictionMarketClient
            self._pm_client = UnifiedPredictionMarketClient(
                kalshi_email=kalshi_email or "",
                kalshi_password=kalshi_password or "",
            )
            logger.info("Unified prediction market client enabled")
        except Exception as e:
            logger.warning("Unified PM client setup failed: %s", e)

        # Smart feed refresh manager — shared tiered cache across optimizer + trader
        self._feed_mgr: Optional[SmartFeedRefreshManager] = None
        if _HAS_SMART_FEED:
            try:
                self._feed_mgr = SmartFeedRefreshManager(
                    fetch_fn=fetch_candles_batch_sync,
                    interval=5.0,
                )
                self._feed_mgr.set_critical([
                    f"{a}-USD" for a in CORE_LONG_TERM_ASSETS
                ])
                self._feed_mgr.start()
                logger.info("SmartFeed refresh manager started")
            except Exception as e:
                logger.warning("SmartFeed initialization failed: %s", e)
                self._feed_mgr = None

        self._arb_scanner: Optional[Any] = None
        if self._pm_client:
            try:
                self._arb_scanner = EventArbitrageScanner(self._pm_client)
                logger.info("Event arbitrage scanner enabled")
            except Exception as e:
                logger.warning("Event arbitrage scanner setup failed: %s", e)

        # Optional legacy event market comparison engine (fallback)
        self.event_engine: Optional[ComparisonEngine] = None
        if not self._pm_client and (enable_polymarket or kalshi_email):
            try:
                from event_markets.polymarket_client import PolymarketClient
                from event_markets.kalshi_client import KalshiClient
                from event_markets.comparison_engine import ComparisonEngine
                pm = PolymarketClient() if enable_polymarket else None
                kc = None
                if kalshi_email and kalshi_password:
                    kc = KalshiClient(email=kalshi_email, password=kalshi_password)
                elif os.getenv("KALSHI_API_KEY_ID") and os.getenv("KALSHI_PRIVATE_KEY_PATH"):
                    kc = KalshiClient(
                        api_key_id=os.getenv("KALSHI_API_KEY_ID", ""),
                        private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH", ""),
                        base_url=os.getenv("KALSHI_API_BASE_URL", ""),
                    )
                self.event_engine = ComparisonEngine(polymarket=pm, kalshi=kc)
                logger.info("Legacy event market engine enabled (polymarket=%s, kalshi=%s)",
                             bool(pm), bool(kc))
            except Exception as e:
                logger.warning("Legacy event market setup failed: %s", e)

        # Knowledge gap analyzer (detects info asymmetries in prediction markets)
        self._knowledge_gap: Optional[Any] = None
        if self._pm_client:
            try:
                from event_markets.knowledge_gap import KnowledgeGapAnalyzer
                self._knowledge_gap = KnowledgeGapAnalyzer(
                    enable_web_search=True,
                    enable_news_search=True,
                    min_gap=0.10,
                    min_evidence=2,
                )
                logger.info("Knowledge gap analyzer enabled")
            except Exception as e:
                logger.warning("Knowledge gap analyzer setup failed: %s", e)

        # Funding rate contrarian — global Binance funding signal
        self._funding_contrarian = _FundingRateContrarian(min_abs_funding_bps=0.1)

        # Candle-based order-flow strategies (deterministic, no external data)
        self._order_flow_cvd = _OrderFlowCVD(lookback=30, divergence_bars=6, min_conf=0.35)
        self._wick_pressure = _WickPressureFlow(lookback=20, threshold=0.12, min_conf=0.35)

        # On-chain exchange-netflow signal (CoinGecko, injectable fetch)
        self._exchange_netflow = _ExchangeNetflowSignal(cache_ttl=600.0, trend_window=24)

        # On-chain stablecoin supply-flow macro gauge (BTC risk appetite)
        self._stablecoin_flow = _StablecoinFlowSignal(cache_ttl=900.0, trend_window=30)

        # Rust rebalancer / stair-step bots — "set an allocation and let it run"
        self.rebalance_preset = os.getenv("REBALANCE_PRESET", "core_balanced")
        self.rebalance_drift_threshold = float(os.getenv("REBALANCE_DRIFT", "0.05"))
        self.rebalance_profit_take_pct = float(os.getenv("REBALANCE_PROFIT_TAKE", "0.25"))
        self.rebalance_min_notional = float(os.getenv("REBALANCE_MIN_NOTIONAL", "10.0"))
        self.stairstep_enabled = str(os.getenv("STAIRSTEP_ENABLED", "true")).lower() in (
            "1", "true", "yes", "y", "on")
        raw_syms = os.getenv("STAIRSTEP_SYMBOLS", "XRP-USD,XLM-USD,MON-USD")
        self._stairstep_symbols = [s.strip() for s in raw_syms.split(",") if s.strip()]
        self._rebalance_bot: Optional[Any] = None
        self._stairstep_engine: Optional[Any] = None

        # On-chain flow analysis — CoinGecko exchange flow per-product signals
        self._onchain_flow: Optional[Any] = None
        try:
            from coinbase.src.strategies.onchain_flows import OnChainFlowStrategy
            self._onchain_flow = OnChainFlowStrategy(
                cache_ttl=300,
                volume_spike_threshold=3.0,
                min_confidence=0.30,
            )
            logger.info("OnChainFlowStrategy enabled")
        except Exception as e:
            logger.debug("OnChainFlowStrategy init failed: %s", e)

        # Cross-asset macro regime engine — gates risk based on DXY, yields, VIX, gold
        self._cross_asset_regime: Optional[_CrossAssetRegimeEngine] = None
        if _HAS_CROSS_ASSET_REGIME:
            try:
                self._cross_asset_regime = _CrossAssetRegimeEngine(cache_ttl_s=300, lookback=90)
                logger.info("CrossAssetRegimeEngine enabled")
            except Exception as e:
                logger.debug("CrossAssetRegimeEngine init failed: %s", e)

        # Macro risk engine — composite macro risk score
        self._macro_risk: Optional[_MacroRiskEngine] = None
        if _HAS_MACRO_RISK:
            try:
                self._macro_risk = _MacroRiskEngine(cache_ttl=600)
                logger.info("MacroRiskEngine enabled")
            except Exception as e:
                logger.debug("MacroRiskEngine init failed: %s", e)

        # Bayesian signal ensemble — tracks strategy win rates, blends signals
        self._ensemble_blender: Optional[_BayesianSignalBlender] = None
        if _HAS_BAYESIAN_ENSEMBLE:
            try:
                self._ensemble_blender = _BayesianSignalBlender(
                    prior_alpha=1.0, prior_beta=1.0, decay_half_life=50,
                )
                logger.info("BayesianSignalBlender enabled")
            except Exception as e:
                logger.debug("BayesianSignalBlender init failed: %s", e)

        # Meta-learning signal source performance tracker
        self._meta_signal_performance: Dict[str, Dict[str, float]] = {}
        self._meta_source_weights: Dict[str, float] = {}

        # Order flow / microstructure engine
        self._order_flow_engine: Optional[_OrderFlowEngine] = None
        if _HAS_ORDER_FLOW:
            try:
                self._order_flow_engine = _OrderFlowEngine(window=100, eval_interval=10.0)
                logger.info("OrderFlowEngine enabled")
            except Exception as e:
                logger.debug("OrderFlowEngine init failed: %s", e)

        # Smart money flow / CVD microstructure strategy
        self._smart_money_flow: Optional[_SmartMoneyFlowStrategy] = None
        if _HAS_SMART_MONEY_FLOW:
            try:
                self._smart_money_flow = _SmartMoneyFlowStrategy(
                    cvd_lookback=14, absorption_vol_mult=2.0,
                )
                logger.info("SmartMoneyFlowStrategy enabled")
            except Exception as e:
                logger.debug("SmartMoneyFlowStrategy init failed: %s", e)

        # Wash-sale tracking — 30-day cooldown per sold currency
        self._wash_sale_cooldown: Dict[str, float] = {}

        # Parameter optimization state
        self._param_opt_ranges: Dict[str, List[_ParamRange]] = {}
        self._param_opt_results: Dict[str, Dict[str, Any]] = {}
        self._last_param_opt_ts: float = 0.0
        self._param_opt_interval: float = 86400.0 * 7  # weekly

        # Optionally instantiate the ConfidenceEngine (signal modifiers)
        self.confidence_engine = None
        if _HAS_CONFIDENCE_ENGINE:
            try:
                self.confidence_engine = ConfidenceEngine(
                    liquidity_tiers={},
                    win_rates={},
                    regime_caps={"volatile": 0.4, "quiet": 0.6, "trending": 1.0, "neutral": 0.8},
                )
                logger.info("ConfidenceEngine enabled")
            except Exception as e:
                logger.warning("ConfidenceEngine init failed: %s", e)

        self._load_from_store()
        self._load_graph_universe()
        self._refresh_capital_policy()

        # Health server state (exposed via /health)
        self._tick_count = 0
        self._last_tick_ts = 0.0
        self._health_alerts: List[str] = []
        self._start_ts = time.time()
        self._health_server: Optional[Any] = None
        health_port = int(os.getenv("OPTIMIZER_HEALTH_PORT", "0") or 0)
        if health_port:
            try:
                from coinbase.src.health_server import HealthServer, build_optimizer_status
                self._health_server = HealthServer(
                    health_port, lambda: build_optimizer_status(self), name="optimizer"
                )
                self._health_server.start()
            except Exception as e:
                logger.warning("Optimizer health server failed: %s", e)

        # Correlation clusters for portfolio risk management
        self._correlation_clusters = {
            "btc_eth": {"BTC", "ETH"},
            "l1_solana": {"SOL", "NEAR", "APT", "SUI", "SEI"},
            "l1_eth_competitors": {"AVAX", "DOT", "POL", "ADA", "ATOM"},
            "defi": {"UNI", "LINK", "AAVE", "CRV", "MKR"},
            "meme": {"DOGE", "SHIB", "PEPE", "BONK", "TRUMP", "FLOKI"},
            "layer2": {"ARB", "OP", "BASE"},
            "oracles": {"LINK", "PYTH", "TRB"},
            "storage": {"FIL", "STORJ", "AR"},
        }
        self._max_cluster_exposure_pct = 0.30  # Max 30% per correlation cluster
        
        # Signal pulse tracking (for signal quality filtering)
        self._signal_pulses: Dict[str, Dict] = {}
        self._pulse_window_s = 300.0  # 5 minutes
        self._min_pulse_count = 2  # Minimum pulses before considering signal valid
        self._max_flip_count = 2  # Max direction flips before signal is noise
        # Last detected opportunities (used as route-planning context).
        # Initialised here so routing helpers are safe before the first tick.
        self._last_detected_opportunities: List["Opportunity"] = []

        # Execution engine — bracket order placement with stop-loss / take-profit
        self._exec_engine: Optional[Any] = None
        self._bracket_mgr: Optional[Any] = None
        self._bracket_state_path: str = "data/optimizer_brackets.json"
        self._init_execution_engine()

    def _load_from_store(self):
        """Restore state from Neo4j (if available) or SQLite."""
        source_store = self.neo4j_store or self.store
        self.trade_log = source_store.load_trades(limit=500)
        cached = source_store.load_bt_cache(ttl=self._bt_cache_ttl)
        for key, data in cached.items():
            self._bt_cache[key] = BacktestVerdict(**data)
        ages = source_store.load_position_ages()
        self.position_ages.update(ages)
        if self.trade_log:
            logger.info("Restored %d trades, %d BT cache entries, %d position ages from %s",
                         len(self.trade_log), len(cached), len(ages),
                         "Neo4j" if self.neo4j_store else "SQLite")
        # Sync Neo4j data into SQLite for fast local reads
        if self.neo4j_store:
            for trade in self.trade_log:
                self.store.save_trade(trade)
            for key, data in cached.items():
                verdict = BacktestVerdict(**data)
                self.store.save_bt_cache(key, verdict)
            self.store.save_position_ages(dict(self.position_ages))

    def _save_state(self):
        """Persist current state to SQLite and optionally Neo4j."""
        if self.state:
            self.store.save_snapshot(self.state)
        self.store.save_position_ages(dict(self.position_ages))
        self._save_capital_policy()
        self.store.prune_bt_cache()
        if self.neo4j_store:
            try:
                if self.state:
                    self.neo4j_store.save_snapshot(self.state)
                self.neo4j_store.save_position_ages(dict(self.position_ages))
                self.neo4j_store.prune_bt_cache()
            except Exception as e:
                logger.warning("Neo4j save failed: %s", e)

    def _init_execution_engine(self):
        if not _HAS_EXECUTION_ENGINE:
            logger.warning("Execution engine not available – falling back to direct CLI orders")
            return
        try:
            env = self.cli.environment
            cb = _CBClient(environment=env)
            self._exec_engine = _NativeExecutionEngine(cb, dry_run=self.dry_run)
            self._bracket_mgr = _BracketManager(self._exec_engine)
            self._restore_brackets()
            logger.info("Execution engine initialised (dry_run=%s)", self.dry_run)
        except Exception as e:
            logger.warning("Execution engine init failed: %s", e)
            self._exec_engine = None
            self._bracket_mgr = None

    def _restore_brackets(self):
        if not self._bracket_mgr:
            return
        try:
            if os.path.exists(self._bracket_state_path):
                with open(self._bracket_state_path) as f:
                    saved = json.load(f)
                for bid, b in saved.items():
                    self._bracket_mgr._brackets[bid] = b
                logger.info("Restored %d bracket(s) from %s", len(saved), self._bracket_state_path)
        except Exception as e:
            logger.debug("Bracket state restore failed: %s", e)

    def _save_brackets(self):
        if not self._bracket_mgr:
            return
        try:
            os.makedirs("data", exist_ok=True)
            with open(self._bracket_state_path, "w") as f:
                json.dump(self._bracket_mgr._brackets, f, indent=2, default=str)
        except Exception as e:
            logger.debug("Bracket state save failed: %s", e)

    def _poll_brackets(self):
        if not self._bracket_mgr:
            return
        if self.dry_run:
            return
        try:
            brackets = self._bracket_mgr.active_brackets()
            if not brackets:
                return
            for bid, b in list(brackets.items()):
                if b.get("status") != "OPEN":
                    continue
                pid = b.get("product_id", "")
                if not pid:
                    continue
                try:
                    resp = self.cli.best_bid_ask(pid)
                    if isinstance(resp, dict) and "bids" in resp and "asks" in resp:
                        mid = (float(resp["bids"][0][0]) + float(resp["asks"][0][0])) / 2.0
                    elif isinstance(resp, list) and len(resp) > 0:
                        ticker = resp[0] if isinstance(resp[0], dict) else resp
                        mid = float(ticker.get("price", 0))
                    else:
                        continue
                    if mid <= 0:
                        continue
                    initial_stop_dist = b.get("initial_stop_dist", 0)
                    side = b.get("side", "BUY")
                    r_multiple = (mid - float(b.get("entry_price", mid))) / max(initial_stop_dist, 0.001)
                    if side.upper() == "SELL":
                        r_multiple = -r_multiple
                    age_s = time.time() - float(b.get("created_at", time.time()))
                    max_hold_s = 86400  # 24h default for optimizer brackets
                    self._bracket_mgr.update_trailing_stop(
                        bid, mid, mid, mid,
                        initial_stop_dist, r_multiple, max_hold_s, age_s,
                    )
                    self._bracket_mgr.update_trailing_take_profit(
                        bid, mid, mid, mid,
                        initial_stop_dist, r_multiple, max_hold_s, age_s,
                    )
                except Exception as e:
                    logger.debug("Bracket %s poll error: %s", bid, e)
            self._save_brackets()
        except Exception as e:
            logger.debug("Bracket poll failed: %s", e)

    def _load_graph_universe(self, *, limit: int = 5000, only_coinbase: bool = True) -> None:
        if not _HAS_COINBASE_GRAPH:
            return
        if self.graph_store is None:
            return
        try:
            assets = self.graph_store.top_graph_assets(limit=limit, only_coinbase=only_coinbase)
            self._graph_signals = {
                f"{sig.product_id}".upper(): sig
                for sig in assets
                if getattr(sig, "product_id", None)
            }
            self._graph_cache_ts = time.time()
            logger.info("Loaded %d graph assets into optimizer cache", len(self._graph_signals))
        except Exception as e:
            logger.warning("Failed to load graph universe: %s", e)

    def _graph_signal_for_product(self, product_id: str) -> Optional[Any]:
        if not product_id:
            return None
        pid = str(product_id).upper()
        cached = self._graph_signals.get(pid)
        if cached is not None:
            return cached
        if not self.graph_store:
            return None
        if self._graph_cache_ts and time.time() - self._graph_cache_ts > self._graph_cache_ttl:
            self._graph_signals.clear()
        try:
            signal = self.graph_store.asset_signal(product_id)
            self._graph_signals[pid] = signal
            self._graph_cache_ts = time.time()
            return signal
        except Exception:
            return None

    def _graph_score_for_product(self, product_id: str) -> float:
        signal = self._graph_signal_for_product(product_id)
        if signal is None:
            return 0.5
        return max(0.0, min(1.0, float(getattr(signal, "graph_score", 0.5) or 0.5)))

    def _graph_multiplier_for_product(self, product_id: str, *, max_boost: float = 0.25) -> float:
        score = self._graph_score_for_product(product_id)
        return max(1.0 - max_boost, min(1.0 + max_boost, 1.0 + (score - 0.5) * (2.0 * max_boost)))

    # ── Pulse tracking for signal quality filtering ──────────────────
    #  (The live implementations of _pulse_key / _record_pulse /
    #   _prune_pulses / _is_pulse_quality_sufficient are defined further
    #   below; _find_best_route_decision / _route_decision_from_payload
    #   are also class methods defined later.)

    def _get_cluster_for_currency(self, currency: str) -> Optional[str]:
        """Get correlation cluster for a currency."""
        c = currency.upper().replace("-USD", "")
        for cluster_name, assets in self._correlation_clusters.items():
            if c in assets:
                return cluster_name
        return None

    def _cluster_exposure_pct(self, cluster: str) -> float:
        """Calculate current portfolio exposure to a correlation cluster."""
        if not self.state:
            return 0.0
        cluster_assets = self._correlation_clusters.get(cluster, set())
        total_value = self.state.total_value
        if total_value <= 0:
            return 0.0
        cluster_value = sum(
            h.get("value", 0) for h in self.state.holdings.values()
            if h.get("currency", "").upper().replace("-USD", "") in cluster_assets
        )
        return cluster_value / total_value

    def _check_cluster_limit(self, currency: str, additional_usd: float) -> bool:
        """Check if adding a position would exceed cluster exposure limits."""
        cluster = self._get_cluster_for_currency(currency)
        if not cluster:
            return True
        if not self.state:
            return True
        current_pct = self._cluster_exposure_pct(cluster)
        new_pct = current_pct + (additional_usd / max(self.state.total_value, 1))
        return new_pct <= self._max_cluster_exposure_pct

    def _normalize_capital_policy(self, policy: Optional[dict] = None) -> dict:
        raw = dict(DEFAULT_CAPITAL_POLICY)
        if policy:
            raw.update({k: v for k, v in policy.items() if v is not None})
        targets = dict(DEFAULT_CAPITAL_POLICY["targets"])
        targets.update((raw.get("targets") or {}))
        target_sum = sum(max(float(v), 0.0) for v in targets.values())
        if target_sum <= 0:
            targets = dict(DEFAULT_CAPITAL_POLICY["targets"])
        else:
            targets = {k: max(float(v), 0.0) / target_sum for k, v in targets.items()}
        allowlist = raw.get("core_allowlist") or DEFAULT_CAPITAL_POLICY["core_allowlist"]
        if isinstance(allowlist, str):
            allowlist = [x.strip() for x in allowlist.split(",") if x.strip()]
        allowlist = [str(x).upper().replace("-USD", "") for x in allowlist if str(x).strip()]
        if not allowlist:
            allowlist = list(DEFAULT_CAPITAL_POLICY["core_allowlist"])
        static_holdings = raw.get("static_holdings") or DEFAULT_CAPITAL_POLICY["static_holdings"]
        if isinstance(static_holdings, str):
            static_holdings = [x.strip() for x in static_holdings.split(",") if x.strip()]
        static_holdings = [str(x).upper().replace("-USD", "") for x in static_holdings if str(x).strip()]
        if not static_holdings:
            static_holdings = list(DEFAULT_CAPITAL_POLICY["static_holdings"])
        max_deployable = raw.get("max_deployable_usd", 0.0)
        try:
            max_deployable = max(0.0, float(max_deployable or 0.0))
        except Exception:
            max_deployable = 0.0
        return {
            "targets": targets,
            "core_allowlist": allowlist,
            "static_holdings": static_holdings,
            "core_min_allocation_pct": max(float(raw.get("core_min_allocation_pct", DEFAULT_CAPITAL_POLICY["core_min_allocation_pct"])), 0.0),
            "core_batch_fraction": _clamp(float(raw.get("core_batch_fraction", DEFAULT_CAPITAL_POLICY["core_batch_fraction"])), 0.0, 0.5),
            "opportunity_batch_fraction": _clamp(float(raw.get("opportunity_batch_fraction", DEFAULT_CAPITAL_POLICY["opportunity_batch_fraction"])), 0.0, 0.5),
            "max_deployable_usd": max_deployable,
            "live_test_started_at": str(raw.get("live_test_started_at", "") or ""),
            "updated_at": raw.get("updated_at", ""),
        }

    def _refresh_capital_policy(self):
        try:
            raw = self.store.get_meta("capital_policy")
            policy = json.loads(raw) if raw else {}
        except Exception:
            policy = {}
        self.capital_policy = self._normalize_capital_policy(policy)
        if self._forced_max_deployable_usd > 0:
            cap_changed = float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0) != self._forced_max_deployable_usd
            started_at = str(self.capital_policy.get("live_test_started_at", "") or "")
            if cap_changed or not started_at:
                self.capital_policy["max_deployable_usd"] = self._forced_max_deployable_usd
                self.capital_policy["live_test_started_at"] = datetime.now(timezone.utc).isoformat()
                self._save_capital_policy()
        return self.capital_policy

    def _apply_bear_market_policy(self):
        """Shift allocation toward core crypto as the portfolio draws down."""

        if not self.state or self.state.total_value <= 0:
            return

        self._portfolio_peak_value = max(self._portfolio_peak_value, self.state.total_value)
        if self._portfolio_peak_value <= 0:
            return

        drawdown = max(0.0, (self._portfolio_peak_value - self.state.total_value) / self._portfolio_peak_value)
        if drawdown < 0.05:
            return

        if drawdown < 0.15:
            targets = {"reserve": 0.45, "core": 0.25, "opportunity": 0.30}
            core_min = 15.0
        elif drawdown < 0.30:
            targets = {"reserve": 0.35, "core": 0.35, "opportunity": 0.30}
            core_min = 25.0
        else:
            targets = {"reserve": 0.25, "core": 0.45, "opportunity": 0.30}
            core_min = 35.0

        # If BTC is still trending down, be a little more aggressive about accumulation.
        btc = self.state.holdings.get("BTC", {})
        btc_change = to_float(btc.get("change_24h", 0))
        if btc_change < 0:
            targets["reserve"] = max(0.20, targets["reserve"] - 0.05)
            targets["core"] = min(0.55, targets["core"] + 0.05)

        policy = dict(self.capital_policy)
        policy["targets"] = targets
        policy["core_min_allocation_pct"] = core_min
        policy["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.capital_policy = self._normalize_capital_policy(policy)
        logger.info(
            "Bear-market overlay: drawdown=%.1f%% peak=$%.0f current=$%.0f targets=%s",
            drawdown * 100.0,
            self._portfolio_peak_value,
            self.state.total_value,
            self.capital_policy.get("targets"),
        )

    def _save_capital_policy(self):
        try:
            payload = json.dumps(self._normalize_capital_policy(self.capital_policy), default=str)
            self.store.set_meta("capital_policy", payload)
        except Exception as e:
            logger.debug("Capital policy save failed: %s", e)

    def update_capital_policy(self, policy: dict):
        self.capital_policy = self._normalize_capital_policy(policy)
        self.capital_policy["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._save_capital_policy()
        return self.capital_policy

    def _first_seen_age_days(self, product_id: str) -> Optional[float]:
        raw = self.store.get_meta(self._seen_products_meta_prefix + product_id)
        if not raw:
            now = datetime.now(timezone.utc)
            self.store.set_meta(self._seen_products_meta_prefix + product_id, now.isoformat())
            return 0.0
        try:
            dt = datetime.fromisoformat(raw)
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 86400)
        except Exception:
            now = datetime.now(timezone.utc)
            self.store.set_meta(self._seen_products_meta_prefix + product_id, now.isoformat())
            return 0.0

    def _kelly_size(
        self,
        win_rate: float,
        avg_win_pct: float,
        avg_loss_pct: float,
        confidence: float,
        *,
        kelly_fraction: float = 0.25,
        max_notional: float = 5000.0,
        min_notional: float = 50.0,
        capital_limit: Optional[float] = None,
    ) -> float:
        """Kelly Criterion position sizing based on backtest statistics.
        
        Kelly % = (win_rate * avg_win - (1-win_rate) * avg_loss) / avg_win
        Then scaled by confidence and capped at kelly_fraction of bankroll.
        """
        if not self.state:
            return min_notional
        
        if win_rate <= 0 or win_rate >= 1:
            win_rate = 0.5
        if avg_win_pct <= 0:
            avg_win_pct = 2.0
        if avg_loss_pct <= 0:
            avg_loss_pct = 1.5
            
        win_rate = _clamp(win_rate, 0.01, 0.99)
        
        kelly_pct = (win_rate * avg_win_pct - (1 - win_rate) * avg_loss_pct) / avg_win_pct
        kelly_pct = max(kelly_pct, 0.0)
        
        effective_kelly = kelly_pct * kelly_fraction * _clamp(confidence, 0.0, 1.0)
        effective_kelly = _clamp(effective_kelly, 0.0, 0.10)
        
        base = self._deployable_capital() * effective_kelly
        lo = min_notional
        hi = max_notional
        if capital_limit is not None:
            base = min(base, max(0.0, capital_limit))
            # Never exceed an explicitly-provided capital limit, even if it is
            # smaller than the minimum-notional floor (prevents over-deployment).
            lo = min(min_notional, capital_limit)
            hi = min(max_notional, capital_limit)
        return _clamp(base, lo, hi)

    def _regime_strategy_weight(self, strategy: str, regime: str) -> float:
        """Return weight multiplier for a strategy given current market regime.
        
        Boosts strategies suited for the regime, penalizes unsuitable ones.
        """
        if regime == "trending":
            trend_boost = {"ema_cross": 1.5, "macd": 1.4, "trix": 1.3, "adx": 1.5, 
                          "psar": 1.3, "hma": 1.3, "aroon": 1.2, "ichimoku": 1.3,
                          "dmi_cross": 1.4, "supertrend": 1.5, "vortex": 1.3, "coppock": 1.2,
                          "kama": 1.2, "dpo": 1.1, "elder_ray": 1.2}
            mr_penalty = {"rsi_revert": 0.5, "zscore_revert": 0.5, "vwap_revert": 0.5,
                         "cmo": 0.6, "williams_r": 0.6, "stoch": 0.6, "rsi_fail": 0.5,
                         "mean_reversion": 0.5, "gap_revert": 0.5, "de_marker": 0.6,
                         "ultimate_osc": 0.6, "fisher": 0.6}
            return trend_boost.get(strategy, mr_penalty.get(strategy, 1.0))
        
        elif regime == "ranging":
            mr_boost = {"rsi_revert": 1.5, "zscore_revert": 1.4, "vwap_revert": 1.4,
                       "cmo": 1.3, "williams_r": 1.3, "stoch": 1.2, "rsi_fail": 1.3,
                       "mean_reversion": 1.4, "gap_revert": 1.3, "de_marker": 1.2,
                       "ultimate_osc": 1.2, "fisher": 1.2}
            trend_penalty = {"ema_cross": 0.5, "macd": 0.5, "trix": 0.5, "adx": 0.5,
                            "psar": 0.5, "hma": 0.6, "aroon": 0.6, "ichimoku": 0.6,
                            "dmi_cross": 0.5, "supertrend": 0.5, "vortex": 0.6, "coppock": 0.6,
                            "kama": 0.6, "dpo": 0.7, "elder_ray": 0.6}
            return mr_boost.get(strategy, trend_penalty.get(strategy, 1.0))
        
        elif regime == "volatile":
            vol_boost = {"boll_break": 1.4, "keltner": 1.3, "donchian": 1.3,
                        "bb_squeeze": 1.5, "atr_channel": 1.3, "std_channel": 1.3,
                        "vol_prof": 1.2, "liq_vac": 1.3, "vcp": 1.2, "choppiness": 1.2,
                        "mass_idx": 1.1, "range_exp_idx": 1.2}
            return vol_boost.get(strategy, 1.0)
        
        elif regime == "quiet":
            return 0.7
        
        return 1.0

    def _pulse_key(self, product_id: str, strategy: str, direction: str) -> str:
        return f"{product_id}:{strategy}:{direction}"

    def _record_pulse(
        self,
        product_id: str,
        strategy: str,
        direction: str,
        confidence: float,
        price: float,
    ) -> Dict[str, Any]:
        """Record a signal pulse for quality tracking."""
        now = time.time()
        key = self._pulse_key(product_id, strategy, direction)
        
        # Simple in-memory pulse tracking
        if not hasattr(self, '_signal_pulses'):
            self._signal_pulses = {}
        
        existing = self._signal_pulses.get(key)
        pulse_window_s = 300.0  # 5 minutes
        
        if existing:
            if now - existing["last_ts"] > pulse_window_s:
                # Reset pulse count if window expired
                existing["pulse_count"] = 1
                existing["first_ts"] = now
                existing["last_ts"] = now
                existing["avg_confidence"] = confidence
                existing["min_price"] = price
                existing["max_price"] = price
                existing["flip_count"] = 0
            else:
                existing["pulse_count"] += 1
                existing["last_ts"] = now
                n = existing["pulse_count"]
                existing["avg_confidence"] = ((existing["avg_confidence"] * (n - 1)) + confidence) / n
                existing["min_price"] = min(existing["min_price"], price)
                existing["max_price"] = max(existing["max_price"], price)
        else:
            self._signal_pulses[key] = {
                "strategy": strategy,
                "direction": direction,
                "product_id": product_id,
                "pulse_count": 1,
                "first_ts": now,
                "last_ts": now,
                "avg_confidence": confidence,
                "min_price": price,
                "max_price": price,
                "flip_count": 0,
            }
            existing = self._signal_pulses[key]
        
        # Track flips (direction changes for same product/strategy)
        opp_key = self._pulse_key(product_id, strategy, "BUY" if direction == "SELL" else "SELL")
        opp = self._signal_pulses.get(opp_key)
        if opp and now - opp["last_ts"] < pulse_window_s:
            existing["flip_count"] += 1
        
        return existing

    def _prune_pulses(self) -> None:
        """Remove stale pulse records."""
        if not hasattr(self, '_signal_pulses'):
            return
        now = time.time()
        pulse_window_s = 300.0
        stale = [k for k, v in self._signal_pulses.items() if now - v["last_ts"] > pulse_window_s * 4]
        for k in stale:
            del self._signal_pulses[k]

    def _is_pulse_valid(self, pulse: Dict) -> bool:
        """Check if a pulse has enough quality to be actionable."""
        if pulse["pulse_count"] < self._min_pulse_count:
            return False
        if pulse["flip_count"] > self._max_flip_count:
            return False
        if pulse["avg_confidence"] < 0.3:
            return False
        return True

    def _is_pulse_quality_sufficient(self, product_id: str, strategy: str, direction: str, min_pulses: int = 2, max_flips: int = 1) -> bool:
        """Check if pulse quality meets minimum threshold."""
        if not hasattr(self, '_signal_pulses'):
            return True  # No tracking = allow
        key = self._pulse_key(product_id, strategy, direction)
        pulse = self._signal_pulses.get(key)
        if not pulse:
            return False
        if pulse["pulse_count"] < min_pulses:
            return False
        if pulse["flip_count"] > max_flips:
            return False
        return True

    def _risk_reward_size(
        self,
        expected_return_pct: float,
        risk_pct: float,
        confidence: float,
        liquidity: float,
        *,
        cap_pct: float = 0.015,
        max_notional: float = 3000.0,
        min_notional: float = 50.0,
        capital_limit: Optional[float] = None,
    ) -> float:
        if not self.state:
            return min_notional
        expected_return_pct = max(expected_return_pct, 0.0)
        risk_pct = max(risk_pct, 0.5)
        rr = expected_return_pct / risk_pct
        rr = _clamp(rr, 0.5, 4.0)
        quality = _clamp(confidence, 0.0, 1.0)
        liq = _clamp(liquidity, 0.1, 1.0)
        base = self._deployable_capital() * cap_pct
        size = base * rr * quality * liq
        if capital_limit is not None:
            size = min(size, max(0.0, capital_limit))
        lo = min_notional
        hi = max_notional
        if capital_limit is not None:
            # Never exceed an explicitly-provided capital limit, even if it is
            # smaller than the minimum-notional floor (prevents over-deployment).
            lo = min(min_notional, capital_limit)
            hi = min(max_notional, capital_limit)
        return _clamp(size, lo, hi)

    def _estimate_trade_volatility_pct(
        self,
        closes: List[float],
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> float:
        """Estimate a trade-friendly volatility percentage from recent candles."""
        if not closes or len(closes) < 3:
            return 30.0

        tr_values = []
        prev_close = closes[0]
        for idx in range(1, len(closes)):
            close = closes[idx]
            high = highs[idx] if highs and idx < len(highs) else close
            low = lows[idx] if lows and idx < len(lows) else close
            tr_values.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
            prev_close = close

        lookback = tr_values[-14:] if len(tr_values) >= 14 else tr_values
        atr = sum(lookback) / max(len(lookback), 1)
        last = closes[-1]
        if last <= 0:
            return 30.0

        atr_pct = (atr / last) * 100.0
        recent_idx = max(0, len(closes) - 24)
        recent_move_pct = abs((closes[-1] / closes[recent_idx]) - 1.0) * 100.0 if closes[recent_idx] else atr_pct

        return _clamp(max(atr_pct * 1.5, recent_move_pct * 0.5, 1.0), 1.0, 20.0)

    def _current_price_for_symbol(self, symbol: str, fallback: float = 0.0) -> float:
        """Fetch a current price if available; otherwise fall back."""
        if not symbol:
            return fallback
        pid = symbol if "-" in symbol else f"{symbol}-USD"
        try:
            price_info = self.cli.get_price(pid)
            price = to_float(price_info.get("price", 0))
            if price > 0:
                return price
        except Exception:
            pass
        if self.state:
            base = symbol.split("-")[0]
            holding = self.state.holdings.get(base, {})
            price = to_float(holding.get("price", 0))
            if price > 0:
                return price
        return fallback

    def _route_market_products(self) -> List[dict]:
        """Return Coinbase products for route planning."""

        try:
            products = self.cli.get_products()
            if isinstance(products, dict):
                return list(products.values())
            if isinstance(products, list):
                return products
        except Exception as e:
            logger.debug("Route product discovery failed: %s", e)
        return []

    def _route_context_for_opportunity(self, opp: Opportunity) -> Optional[Any]:
        if not _HAS_MULTI_HOP or MultiHopContext is None:
            return None

        holdings = {}
        current_prices = {}
        drawdown = 0.0
        if self.state:
            current_prices = {cur: float(h.get("price", 0.0) or 0.0) for cur, h in self.state.holdings.items()}
            holdings = {
                cur: {
                    **h,
                    "holding_days": float(self.position_ages.get(cur, 0) or 0),
                }
                for cur, h in self.state.holdings.items()
            }
            if self._portfolio_peak_value > 0:
                drawdown = max(0.0, (self._portfolio_peak_value - self.state.total_value) / self._portfolio_peak_value)

        # Use currently detected opportunities as context so the planner can
        # prefer routes that align with the strongest nearby edges.
        opps = [
            {
                "currency": o.currency,
                "side": o.side,
                "priority": o.priority,
                "reason": o.reason,
                "product_id": o.product_id,
                "opp_type": o.opp_type.value,
            }
            for o in (self._last_detected_opportunities or [opp])
        ]

        # For sells, prefer routing into the strongest nearby buy opportunities
        # plus the core/stable reserve currencies.
        top_buys = [o.currency for o in self._last_detected_opportunities if o.side == "BUY"]
        targets = []
        if opp.side == "BUY":
            targets = [opp.currency]
        else:
            targets = top_buys[:3] + ["USD", "USDC", "BTC", "ETH"]

        return MultiHopContext(
            amount_in=float(opp.size_usd or 0.0),
            candidate_targets=list(dict.fromkeys([t for t in targets if t])),
            opportunities=opps,
            holdings=holdings,
            current_prices=current_prices,
            drawdown_pct=drawdown,
            regime=str(_detect_regime({"change_pct": self.state.holdings.get("BTC", {}).get("change_24h", 0) if self.state else 0})),
            max_hops=3,
        )

    def _best_route_decision_for_opportunity(self, opp: Opportunity) -> Optional[Any]:
        if not _HAS_MULTI_HOP or _find_best_route_decision is None:
            return None

        ctx = self._route_context_for_opportunity(opp)
        if ctx is None:
            return None

        source = opp.currency if opp.side == "SELL" else ("USDC" if self.state and self.state.usdc_balance > 0 else "USD")
        if opp.side == "BUY":
            targets = [opp.currency]
        else:
            # Let the route engine choose among cash, core, or stronger nearby buys.
            targets = [t for t in ctx.candidate_targets if t != opp.currency]
            if not targets:
                targets = ["USD", "USDC", "BTC", "ETH"]

        products = self._route_market_products()
        if not products:
            return None

        try:
            return _find_best_route_decision(
                source,
                targets,
                products,
                context=ctx,
                max_hops=ctx.max_hops,
            )
        except Exception as e:
            logger.debug("Route decision failed for %s: %s", opp.currency, e)
            return None

    def _route_decision_from_payload(self, payload: Dict[str, Any]) -> Optional[Any]:
        """Reconstruct a multi-hop decision from persisted JSON."""

        if not _HAS_MULTI_HOP or MultiHopRoutePlan is None:
            return None

        try:
            steps = []
            for s in payload.get("steps", []):
                if MultiHopRouteStep is None:
                    continue
                steps.append(MultiHopRouteStep(
                    product_id=str(s.get("product_id", "")),
                    from_currency=str(s.get("from_currency", "")),
                    to_currency=str(s.get("to_currency", "")),
                    direction=str(s.get("direction", "BUY")),
                    price=float(s.get("price", 0.0) or 0.0),
                    effective_rate=float(s.get("effective_rate", 0.0) or 0.0),
                ))
            plan = MultiHopRoutePlan(
                source=str(payload.get("source", "")).upper(),
                target=str(payload.get("target", "")).upper(),
                steps=steps,
                effective_rate=float(payload.get("effective_rate", payload.get("score", 0.0)) or 0.0),
                fee_bps=float(payload.get("fee_bps", 10.0) or 10.0),
                spread_bps=float(payload.get("spread_bps", 5.0) or 5.0),
            )
            return type("Decision", (), {
                "plan": plan,
                "score": float(payload.get("score", 0.0) or 0.0),
                "expected_tax_impact_usd": float(payload.get("expected_tax_impact_usd", 0.0) or 0.0),
                "opportunity_bonus": float(payload.get("opportunity_bonus", 0.0) or 0.0),
                "drawdown_bonus": float(payload.get("drawdown_bonus", 0.0) or 0.0),
                "regime_bonus": float(payload.get("regime_bonus", 0.0) or 0.0),
                "hop_penalty": float(payload.get("hop_penalty", 0.0) or 0.0),
                "liquidity_bonus": float(payload.get("liquidity_bonus", 0.0) or 0.0),
                "factor_breakdown": dict(payload.get("factor_breakdown", {})),
            })()
        except Exception as e:
            logger.debug("Failed to reconstruct route decision: %s", e)
            return None

    def _route_amount_for_source(self, source: str, notional_usd: float) -> float:
        """Convert USD notional to source-currency units when needed."""

        src = str(source or "").upper().replace("-USD", "")
        if src in ("USD", "USDC", "USDT", "DAI", "USD1", "USDS"):
            return float(notional_usd)
        price = self._current_price_for_symbol(src, fallback=0.0)
        if price <= 0:
            return 0.0
        return float(notional_usd) / price

    def _execute_route_decision(self, opp: Opportunity, decision: Any) -> bool:
        """Execute a multi-hop route sequentially."""

        plan = getattr(decision, "plan", None)
        if plan is None or not getattr(plan, "steps", None):
            return False

        source = plan.source.upper().replace("-USD", "")
        amount = self._route_amount_for_source(source, opp.size_usd)
        if amount <= 0:
            logger.warning("  → Route amount unavailable for %s", opp.currency)
            return False

        logger.info("  → Multi-hop route: %s", getattr(plan, "path", [source, opp.currency]))
        logger.info("  → Route score=%.3f details=%s", float(getattr(decision, "score", 0.0)), getattr(decision, "factor_breakdown", {}))

        executed_steps = []
        for idx, step in enumerate(plan.steps):
            step_amount = amount
            if step.direction == "BUY":
                preview = self.cli.preview_order(step.product_id, "BUY", step_amount, is_quote=True)
            else:
                preview = self.cli.preview_order(step.product_id, "SELL", step_amount, is_quote=False)
            if not preview:
                logger.warning("  → Route preview failed on hop %d (%s)", idx + 1, step.product_id)
                return False

            fee = to_float(preview.get("total_fee", 0))
            if fee > max(opp.size_usd * 0.03, 1.0):
                logger.warning("  → Route hop fee too high on %s, aborting", step.product_id)
                return False

            if self.dry_run:
                logger.info(
                    "  → DRY-RUN hop %d/%d: %s %s amount=%.8f fee=$%.2f",
                    idx + 1, len(plan.steps), step.direction, step.product_id, step_amount, fee,
                )
                order_id = f"dry-hop-{idx+1}"
            else:
                if step.direction == "BUY":
                    order = self.cli.create_order(step.product_id, "BUY", step_amount, is_quote=True)
                else:
                    order = self.cli.create_order(step.product_id, "SELL", step_amount, is_quote=False)
                if not order:
                    logger.error("  → Route execution failed on hop %d (%s)", idx + 1, step.product_id)
                    return False
                order_id = order.get("id", f"hop-{idx+1}")

            executed_steps.append({
                "product_id": step.product_id,
                "direction": step.direction,
                "input_amount": step_amount,
                "order_id": order_id,
            })
            amount = max(amount * float(step.effective_rate or 0.0), 0.0)
            if amount <= 0:
                logger.warning("  → Route output collapsed after hop %d (%s)", idx + 1, step.product_id)
                return False

        self.last_execution[f"route:{opp.opp_type.value}"] = time.time()
        opp.meta["route_decision"] = {
            "path": getattr(plan, "path", []),
            "score": float(getattr(decision, "score", 0.0)),
            "factor_breakdown": getattr(decision, "factor_breakdown", {}),
            "steps": executed_steps,
        }
        opp.executed = True
        opp.order_id = executed_steps[-1]["order_id"] if executed_steps else "route"
        return True

    def _detect_sr_levels_for_product(
        self, closes: List[float], highs: List[float], lows: List[float]
    ) -> Tuple[List[_SRLevel], float]:
        """Detect support/resistance levels and ATR for a product.

        Returns (levels, atr_value).
        """
        if len(closes) < 30 or len(highs) < 30 or len(lows) < 30:
            return [], 0.0
        atr = _estimate_atr(closes, highs, lows, 14)
        levels = _build_sr_levels(highs, lows, closes, min_touches=2)
        # Filter to levels within reasonable distance (5 ATR max)
        current = closes[-1] if closes else 0.0
        if current > 0 and atr > 0:
            levels = [L for L in levels if abs(L.price - current) / max(atr, 1e-9) < 5.0]
        return levels, atr

    def _compute_dynamic_stop(
        self,
        entry_price: float,
        side: str,
        atr: float,
        regime: str,
        levels: List[_SRLevel],
        base_stop_pct: float,
    ) -> Tuple[float, float, str]:
        """Compute a volatility-regime-adjusted, S/R-aware stop loss.

        Returns (stop_loss_pct, new_atr_distance, reason_detail).
        """
        if entry_price <= 0 or atr <= 0:
            return base_stop_pct, 0.0, "default"

        # Volatility regime multiplier
        vol_mult = {"volatile": 1.4, "trending": 1.1, "ranging": 0.8, "quiet": 0.7,
                     "neutral": 1.0}.get(regime, 1.0)

        atr_distance = max(base_stop_pct / 100.0, atr / entry_price * vol_mult)
        stop_price = entry_price * (1.0 - atr_distance) if side == "BUY" else entry_price * (1.0 + atr_distance)

        # Snap to nearest S/R level
        sr_adjust = ""
        if levels:
            relevant = [L for L in levels
                        if (L.kind == "support" and side == "BUY" and L.price < entry_price) or
                           (L.kind == "resistance" and side == "SELL" and L.price > entry_price)]
            if relevant:
                nearest = min(relevant, key=lambda L: abs(L.price - stop_price))
                snap_dist = abs(nearest.price - stop_price) / max(entry_price, 1e-9)
                # Only snap if within 0.5 ATR of the level
                if snap_dist < atr / entry_price * 0.5:
                    stop_price = nearest.price * 0.995 if side == "BUY" else nearest.price * 1.005
                    sr_adjust = f"sr_snap({nearest.kind}@{nearest.price:.2f})"

        new_stop_pct = abs(stop_price - entry_price) / max(entry_price, 1e-9) * 100.0
        new_stop_pct = max(new_stop_pct, 0.5)  # minimum 0.5% stop
        new_atr = abs(stop_price - entry_price) / max(atr, 1e-9)

        reason_parts = [f"atr={atr_distance*entry_price:.2f}"]
        if vol_mult != 1.0:
            reason_parts.append(f"vol_regime={regime}(x{vol_mult})")
        if sr_adjust:
            reason_parts.append(sr_adjust)
        reason_detail = " ".join(reason_parts)

        return round(new_stop_pct, 2), round(new_atr, 1), reason_detail

    def _compute_sr_aware_exit_plan(
        self,
        currency: str,
        confidence: float,
        expected_return_pct: float = 0.0,
        *,
        trade_style: str = "momentum",
        volatility_pct: float = 60.0,
        spread_pct: float = 0.0,
        hold_hint_hours: Optional[float] = None,
        side: str = "BUY",
        closes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Dict[str, float]:
        """Compute exit plan with automatic S/R detection and volatility-regime adjustment."""
        sr_levels: List[_SRLevel] = []
        atr_value = 0.0
        regime = _detect_market_regime(
            highs or [], lows or [], closes or []
        ) if closes and highs and lows else "neutral"

        if closes and highs and lows and len(closes) >= 30:
            sr_levels, atr_value = self._detect_sr_levels_for_product(closes, highs, lows)

        entry_price = closes[-1] if closes else 0.0

        return self._compute_exit_plan(
            currency=currency,
            confidence=confidence,
            expected_return_pct=expected_return_pct,
            trade_style=trade_style,
            volatility_pct=volatility_pct,
            spread_pct=spread_pct,
            hold_hint_hours=hold_hint_hours,
            side=side,
            sr_levels=sr_levels if sr_levels else None,
            regime=regime,
            atr_value=atr_value,
            entry_price=entry_price,
        )

    def _compute_exit_plan(
        self,
        currency: str,
        confidence: float,
        expected_return_pct: float = 0.0,
        *,
        trade_style: str = "momentum",
        volatility_pct: float = 60.0,
        spread_pct: float = 0.0,
        hold_hint_hours: Optional[float] = None,
        side: str = "BUY",
        sr_levels: Optional[List[_SRLevel]] = None,
        regime: str = "neutral",
        atr_value: float = 0.0,
        entry_price: float = 0.0,
    ) -> Dict[str, float]:
        """Compute a conservative execution plan for a candidate trade.

        When sr_levels, regime, and atr_value are supplied, the stop loss
        is dynamically adjusted for the volatility regime and snapped to
        nearby support/resistance levels for tighter, more intelligent placement.
        """
        profiles = {
            "momentum": {"stop_mult": 1.00, "rr_min": 1.8, "hold": 36.0, "hold_cap": 120.0, "target_floor": 6.0, "stop_floor": 2.0, "target_cap": 45.0, "spread_mult": 0.35},
            "new_listing": {"stop_mult": 1.20, "rr_min": 2.2, "hold": 18.0, "hold_cap": 72.0, "target_floor": 8.0, "stop_floor": 2.5, "target_cap": 50.0, "spread_mult": 0.45},
            "equity_momentum": {"stop_mult": 0.90, "rr_min": 1.7, "hold": 72.0, "hold_cap": 240.0, "target_floor": 5.0, "stop_floor": 1.5, "target_cap": 35.0, "spread_mult": 0.15},
            "prediction_market": {"stop_mult": 0.70, "rr_min": 1.6, "hold": 24.0, "hold_cap": 72.0, "target_floor": 4.0, "stop_floor": 1.5, "target_cap": 30.0, "spread_mult": 0.60},
            "event": {"stop_mult": 0.85, "rr_min": 1.5, "hold": 16.0, "hold_cap": 48.0, "target_floor": 4.0, "stop_floor": 1.5, "target_cap": 25.0, "spread_mult": 0.55},
            "mean_reversion": {"stop_mult": 0.75, "rr_min": 1.3, "hold": 18.0, "hold_cap": 72.0, "target_floor": 3.5, "stop_floor": 1.5, "target_cap": 20.0, "spread_mult": 0.20},
            "arbitrage": {"stop_mult": 0.35, "rr_min": 1.1, "hold": 6.0, "hold_cap": 24.0, "target_floor": 1.0, "stop_floor": 0.5, "target_cap": 10.0, "spread_mult": 1.10},
            "rebalance": {"stop_mult": 0.35, "rr_min": 1.1, "hold": 4.0, "hold_cap": 12.0, "target_floor": 1.0, "stop_floor": 0.5, "target_cap": 10.0, "spread_mult": 0.10},
            "cycle": {"stop_mult": 0.25, "rr_min": 1.0, "hold": 1.0, "hold_cap": 4.0, "target_floor": 0.5, "stop_floor": 0.5, "target_cap": 5.0, "spread_mult": 0.05},
            "tax_loss": {"stop_mult": 0.25, "rr_min": 1.0, "hold": 1.0, "hold_cap": 4.0, "target_floor": 0.5, "stop_floor": 0.5, "target_cap": 5.0, "spread_mult": 0.05},
        }
        profile = profiles.get(trade_style, profiles["momentum"])

        vol = max(volatility_pct, 1.0)
        conf = _clamp(confidence, 0.05, 0.99)
        spread = max(spread_pct, 0.0)

        # Dynamic S/R-aware stop when sufficient data is available
        dynamic_stop_detail = ""
        if sr_levels and atr_value > 0 and entry_price > 0 and regime:
            dynamic_stop_pct, atr_dist, detail = self._compute_dynamic_stop(
                entry_price=entry_price,
                side=side,
                atr=atr_value,
                regime=regime,
                levels=sr_levels,
                base_stop_pct=20.0,  # generous default, overridden below
            )
            # Use the dynamic stop as stop-loss (capped by profile bounds)
            stop_pct = _clamp(dynamic_stop_pct, max(profile["stop_floor"], 0.5), profile["target_cap"])
            dynamic_stop_detail = f" dyn_stop({detail})"
        else:
            stop_pct = profile["stop_floor"] + (vol * profile["stop_mult"] * (1.18 - (conf * 0.45))) + (spread * profile["spread_mult"])
            stop_pct = _clamp(stop_pct, profile["stop_floor"], profile["target_cap"])

        target_pct = max(expected_return_pct, stop_pct * profile["rr_min"], profile["target_floor"])
        target_pct = _clamp(target_pct, profile["target_floor"], profile["target_cap"])

        vol_adjust = _clamp(30.0 / vol, 0.45, 1.35)
        hold_hours = profile["hold"] * vol_adjust * (1.10 - (conf * 0.18))
        if hold_hint_hours is not None:
            hold_hours = (hold_hours * 0.65) + (hold_hint_hours * 0.35)
        hold_hours = _clamp(hold_hours, 0.0, profile["hold_cap"])

        expected_return_pct = max(expected_return_pct, target_pct * 0.55)

        return {
            "trade_style": trade_style,
            "stop_loss_pct": round(stop_pct, 1),
            "take_profit_pct": round(target_pct, 1),
            "holding_period_hours": round(hold_hours, 1),
            "risk_pct": round(stop_pct, 1),
            "expected_return_pct": round(expected_return_pct, 1),
            "_dynamic_stop_detail": dynamic_stop_detail,
        }

    def _latency_adjusted_priority(
        self,
        base_priority: float,
        *,
        trade_style: str = "momentum",
        expected_delay_ms: Optional[float] = None,
    ) -> float:
        """Down-weight short-horizon strategies when the environment is slow."""

        delay_ms = expected_delay_ms
        if delay_ms is None:
            # Average detection delay from the optimizer tick plus a small execution buffer.
            delay_ms = max(250.0, (self.interval * 500.0) + 250.0)
        return _latency_tuned_priority(
            base_priority,
            trade_style=trade_style,
            expected_delay_ms=delay_ms,
        )

    def _usdc_reserve_amount(self) -> float:
        if not self.state:
            return 0.0
        reserve_fraction = float(self.capital_policy.get("targets", {}).get("reserve", USDC_YIELD_RESERVE_FRACTION))
        reserve = max(self.state.total_value * reserve_fraction, self.min_value * 2)
        return _clamp(reserve, 0.0, self.state.total_value)

    def _deployable_capital(self) -> float:
        if not self.state:
            return 0.0
        base = max(self.state.total_value - self._usdc_reserve_amount(), 0.0)
        cap = float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0)
        if cap > 0:
            return min(base, self._remaining_deployable_capital())
        return base

    def _parse_iso_ts(self, value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            text = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            return None

    def _live_test_started_at(self) -> Optional[datetime]:
        return self._parse_iso_ts(self.capital_policy.get("live_test_started_at") or self.capital_policy.get("updated_at"))

    def _live_test_capital_in_play(self) -> float:
        cap = float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0)
        if cap <= 0:
            return 0.0
        start = self._live_test_started_at()
        if not start:
            return 0.0
        spent = 0.0
        for trade in self.store.load_trades(limit=2000):
            if not trade:
                continue
            if bool(trade.get("dry_run", 1)):
                continue
            trade_ts = self._parse_iso_ts(trade.get("timestamp"))
            if not trade_ts or trade_ts < start:
                continue
            side = str(trade.get("side", "")).upper()
            if side not in {"BUY", "SELL"}:
                continue
            size = max(float(trade.get("size_usd", 0) or 0), 0.0)
            spent += size if side == "BUY" else -size
        return max(spent, 0.0)

    def _remaining_deployable_capital(self) -> float:
        cap = float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0)
        if cap <= 0:
            return 0.0
        return max(cap - self._live_test_capital_in_play(), 0.0)

    def _buy_capacity(self) -> float:
        if not self.state:
            return 0.0
        spendable = max(self.state.usdc_balance - self._usdc_reserve_amount(), 0.0)
        remaining_cap = self._remaining_deployable_capital()
        if float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0) > 0:
            return min(spendable, remaining_cap)
        if remaining_cap > 0:
            return min(spendable, remaining_cap)
        return spendable

    def _core_batch_cap(self) -> float:
        if not self.state:
            return 0.0
        frac = float(self.capital_policy.get("core_batch_fraction", CORE_BATCH_FRACTION))
        cap = max(self.state.total_value * frac, self.min_value)
        remaining_cap = self._remaining_deployable_capital()
        if float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0) > 0:
            return min(cap, remaining_cap)
        return cap

    def _opportunity_batch_cap(self) -> float:
        if not self.state:
            return 0.0
        frac = float(self.capital_policy.get("opportunity_batch_fraction", OPPORTUNITY_BATCH_FRACTION))
        cap = max(self.state.total_value * frac, self.min_value)
        remaining_cap = self._remaining_deployable_capital()
        if float(self.capital_policy.get("max_deployable_usd", 0.0) or 0.0) > 0:
            return min(cap, remaining_cap)
        return cap

    def _bucket_targets(self) -> Dict[str, float]:
        if not self.state:
            return {k: 0.0 for k in PORTFOLIO_BUCKET_TARGETS}
        total = self.state.total_value
        targets = self.capital_policy.get("targets", PORTFOLIO_BUCKET_TARGETS)
        return {bucket: total * float(targets.get(bucket, fraction)) for bucket, fraction in PORTFOLIO_BUCKET_TARGETS.items()}

    def _is_core_holding(self, holding: Dict[str, Any]) -> bool:
        currency = str(holding.get("currency", "")).upper().replace("-USD", "")
        allowlist = {str(x).upper().replace("-USD", "") for x in self.capital_policy.get("core_allowlist", CORE_LONG_TERM_ASSETS)}
        if currency in allowlist:
            return True
        classification = str(holding.get("classification", "")).lower()
        allocation_pct = to_float(holding.get("allocation_pct", 0))
        min_alloc = float(self.capital_policy.get("core_min_allocation_pct", 10.0))
        return classification == "safe" and allocation_pct >= min_alloc

    def _static_holdings_set(self) -> set[str]:
        raw = self.capital_policy.get("static_holdings", DEFAULT_CAPITAL_POLICY["static_holdings"])
        if isinstance(raw, str):
            raw = [x.strip() for x in raw.split(",") if x.strip()]
        return {str(x).upper().replace("-USD", "") for x in raw if str(x).strip()}

    def _is_static_currency(self, currency: str) -> bool:
        return currency.upper().replace("-USD", "") in self._static_holdings_set()

    def _bucket_values(self) -> Dict[str, float]:
        if not self.state:
            return {k: 0.0 for k in PORTFOLIO_BUCKET_TARGETS}
        reserve = self.state.usdc_balance
        core = sum(float(h.get("value", 0) or 0) for h in self.state.holdings.values() if self._is_core_holding(h))
        opportunity = max(self.state.total_value - reserve - core, 0.0)
        return {"reserve": reserve, "core": core, "opportunity": opportunity}

    def _bucket_gap(self, bucket: str) -> float:
        targets = self._bucket_targets()
        values = self._bucket_values()
        return max(targets.get(bucket, 0.0) - values.get(bucket, 0.0), 0.0)

    def _capital_bucket_for(self, opp: Opportunity) -> str:
        currency = opp.currency.upper().replace("-USD", "")
        allowlist = {str(x).upper().replace("-USD", "") for x in self.capital_policy.get("core_allowlist", CORE_LONG_TERM_ASSETS)}
        if currency in allowlist:
            return "core"
        if opp.meta.get("capital_bucket") in PORTFOLIO_BUCKET_TARGETS:
            return opp.meta["capital_bucket"]
        if opp.opp_type in (OpportunityType.NEW_LISTING_MOMENTUM, OpportunityType.EVENT_MARKET, OpportunityType.EVENT_ARBITRAGE):
            return "opportunity"
        if opp.opp_type == OpportunityType.ACCUMULATOR_SIGNAL:
            return opp.meta.get("capital_bucket", "opportunity")
        if opp.opp_type == OpportunityType.STOCK_SIGNAL:
            return "opportunity"
        if opp.side == "BUY":
            return "core" if classify_asset(currency) == "safe" else "opportunity"
        return "opportunity"

    def _check_pending_approvals(self):
        if not self.require_approval:
            return
        if not os.path.exists(self.pending_file):
            return
        try:
            with open(self.pending_file, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                pending = json.load(f)
        except (json.JSONDecodeError, OSError):
            return

        executed_any = False
        for token, entry in list(pending.items()):
            if entry.get("status") != "approved":
                continue
            logger.info("Executing approved trade: %s %s $%.0f",
                         entry.get("side", ""), entry.get("currency", ""),
                         float(entry.get("size_usd", 0)))
            self._execute_approved(entry)
            pending.pop(token, None)
            executed_any = True

        if executed_any:
            with open(self.pending_file, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(pending, f, indent=2, default=str)

    def _execute_approved(self, entry: dict):
        """Execute a previously-approved trade from pending."""
        side = entry["side"]
        currency = entry["currency"]
        size_usd = float(entry.get("size_usd", 0))
        product_id = entry.get("product_id", "")
        reason = entry.get("reason", "Approved trade")

        if not product_id or size_usd <= 0:
            logger.warning("  → Invalid approved entry, skipping")
            return

        is_quote = side == "BUY"
        if is_quote:
            bucket = entry.get("capital_bucket", "opportunity")
            bucket_limit = self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap()
            size_usd = min(size_usd, self._buy_capacity(), bucket_limit, self._bucket_gap(bucket))
            if size_usd < self.min_value:
                logger.warning("  → Buy capacity below minimum, skipping")
                return
        base_qty = 0.0
        if not is_quote and self.state:
            holder = self.state.holdings.get(currency, {})
            price = holder.get("price", 0) or 1
            base_qty = size_usd / price if price > 0 else 0
            if base_qty <= 0:
                logger.warning("  → Cannot compute base quantity, skipping")
                return

        route_payload = entry.get("route_decision")
        if route_payload and _HAS_MULTI_HOP:
            route_decision = self._route_decision_from_payload(route_payload)
            if route_decision is not None:
                type_str = entry.get("type", "strategy")
                try:
                    opp_type = OpportunityType(type_str)
                except ValueError:
                    opp_type = OpportunityType.STRATEGY_SIGNAL
                route_opp = Opportunity(
                    opp_type=opp_type,
                    currency=currency,
                    side=side,
                    size_usd=size_usd,
                    reason=reason,
                    priority=float(entry.get("priority", 0.0) or 0.0),
                    product_id=product_id,
                    meta={"route_decision": route_payload},
                )
                if self._execute_route_decision(route_opp, route_decision):
                    logger.info("  → EXECUTED approved route trade: %s", route_opp.order_id)
                    self.last_execution[entry.get("type", "strategy")] = time.time()
                    trade_entry = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "type": entry.get("type", "approved"),
                        "side": side,
                        "currency": currency,
                        "size_usd": round(size_usd, 2),
                        "fee": 0.0,
                        "reason": f"[APPROVED] {reason}",
                        "order_id": route_opp.order_id,
                        "dry_run": self.dry_run,
                        "route_decision": route_payload,
                    }
                    self.trade_log.append(trade_entry)
                    self.store.save_trade(trade_entry)
                    if self.neo4j_store:
                        try:
                            self.neo4j_store.save_trade(trade_entry)
                        except Exception as e:
                            logger.warning("Neo4j trade save failed: %s", e)
                    return
                logger.warning("  → Approved route failed; aborting without direct fallback")
                return

        # If this was a bracket trade, place the bracket directly with approved prices
        if entry.get("bracket") and self._bracket_mgr:
            stop_price = float(entry.get("stop_price", 0))
            target_price = float(entry.get("target_price", 0))
            entry_price = float(entry.get("entry_price_est", 0))
            approved_base_qty = float(entry.get("base_qty", 0))
            bracket_base_size = max(approved_base_qty, base_qty, 0.0001)
            use_entry_price = entry_price or (stop_price * 1.2 if side.upper() == "BUY" else stop_price * 0.8)
            bracket = self._bracket_mgr.place_bracket(
                product_id=product_id,
                side=side.upper(),
                base_size=bracket_base_size,
                entry_price=use_entry_price,
                stop_price=stop_price,
                target_price=target_price,
                strategy_id="opt_approved",
            )
            if bracket.get("status") == "OPEN":
                bid = bracket.get("bracket_id", bracket.get("entry_result", {}).get("client_order_id", "unknown"))
                logger.info("  → APPROVED BRACKET PLACED id=%s entry=$%.2f stop=$%.2f target=$%.2f",
                             bid, use_entry_price, stop_price, target_price)
                self._record_trade(Opportunity(
                    opp_type=OpportunityType.STRATEGY_SIGNAL,
                    currency=currency, side=side, size_usd=size_usd,
                    reason=f"[APPROVED] {reason}", product_id=product_id,
                    entry_price_est=use_entry_price,
                ), total_fee=0.0)
            else:
                err = bracket.get("entry_result", {}).get("error", "unknown")
                logger.warning("  → Approved bracket placement failed: %s", err)
                entry_result = bracket.get("entry_result", {})
                if entry_result.get("success") and entry_result.get("order_id"):
                    try:
                        self._bracket_mgr.force_flatten_bracket(
                            bracket.get("bracket_id", ""), reason="approved_bracket_failed"
                        )
                    except Exception as flatten_err:
                        logger.error("  → Flat close after approved bracket failure also failed: %s", flatten_err)
            return

        if is_quote:
            preview = self.cli.preview_order(product_id, side, size_usd, is_quote=True)
        else:
            preview = self.cli.preview_order(product_id, side, base_qty, is_quote=False)
        if not preview:
            logger.warning("  → Preview failed for approved trade, skipping")
            return

        total_fee = to_float(preview.get("total_fee", 0))

        if self.dry_run:
            logger.info("  → DRY-RUN: would execute %s %s $%.0f (fee=$%.2f)",
                         side, product_id, size_usd, total_fee)
            order_id = "dry-run"
        else:
            if is_quote:
                order = self.cli.create_order(product_id, side, size_usd, is_quote=True)
            else:
                order = self.cli.create_order(product_id, side, base_qty, is_quote=False)
            if not order:
                logger.error("  → Execution failed for approved trade")
                return
            order_id = order.get("id", "unknown")
            logger.info("  → EXECUTED approved trade: %s order_id=%s", side, order_id)

        self.last_execution[entry.get("type", "strategy")] = time.time()
        trade_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": entry.get("type", "approved"),
            "side": side,
            "currency": currency,
            "size_usd": round(size_usd, 2),
            "fee": round(total_fee, 2),
            "reason": f"[APPROVED] {reason}",
            "order_id": order_id,
            "dry_run": self.dry_run,
        }
        self.trade_log.append(trade_entry)
        self.store.save_trade(trade_entry)
        if self.neo4j_store:
            try:
                self.neo4j_store.save_trade(trade_entry)
            except Exception as e:
                logger.warning("Neo4j trade save failed: %s", e)
        logger.info("  → Approved trade #%d logged", len(self.trade_log))

    # ── Main loop ──────────────────────────────────────────────────

    def run(self):
        self.running = True
        logger.info("Optimizer started (dry_run=%s, interval=%ds)", self.dry_run, self.interval)
        while self.running:
            # KILL_SWITCH check — immediate halt
            ks = os.environ.get("KILL_SWITCH", "false").strip().lower()
            if ks in ("true", "1", "yes"):
                logger.warning("KILL_SWITCH active — halting optimizer")
                self.running = False
                break
            try:
                self._tick()
            except KeyboardInterrupt:
                self.running = False
                logger.info("Shutdown requested")
                break
            except Exception as e:
                logger.error("Tick failed: %s", e, exc_info=True)
            self._tick_count += 1
            self._last_tick_ts = time.time()
            # Poll active brackets between ticks (updates trailing stops / take-profits)
            self._poll_brackets()
            # Alert if ticks stall
            self._health_alerts = [
                a for a in self._health_alerts
                if "stale" not in a
            ]
            logger.info("Sleeping %ds...", self.interval)
            time.sleep(self.interval)

    def stop(self):
        self.running = False
        if self._bracket_mgr:
            self._bracket_mgr.stop_polling()
            self._save_brackets()
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
            except Exception:
                pass
            self._lock_fd = None
            try:
                os.remove("data/optimizer.lock")
            except Exception:
                pass
        if self._health_server:
            try:
                self._health_server.stop()
            except Exception:
                pass
        if self._feed_mgr:
            try:
                self._feed_mgr.stop()
            except Exception:
                pass
        if self.graph_store:
            try:
                self.graph_store.close()
            except Exception:
                pass

    # ── Single tick ────────────────────────────────────────────────

    def _tick(self):
        logger.info("─" * 50)
        logger.info("TICK at %s", datetime.now(timezone.utc).strftime("%H:%M:%S"))
        self._refresh_capital_policy()
        self._check_pending_approvals()
        self._fetch_state()
        self._apply_bear_market_policy()

        # Promote held positions to critical tier so their feeds stay fresh
        if self._feed_mgr and self.state:
            for currency, h in self.state.holdings.items():
                if h.get("value", 0) >= self.min_value and currency not in ("USDC", "USDT", "DAI"):
                    pid = h.get("product_id", f"{currency}-USD")
                    self._feed_mgr.add_position(pid)

        # Instant-refresh all critical feeds (core pairs + positions) before detection
        if self._feed_mgr:
            try:
                self._feed_mgr.refresh_critical_now()
            except Exception as e:
                logger.debug("SmartFeed instant refresh: %s", e)

        opportunities = self._detect_opportunities()
        opportunities = self._apply_cross_asset_risk_filter(opportunities)
        self._last_detected_opportunities = list(opportunities)
        opportunities.sort(key=lambda o: o.priority, reverse=True)
        self._write_trade_plans(opportunities)
        self._write_signal_cache(opportunities)
        self._write_enhanced_state()
        logger.info("Found %d opportunities", len(opportunities))
        for opp in opportunities[:5]:  # max 5 per tick
            self._process_opportunity(opp)
        self._save_state()

    # ── State ──────────────────────────────────────────────────────

    def _fetch_state(self):
        try:
            # Run three independent CLI calls in parallel
            futs = {
                _IO_EXECUTOR.submit(self.cli.get_products): "products",
                _IO_EXECUTOR.submit(self.cli.get_balances): "balances",
                _IO_EXECUTOR.submit(self.cli.get_fees): "fees",
            }
            results = {}
            for fut in as_completed(futs):
                key = futs[fut]
                try:
                    results[key] = fut.result()
                except Exception as e:
                    logger.warning("Coinbase %s fetch failed: %s", key, e)
                    results[key] = None
            products_data = results.get("products")
            balances = results.get("balances")
            fees_data = results.get("fees")
            if balances is None:
                raise Exception("balances fetch failed")
        except Exception as exc:
            logger.warning("Coinbase state unavailable; continuing with empty portfolio: %s", exc)
            self.cost_bases = {}
            self.state = PortfolioState(
                holdings={},
                total_value=0.0,
                usdc_balance=0.0,
                fee_volume_30d=0.0,
                fee_tier=(0, 0.006, 0.012),
                volume_to_next_tier=0.0,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            return

        self.cost_bases = self._compute_cost_bases()

        holdings = {}
        total_value = 0.0
        usdc_balance = 0.0

        for acct in balances:
            currency = acct.get("currency", "")
            balance = to_float(acct.get("available_balance", {}).get("value", 0))
            held = to_float(acct.get("hold", {}).get("value", 0))
            total = balance + held
            if total <= 0:
                continue

            if currency in ("USDC", "USDT", "DAI"):
                price = 1.0
                product_id = ""
                price_info = {"price_percentage_change_24h": 0.0, "volume_24h": 0.0}
            else:
                product_id = self.cli.best_product(currency, "SELL") or f"{currency}-USD"
                price_info = self.cli.get_price(product_id)
                price = to_float(price_info.get("price", 0))
            value = total * price
            classification = classify_asset(currency)
            cost_basis = self.cost_bases.get(currency)

            if currency == "USDC":
                usdc_balance = total

            holdings[currency] = {
                "currency": currency,
                "product_id": product_id,
                "balance": balance,
                "held": held,
                "total": total,
                "price": price,
                "value": value,
                "classification": classification,
                "allocation_pct": 0.0,  # computed below
                "cost_basis": cost_basis,
                "unrealized_pnl_pct": ((price / cost_basis) - 1) * 100 if (cost_basis and cost_basis > 0) else None,
                "change_24h": to_float(price_info.get("price_percentage_change_24h")),
                "volume_24h": to_float(price_info.get("volume_24h")),
            }
            total_value += value

        for h in holdings.values():
            h["allocation_pct"] = (h["value"] / total_value * 100) if total_value > 0 else 0

        if fees_data is None:
            fee_volume = 0.0
        else:
            fee_volume = to_float(fees_data.get("advanced_trade_only_volume", 0))
        fee_tier = current_fee_tier(fee_volume)

        self.state = PortfolioState(
            holdings=holdings,
            total_value=total_value,
            usdc_balance=usdc_balance,
            fee_volume_30d=fee_volume,
            fee_tier=fee_tier,
            volume_to_next_tier=volume_to_next(fee_volume),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # Print summary
        logger.info("Portfolio: $%.0f total, %d holdings, fee tier $%s+",
                     total_value, len(holdings), f"{fee_tier[0]:,.0f}" if fee_tier[0] > 0 else "0")

    def _compute_cost_bases(self) -> Dict[str, float]:
        all_fills = self.cli.get_fills()
        buys = {}
        for fill in all_fills:
            product = fill.get("product_id", "")
            currency = product.replace("-USD", "") if product else fill.get("currency", "")
            side = fill.get("side", "").upper()
            size = to_float(fill.get("size", 0))
            price = to_float(fill.get("price", 0))
            if size <= 0 or price <= 0:
                continue
            tc, ts = buys.get(currency, (0.0, 0.0))
            if side == "BUY":
                buys[currency] = (tc + size * price, ts + size)
            elif side == "SELL" and ts > 0:
                avg = tc / ts
                buys[currency] = (tc - min(tc, size * avg), max(0.0, ts - size))
        return {cur: tc / ts for cur, (tc, ts) in buys.items() if ts > 0}

    # ── Signal Ensemble / Meta Learning ─────────────────────────────

    def _signal_ensemble_blend(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        """Apply Bayesian signal ensemble to blend opportunity priorities.

        Uses per-strategy win-rate posteriors to weight signals, then
        ranks by ensemble score (score × direction agreement boost).
        Also performs meta-learning: tracks per-source performance and
        adjusts source weights online.
        """
        if not opportunities or not self._ensemble_blender:
            return opportunities

        try:
            regime = "unknown"
            if self.state:
                for opp in opportunities[:1]:
                    if opp.product_id and self.state:
                        pass
            # Wrap opportunities into ensemble format
            from coinbase.src.protocols import (
                Direction,
                InstrumentType,
                Opportunity as EnsembleOpp,
            )

            ensemble_opps = []
            for opp in opportunities:
                stop = round(opp.entry_price_est * (1 - opp.stop_loss_pct / 100.0), 6) if opp.stop_loss_pct else 0.0
                target = round(opp.entry_price_est * (1 + opp.take_profit_pct / 100.0), 6) if opp.take_profit_pct else 0.0
                rr = (opp.take_profit_pct / opp.stop_loss_pct) if opp.stop_loss_pct else 0.0
                conf = float(opp.meta.get("confidence", max(0.0, min(1.0, opp.priority))))
                eo = EnsembleOpp(
                    product_id=opp.product_id or f"{opp.currency}-USD",
                    direction=Direction.LONG if opp.side == "BUY" else Direction.SHORT,
                    instrument_type=InstrumentType.SPOT,
                    entry_price=opp.entry_price_est,
                    stop_price=stop,
                    target_price=target,
                    risk_reward=rr,
                    confidence=conf,
                    reason=opp.reason,
                    strategy_name=opp.meta.get("strategy", opp.meta.get("source", "unknown")),
                    score=opp.priority,
                    meta=dict(opp.meta),
                )
                ensemble_opps.append(eo)

            blended = self._ensemble_blender.blend_signals(ensemble_opps, regime)

            # Map blended scores back to original opportunities
            blended_by_reason = {b.reason: b for b in blended}
            for opp in opportunities:
                eb = blended_by_reason.get(opp.reason)
                if eb:
                    opp.meta["ensemble_weight"] = eb.meta.get("bayesian_weight", 1.0)
                    opp.meta["ensemble_win_rate"] = eb.meta.get("bayesian_win_rate", 0.0)
                    opp.priority = min(eb.score, 0.99)

            # Meta-learning: update source weight tracking
            self._update_meta_source_weights(opportunities)

        except Exception as e:
            logger.debug("Signal ensemble blend failed: %s", e)

        return opportunities

    def _update_meta_source_weights(self, opportunities: List[Opportunity]) -> None:
        """Track signal source performance and update meta-weights.

        Each signal source (strategy_engine, funding, onchain, orderflow, etc.)
        gets a weight based on the average priority of its opportunities.
        Sources with consistently low-priority signals are down-weighted.
        """
        source_priorities: Dict[str, List[float]] = {}
        for opp in opportunities:
            source = opp.meta.get("source", opp.meta.get("strategy", "unknown"))
            if source not in source_priorities:
                source_priorities[source] = []
            source_priorities[source].append(opp.priority)

        decay = 0.95
        for source, priorities in source_priorities.items():
            avg_p = sum(priorities) / max(len(priorities), 1)
            prev = self._meta_source_weights.get(source, 1.0)
            smoothed = prev * decay + avg_p * (1.0 - decay)
            self._meta_source_weights[source] = round(smoothed, 3)

    def _apply_meta_source_weights(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        """Apply learned meta source weights to scale opportunity priorities."""
        if not self._meta_source_weights:
            return opportunities
        for opp in opportunities:
            source = opp.meta.get("source", opp.meta.get("strategy", "unknown"))
            meta_w = self._meta_source_weights.get(source, 1.0)
            if meta_w < 0.5:
                opp.meta["meta_source_penalty"] = meta_w
                opp.priority *= meta_w
        return opportunities

    # ── Order Flow / Microstructure Detection ────────────────────────

    def _detect_order_flow_signals(self) -> List[Opportunity]:
        """Detect order flow and microstructure edge signals.

        Uses OrderFlowEngine (spread z-score) and SmartMoneyFlowStrategy
        (CVD divergence, volume absorption) to generate microstructure-aware
        opportunities.
        """
        if time.time() - self.last_execution.get("order_flow", 0) < OP_COOLDOWN.get("order_flow", 600):
            return []
        if not self.state:
            return []

        ops: List[Opportunity] = []
        remaining_buy_capacity = self._buy_capacity()
        sell_candidates = {
            h["currency"]: h for h in self.state.holdings.values()
            if h["value"] >= self.min_value
        } if self.state else {}

        tracked_products = []
        for cur, h in self.state.holdings.items():
            if cur in ("USDC", "USDT", "DAI") or h["value"] < self.min_value:
                continue
            pid = h.get("product_id", f"{cur}-USD")
            tracked_products.append((cur, pid, h))

        # ── OrderFlowEngine: spread-based signals ────────────────
        if self._order_flow_engine:
            try:
                for cur, pid, h in tracked_products:
                    price = to_float(h.get("price", 0))
                    volume = to_float(h.get("volume_24h", 0))
                    if price <= 0:
                        continue
                    # Estimate bid/ask from spread estimate
                    spread_est = to_float(h.get("spread", 0.005))
                    bid = price * (1.0 - spread_est / 2.0)
                    ask = price * (1.0 + spread_est / 2.0)

                    sig = self._order_flow_engine.evaluate(pid, bid, ask, price, volume)
                    if not sig or sig.confidence < 0.30:
                        continue

                    conf = min(sig.confidence, 0.60)
                    side = sig.action
                    if side == "BUY" and remaining_buy_capacity < self.min_value:
                        continue
                    sell_h = sell_candidates.get(cur)
                    if side == "SELL" and (not sell_h or sell_h.get("value", 0) < self.min_value):
                        continue

                    size = self._risk_reward_size(
                        expected_return_pct=conf * 8.0,
                        risk_pct=max((1.0 - conf) * 6.0 + 1.5, 1.0),
                        confidence=conf,
                        liquidity=0.65,
                        cap_pct=0.006,
                        max_notional=1200.0,
                        min_notional=self.min_value,
                        capital_limit=remaining_buy_capacity if side == "BUY" else None,
                    )
                    if side == "SELL" and sell_h:
                        size = min(size, sell_h.get("value", 0))
                    if size < self.min_value:
                        continue
                    if side == "BUY":
                        remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)

                    exit_plan = self._compute_exit_plan(
                        cur, conf,
                        expected_return_pct=conf * 8.0,
                        trade_style="momentum" if side == "BUY" else "mean_reversion",
                        side=side,
                        volatility_pct=50.0,
                    )
                    ops.append(Opportunity(
                        opp_type=OpportunityType.STRATEGY_SIGNAL,
                        currency=pid,
                        side=side,
                        size_usd=size,
                        reason=f"orderflow:spread_z={sig.spread_z:.1f} tight={sig.spread_tight} vol={sig.volume_24h:.0f}",
                        priority=self._latency_adjusted_priority(
                            conf * 0.6, trade_style="momentum",
                        ),
                        product_id=pid,
                        entry_price_est=price,
                        stop_loss_pct=exit_plan["stop_loss_pct"],
                        take_profit_pct=exit_plan["take_profit_pct"],
                        holding_period_hours=exit_plan["holding_period_hours"],
                        expected_return_pct=exit_plan["expected_return_pct"],
                        risk_pct=exit_plan["risk_pct"],
                        meta={
                            "source": "order_flow",
                            "strategy": "order_flow",
                            "confidence": conf,
                            "spread_bps": sig.spread_bps,
                            "spread_z": sig.spread_z,
                            "signal_type": "microstructure",
                            "trade_style": "momentum",
                            "exit_plan": exit_plan,
                        },
                    ))
            except Exception as e:
                logger.debug("OrderFlowEngine detection failed: %s", e)

        # ── SmartMoneyFlowStrategy: CVD / volume absorption ─────
        if self._smart_money_flow:
            try:
                from coinbase.src.protocols import Bar, InstrumentType
                for cur, pid, h in tracked_products:
                    if self._feed_mgr:
                        candles = self._feed_mgr.get_candles_batch([pid], granularity=3600, limit=60)
                        clist = candles.get(pid, [])
                        if not clist or len(clist) < 30:
                            continue
                        bars = []
                        for c in reversed(clist):
                            if isinstance(c, dict):
                                bars.append(Bar(
                                    open=to_float(c.get("open", 0)),
                                    high=to_float(c.get("high", 0)),
                                    low=to_float(c.get("low", 0)),
                                    close=to_float(c.get("close", 0)),
                                    volume=to_float(c.get("volume", 0)),
                                    timestamp=c.get("time", c.get("timestamp", 0)),
                                    instrument_type=InstrumentType.SPOT,
                                ))
                        if len(bars) < 30:
                            continue
                        setup = self._smart_money_flow.on_bar(bars[-1], bars[:-1])
                        if not setup:
                            continue
                        conf = min(setup.confidence, 0.55)
                        side = "BUY" if setup.direction.value == "long" else "SELL"
                        if side == "BUY" and remaining_buy_capacity < self.min_value:
                            continue
                        sell_h = sell_candidates.get(cur)
                        if side == "SELL" and (not sell_h or sell_h.get("value", 0) < self.min_value):
                            continue
                        size = self._risk_reward_size(
                            expected_return_pct=conf * 10.0,
                            risk_pct=max((1.0 - conf) * 8.0 + 2.0, 1.5),
                            confidence=conf,
                            liquidity=0.6,
                            cap_pct=0.007,
                            max_notional=1500.0,
                            min_notional=self.min_value,
                            capital_limit=remaining_buy_capacity if side == "BUY" else None,
                        )
                        if side == "SELL" and sell_h:
                            size = min(size, sell_h.get("value", 0))
                        if size < self.min_value:
                            continue
                        if side == "BUY":
                            remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                        exit_plan = self._compute_exit_plan(
                            cur, conf,
                            expected_return_pct=conf * 10.0,
                            trade_style=setup.reason[:20],
                            side=side,
                            volatility_pct=55.0,
                        )
                        ops.append(Opportunity(
                            opp_type=OpportunityType.STRATEGY_SIGNAL,
                            currency=pid,
                            side=side,
                            size_usd=size,
                            reason=f"smartflow:{setup.reason}",
                            priority=self._latency_adjusted_priority(
                                conf * 0.55, trade_style="mean_reversion",
                            ),
                            product_id=pid,
                            entry_price_est=setup.entry_price,
                            stop_loss_pct=exit_plan["stop_loss_pct"],
                            take_profit_pct=exit_plan["take_profit_pct"],
                            holding_period_hours=exit_plan["holding_period_hours"],
                            expected_return_pct=exit_plan["expected_return_pct"],
                            risk_pct=exit_plan["risk_pct"],
                            meta={
                                "source": "smart_money_flow",
                                "strategy": "smart_money_flow",
                                "confidence": conf,
                                "cvd_divergence": True,
                                "signal_type": "microstructure",
                                "trade_style": "mean_reversion",
                                "exit_plan": exit_plan,
                            },
                        ))
            except Exception as e:
                logger.debug("SmartMoneyFlowStrategy detection failed: %s", e)

        # ── Candle-based order-flow (CVD divergence + wick pressure) ──
        if self._feed_mgr:
            try:
                for cur, pid, h in tracked_products:
                    price = to_float(h.get("price", 0)) or self._current_price_for_symbol(pid, fallback=0.0)
                    if price <= 0:
                        continue
                    candles = self._feed_mgr.get_candles_batch([pid], granularity=3600, limit=60).get(pid)
                    if not candles or len(candles) < 40:
                        continue
                    candles = _oldest_first_candles(candles)
                    closes, vols, highs, lows = [], [], [], []
                    for c in candles:
                        if isinstance(c, dict):
                            closes.append(to_float(c.get("close", 0)))
                            vols.append(to_float(c.get("volume", 0)))
                            highs.append(to_float(c.get("high", 0)))
                            lows.append(to_float(c.get("low", 0)))
                        elif isinstance(c, (list, tuple)) and len(c) >= 6:
                            closes.append(to_float(c[4]))
                            vols.append(to_float(c[5]))
                            highs.append(to_float(c[2]))
                            lows.append(to_float(c[1]))
                    if len(closes) < 40:
                        continue
                    for strat, tag in ((self._order_flow_cvd, "order_flow_cvd"),
                                       (self._wick_pressure, "wick_pressure")):
                        of_sig = strat.on_bar(price, closes, volumes=vols, highs=highs, lows=lows, currency=cur)
                        if not of_sig or of_sig.action not in ("BUY", "SELL"):
                            continue
                        side = of_sig.action
                        conf = min(of_sig.confidence, 0.65)
                        if conf < 0.30:
                            continue
                        if side == "BUY" and remaining_buy_capacity < self.min_value:
                            continue
                        sell_h = sell_candidates.get(cur)
                        if side == "SELL" and (not sell_h or sell_h.get("value", 0) < self.min_value):
                            continue
                        size = self._risk_reward_size(
                            expected_return_pct=conf * 9.0,
                            risk_pct=max((1.0 - conf) * 7.0 + 2.0, 1.5),
                            confidence=conf,
                            liquidity=0.6,
                            cap_pct=0.007,
                            max_notional=1500.0,
                            min_notional=self.min_value,
                            capital_limit=remaining_buy_capacity if side == "BUY" else None,
                        )
                        if side == "SELL" and sell_h:
                            size = min(size, sell_h.get("value", 0))
                        if size < self.min_value:
                            continue
                        if side == "BUY":
                            remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                        exit_plan = self._compute_exit_plan(
                            cur, conf,
                            expected_return_pct=conf * 9.0,
                            trade_style="mean_reversion" if side == "BUY" else "momentum",
                            volatility_pct=50.0,
                        )
                        ops.append(Opportunity(
                            opp_type=OpportunityType.STRATEGY_SIGNAL,
                            currency=pid,
                            side=side,
                            size_usd=size,
                            reason=of_sig.reason,
                            priority=self._latency_adjusted_priority(conf * 0.6, trade_style="mean_reversion"),
                            product_id=pid,
                            entry_price_est=price,
                            stop_loss_pct=exit_plan["stop_loss_pct"],
                            take_profit_pct=exit_plan["take_profit_pct"],
                            holding_period_hours=exit_plan["holding_period_hours"],
                            expected_return_pct=exit_plan["expected_return_pct"],
                            risk_pct=exit_plan["risk_pct"],
                            meta={
                                "source": "order_flow_candle",
                                "strategy": tag,
                                "confidence": conf,
                                "signal_type": "microstructure",
                                "trade_style": "mean_reversion",
                                "exit_plan": exit_plan,
                            },
                        ))
            except Exception as e:
                logger.debug("Candle order-flow detection failed: %s", e)

        self.last_execution["order_flow"] = time.time()
        if ops:
            logger.info("Order flow signals: %d opportunities", len(ops))
        return ops

    # ── Tax Loss Harvesting Automation ───────────────────────────────

    def _check_wash_sale(self, currency: str) -> bool:
        """Check if currency is in wash-sale cooldown (30 days since last TLH sell)."""
        now = time.time()
        last_sold = self._wash_sale_cooldown.get(currency, 0.0)
        if last_sold > 0 and (now - last_sold) < 86400.0 * 30:
            return True
        return False

    def _get_tlh_replacement(self, currency: str) -> Optional[str]:
        """Suggest a correlated-but-not-substantially-identical replacement.

        Maps sold currencies to correlated alternatives for tax-loss swap:
          BTC → ETH (different chain, correlated macro)
          ETH → SOL
          SOL → ADA
          etc.
        """
        tlh_pairs = {
            "BTC": "ETH", "ETH": "SOL", "SOL": "ADA",
            "ADA": "DOT", "DOT": "AVAX", "AVAX": "LINK",
            "LINK": "UNI", "UNI": "ATOM", "ATOM": "NEAR",
            "NEAR": "APT", "APT": "SUI", "SUI": "ARB",
            "ARB": "OP", "OP": "MATIC", "DOGE": "SHIB",
            "SHIB": "PEPE", "PEPE": "BONK",
        }
        replacement = tlh_pairs.get(currency)
        if replacement and not self._check_wash_sale(replacement):
            return f"{replacement}-USD"
        return None

    def _detect_enhanced_tlh(self) -> List[Opportunity]:
        """Enhanced tax-loss harvesting with wash-sale avoidance and replacement suggestions.

        Scans holdings for >5% unrealized loss, checks wash-sale cooldown,
        suggests correlated replacements, and prioritizes by tax savings.
        """
        if time.time() - self.last_execution.get("tlh", 0) < OP_COOLDOWN["tlh"]:
            return []
        if not self.state:
            return []

        candidates = []
        for cur, h in self.state.holdings.items():
            if cur in ("USDC", "USDT", "DAI") or h["value"] < self.min_value:
                continue
            if self._is_static_currency(cur):
                continue
            if self._check_wash_sale(cur):
                logger.debug("  Wash-sale cooldown active for %s, skipping TLH", cur)
                continue
            pnl = h.get("unrealized_pnl_pct")
            if pnl is None or pnl >= -5:
                continue
            loss_usd = abs(h["value"] * pnl / 100)
            tax_savings = loss_usd * 0.20
            priority = min(abs(pnl) / 25.0, 1.0) * (1.0 + min(tax_savings / 100.0, 0.5))
            replacement = self._get_tlh_replacement(cur)
            candidates.append({
                "currency": cur,
                "value": h["value"],
                "pnl": pnl,
                "loss_usd": loss_usd,
                "tax_savings": tax_savings,
                "priority": priority,
                "replacement": replacement,
                "price": h.get("price", 0),
            })

        if not candidates:
            return []

        # Sort by priority (highest tax savings + deepest loss first)
        candidates.sort(key=lambda c: c["priority"], reverse=True)

        ops = []
        remaining_buy_capacity = self._buy_capacity()
        for c in candidates[:5]:
            pid = self.cli.best_product(c["currency"], "SELL")
            if not pid:
                continue
            ops.append(Opportunity(
                opp_type=OpportunityType.TLH,
                currency=c["currency"],
                side="SELL",
                size_usd=c["value"],
                reason=(
                    f"TLH: {c['pnl']:.1f}% loss, est. savings ${c['tax_savings']:.0f}"
                    + (f" → replace with {c['replacement']}" if c["replacement"] else "")
                ),
                priority=c["priority"],
                product_id=pid,
                entry_price_est=c["price"],
                stop_loss_pct=0,
                take_profit_pct=0,
                holding_period_hours=0,
                expected_return_pct=c["tax_savings"] / max(c["value"], 1) * 100,
                risk_pct=0,
                meta={
                    "tax_savings": round(c["tax_savings"], 2),
                    "loss_pct": round(c["pnl"], 1),
                    "replacement": c["replacement"] or "",
                    "wash_sale_free": True,
                },
            ))

        if ops:
            logger.info("Enhanced TLH: %d candidates (savings up to $%.0f)",
                         len(ops), max(c["tax_savings"] for c in candidates[:5]))

        # Record wash-sale cooldowns for TLH targets
        now = time.time()
        for opp in ops:
            self._wash_sale_cooldown[opp.currency] = now

        return ops

    # ── Backtest-Driven Parameter Optimization ───────────────────────

    def _run_periodic_param_optimization(self) -> Dict[str, Any]:
        """Run walk-forward parameter optimization on key strategy parameters.

        Runs weekly (configurable via _param_opt_interval). Optimizes:
          - ATR period
          - RSI overbought/oversold thresholds
          - Moving average periods
          - Stop-loss multipliers
        Results are stored in _param_opt_results and applied via _apply_optimized_params.
        """
        now = time.time()
        if now - self._last_param_opt_ts < self._param_opt_interval:
            return self._param_opt_results
        if not _HAS_WALK_FORWARD:
            return {}

        # Define parameter ranges to optimize
        param_defs = {
            "atr": [_ParamRange("atr_period", 7, 28, 3, is_int=True)],
            "rsi": [
                _ParamRange("rsi_oversold", 20, 40, 5, is_int=True),
                _ParamRange("rsi_overbought", 60, 80, 5, is_int=True),
            ],
            "ma": [
                _ParamRange("ma_fast", 5, 25, 5, is_int=True),
                _ParamRange("ma_slow", 20, 80, 10, is_int=True),
            ],
            "stop": [_ParamRange("stop_atr_mult", 1.0, 3.0, 0.5)],
        }
        self._param_opt_ranges = param_defs

        # For each parameter group, run walk-forward on BTC-USD as a proxy
        results = {}
        try:
            btc_closes = []
            if self._feed_mgr:
                batched = self._feed_mgr.get_candles_batch(["BTC-USD"], granularity=86400, limit=500)
                candles = batched.get("BTC-USD", [])
                if len(candles) >= 200:
                    btc_closes = [to_float(c[4]) for c in candles]

            if len(btc_closes) < 200:
                logger.debug("Insufficient data for param optimization (need 200+ closes, got %d)", len(btc_closes))
                self._last_param_opt_ts = now
                return {}

            optimizer = _WalkForwardOptimizer(
                n_windows=5, train_pct=0.7, random_search_iters=100,
            )

            for group_name, ranges in param_defs.items():
                def _make_objective_fn(closes=btc_closes):
                    def _objective(params: Dict[str, float], train_start: int, train_end: int) -> Any:
                        from archive.coinbase_src.walk_forward import TrialResult
                        train_closes = closes[train_start:train_end]
                        if len(train_closes) < 50:
                            return TrialResult(params=params, metric=-999.0)
                        # Simple metric: Sharpe on training period trades
                        atr_p = int(params.get("atr_period", 14))
                        rsi_os = int(params.get("rsi_oversold", 30))
                        rsi_ob = int(params.get("rsi_overbought", 70))
                        ma_f = int(params.get("ma_fast", 10))
                        ma_s = int(params.get("ma_slow", 40))
                        stop_m = params.get("stop_atr_mult", 2.0)

                        train_returns = []
                        prev_close = train_closes[0]
                        position = 0
                        entry_price = 0.0
                        for i in range(1, len(train_closes)):
                            c = train_closes[i]
                            ret = (c - prev_close) / max(prev_close, 1e-9) * 100.0
                            if position == 0:
                                # Check entry signal
                                if i > ma_s:
                                    ma_f_v = sum(train_closes[i-ma_f:i]) / ma_f
                                    ma_s_v = sum(train_closes[i-ma_s:i]) / ma_s
                                    if ma_f_v > ma_s_v:
                                        position = 1
                                        entry_price = c
                            else:
                                stop_price = entry_price * (1.0 - stop_m * 0.01)
                                if c < stop_price:
                                    train_returns.append((c - entry_price) / max(entry_price, 1e-9) * 100.0)
                                    position = 0
                                    entry_price = 0.0
                        if train_returns:
                            sharpe = (sum(train_returns) / max(len(train_returns), 1)) / max(
                                (sum(r*r for r in train_returns) / len(train_returns)) ** 0.5, 1e-9
                            ) * (252 ** 0.5)
                            return TrialResult(params=params, metric=max(-5.0, min(5.0, sharpe)))
                        return TrialResult(params=params, metric=0.0)

                    return _objective

                opt_result = optimizer.optimize(
                    data_length=len(btc_closes),
                    param_ranges=ranges,
                    objective_fn=_make_objective_fn(),
                )
                best = opt_result.get("best_overall", {})
                results[group_name] = {
                    "best_params": best,
                    "windows": [
                        {"train_score": w.train_score, "test_score": w.test_score}
                        for w in opt_result.get("windows", [])
                    ],
                    "timestamp": now,
                }
                if best:
                    logger.info("Param opt %s: best=%s", group_name, best)

            self._param_opt_results = results
        except Exception as e:
            logger.warning("Parameter optimization failed: %s", e)

        self._last_param_opt_ts = now
        return results

    def _apply_optimized_params(self) -> None:
        """Apply parameter optimization results to strategy configuration.

        Adjusts internal parameters based on walk-forward optimization:
          - stop_atr_mult from 'stop' group
          - atr_period for exit planning
          - ma_fast/ma_slow for trend detection
        These modify strategy behavior in the optimizer.
        """
        if not self._param_opt_results:
            return
        for group, result in list(self._param_opt_results.items()):
            params = result.get("best_params", {})
            if not params:
                continue
            # Store optimized params as meta for strategy use
            if "stop_atr_mult" in params:
                self._param_opt_results["_active_stop_mult"] = params["stop_atr_mult"]
            if "atr_period" in params:
                self._param_opt_results["_active_atr_period"] = params["atr_period"]
            if "ma_fast" in params and "ma_slow" in params:
                pass  # Available for trend strategy integration

    # ── Opportunity detection ─────────────────────────────────────

    def _apply_cross_asset_risk_filter(self, opportunities: List[Opportunity]) -> List[Opportunity]:
        """Gate and scale opportunities based on cross-asset macro regime.

        Uses CrossAssetRegimeEngine (DXY, VIX, yields, SPY, QQQ) to:
          - Suppress BUY opportunities when regime disallows new longs
          - Scale position sizes by risk_multiplier
          - Reduce confidence in risk-off environments

        Uses MacroRiskEngine as an additional confidence penalty when
        macro composite score is extreme.
        """
        if not opportunities:
            return opportunities

        # Get cross-asset regime state
        regime_state = None
        if self._cross_asset_regime:
            try:
                regime_state = self._cross_asset_regime.get_state(refresh=False)
            except Exception as e:
                logger.debug("Cross-asset regime refresh failed: %s", e)

        # Get macro risk signal
        macro_sig = None
        if self._macro_risk:
            try:
                macro_sig = self._macro_risk.get_signal()
            except Exception as e:
                logger.debug("Macro risk signal failed: %s", e)

        if not regime_state and not macro_sig:
            return opportunities

        filtered: List[Opportunity] = []
        for opp in opportunities:
            # ── Cross-asset regime gate ──────────────────────────
            if regime_state:
                # Suppress BUY when regime forbids new longs
                if opp.side == "BUY" and not regime_state.allows_new_longs:
                    logger.debug("  Suppressing %s BUY (regime=%s forbids new longs)",
                                 opp.currency, regime_state.regime)
                    continue

                # Scale size by risk multiplier
                risk_mult = regime_state.risk_multiplier
                if risk_mult < 1.0:
                    opp.size_usd = max(opp.size_usd * risk_mult, 0.0)
                    if opp.size_usd < self.min_value:
                        logger.debug("  Dropping %s %s (size=%.0f below min after risk_mult=%.2f)",
                                     opp.currency, opp.side, opp.size_usd, risk_mult)
                        continue

                # Reduce priority in risk-off
                if regime_state.regime in ("crash", "risk_off"):
                    opp.priority *= 0.6
                elif regime_state.regime == "rebound":
                    opp.priority *= 1.15

                # Tag metadata
                opp.meta["cross_asset_regime"] = regime_state.regime
                opp.meta["cross_asset_risk_mult"] = round(regime_state.risk_multiplier, 3)
                opp.meta["cross_asset_trend_bias"] = regime_state.trend_bias

            # ── Macro risk penalty ──────────────────────────────
            if macro_sig:
                macro_score = macro_sig.macro_score
                # Extreme macro risk-off: confidence penalty
                if macro_score >= 1.5 and opp.side == "BUY":
                    opp.priority *= 0.7
                    opp.meta["macro_penalty"] = f"risk_off_score={macro_score:.2f}"
                elif macro_score <= -1.5 and opp.side == "SELL":
                    opp.priority *= 0.7
                    opp.meta["macro_penalty"] = f"risk_on_score={macro_score:.2f}"

                opp.meta["macro_risk_score"] = round(macro_score, 3)

            filtered.append(opp)

        return filtered

    def _detect_opportunities(self) -> List[Opportunity]:
        ops = []

        # Run parameter optimization periodically (weekly)
        self._run_periodic_param_optimization()
        self._apply_optimized_params()

        # Enhanced TLH with wash-sale avoidance
        ops.extend(self._detect_enhanced_tlh())
        ops.extend(self._detect_coinbase_universe_signals())
        ops.extend(self._detect_stock_opportunities())
        ops.extend(self._detect_fee_tier_volume())
        ops.extend(self._detect_rebalance())
        ops.extend(self._detect_rebalance_bot())
        ops.extend(self._detect_stairstep())
        ops.extend(self._detect_strategy_signals())
        ops.extend(self._detect_funding_and_onchain_signals())
        ops.extend(self._detect_volume_cycles())
        ops.extend(self._detect_accumulator_signals())
        ops.extend(self._detect_aggregator_signals())
        ops.extend(self._detect_event_markets())

        # Order flow / microstructure signals
        ops.extend(self._detect_order_flow_signals())

        # Signal ensemble blending (Bayesian weight × meta source weights)
        ops = self._signal_ensemble_blend(ops)
        ops = self._apply_meta_source_weights(ops)

        return ops

    def _detect_coinbase_universe_signals(self) -> List[Opportunity]:
        try:
            products = self.cli.get_products()
        except Exception as e:
            logger.warning("Coinbase universe scan unavailable: %s", e)
            return []

        rows = []
        best_by_base: Dict[str, Tuple[str, dict]] = {}
        quote_rank = {"USD": 0, "USDC": 1}
        for pid, p in products.items():
            if not pid or not isinstance(p, dict):
                continue
            if p.get("trading_disabled"):
                continue
            if not (pid.endswith("-USD") or pid.endswith("-USDC")):
                continue
            base = pid.split("-")[0]
            if base in ("USDC", "USDT", "DAI"):
                continue
            if self._is_static_currency(base):
                continue
            prev = best_by_base.get(base)
            if not prev:
                best_by_base[base] = (pid, p)
                continue
            prev_pid, prev_p = prev
            prev_quote = prev_pid.split("-")[-1]
            new_score = (quote_rank.get(pid.split("-")[-1], 9), -to_float(p.get("volume_24h", 0)))
            prev_score = (quote_rank.get(prev_quote, 9), -to_float(prev_p.get("volume_24h", 0)))
            if new_score < prev_score:
                best_by_base[base] = (pid, p)

        rows = list(best_by_base.values())

        rows.sort(key=lambda kv: to_float(kv[1].get("volume_24h", 0)), reverse=True)
        ops: List[Opportunity] = []
        remaining_buy_capacity = self._buy_capacity()
        remaining_core_capacity = self._bucket_gap("core")
        remaining_opportunity_capacity = self._bucket_gap("opportunity")

        # Fetch candles via smart feed manager (shared cache) or fallback to CLI
        candle_results: List[Tuple[str, dict, List[float], List[float], List[float], List[float]]] = []
        top_rows = rows[:25]
        top_pids = [r[0] for r in top_rows]

        if self._feed_mgr:
            batched = self._feed_mgr.get_candles_batch(top_pids, granularity=3600, limit=100)
            for pid, p in top_rows:
                candles = batched.get(pid)
                if not candles or len(candles) < 40:
                    continue
                candles = _oldest_first_candles(candles)
                closes, vols, highs, lows = [], [], [], []
                for c in candles:
                    if isinstance(c, dict):
                        closes.append(to_float(c.get("close", 0)))
                        vols.append(to_float(c.get("volume", 0)))
                        highs.append(to_float(c.get("high", 0)))
                        lows.append(to_float(c.get("low", 0)))
                    elif isinstance(c, (list, tuple)) and len(c) >= 6:
                        closes.append(to_float(c[4]))
                        vols.append(to_float(c[5]))
                        highs.append(to_float(c[2]))
                        lows.append(to_float(c[1]))
                if len(closes) < 40:
                    continue
                candle_results.append((pid, p, closes, vols, highs, lows))
        else:
            candle_futs = {}
            for pid, p in top_rows:
                fut = _IO_EXECUTOR.submit(self.cli.get_candles, pid, "1h", 100)
                candle_futs[fut] = (pid, p)
            for fut in as_completed(candle_futs):
                pid, p = candle_futs[fut]
                try:
                    candles = fut.result()
                except Exception as e:
                    logger.debug("Candle fetch failed for %s: %s", pid, e)
                    continue
                if not candles or len(candles) < 40:
                    continue
                candles = _oldest_first_candles(candles)
                closes, vols, highs, lows = [], [], [], []
                for c in candles:
                    closes.append(to_float(c.get("close", 0)))
                    vols.append(to_float(c.get("volume", 0)))
                    highs.append(to_float(c.get("high", 0)))
                    lows.append(to_float(c.get("low", 0)))
                if len(closes) < 40:
                    continue
                candle_results.append((pid, p, closes, vols, highs, lows))

        # Batch-compute trend/volume metrics via compute backend
        try:
            from trading_system.core.compute_backend import get_compute_backend
            _cb = get_compute_backend()
            n = len(candle_results)
            if n > 0:
                all_closes_arr = []
                all_vols_arr = []
                for _, _, c, v, _, _ in candle_results:
                    all_closes_arr.append(c[-40:])
                    all_vols_arr.append(v[-40:])
                # Shape: (n_products, 40)
                import numpy as np
                closes_np = np.array(all_closes_arr, dtype=np.float64)
                vols_np = np.array(all_vols_arr, dtype=np.float64)
                trend_10_arr = closes_np[:, -1] / np.maximum(closes_np[:, -10], 1e-9) - 1.0
                trend_30_arr = closes_np[:, -1] / np.maximum(closes_np[:, -30], 1e-9) - 1.0
                vol_recent_arr = np.mean(vols_np[:, -10:], axis=1)
                vol_prior_arr = np.mean(vols_np[:, -40:-10], axis=1)
                vol_ratio_arr = np.divide(vol_recent_arr, np.maximum(vol_prior_arr, 1e-9),
                                          out=np.ones_like(vol_recent_arr), where=vol_prior_arr > 0)
        except Exception as e:
            logger.debug("Batch metric computation unavailable: %s", e)
            trend_10_arr = trend_30_arr = vol_ratio_arr = None

        # Process each product sequentially (fast, no I/O)
        for idx, (pid, p, closes, vols, highs, lows) in enumerate(candle_results):
            try:
                base = pid.split("-")[0]
                if trend_10_arr is not None and idx < len(trend_10_arr):
                    trend_10 = float(trend_10_arr[idx])
                    trend_30 = float(trend_30_arr[idx])
                    volume_ratio = float(vol_ratio_arr[idx])
                else:
                    recent = closes[-10:]
                    vol_recent = sum(vols[-10:]) / max(len(vols[-10:]), 1)
                    vol_prior = sum(vols[-40:-10]) / max(len(vols[-40:-10]), 1)
                    trend_10 = (recent[-1] / recent[0] - 1) if recent[0] else 0
                    trend_30 = (closes[-1] / closes[-30] - 1) if closes[-30] else 0
                    volume_ratio = (vol_recent / vol_prior) if vol_prior > 0 else 1.0
                age_days = self._first_seen_age_days(pid)
                quality_asset = classify_asset(base)
                quality_volume = to_float(p.get("volume_24h", 0))
                if quality_asset not in ("safe", "growth") and quality_volume < 150_000_000:
                    continue
                liquidity = min(quality_volume / 5_000_000, 1.0)
                listing_bonus = _clamp((21.0 - age_days) / 21.0, 0.0, 1.0) if age_days is not None else 0.0
                quality_score = _clamp((liquidity * 0.45) + (min(volume_ratio, 3.0) / 3.0 * 0.25) + (1.0 - min(abs(trend_30), 0.5) * 1.4) * 0.30, 0.0, 1.0)
                momentum = max(min((trend_10 * 3.5) + (trend_30 * 2.0) + ((volume_ratio - 1) * 0.75) + (listing_bonus * 0.8), 1.0), -1.0)
                if abs(momentum) < 0.22 or liquidity < 0.2:
                    continue
                side = "BUY" if momentum > 0 else "SELL"
                # ── Quality tuning: skip exhaustion + directionless chop ──
                rsi = _rsi_14(closes)
                atr = _estimate_atr(closes, highs, lows, 14)
                vol_pct = (atr / closes[-1]) if closes[-1] > 0 else 0.0
                if side == "BUY" and rsi > 76:
                    continue  # chasing overbought
                if side == "SELL" and rsi < 24:
                    continue  # fading oversold
                if vol_pct > 0.06 and abs(trend_30) < 0.02:
                    continue  # high vol, no trend -> noise
                if (side == "BUY" and rsi > 68) or (side == "SELL" and rsi < 32):
                    momentum *= 0.7  # dampen stretched entries
                score = min(abs(momentum) * quality_score * 0.9 + min(volume_ratio, 2.0) * 0.1 + listing_bonus * 0.15, 0.95)
                is_new_listing = age_days <= 21
                holding_value = float(self.state.holdings.get(base, {}).get("value", 0) or 0)
                opp_type = OpportunityType.NEW_LISTING_MOMENTUM if is_new_listing else OpportunityType.STRATEGY_SIGNAL
                bucket = "core" if base in CORE_LONG_TERM_ASSETS else "opportunity"
                expected_return_pct = max(abs(trend_30) * 100 * 1.1 + listing_bonus * 6.0, 0.5)
                risk_pct = max(abs(trend_30) * 100 * 0.9 + (1.0 - liquidity) * 8.0 + 2.5, 1.0)
                size_usd = self._risk_reward_size(
                    expected_return_pct,
                    risk_pct,
                    score,
                    liquidity,
                    cap_pct=0.01,
                    max_notional=3000.0,
                    min_notional=self.min_value,
                    capital_limit=(remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity),
                )
                if is_new_listing and momentum < -0.18:
                    if holding_value < self.min_value:
                        continue
                    side = "SELL"
                    opp_type = OpportunityType.NEW_LISTING_MOMENTUM
                    size_usd = min(
                        self._risk_reward_size(
                            expected_return_pct=max(abs(momentum) * 100 * 0.8 + listing_bonus * 3.0, 0.5),
                            risk_pct=max((1.0 - quality_score) * 12.0 + (1.0 - liquidity) * 8.0 + 2.0, 1.0),
                            confidence=score,
                            liquidity=liquidity,
                            cap_pct=0.008,
                            max_notional=2000.0,
                            min_notional=self.min_value,
                        ),
                        holding_value,
                    )
                    expected_return_pct = max(abs(momentum) * 100 * 0.8 + listing_bonus * 3.0, 0.5)
                    risk_pct = max((1.0 - quality_score) * 12.0 + (1.0 - liquidity) * 8.0 + 2.0, 1.0)
                elif side == "BUY":
                    batch_cap = self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap()
                    size_usd = min(size_usd, remaining_buy_capacity, batch_cap, remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity)
                    remaining_buy_capacity = max(remaining_buy_capacity - size_usd, 0.0)
                    if bucket == "core":
                        remaining_core_capacity = max(remaining_core_capacity - size_usd, 0.0)
                    else:
                        remaining_opportunity_capacity = max(remaining_opportunity_capacity - size_usd, 0.0)
                entry_price = closes[-1] if closes else 0
                trade_style = "new_listing" if is_new_listing else "momentum"
                if is_new_listing and side == "SELL":
                    trade_style = "mean_reversion"
                exit_plan = self._compute_exit_plan(
                    pid.split("-")[0], score,
                    expected_return_pct=max(expected_return_pct, 0.5),
                    trade_style=trade_style,
                    volatility_pct=self._estimate_trade_volatility_pct(closes, highs, lows),
                    hold_hint_hours=18.0 if is_new_listing else 36.0,
                )
                ops.append(Opportunity(
                    opp_type=opp_type,
                    currency=pid.split("-")[0],
                    side=side,
                    size_usd=size_usd,
                    reason=(
                        f"Coinbase universe: {pid} trend={trend_10*100:.1f}%/30d={trend_30*100:.1f}% "
                        f"volx={volume_ratio:.2f} q={quality_score:.2f} age={age_days:.1f}d"
                        + (" fade" if is_new_listing and side == "SELL" else "")
                    ),
                    priority=score,
                    product_id=pid,
                    entry_price_est=entry_price,
                    stop_loss_pct=exit_plan["stop_loss_pct"],
                    take_profit_pct=exit_plan["take_profit_pct"],
                    holding_period_hours=exit_plan["holding_period_hours"],
                    expected_return_pct=exit_plan["expected_return_pct"],
                    risk_pct=exit_plan["risk_pct"],
                    meta={
                        "platform": "coinbase",
                        "category": "coinbase_universe",
                        "market_question": pid,
                        "probability": 0.5 + (0.5 * momentum),
                        "volume": quality_volume,
                        "spread": 0.0,
                        "liquidity_score": liquidity,
                        "quality_score": quality_score,
                        "listing_age_days": age_days,
                        "listing_phase": "new_listing" if is_new_listing else "seasoned",
                        "fade_signal": is_new_listing and side == "SELL",
                        "capital_bucket": bucket,
                        "trade_style": trade_style,
                        "signal_type": "coinbase_universe:momentum" if opp_type == OpportunityType.STRATEGY_SIGNAL else "coinbase_universe:new_listing_momentum",
                        "confidence": score,
                        "trend_10d": trend_10,
                        "trend_30d": trend_30,
                        "volume_ratio": volume_ratio,
                        "exit_plan": exit_plan,
                    },
                ))
            except Exception as e:
                logger.debug("Coinbase universe candidate failed for %s: %s", pid, e)
        if ops:
            logger.info("Coinbase universe scan: %d opportunities", len(ops))
        return ops

    def _detect_stock_opportunities(self) -> List[Opportunity]:
        if UnifiedMarketDataAdapter is None:
            return []
        try:
            adapter = UnifiedMarketDataAdapter()
        except Exception as e:
            logger.debug("Stock adapter unavailable: %s", e)
            return []

        ops: List[Opportunity] = []
        for symbol in _DEFAULT_STOCK_WATCHLIST:
            try:
                bars = adapter.fetch_historical_data(symbol, (datetime.now(timezone.utc) - timedelta(days=180)).strftime("%Y-%m-%d"), datetime.now(timezone.utc).strftime("%Y-%m-%d"))
                if len(bars) < 60:
                    continue
                closes = [to_float(b.get("close", 0)) for b in bars if to_float(b.get("close", 0)) > 0]
                vols = [to_float(b.get("volume", 0)) for b in bars if to_float(b.get("volume", 0)) > 0]
                if len(closes) < 60:
                    continue
                last = closes[-1]
                ma20 = sum(closes[-20:]) / 20
                ma50 = sum(closes[-50:]) / 50
                vol_ratio = (sum(vols[-10:]) / 10) / ((sum(vols[-40:-10]) / 30) if len(vols) >= 40 else max(sum(vols) / len(vols), 1))
                return_90d = (closes[-1] / closes[-60] - 1) if closes[-60] else 0
                upside = ((last / ma20) - 1) if ma20 else 0
                quality = 0.0
                try:
                    info = adapter.yfinance.get_stock_info(symbol)
                    market_cap = to_float((info or {}).get("market_capital", 0))
                    quality += 0.3 if market_cap >= 10_000_000_000 else 0.15 if market_cap >= 2_000_000_000 else 0.05
                except Exception:
                    quality += 0.05
                trend = ((ma20 / ma50) - 1) if ma50 else 0
                score = max(min((return_90d * 0.45) + (trend * 0.55) + ((vol_ratio - 1) * 0.15) + quality, 0.95), 0.0)
                if score < 0.35:
                    continue
                risk_pct = max(abs(return_90d) * 100 * 0.7 + (1.0 - min(vol_ratio / 3.0, 1.0)) * 6.0 + 2.5, 1.0)
                size_usd = self._risk_reward_size(
                    expected_return_pct=max(return_90d * 100, 0.5),
                    risk_pct=risk_pct,
                    confidence=score,
                    liquidity=min(vol_ratio / 2.0, 1.0),
                    cap_pct=0.006,
                    max_notional=2000.0,
                    min_notional=self.min_value,
                )
                side = "BUY" if upside >= 0 else "SELL"
                exit_plan = self._compute_exit_plan(
                    symbol, score,
                    expected_return_pct=max(abs(upside) * 100 * (1.4 if side == "BUY" else 1.1), 0.5),
                    trade_style="equity_momentum" if side == "BUY" else "mean_reversion",
                    volatility_pct=max((1.0 - min(vol_ratio / 3.0, 1.0)) * 25.0 + abs(trend) * 100.0 * 6.0 + 12.0, 12.0),
                    hold_hint_hours=72.0,
                )
                ops.append(Opportunity(
                    opp_type=OpportunityType.STOCK_SIGNAL,
                    currency=symbol,
                    side=side,
                    size_usd=size_usd,
                    reason=f"Stock screen: {symbol} return90d={return_90d*100:.1f}% trend={trend*100:.1f}% volx={vol_ratio:.2f}",
                    priority=score,
                    product_id=symbol,
                    entry_price_est=closes[-1] if closes else 0,
                    stop_loss_pct=exit_plan["stop_loss_pct"],
                    take_profit_pct=exit_plan["take_profit_pct"],
                    holding_period_hours=exit_plan["holding_period_hours"],
                    expected_return_pct=exit_plan["expected_return_pct"],
                    risk_pct=exit_plan["risk_pct"],
                    meta={
                        "platform": "equity",
                        "category": "stock_watchlist",
                        "market_question": symbol,
                        "probability": 0.5 + min(max(upside * 3, -0.4), 0.4),
                        "volume": sum(vols[-10:]) if vols else 0,
                        "spread": 0.0,
                        "liquidity_score": min(vol_ratio / 2.0, 1.0),
                        "signal_type": "equity_screen:quality_momentum",
                        "trade_style": "equity_momentum" if side == "BUY" else "mean_reversion",
                        "confidence": score,
                        "return_90d": return_90d,
                        "trend_20_50": trend,
                        "volume_ratio": vol_ratio,
                        "exit_plan": exit_plan,
                    },
                ))
            except Exception as e:
                logger.debug("Stock scan failed for %s: %s", symbol, e)
        if ops:
            logger.info("Stock screen: %d opportunities", len(ops))
        return ops

    def _detect_tlh(self) -> List[Opportunity]:
        if time.time() - self.last_execution.get("tlh", 0) < OP_COOLDOWN["tlh"]:
            return []
        if not self.state:
            return []
        ops = []
        for cur, h in self.state.holdings.items():
            if cur in ("USDC", "USDT", "DAI") or h["value"] < self.min_value:
                continue
            if self._is_static_currency(cur):
                continue
            pnl = h.get("unrealized_pnl_pct")
            if pnl is None or pnl >= -5:
                continue
            loss_usd = abs(h["value"] * pnl / 100)
            tax_savings = loss_usd * 0.20
            priority = min(abs(pnl) / 30, 1.0)
            pid = self.cli.best_product(cur, "SELL")
            if not pid:
                continue
            ops.append(Opportunity(
                opp_type=OpportunityType.TLH,
                currency=pid,
                side="SELL",
                size_usd=h["value"],
                reason=f"TLH: {pnl:.1f}% loss, est. tax savings ${tax_savings:.0f}",
                priority=priority,
                product_id=pid,
                entry_price_est=h.get("price", 0),
                stop_loss_pct=0,
                take_profit_pct=0,
                holding_period_hours=0,
                expected_return_pct=tax_savings / max(h["value"], 1) * 100,
                risk_pct=0,
            ))
        if ops:
            logger.info("TLH: %d candidates", len(ops))
        return ops

    def _detect_fee_tier_volume(self) -> List[Opportunity]:
        if time.time() - self.last_execution.get("fee_tier", 0) < OP_COOLDOWN["fee_tier"]:
            return []
        if not self.state:
            return []
        needed = self.state.volume_to_next_tier
        buy_capacity = self._buy_capacity()
        if needed <= 0 or buy_capacity < self.min_value:
            return []

        # We need to generate volume. Find the best asset to trade.
        # Look for holdings with enough value and decent liquidity.
        candidates = [
            h for h in self.state.holdings.values()
            if h["currency"] not in ("USDC",) and h["value"] >= self.min_value and not self._is_static_currency(h["currency"])
        ]
        if not candidates:
            return []

        # Pick the most liquid one, but avoid very volatile names for fee cycling.
        best = max(
            candidates,
            key=lambda h: (
                h.get("volume_24h", 0) or h["value"],
                -abs(to_float(h.get("change_24h", 0))),
            ),
        )
        if abs(to_float(best.get("change_24h", 0))) > 20:
            return []
        trade_size = min(needed, buy_capacity, self._opportunity_batch_cap(), max(best["value"] * 0.25, self.min_value))
        if trade_size < 10:
            return []
        pid = self.cli.best_product(best["currency"], "BUY")
        if not pid:
            return []
        ops = [Opportunity(
            opp_type=OpportunityType.FEE_TIER_VOLUME,
            currency=best["currency"],
            side="BUY",
            size_usd=trade_size,
            reason=f"Fee tier: generate ${trade_size:.0f} volume toward ${needed:,.0f} needed",
            priority=0.7,
            product_id=pid,
            entry_price_est=best.get("price", 0),
            stop_loss_pct=3.0,
            take_profit_pct=3.0,
            holding_period_hours=1,
            expected_return_pct=-0.5,
            risk_pct=3.0,
        )]
        logger.info("Fee tier: need $%.0f more volume, trading $%.0f of %s",
                     needed, trade_size, best["currency"])
        return ops

    def _detect_rebalance(self) -> List[Opportunity]:
        if time.time() - self.last_execution.get("rebalance", 0) < OP_COOLDOWN["rebalance"]:
            return []
        if not self.state:
            return []
        by_class: Dict[str, float] = defaultdict(float)
        for h in self.state.holdings.values():
            by_class[h["classification"]] += h["value"]

        ops = []
        for cls, target_pct in TARGET_ALLOCATION.items():
            current_pct = (by_class.get(cls, 0) / self.state.total_value * 100
                           if self.state.total_value > 0 else 0)
            diff = current_pct - target_pct * 100
            if abs(diff) <= 5 or abs(diff) * self.state.total_value / 100 < self.min_value:
                continue
            move_usd = abs(diff) / 100 * self.state.total_value

            if diff > 0:
                # Overweight — sell specific assets in this class
                candidates = sorted(
                    [h for h in self.state.holdings.values()
                     if h["classification"] == cls and h["currency"] not in ("USDC", "USDT", "DAI")
                     and h["value"] >= self.min_value and not self._is_static_currency(h["currency"])],
                    key=lambda x: (
                        x["allocation_pct"],
                        -self._graph_score_for_product(x.get("product_id") or f"{x['currency']}-USD"),
                    ),
                    reverse=True,
                )
                for h in candidates:
                    sell_size = min(move_usd, h["value"] * 0.5)
                    if sell_size >= self.min_value:
                        pid = self.cli.best_product(h["currency"], "SELL")
                        if not pid:
                            continue
                        ops.append(Opportunity(
                            opp_type=OpportunityType.REBALANCE,
                            currency=h["currency"],
                            side="SELL",
                            size_usd=sell_size,
                            reason=f"Rebalance: reduce {cls} by ${move_usd:.0f}",
                            priority=0.5,
                            product_id=pid,
                            entry_price_est=h.get("price", 0),
                            stop_loss_pct=0,
                            take_profit_pct=0,
                            holding_period_hours=0,
                            expected_return_pct=0,
                            risk_pct=0,
                        ))
                        break
            else:
                # Underweight — buy a specific asset in this class with USDC
                available = self._buy_capacity()
                if available < self.min_value or move_usd < self.min_value:
                    continue
                move_usd = min(move_usd, available)
                # Pick the best asset to buy (highest tradability, not already held large)
                buy_candidates = [
                    h for h in self.state.holdings.values()
                    if h["classification"] == cls and h["currency"] not in ("USDC", "USDT", "DAI") and not self._is_static_currency(h["currency"])
                ]
                if not buy_candidates:
                    # No existing holding in this class — use a representative asset
                    representatives = {"growth": "ETH", "speculative": "SOL", "safe": "BTC"}
                    target = representatives.get(cls, "ETH")
                    if self._is_static_currency(target):
                        continue
                    if cls == "growth":
                        buy_candidates = [{"currency": "ETH", "value": 0, "product_id": "ETH-USD"}]
                    elif cls == "speculative":
                        buy_candidates = [{"currency": "SOL", "value": 0, "product_id": "SOL-USD"}]
                    else:
                        buy_candidates = [{"currency": "BTC", "value": 0, "product_id": "BTC-USD"}]
                best = min(
                    buy_candidates,
                    key=lambda h: (
                        h["allocation_pct"],
                        -self._graph_score_for_product(h.get("product_id") or f"{h['currency']}-USD"),
                    ),
                )
                graph_multiplier = self._graph_multiplier_for_product(best.get("product_id") or f"{best['currency']}-USD", max_boost=0.20)
                bucket = "core" if best["currency"].upper() in CORE_LONG_TERM_ASSETS else "opportunity"
                move_usd = min(
                    move_usd * graph_multiplier,
                    self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap(),
                )
                pid = self.cli.best_product(best["currency"], "BUY")
                if not pid:
                    continue
                exit_plan = self._compute_exit_plan(
                    best["currency"], 0.65,
                    expected_return_pct=6.0,
                )
                ops.append(Opportunity(
                    opp_type=OpportunityType.REBALANCE,
                    currency=best["currency"],
                    side="BUY",
                    size_usd=move_usd,
                    reason=f"Rebalance: increase {cls} by ${move_usd:.0f}",
                    priority=0.5,
                    product_id=pid,
                    entry_price_est=best.get("price", 0),
                    stop_loss_pct=exit_plan["stop_loss_pct"],
                    take_profit_pct=exit_plan["take_profit_pct"],
                    holding_period_hours=exit_plan["holding_period_hours"],
                    expected_return_pct=exit_plan["expected_return_pct"],
                    risk_pct=exit_plan["risk_pct"],
                    meta={"capital_bucket": bucket, "graph_overlay": graph_multiplier, "graph_score": self._graph_score_for_product(best.get("product_id") or f"{best['currency']}-USD"), "exit_plan": exit_plan},
                ))
        if ops:
            logger.info("Rebalance: %d actions", len(ops))
        return ops

    def _holding_for_product(self, product_id: str) -> Optional[Dict]:
        """Find a holding dict by product_id (or base+USD)."""
        if not self.state:
            return None
        for h in self.state.holdings.values():
            if h.get("product_id") == product_id or f"{h.get('currency', '')}-USD" == product_id:
                return h
        return None

    def _detect_rebalance_bot(self) -> List[Opportunity]:
        """Drift-threshold portfolio rebalancer driven by the Rust RebalanceBot.

        Uses the configured allocation preset (REBALANCE_PRESET, default
        core_balanced) and only emits trades when max drift exceeds the threshold,
        selling only a slim slice (REBALANCE_PROFIT_TAKE) of overweight excess.
        """
        if time.time() - self.last_execution.get("rebalance_bot", 0) < OP_COOLDOWN.get("rebalance_bot", 3600):
            return []
        if not self.state or self.state.total_value <= 0:
            return []
        if self._rebalance_bot is None:
            try:
                from coinbase.src.rebalance_engine import RebalanceBot, RebalanceEngine
            except Exception as e:
                logger.warning("Rebalance bot unavailable: %s", e)
                return []
            self._rebalance_bot = RebalanceBot(
                engine=RebalanceEngine.from_preset(
                    self.rebalance_preset,
                    drift_threshold=self.rebalance_drift_threshold,
                    profit_take_pct=self.rebalance_profit_take_pct,
                    min_trade_notional=self.rebalance_min_notional,
                )
            )
        targets = self._rebalance_bot.engine.targets
        current_values = {}
        for pid in targets:
            h = self._holding_for_product(pid)
            current_values[pid] = float(h["value"]) if h else 0.0
        rec = self._rebalance_bot.engine.compute(current_values, self.state.total_value)
        if rec.max_drift < self.rebalance_drift_threshold:
            return []
        ops = []
        for o in rec.orders:
            base = o.asset.split("-")[0]
            pid = self.cli.best_product(base, o.side)
            if not pid:
                continue
            h = self._holding_for_product(o.asset)
            priority = min(1.0, 0.4 + abs(o.drift))
            reason = (f"Rebalance bot [{self.rebalance_preset}]: {o.side} {base} "
                      f"drift {o.drift:+.2%}")
            ops.append(Opportunity(
                opp_type=OpportunityType.REBALANCE_BOT,
                currency=base,
                side=o.side,
                size_usd=o.notional,
                reason=reason,
                priority=priority,
                product_id=pid,
                entry_price_est=float(h["price"]) if h else 0.0,
                meta={
                    "rebalance_preset": self.rebalance_preset,
                    "target_weight": o.target_weight,
                    "current_weight": o.current_weight,
                    "drift": o.drift,
                    "turnover": rec.turnover,
                },
            ))
        if ops:
            logger.info("Rebalance bot: %d actions (max_drift=%.3f)", len(ops), rec.max_drift)
            self.last_execution["rebalance_bot"] = time.time()
        return ops

    def _detect_stairstep(self) -> List[Opportunity]:
        """Range-bound stair-step profit taker driven by the Rust StairStepEngine.

        For each configured volatile symbol it auto-calibrates a grid around the
        live price on first sighting, then emits BUY as price falls to a grid
        level and SELL when price recovers enough to bank the spread.
        """
        if not self.stairstep_enabled or not self.state:
            return []
        if self._stairstep_engine is None:
            try:
                from coinbase.src.rebalance_engine import StairStepEngine
            except Exception as e:
                logger.warning("Stair-step engine unavailable: %s", e)
                return []
            self._stairstep_engine = StairStepEngine()
        ops = []
        for sym in self._stairstep_symbols:
            h = self._holding_for_product(sym)
            if not h or not h.get("price"):
                continue
            price = float(h["price"])
            if sym not in self._stairstep_engine._symbols:
                self._stairstep_engine.add_symbol(
                    sym, low=price * 0.85, high=price * 1.15, steps=5,
                    budget=max(self.min_value * 5, 50.0),
                    take_profit_pct=0.02, base_size_pct=0.2,
                )
            order = self._stairstep_engine.on_price(sym, price)
            if order is None:
                continue
            base = sym.split("-")[0]
            pid = self.cli.best_product(base, order.side)
            if not pid:
                continue
            state = self._stairstep_engine.state(sym)
            reason = f"Stair-step {order.side} {base} @ {price:.4f}"
            ops.append(Opportunity(
                opp_type=OpportunityType.STAIRSTEP,
                currency=base,
                side=order.side,
                size_usd=order.notional,
                reason=reason,
                priority=0.4,
                product_id=pid,
                entry_price_est=price,
                meta={
                    "symbol": sym,
                    "price": price,
                    "filled_buys": state[1],
                    "filled_sells": state[2],
                    "realized_pnl": state[4],
                    "next_buy_index": state[0],
                },
            ))
        return ops


    def _detect_volume_cycles(self) -> List[Opportunity]:
        if time.time() - self.last_execution.get("cycle", 0) < OP_COOLDOWN["cycle"]:
            return []
        ops = []
        now = time.time()
        for cur, h in self.state.holdings.items():
            if cur in ("USDC", "USDT", "DAI") or h["value"] < self.min_value:
                continue
            if self._is_static_currency(cur):
                continue
            if self.position_ages.get(cur, 0) == 0:
                self.position_ages[cur] = now  # first time seeing this hold
            age_hours = (now - self.position_ages[cur]) / 3600
            if age_hours >= CYCLE_MAX_HOLD_HOURS:
                pid = self.cli.best_product(cur, "SELL")
                if not pid:
                    continue
                ops.append(Opportunity(
                    opp_type=OpportunityType.VOLUME_CYCLE,
                    currency=pid,
                    side="SELL",
                    size_usd=h["value"],
                    reason=f"Volume cycle: close after {age_hours:.1f}h",
                    priority=0.3,
                    product_id=pid,
                    entry_price_est=h.get("price", 0),
                    stop_loss_pct=0,
                    take_profit_pct=0,
                    holding_period_hours=0,
                    expected_return_pct=0,
                    risk_pct=0,
                ))
        if ops:
            logger.info("Volume cycles: %d positions stale (age > %dh)", len(ops), CYCLE_MAX_HOLD_HOURS)
        return ops

    def _batch_uncached_backtests(
        self,
        candidates_with_sigs: List[Tuple[dict, str, List[float], List[float], List[float], List[float], List]],
    ) -> None:
        """Batch-backtest all un-cached strategy×product pairs across products in parallel.
        Uses ThreadPoolExecutor to run backtests for different products concurrently
        (Rust releases GIL during computation, so threads run in parallel).
        """
        # Collect all un-cached (strategy, currency, closes, volumes) across all products
        uncached_by_product: Dict[str, List] = {}
        for h, pid, closes, volumes, highs, lows, signals in candidates_with_sigs:
            currency = h["currency"]
            for sig in signals:
                ck = f"{sig.strategy}/{currency}"
                if ck in self._bt_cache:
                    continue
                if currency not in uncached_by_product:
                    uncached_by_product[currency] = []
                uncached_by_product[currency].append(
                    (sig.strategy, closes, volumes, highs, lows)
                )

        if not uncached_by_product:
            return

        # Submit one backtest job per product to the BT executor
        # Each product gets its strategies batched via backtest_multi_py (rayon)
        bt_futs = {}
        for currency, strategy_list in uncached_by_product.items():
            def _do_backtest(cur=currency, slist=strategy_list):
                strategies_for_rust = [
                    (s_name, cur, closes, vols, highs, lows)
                    for s_name, closes, vols, highs, lows in slist
                ]
                return _batch_backtest_rust(strategies_for_rust)
            bt_futs[_BT_EXECUTOR.submit(_do_backtest)] = currency

        # Collect results
        for fut in as_completed(bt_futs):
            currency = bt_futs[fut]
            try:
                batch_results = fut.result()
                for ck, verdict in batch_results.items():
                    if ck not in self._bt_cache:
                        self._bt_cache[ck] = verdict
                        self.store.save_bt_cache(ck, verdict)
                        if self.neo4j_store:
                            try:
                                self.neo4j_store.save_bt_cache(ck, verdict)
                            except Exception as e:
                                logger.warning("Neo4j BT cache write failed: %s", e)
            except Exception as e:
                logger.debug("Batch backtest failed for %s: %s", currency, e)

        # Fallback: backtest any remaining un-cached strategies sequentially (Python path)
        for h, pid, closes, volumes, highs, lows, signals in candidates_with_sigs:
            currency = h["currency"]
            for sig in signals:
                ck = f"{sig.strategy}/{currency}"
                if ck in self._bt_cache:
                    continue
                try:
                    verdict = _backtest_strategy(
                        sig.strategy, currency, closes, volumes,
                        highs=highs if highs else None,
                        lows=lows if lows else None,
                    )
                    self._bt_cache[ck] = verdict
                    self.store.save_bt_cache(ck, verdict)
                except Exception as e:
                    logger.debug("Backtest failed for %s: %s", ck, e)

    def _detect_strategy_signals(self) -> List[Opportunity]:
        """Run 5 strategies on each meaningful holding; return top signals as opportunities."""
        if time.time() - self.last_execution.get("strategy", 0) < OP_COOLDOWN["strategy"]:
            return []
        if not self.state:
            return []
        candidates = [
            h for h in self.state.holdings.values()
            if h["currency"] not in ("USDC", "USDT", "DAI") and h["value"] >= self.min_value and not self._is_static_currency(h["currency"])
        ]
        if not candidates:
            return []

        ops = []
        remaining_buy_capacity = self._buy_capacity()
        remaining_core_capacity = self._bucket_gap("core")
        remaining_opportunity_capacity = self._bucket_gap("opportunity")

        # Fetch candles via smart feed manager (shared cache) or fallback to CLI
        parsed_data: List[Tuple[dict, str, List[float], List[float], List[float], List[float]]] = []
        candidate_pids = [(h, h.get("product_id", f"{h['currency']}-USD")) for h in candidates]

        if self._feed_mgr:
            all_pids = [pid for _, pid in candidate_pids]
            batched = self._feed_mgr.get_candles_batch(all_pids, granularity=3600, limit=100)
            for h, pid in candidate_pids:
                candles = batched.get(pid)
                if not candles or len(candles) < 30:
                    continue
                closes, volumes, highs, lows = [], [], [], []
                for c in reversed(candles):
                    if isinstance(c, dict):
                        closes.append(to_float(c.get("close", 0)))
                        volumes.append(to_float(c.get("volume", 0)))
                        highs.append(to_float(c.get("high", 0)))
                        lows.append(to_float(c.get("low", 0)))
                    elif isinstance(c, (list, tuple)) and len(c) >= 6:
                        closes.append(to_float(c[4]))
                        volumes.append(to_float(c[5]))
                        highs.append(to_float(c[2]))
                        lows.append(to_float(c[1]))
                if len(closes) < 30:
                    continue
                parsed_data.append((h, pid, closes, volumes, highs, lows))
        else:
            candle_futs = {}
            for h, pid in candidate_pids:
                fut = _IO_EXECUTOR.submit(self.cli.get_candles, pid, "1h", 100)
                candle_futs[fut] = (h, pid)
            for fut in as_completed(candle_futs):
                h, pid = candle_futs[fut]
                try:
                    candles = fut.result()
                except Exception as e:
                    logger.debug("Candle fetch failed for %s: %s", pid, e)
                    continue
                if not candles or len(candles) < 30:
                    continue
                closes, volumes, highs, lows = [], [], [], []
                for c in reversed(candles):
                    if isinstance(c, dict):
                        closes.append(to_float(c.get("close", 0)))
                        volumes.append(to_float(c.get("volume", 0)))
                        highs.append(to_float(c.get("high", 0)))
                        lows.append(to_float(c.get("low", 0)))
                    elif isinstance(c, (list, tuple)) and len(c) >= 6:
                        closes.append(to_float(c[4]))
                        volumes.append(to_float(c[5]))
                        highs.append(to_float(c[2]))
                        lows.append(to_float(c[1]))
                if len(closes) < 30:
                    continue
                parsed_data.append((h, pid, closes, volumes, highs, lows))

        # Batch-compute all signals via vectorized compute backend (GPU/NumPy)
        try:
            products_list = [(h["currency"] + "-USD", h["classification"]) for h, _, _, _, _, _ in parsed_data]
            closes_dict = {pid: c for _, pid, c, _, _, _ in parsed_data}
            volumes_dict = {pid: v for _, pid, _, v, _, _ in parsed_data}
            highs_dict = {pid: hi for _, pid, _, _, hi, _ in parsed_data}
            lows_dict = {pid: lo for _, pid, _, _, _, lo in parsed_data}
            batch_results = _batch_signals_fast(products_list, closes_dict, volumes_dict, highs_dict, lows_dict)
        except Exception as e:
            logger.debug("Batch signal generation failed: %s", e)
            batch_results = {}

        # Phase 1: collect signals for all products (batch or fallback)
        candidates_with_sigs: List[Tuple[dict, str, List[float], List[float], List[float], List[float], List]] = []
        for h, pid, closes, volumes, highs, lows in parsed_data:
            currency = h["currency"]
            pid_results = batch_results.get(pid) if batch_results else None
            if pid_results:
                signals = []
                for s_name, action in pid_results.items():
                    if action != "HOLD":
                        signals.append(StrategySignal(
                            strategy=s_name, action=action, confidence=0.5,
                            reason=f"batch:{s_name}",
                        ))
            else:
                signals = _run_strategies(
                    currency=currency, asset_class=h["classification"],
                    closes=closes, volumes=volumes, current_price=h["price"],
                    highs=highs if highs else None, lows=lows if lows else None,
                )
            
            # ── Regime filtering: skip strategies unsuited for current market regime ──
            if signals and highs and lows and len(closes) >= 30:
                regime = _detect_market_regime(highs, lows, closes)
                filtered_signals = []
                for sig in signals:
                    strat = sig.strategy
                    # Skip trend strategies in ranging markets
                    if regime == "ranging" and strat in TREND_STRATEGIES:
                        logger.debug("  Skipping %s (trend strategy) in %s regime for %s", strat, regime, currency)
                        continue
                    # Skip mean-reversion in trending markets
                    if regime == "trending" and strat in MEAN_REVERSION_STRATEGIES:
                        logger.debug("  Skipping %s (mean-reversion) in %s regime for %s", strat, regime, currency)
                        continue
                    # Skip volatility strategies in quiet markets
                    if regime == "quiet" and strat in VOLATILITY_STRATEGIES:
                        logger.debug("  Skipping %s (vol strategy) in %s regime for %s", strat, regime, currency)
                        continue
                    filtered_signals.append(sig)
                signals = filtered_signals
            
            if signals:
                candidates_with_sigs.append((h, pid, closes, volumes, highs, lows, signals))

        # Phase 2: batch-backtest all un-cached strategy×product pairs in parallel
        self._batch_uncached_backtests(candidates_with_sigs)

        # Phase 3: process each candidate with cached backtest results
        for h, pid, closes, volumes, highs, lows, signals in candidates_with_sigs:
            currency = h["currency"]
            buy_capacity = self._buy_capacity()

            passed_signals = []
            for sig in signals:
                if sig.action == "BUY" and buy_capacity < self.min_value:
                    continue
                if sig.action == "SELL" and h["value"] < self.min_value:
                    continue

                cache_key = f"{sig.strategy}/{currency}"
                verdict = self._bt_cache.get(cache_key)
                if verdict is None:
                    continue

                logger.info("  BT %s/%s: trades=%d WR=%.0f%% Sharpe=%.1f Pf=%.1f dd=%.1f%% → %s (%s)",
                             sig.strategy, currency, verdict.total_trades,
                             verdict.win_rate * 100, verdict.sharpe_ratio,
                             verdict.profit_factor, verdict.max_drawdown_pct,
                             "PASS" if verdict.passed else "SKIP", verdict.reason)

                if verdict.passed:
                    passed_signals.append(sig)

            if not passed_signals:
                continue

            # Record pulses for signal quality tracking
            current_price = h.get("price", 0)
            for sig in passed_signals:
                self._record_pulse(
                    product_id=pid,
                    strategy=sig.strategy,
                    direction=sig.action,
                    confidence=sig.confidence,
                    price=current_price
                )

            # Prune stale pulses periodically
            if self._tick_count % 10 == 0:
                self._prune_pulses()

            # Detect regime for regime-aware strategy weighting
            regime = "neutral"
            if highs and lows and len(closes) >= 30:
                regime = _detect_market_regime(highs, lows, closes)
            
            # Apply regime weights to signals before aggregation
            regime_weighted_signals = []
            for sig in passed_signals:
                weight = self._regime_strategy_weight(sig.strategy, regime)
                # Create modified signal with adjusted confidence
                mod_sig = StrategySignal(
                    strategy=sig.strategy,
                    action=sig.action,
                    confidence=min(sig.confidence * weight, 1.0),
                    reason=sig.reason,
                )
                regime_weighted_signals.append(mod_sig)

            # Aggregate through confidence matrix
            bt_cache_dict = {
                k: {
                    "win_rate": v.win_rate,
                    "sharpe_ratio": v.sharpe_ratio,
                    "profit_factor": v.profit_factor,
                }
                for k, v in self._bt_cache.items()
            }
            matrix = ConfidenceMatrix(bt_cache=bt_cache_dict)
            aggregated = matrix.aggregate(
                regime_weighted_signals,
                asset_class=h["classification"],
                currency=currency,
            )

            for agg in aggregated[:2]:
                side = agg.direction
                bucket = "core" if currency.upper().replace("-USD", "") in CORE_LONG_TERM_ASSETS else "opportunity"
                
                # Use Kelly sizing based on backtest stats from aggregated strategies
                best_verdict = None
                for s in agg.strategies:
                    ck = f"{s}/{currency}"
                    v = self._bt_cache.get(ck)
                    if v and v.passed and (best_verdict is None or v.win_rate > best_verdict.win_rate):
                        best_verdict = v
                
                if best_verdict:
                    size = self._kelly_size(
                        win_rate=best_verdict.win_rate,
                        avg_win_pct=best_verdict.total_return_pct / max(best_verdict.total_trades, 1) * 100 if best_verdict.total_trades > 0 else 2.0,
                        avg_loss_pct=best_verdict.max_drawdown_pct if best_verdict.max_drawdown_pct > 0 else 1.5,
                        confidence=agg.confidence,
                        kelly_fraction=0.25,
                        max_notional=5000.0,
                        min_notional=self.min_value,
                        capital_limit=(remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity),
                    )
                else:
                    size = self._risk_reward_size(
                        expected_return_pct=max(agg.confidence * 10.0, 0.5),
                        risk_pct=max((1.0 - agg.confidence) * 8.0 + 2.0, 1.0),
                        confidence=agg.confidence,
                        liquidity=min(to_float(h.get("liquidity_score", 0.7) or 0.7), 1.0),
                        cap_pct=0.01,
                        max_notional=4000.0,
                        min_notional=self.min_value,
                        capital_limit=(remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity),
                    )
                if side == "SELL":
                    size = min(size, h["value"])
                else:
                    batch_cap = self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap()
                    size = min(size, remaining_buy_capacity, batch_cap, remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity)
                    remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                    if bucket == "core":
                        remaining_core_capacity = max(remaining_core_capacity - size, 0.0)
                    else:
                        remaining_opportunity_capacity = max(remaining_opportunity_capacity - size, 0.0)

                # Pulse quality filter: check if aggregated strategies have valid pulses
                pulse_valid = False
                for s in agg.strategies:
                    pulse = self._signal_pulses.get(self._pulse_key(pid, s, side))
                    if pulse and self._is_pulse_valid(pulse):
                        pulse_valid = True
                        break
                
                # Also check cluster exposure limit
                if side == "BUY" and not self._check_cluster_limit(currency, size):
                    logger.debug("  Skipping %s: cluster exposure limit reached", currency)
                    continue
                
                if not pulse_valid:
                    logger.debug("  Skipping %s: pulse quality insufficient", currency)
                    continue

                # Apply ConfidenceEngine modifiers if available
                final_confidence = agg.confidence
                if self.confidence_engine:
                    try:
                        class _Sig: pass
                        sig_stub = _Sig()
                        sig_stub.symbol = currency
                        sig_stub.strategy = "confidence_matrix_aggregated"
                        sig_stub.strength = agg.confidence
                        sig_stub.action = "BUY" if side == "BUY" else "SELL"
                        mod_result = self.confidence_engine.apply_modifiers(
                            signal=sig_stub,
                            market_data={
                                "spread": h.get("spread", 0.0),
                                "volume": h.get("volume_24h", 0),
                                "price": h.get("price", 0),
                            },
                            regime=_detect_regime({"change_pct": h.get("change_24h", 0)}),
                            market_leaders=[],  # No per-leader data available; cross-correlation penalty skipped
                            sentiment_score=0.0,
                            global_consensus=0.5,
                        )
                        final_confidence = mod_result.modified_confidence
                    except Exception as e:
                        logger.debug("ConfidenceEngine modifier failed: %s", e)

                buy_pid = self.cli.best_product(currency, "BUY")
                sell_pid = self.cli.best_product(currency, "SELL")
                use_pid = buy_pid if side == "BUY" else sell_pid
                if not use_pid:
                    continue

                graph_multiplier = self._graph_multiplier_for_product(use_pid, max_boost=0.25)
                final_confidence = min(0.95, final_confidence * graph_multiplier)
                size = min(size * graph_multiplier, self._buy_capacity() if side == "BUY" else h["value"])
                if size < self.min_value:
                    continue

                daily_chg = abs(to_float(h.get("change_24h", 0)))
                exit_plan = self._compute_exit_plan(
                    currency, final_confidence,
                    expected_return_pct=max(final_confidence * 10.0, 0.5),
                    trade_style="momentum" if side == "BUY" else "mean_reversion",
                    side=side,
                    volatility_pct=max(daily_chg * 1.5, 5.0),
                )
                ops.append(Opportunity(
                    opp_type=OpportunityType.STRATEGY_SIGNAL,
                    currency=currency,
                    side=side,
                    size_usd=size,
                    reason=f"{agg.best_reason} (agg_conf={agg.confidence:.2f}, graph={graph_multiplier:.2f}, {agg.strategy_count} strats)",
                    priority=self._latency_adjusted_priority(
                        min(final_confidence * 0.8 + 0.1, 0.95),
                        trade_style="momentum" if side == "BUY" else "mean_reversion",
                    ),
                    product_id=use_pid,
                    entry_price_est=h.get("price", 0),
                    stop_loss_pct=exit_plan["stop_loss_pct"],
                    take_profit_pct=exit_plan["take_profit_pct"],
                    holding_period_hours=exit_plan["holding_period_hours"],
                    expected_return_pct=exit_plan["expected_return_pct"],
                    risk_pct=exit_plan["risk_pct"],
                    meta={
                        "aggregated": True,
                        "confidence": agg.confidence,
                        "graph_overlay": graph_multiplier,
                        "graph_score": self._graph_score_for_product(use_pid),
                        "strategy_count": agg.strategy_count,
                        "strategies": agg.strategies,
                        "agreeing_groups": agg.agreeing_groups,
                        "capital_bucket": bucket,
                        "trade_style": "momentum" if side == "BUY" else "mean_reversion",
                        "exit_plan": exit_plan,
                    },
                ))

        if ops:
            logger.info("Strategy signals: %d opportunities", len(ops))
        return ops

    def _detect_funding_and_onchain_signals(self) -> List[Opportunity]:
        """Detect funding rate contrarian + on-chain flow alpha signals.

        FundingRateContrarian — global Binance perp funding signal applied
        as a meta-signal on BTC-USD (most liquid proxy).

        OnChainFlowStrategy — per-product CoinGecko exchange flow volume
        anomaly signals for all tracked positions.
        """
        cooldown = OP_COOLDOWN.get("funding_onchain", 600)
        if time.time() - self.last_execution.get("funding_onchain", 0) < cooldown:
            return []
        if not self.state:
            return []

        ops: List[Opportunity] = []
        remaining_buy_capacity = self._buy_capacity()
        sell_candidates = {
            h["currency"]: h for h in self.state.holdings.values()
            if h["value"] >= self.min_value
        } if self.state else {}

        # ── Step 1: FundingRateContrarian ────────────────────────────
        if self._funding_contrarian:
            try:
                btc_price = self._current_price_for_symbol("BTC-USD")
                if btc_price > 0:
                    funding_sig = self._funding_contrarian.on_bar(
                        close=btc_price,
                        closes=[btc_price],
                        volumes=None,
                        highs=None,
                        lows=None,
                        currency="BTC",
                    )
                    if funding_sig and funding_sig.action in ("BUY", "SELL"):
                        side = funding_sig.action
                        conf = min(funding_sig.confidence, 0.70)
                        if side == "BUY":
                            size = self._risk_reward_size(
                                expected_return_pct=conf * 10.0,
                                risk_pct=max((1.0 - conf) * 8.0 + 2.0, 1.0),
                                confidence=conf,
                                liquidity=0.6,
                                cap_pct=0.01,
                                max_notional=2000.0,
                                min_notional=self.min_value,
                                capital_limit=remaining_buy_capacity,
                            )
                            if size >= self.min_value:
                                remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                        else:
                            sell_holding = sell_candidates.get("BTC")
                            if sell_holding:
                                size = min(
                                    self._risk_reward_size(
                                        expected_return_pct=conf * 8.0,
                                        risk_pct=max((1.0 - conf) * 8.0 + 2.0, 1.0),
                                        confidence=conf,
                                        liquidity=0.6,
                                        cap_pct=0.015,
                                        max_notional=3000.0,
                                        min_notional=self.min_value,
                                    ),
                                    sell_holding.get("value", 0),
                                )
                            else:
                                size = 0
                        if size >= self.min_value:
                            exit_plan = self._compute_exit_plan(
                                "BTC", conf,
                                expected_return_pct=conf * 10.0,
                                trade_style="mean_reversion" if side == "BUY" else "momentum",
                                volatility_pct=45.0,
                            )
                            ops.append(Opportunity(
                                opp_type=OpportunityType.STRATEGY_SIGNAL,
                                currency="BTC",
                                side=side,
                                size_usd=size,
                                reason=funding_sig.reason,
                                priority=self._latency_adjusted_priority(
                                    conf * 0.7, trade_style="mean_reversion",
                                ),
                                product_id="BTC-USD",
                                entry_price_est=btc_price,
                                stop_loss_pct=exit_plan["stop_loss_pct"],
                                take_profit_pct=exit_plan["take_profit_pct"],
                                holding_period_hours=exit_plan["holding_period_hours"],
                                expected_return_pct=exit_plan["expected_return_pct"],
                                risk_pct=exit_plan["risk_pct"],
                                meta={
                                    "source": "funding_rate",
                                    "strategy": "funding_contrarian",
                                    "confidence": conf,
                                    "trade_style": "mean_reversion",
                                    "signal_type": "funding_global",
                                    "exit_plan": exit_plan,
                                },
                            ))
            except Exception as e:
                logger.debug("Funding rate contrarian detection failed: %s", e)

        # ── Step 2: OnChainFlowStrategy ─────────────────────────────
        if self._onchain_flow:
            try:
                tracked_pids = []
                for h in self.state.holdings.values():
                    if h["currency"] not in ("USDC", "USDT", "DAI") and h["value"] >= self.min_value:
                        pid = h.get("product_id", f"{h['currency']}-USD")
                        tracked_pids.append(pid)
                if tracked_pids:
                    onchain_results = self._onchain_flow.get_signals(tracked_pids)
                    for sig in onchain_results:
                        if sig.get("action") not in ("BUY", "SELL"):
                            continue
                        side = sig["action"]
                        pid = sig.get("product_id", "")
                        currency = sig.get("currency", "")
                        conf = min(sig.get("confidence", 0.0), 0.70)
                        price = sig.get("price", 0.0)
                        if price <= 0:
                            price = self._current_price_for_symbol(pid, fallback=0.0)
                        if price <= 0 or conf < 0.25:
                            continue
                        if side == "BUY" and remaining_buy_capacity < self.min_value:
                            continue
                        sell_holding = sell_candidates.get(currency)
                        if side == "SELL" and (not sell_holding or sell_holding.get("value", 0) < self.min_value):
                            continue
                        size = self._risk_reward_size(
                            expected_return_pct=conf * 8.0,
                            risk_pct=max((1.0 - conf) * 8.0 + 3.0, 1.5),
                            confidence=conf,
                            liquidity=0.55,
                            cap_pct=0.008,
                            max_notional=1500.0,
                            min_notional=self.min_value,
                            capital_limit=remaining_buy_capacity if side == "BUY" else None,
                        )
                        if side == "SELL":
                            size = min(size, sell_holding.get("value", 0)) if sell_holding else 0
                        if size < self.min_value:
                            continue
                        if side == "BUY":
                            remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                        vol_anomaly = sig.get("volume_anomaly", 0.0)
                        price_trend = sig.get("price_trend", 0.0)
                        exit_plan = self._compute_exit_plan(
                            currency, conf,
                            expected_return_pct=conf * 8.0,
                            trade_style="mean_reversion" if side == "BUY" else "momentum",
                            volatility_pct=max(abs(price_trend) * 50.0 + 20.0, 20.0),
                        )
                        ops.append(Opportunity(
                            opp_type=OpportunityType.STRATEGY_SIGNAL,
                            currency=currency,
                            side=side,
                            size_usd=size,
                            reason=sig.get("reason", f"onchain:{side}_{pid}_vol={vol_anomaly:.1f}x"),
                            priority=self._latency_adjusted_priority(
                                conf * 0.65, trade_style="mean_reversion",
                            ),
                            product_id=pid,
                            entry_price_est=price,
                            stop_loss_pct=exit_plan["stop_loss_pct"],
                            take_profit_pct=exit_plan["take_profit_pct"],
                            holding_period_hours=exit_plan["holding_period_hours"],
                            expected_return_pct=exit_plan["expected_return_pct"],
                            risk_pct=exit_plan["risk_pct"],
                            meta={
                                "source": "onchain_flow",
                                "strategy": "onchain_flow",
                                "confidence": conf,
                                "volume_anomaly": vol_anomaly,
                                "price_trend": price_trend,
                                "signal_type": "exchange_flow",
                                "trade_style": "mean_reversion",
                                "exit_plan": exit_plan,
                            },
                        ))
            except Exception as e:
                logger.debug("OnChain flow detection failed: %s", e)

        # ── Step 3: ExchangeNetflowSignal (on-chain chain analytics) ──
        if self._exchange_netflow:
            try:
                for cur, h in self.state.holdings.items():
                    if cur in ("USDC", "USDT", "DAI") or h["value"] < self.min_value:
                        continue
                    pid = h.get("product_id", f"{cur}-USD")
                    price = to_float(h.get("price", 0)) or self._current_price_for_symbol(pid, fallback=0.0)
                    if price <= 0:
                        continue
                    nf_sig = self._exchange_netflow.on_bar(
                        close=price, closes=[price], volumes=None, currency=pid,
                    )
                    if not nf_sig or nf_sig.action not in ("BUY", "SELL"):
                        continue
                    side = nf_sig.action
                    conf = min(nf_sig.confidence, 0.70)
                    if conf < 0.25:
                        continue
                    if side == "BUY" and remaining_buy_capacity < self.min_value:
                        continue
                    sell_h = sell_candidates.get(cur)
                    if side == "SELL" and (not sell_h or sell_h.get("value", 0) < self.min_value):
                        continue
                    size = self._risk_reward_size(
                        expected_return_pct=conf * 8.0,
                        risk_pct=max((1.0 - conf) * 8.0 + 3.0, 1.5),
                        confidence=conf,
                        liquidity=0.55,
                        cap_pct=0.008,
                        max_notional=1500.0,
                        min_notional=self.min_value,
                        capital_limit=remaining_buy_capacity if side == "BUY" else None,
                    )
                    if side == "SELL":
                        size = min(size, sell_h.get("value", 0)) if sell_h else 0
                    if size < self.min_value:
                        continue
                    if side == "BUY":
                        remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                    exit_plan = self._compute_exit_plan(
                        cur, conf,
                        expected_return_pct=conf * 8.0,
                        trade_style="mean_reversion" if side == "BUY" else "momentum",
                        volatility_pct=40.0,
                    )
                    ops.append(Opportunity(
                        opp_type=OpportunityType.STRATEGY_SIGNAL,
                        currency=pid,
                        side=side,
                        size_usd=size,
                        reason=nf_sig.reason,
                        priority=self._latency_adjusted_priority(conf * 0.65, trade_style="mean_reversion"),
                        product_id=pid,
                        entry_price_est=price,
                        stop_loss_pct=exit_plan["stop_loss_pct"],
                        take_profit_pct=exit_plan["take_profit_pct"],
                        holding_period_hours=exit_plan["holding_period_hours"],
                        expected_return_pct=exit_plan["expected_return_pct"],
                        risk_pct=exit_plan["risk_pct"],
                        meta={
                            "source": "onchain_netflow",
                            "strategy": "exchange_netflow",
                            "confidence": conf,
                            "trade_style": "mean_reversion",
                            "exit_plan": exit_plan,
                        },
                    ))
            except Exception as e:
                logger.debug("Exchange netflow detection failed: %s", e)

        # ── Step 4: StablecoinFlowSignal (chain liquidity / risk gauge) ──
        if self._stablecoin_flow:
            try:
                price = self._current_price_for_symbol("BTC-USD", fallback=0.0)
                if price > 0:
                    sf_sig = self._stablecoin_flow.on_bar(
                        close=price, closes=[price], volumes=None, currency="BTC-USD",
                    )
                    if sf_sig and sf_sig.action in ("BUY", "SELL"):
                        side = sf_sig.action
                        conf = min(sf_sig.confidence, 0.70)
                        if conf >= 0.25:
                            if side == "BUY" and remaining_buy_capacity < self.min_value:
                                pass
                            else:
                                sell_h = sell_candidates.get("BTC")
                                if side == "SELL" and (not sell_h or sell_h.get("value", 0) < self.min_value):
                                    pass
                                else:
                                    size = self._risk_reward_size(
                                        expected_return_pct=conf * 8.0,
                                        risk_pct=max((1.0 - conf) * 8.0 + 3.0, 1.5),
                                        confidence=conf,
                                        liquidity=0.7,
                                        cap_pct=0.01,
                                        max_notional=2500.0,
                                        min_notional=self.min_value,
                                        capital_limit=remaining_buy_capacity if side == "BUY" else None,
                                    )
                                    if side == "SELL" and sell_h:
                                        size = min(size, sell_h.get("value", 0))
                                    if size >= self.min_value:
                                        if side == "BUY":
                                            remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                                        exit_plan = self._compute_exit_plan(
                                            "BTC", conf,
                                            expected_return_pct=conf * 8.0,
                                            trade_style="mean_reversion" if side == "BUY" else "momentum",
                                            volatility_pct=45.0,
                                        )
                                        ops.append(Opportunity(
                                            opp_type=OpportunityType.STRATEGY_SIGNAL,
                                            currency="BTC",
                                            side=side,
                                            size_usd=size,
                                            reason=sf_sig.reason,
                                            priority=self._latency_adjusted_priority(conf * 0.7, trade_style="mean_reversion"),
                                            product_id="BTC-USD",
                                            entry_price_est=price,
                                            stop_loss_pct=exit_plan["stop_loss_pct"],
                                            take_profit_pct=exit_plan["take_profit_pct"],
                                            holding_period_hours=exit_plan["holding_period_hours"],
                                            expected_return_pct=exit_plan["expected_return_pct"],
                                            risk_pct=exit_plan["risk_pct"],
                                            meta={
                                                "source": "stablecoin_flow",
                                                "strategy": "stablecoin_flow",
                                                "confidence": conf,
                                                "trade_style": "mean_reversion",
                                                "exit_plan": exit_plan,
                                            },
                                        ))
            except Exception as e:
                logger.debug("Stablecoin flow detection failed: %s", e)

        self.last_execution["funding_onchain"] = time.time()
        if ops:
            logger.info("Funding/OnChain signals: %d opportunities", len(ops))
        return ops

    def _detect_accumulator_signals(self) -> List[Opportunity]:
        if not _HAS_ACCUMULATOR or UnifiedSignalAccumulator is None:
            return []
        if time.time() - self.last_execution.get("accumulator", 0) < OP_COOLDOWN["accumulator"]:
            return []
        try:
            acc = UnifiedSignalAccumulator(max_queue_size=50)
            signals = acc.accumulate()
        except Exception as e:
            logger.warning("Unified signal accumulator error: %s", e)
            return []

        ops = []
        remaining_buy_capacity = self._buy_capacity()
        remaining_core_capacity = self._bucket_gap("core")
        remaining_opportunity_capacity = self._bucket_gap("opportunity")
        for sig in signals:
            if sig.action not in ("BUY", "SELL"):
                continue
            if sig.final_confidence < 0.15:
                continue
            currency = sig.symbol.replace("-USD", "")
            if self._is_static_currency(currency):
                continue
            classification = classify_asset(currency)
            bucket = "core" if currency.upper() in CORE_LONG_TERM_ASSETS else "opportunity"

            graph_multiplier = 1.0
            buy_pid = self.cli.best_product(currency, "BUY")
            sell_pid = self.cli.best_product(currency, "SELL")
            use_pid = buy_pid if sig.action == "BUY" else sell_pid
            if not use_pid:
                continue
            graph_multiplier = self._graph_multiplier_for_product(use_pid, max_boost=0.25)

            final_confidence = min(0.95, sig.final_confidence * graph_multiplier)
            holding = self.state.holdings.get(currency) if self.state else None
            price = (holding.get("price", 0) or 0) if holding else sig.market_data.get("price", 0)
            liquidity = min(float(holding.get("liquidity_score", 0.7) or 0.7), 1.0) if holding else 0.7

            size = self._risk_reward_size(
                expected_return_pct=max(final_confidence * 8.0, 0.5),
                risk_pct=max((1.0 - final_confidence) * 10.0 + 2.0, 1.0),
                confidence=final_confidence,
                liquidity=liquidity,
                cap_pct=0.008,
                max_notional=2500.0,
                min_notional=self.min_value,
                capital_limit=(remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity),
            )
            if sig.action == "SELL" and holding:
                size = min(size, holding.get("value", 0))
            else:
                batch_cap = self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap()
                cap = remaining_buy_capacity if sig.action == "BUY" else size
                size = min(size, cap, batch_cap, remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity)
                if sig.action == "BUY":
                    remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                if bucket == "core":
                    remaining_core_capacity = max(remaining_core_capacity - size, 0.0)
                else:
                    remaining_opportunity_capacity = max(remaining_opportunity_capacity - size, 0.0)

            if size < self.min_value:
                continue

            classification_label = {
                "NewsSentiment": "news",
                "NewsHackAlert": "news",
                "NewsRegulationWatch": "news",
                "NewsAdoptionSignal": "news",
                "NewsTechSignal": "news",
            }.get(sig.strategy_name.split(":", 1)[0] if ":" in sig.strategy_name else sig.strategy_name, sig.strategy_name)

            trade_style = "event"
            strategy_name = sig.strategy_name.lower()
            if "momentum" in strategy_name or "trend" in strategy_name:
                trade_style = "momentum"
            elif "reversion" in strategy_name or "mean" in strategy_name:
                trade_style = "mean_reversion"
            exit_plan = self._compute_exit_plan(
                currency, final_confidence,
                expected_return_pct=max(final_confidence * 10.0, 0.5),
                trade_style=trade_style,
                volatility_pct=max(abs(to_float(sig.market_data.get("change_pct", 0))) * 18.0 + 12.0, 12.0),
                hold_hint_hours=24.0 if trade_style == "event" else 36.0,
            )

            ops.append(Opportunity(
                opp_type=OpportunityType.ACCUMULATOR_SIGNAL,
                currency=currency,
                side=sig.action,
                size_usd=size,
                reason=f"{sig.strategy_name}: {sig.signal_reason[:80]}",
                priority=min(final_confidence * 0.85, 0.9),
                product_id=use_pid,
                    entry_price_est=price,
                stop_loss_pct=exit_plan["stop_loss_pct"],
                take_profit_pct=exit_plan["take_profit_pct"],
                holding_period_hours=exit_plan["holding_period_hours"],
                expected_return_pct=exit_plan["expected_return_pct"],
                risk_pct=exit_plan["risk_pct"],
                meta={
                    "strategy_name": sig.strategy_name,
                    "final_confidence": round(final_confidence, 3),
                    "base_confidence": round(sig.base_confidence, 3),
                    "opportunity_score": sig.opportunity_score,
                    "capital_bucket": bucket,
                    "graph_overlay": graph_multiplier,
                    "graph_score": self._graph_score_for_product(use_pid),
                    "signal_type": classification_label,
                    "trade_style": trade_style,
                    "market_data": sig.market_data,
                    "exit_plan": exit_plan,
                },
            ))

        if ops:
            logger.info("Accumulator signals: %d opportunities", len(ops))
        return ops

    def _detect_aggregator_signals(self) -> List[Opportunity]:
        """Run SignalAggregator on top-N universe pairs, rank cross-product, create opportunities."""
        if not _HAS_AGGREGATOR or SignalAggregator is None:
            return []
        if time.time() - self.last_execution.get("aggregator", 0) < OP_COOLDOWN["aggregator"]:
            return []

        try:
            from coinbase.src.pair_discovery import top_coinbase_pairs
            import urllib3, json

            pairs = top_coinbase_pairs(n=50, min_volume_usd=500_000)
            if not pairs:
                return []
            pids = [p[0] for p in pairs]

            # Fetch candles via smart feed manager (shared cache) or fallback
            if self._feed_mgr:
                candles = self._feed_mgr.get_candles_batch(pids, granularity=3600, limit=100)
            else:
                from coinbase.src.rest_feed import fetch_candles_batch
                candles = fetch_candles_batch(pids, granularity=3600, limit=100, max_workers=12)

            # Retry empty pairs individually (rate-limit recovery)
            http = urllib3.PoolManager()
            for pid, _ in pairs:
                if pid not in candles or not candles[pid]:
                    try:
                        r = http.request("GET",
                            f"https://api.exchange.coinbase.com/products/{pid}/candles?granularity=3600&limit=100",
                            timeout=15)
                        if r.status == 200:
                            data = json.loads(r.data)
                            if isinstance(data, list) and len(data) >= 30:
                                candles[pid] = data
                    except Exception:
                        pass

            closes = {pid: [float(c[4]) for c in clist] for pid, clist in candles.items()}
            volumes = {pid: [float(c[5]) for c in clist] for pid, clist in candles.items()}
            highs = {pid: [float(c[2]) for c in clist] for pid, clist in candles.items()}
            lows = {pid: [float(c[3]) for c in clist] for pid, clist in candles.items()}
            products = [(pid, base) for pid, base in pairs if pid in closes and len(closes[pid]) >= 60]

            if len(products) < 3:
                return []

            agg = SignalAggregator()
            results = agg.scan_universe(products, closes, volumes, highs, lows, min_candles=60)
        except Exception as e:
            logger.warning("Aggregator scan failed: %s", e, exc_info=True)
            return []

        self.last_execution["aggregator"] = time.time()

        # Filter to actionable signals
        actionable = [r for r in results if r.direction in ("BUY", "SELL") and r.priority >= 0.05]
        if not actionable:
            return []

        # Check capacities
        remaining_buy = self._buy_capacity()
        sell_candidates = {
            h["currency"]: h for h in self.state.holdings.values()
            if h["value"] >= self.min_value
        } if self.state else {}

        ops = []
        for us in actionable[:5]:
            direction = us.direction
            currency = us.base
            pid = us.product_id

            # Skip if BUY but no capacity
            if direction == "BUY" and remaining_buy < self.min_value:
                continue
            # Skip if SELL but no position
            if direction == "SELL" and currency not in sell_candidates:
                continue

            # Use existing trend for sizing boost (trend-aligned trades get higher confidence)
            trend_aligned = (direction == "BUY" and us.trend_score > 0) or (direction == "SELL" and us.trend_score < 0)
            confidence = abs(us.unified_score) * (1.2 if trend_aligned else 0.9)
            confidence = min(confidence, 0.99)

            # Estimate volatility from candle data
            pix_closes = closes.get(pid, [])
            vol = self._estimate_trade_volatility_pct(pix_closes) if pix_closes else 60.0

            # Trade style based on top strategies
            momentum_strats = {"ema_cross", "macd", "trix", "adx", "psar", "hma", "aroon", "force_idx", "vpt",
                               "kama", "dmi_cross", "vma", "dpo", "elder_ray", "ichimoku",
                               "mom_accel", "linreg_slope", "multi_rsi", "kst"}
            reversion_strats = {"rsi_revert", "zscore_revert", "boll_break", "vwap_revert", "cmo", "williams_r",
                                "keltner", "donchian", "scci", "stoch", "rvi", "de_marker",
                                "gap_revert", "envelope", "std_channel", "atr_channel"}
            has_momentum = any(s in momentum_strats for s in us.top_strategies)
            has_reversion = any(s in reversion_strats for s in us.top_strategies)
            trade_style = "momentum" if has_momentum else ("mean_reversion" if has_reversion else "momentum")

            # Read current price from ticker or stored state
            cur_price = us.price
            if cur_price <= 0:
                cur_price = self._current_price_for_symbol(pid)
            if cur_price <= 0:
                continue

            # Compute exit plan
            exit_plan = self._compute_exit_plan(
                currency=currency,
                confidence=confidence,
                expected_return_pct=abs(us.trend_score) * 5.0 if us.trend_score != 0 else 3.0,
                trade_style=trade_style,
                volatility_pct=vol,
            )

            # Size the trade
            size = self._risk_reward_size(
                expected_return_pct=exit_plan["expected_return_pct"],
                risk_pct=exit_plan["risk_pct"],
                confidence=confidence,
                liquidity=min(1.0, us.backtest_quality + 0.3),
                cap_pct=0.012,
                max_notional=2000.0,
            )
            if size < self.min_value:
                continue

            # Track remaining buy capacity
            if direction == "BUY":
                remaining_buy -= size

            reason = (
                f"Aggregator {direction} {currency} "
                f"(score={us.unified_score:+.2f} conv={us.conviction:.0%} "
                f"bt={us.backtest_quality:.2f} trend={us.trend_score:+.2f} "
                f"top={','.join(us.top_strategies[:4])})"
            )

            ops.append(Opportunity(
                opp_type=OpportunityType.STRATEGY_SIGNAL,
                currency=currency,
                side=direction,
                size_usd=size,
                reason=reason,
                priority=self._latency_adjusted_priority(
                    abs(us.unified_score) * confidence,
                    trade_style=trade_style,
                ),
                product_id=pid,
                expected_fee=0.0,
                entry_price_est=cur_price,
                stop_loss_pct=exit_plan["stop_loss_pct"],
                take_profit_pct=exit_plan["take_profit_pct"],
                holding_period_hours=exit_plan["holding_period_hours"],
                expected_return_pct=exit_plan["expected_return_pct"],
                risk_pct=exit_plan["risk_pct"],
                meta={
                    "source": "signal_aggregator",
                    "unified_score": us.unified_score,
                    "conviction": us.conviction,
                    "backtest_quality": us.backtest_quality,
                    "trend_score": us.trend_score,
                    "top_strategies": us.top_strategies,
                    "trade_style": trade_style,
                    "trend_aligned": trend_aligned,
                    "buy_strategies": us.details.get("buy_strategies", []),
                    "sell_strategies": us.details.get("sell_strategies", []),
                },
            ))

        if ops:
            logger.info("Aggregator signals: %d opportunities", len(ops))
        return ops

    def _detect_event_markets(self) -> List[Opportunity]:
        if not self.event_engine and not self._pm_client:
            return []
        try:
            if self._pm_client:
                # Fetch ALL categories
                cat_markets = self._pm_client.search_all_categories(
                    limit_per_platform=12, min_volume=0, max_spread=0.25
                )
                markets = []
                for cat, items in cat_markets.items():
                    for m in items:
                        m.category = cat
                        markets.append(m)
            else:
                holdings = {
                    k: v for k, v in self.state.holdings.items()
                    if k not in ("USDC", "USDT", "DAI")
                } if self.state else {}
                signals = self.event_engine.find_opportunities(holdings) if self.event_engine else []
                return self._event_signals_to_ops(signals)
        except Exception as e:
            logger.warning("Event market detection failed: %s", e)
            return []

        if not markets:
            return []

        # Run knowledge gap analysis on top markets across categories
        kg_assessments = {}
        if self._knowledge_gap:
            try:
                for m in sorted(markets, key=lambda x: x.volume, reverse=True)[:8]:
                    a = self._knowledge_gap.analyze(m)
                    if a:
                        kg_assessments[m.market_id] = a
                if kg_assessments:
                    logger.info("Knowledge gaps found for %d markets",
                                len(kg_assessments))
            except Exception as e:
                logger.warning("Knowledge gap analysis failed: %s", e)

        # Categories that generate actionable Coinbase trades
        actionable_categories = {"crypto", "economics", "technology"}
        # Minimum volume thresholds by category
        min_vol_by_cat = {
            "crypto": 2000, "economics": 1000, "technology": 500,
            "sports": 500, "politics": 500, "entertainment": 500,
        }

        ops = []
        remaining_buy_capacity = self._buy_capacity()
        remaining_core_capacity = self._bucket_gap("core")
        remaining_opportunity_capacity = self._bucket_gap("opportunity")
        if self._arb_scanner:
            try:
                arbs = self._arb_scanner.scan(limit_per_category=20)
                for arb in arbs[:10]:
                    arb_size = self._risk_reward_size(
                        expected_return_pct=max(arb.edge_pct * 100.0, 0.5),
                        risk_pct=max((1.0 - arb.confidence) * 10.0 + 2.0, 1.0),
                        confidence=arb.confidence,
                        liquidity=0.8,
                        cap_pct=0.01,
                        max_notional=3000.0,
                        min_notional=self.min_value,
                        capital_limit=remaining_opportunity_capacity,
                    )
                    arb_size = min(arb_size, remaining_buy_capacity, self._opportunity_batch_cap(), remaining_opportunity_capacity)
                    remaining_buy_capacity = max(remaining_buy_capacity - arb_size, 0.0)
                    remaining_opportunity_capacity = max(remaining_opportunity_capacity - arb_size, 0.0)
                    arb_plan = self._compute_exit_plan(
                        "ARB", arb.confidence,
                        expected_return_pct=max(arb.edge_pct * 100.0, 0.5),
                        trade_style="arbitrage",
                        volatility_pct=max(arb.edge_pct * 100.0 * 6.0 + 2.0, 2.0),
                        spread_pct=arb.edge_pct * 100.0,
                        hold_hint_hours=6.0,
                    )
                    ops.append(Opportunity(
                        opp_type=OpportunityType.EVENT_ARBITRAGE,
                        currency="ARB",
                        side="BUY",
                        size_usd=arb_size,
                        reason=arb.reason,
                        priority=min(arb.confidence * 0.95, 0.95),
                        product_id=f"{arb.platform_buy}:{arb.platform_hedge}",
                        entry_price_est=arb.leg_buy.price,
                        stop_loss_pct=arb_plan["stop_loss_pct"],
                        take_profit_pct=arb_plan["take_profit_pct"],
                        holding_period_hours=arb_plan["holding_period_hours"],
                        expected_return_pct=arb_plan["expected_return_pct"],
                        risk_pct=arb_plan["risk_pct"],
                        meta={
                            "event_key": arb.event_key,
                            "category": arb.category,
                            "platform_buy": arb.platform_buy,
                            "platform_hedge": arb.platform_hedge,
                            "buy_leg": {
                                "platform": arb.leg_buy.platform,
                                "market_id": arb.leg_buy.market_id,
                                "question": arb.leg_buy.question,
                                "outcome": arb.leg_buy.outcome,
                                "side": arb.leg_buy.side,
                                "price": arb.leg_buy.price,
                            },
                            "hedge_leg": {
                                "platform": arb.leg_hedge.platform,
                                "market_id": arb.leg_hedge.market_id,
                                "question": arb.leg_hedge.question,
                                "outcome": arb.leg_hedge.outcome,
                                "side": arb.leg_hedge.side,
                                "price": arb.leg_hedge.price,
                            },
                            "edge": arb.edge,
                            "edge_pct": arb.edge_pct,
                            "confidence": arb.confidence,
                            "signal_type": "cross_platform_arbitrage",
                            "trade_style": "arbitrage",
                            "exit_plan": arb_plan,
                        },
                    ))
                if arbs:
                    logger.info("Event arb scanner: %d opportunities", len(arbs))
            except Exception as e:
                logger.warning("Event arbitrage scan failed: %s", e)

        for m in sorted(markets, key=lambda x: x.volume, reverse=True)[:15]:
            min_vol = min_vol_by_cat.get(m.category, 2000)
            if not m.is_open or m.volume < min_vol:
                continue
            extremity = m.probability_extremity
            if extremity < 0.2:
                continue
            conf = min(extremity * m.liquidity_score * 1.5, 0.95)

            # Penalize non-actionable categories
            if m.category not in actionable_categories:
                conf *= 0.6

            kg = kg_assessments.get(m.market_id)
            if kg and kg.is_significant:
                if (kg.direction == "overvalued" and m.mid_price > 0.5) or \
                   (kg.direction == "undervalued" and m.mid_price < 0.5):
                    conf = min(conf * 1.4, 0.95)
                elif (kg.direction == "overvalued" and m.mid_price < 0.5) or \
                     (kg.direction == "undervalued" and m.mid_price > 0.5):
                    conf = min(conf * 1.2, 0.95)

            question_lower = m.question.lower()
            crypto_symbol = None
            for kw, sym in self._SYMBOL_MAP:
                if kw in question_lower:
                    crypto_symbol = sym
                    break
            # Fallback: use category default
            if not crypto_symbol:
                cat_defaults = {
                    "sports": "BTC-USD", "politics": "BTC-USD",
                    "entertainment": "BTC-USD",
                }
                crypto_symbol = cat_defaults.get(m.category)

            kg_meta = {}
            if kg:
                kg_meta = {
                    "knowledge_gap_gap": round(kg.gap, 3),
                    "knowledge_gap_direction": kg.direction,
                    "knowledge_gap_evidence_score": round(kg.evidence_score, 3),
                    "knowledge_gap_evidence_count": kg.evidence_count,
                    "knowledge_gap_sentiment": kg.sentiment_label,
                    "knowledge_gap_confidence": round(kg.confidence, 3),
                    "knowledge_gap_sources": ",".join(kg.sources_used),
                }

            is_actionable = m.category in actionable_categories
            if crypto_symbol and conf > 0.3 and is_actionable:
                side = "BUY" if m.mid_price > 0.5 else "SELL"
                bucket = "core" if crypto_symbol.replace("-USD", "").upper() in CORE_LONG_TERM_ASSETS else "opportunity"
                size = self._risk_reward_size(
                    expected_return_pct=max(conf * 12.0, 0.5),
                    risk_pct=max((1.0 - m.liquidity_score) * 10.0 + m.spread * 100.0 + 2.0, 1.0),
                    confidence=conf,
                    liquidity=m.liquidity_score,
                    cap_pct=0.008,
                    max_notional=2500.0,
                    min_notional=self.min_value,
                    capital_limit=(remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity),
                )
                if m.mid_price > 0.5:
                    batch_cap = self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap()
                    size = min(size, remaining_buy_capacity, batch_cap, remaining_core_capacity if bucket == "core" else remaining_opportunity_capacity)
                    remaining_buy_capacity = max(remaining_buy_capacity - size, 0.0)
                    if bucket == "core":
                        remaining_core_capacity = max(remaining_core_capacity - size, 0.0)
                    else:
                        remaining_opportunity_capacity = max(remaining_opportunity_capacity - size, 0.0)
                reason = f"{m.platform} [{m.category}]: {m.question[:65]} → {m.mid_price*100:.0f}% "
                if kg and kg.is_significant:
                    reason += f"[kg: {kg.direction} gap={kg.gap_pct:.0f}%]"
                else:
                    reason += f"(vol=${m.volume:.0f})"
                spot_entry = self._current_price_for_symbol(crypto_symbol, fallback=0.0)
                exit_plan_pm = self._compute_exit_plan(
                    crypto_symbol.replace("-USD", ""), conf,
                    expected_return_pct=max(abs((kg.gap if kg else (m.mid_price - 0.5))) * 100.0, 0.5),
                    trade_style="prediction_market",
                    volatility_pct=max((1.0 - m.liquidity_score) * 35.0 + m.spread * 1000.0 + 18.0, 18.0),
                    spread_pct=m.spread * 100.0,
                    hold_hint_hours=24.0 if m.category == "crypto" else 16.0,
                )
                ops.append(Opportunity(
                    opp_type=OpportunityType.STRATEGY_SIGNAL,
                    currency=crypto_symbol.replace("-USD", ""),
                    side=side,
                    size_usd=size,
                    reason=reason,
                    priority=min(conf * (0.9 if m.category == "crypto" else 0.7), 0.8),
                    product_id=crypto_symbol,
                    entry_price_est=spot_entry,
                    stop_loss_pct=exit_plan_pm["stop_loss_pct"],
                    take_profit_pct=exit_plan_pm["take_profit_pct"],
                    holding_period_hours=exit_plan_pm["holding_period_hours"],
                    expected_return_pct=exit_plan_pm["expected_return_pct"],
                    risk_pct=exit_plan_pm["risk_pct"],
                    meta=dict({
                        "platform": m.platform,
                        "category": m.category,
                        "market_question": m.question,
                        "probability": m.mid_price,
                        "volume": m.volume,
                        "spread": m.spread,
                        "liquidity_score": m.liquidity_score,
                        "signal_type": f"prediction_market:{m.category}",
                        "trade_style": "prediction_market",
                        "confidence": conf,
                        "capital_bucket": bucket,
                        "exit_plan": exit_plan_pm,
                    }, **kg_meta),
                ))
            else:
                reason = f"{m.platform} [{m.category}]: {m.question[:65]} → {m.mid_price*100:.0f}%"
                if kg and kg.is_significant:
                    reason += f" [kg: {kg.direction} gap={kg.gap_pct:.0f}%]"
                ops.append(Opportunity(
                    opp_type=OpportunityType.EVENT_MARKET,
                    currency="?",
                    side="NONE",
                    size_usd=0,
                    reason=reason,
                    priority=min(conf * 0.4, 0.25),
                    product_id=f"{m.platform}:{m.market_id}",
                    meta=dict({
                        "platform": m.platform,
                        "category": m.category,
                        "market_question": m.question,
                        "probability": m.mid_price,
                        "volume": m.volume,
                        "spread": m.spread,
                        "signal_type": f"event_notification:{m.category}",
                        "confidence": conf,
                    }, **kg_meta),
                ))

        if ops:
            actionable = sum(1 for o in ops if o.opp_type == OpportunityType.STRATEGY_SIGNAL)
            kg_count = sum(1 for o in ops if o.meta.get("knowledge_gap_direction"))
            cats = set(o.meta.get("category", "?") for o in ops)
            logger.info("Event markets: %d ops (%d actionable, %d kg) cats=%s",
                         len(ops), actionable, kg_count, ",".join(sorted(cats)))
        return ops

    def _write_trade_plans(self, opportunities: List[Opportunity]):
        """Persist trade plans to JSON for dashboard consumption."""
        style_map = {
            "TLH": "tax_loss",
            "FEE_TIER_VOLUME": "rebalance",
            "REBALANCE": "rebalance",
            "VOLUME_CYCLE": "cycle",
            "STOCK_SIGNAL": "equity_momentum",
            "EVENT_ARBITRAGE": "arbitrage",
            "EVENT_MARKET": "event",
            "NEW_LISTING_MOMENTUM": "new_listing",
            "ACCUMULATOR_SIGNAL": "event",
            "STRATEGY_SIGNAL": "momentum",
        }
        plans = []
        for opp in opportunities[:50]:
            plan = asdict(opp)
            plan["opp_type"] = opp.opp_type.name
            plan["trade_style"] = opp.meta.get("trade_style", "") or style_map.get(opp.opp_type.name, "")
            plans.append(plan)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "source": "portfolio_optimizer",
            "plans": plans,
            "total": len(plans),
        }
        try:
            tmp_path = "trade_plans.json.tmp"
            with open(tmp_path, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            os.replace(tmp_path, "trade_plans.json")
        except Exception as e:
            logger.debug("Failed to write trade_plans.json: %s", e)

    def _write_enhanced_state(self):
        """Persist enhanced optimizer state for the dashboard."""
        try:
            # Meta-learning source weights
            if self._meta_source_weights:
                with open("data/meta_source_weights.json.tmp", "w") as f:
                    json.dump({
                        "weights": dict(self._meta_source_weights),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }, f, indent=2)
                os.replace("data/meta_source_weights.json.tmp", "data/meta_source_weights.json")

            # Cross-asset regime
            if self._cross_asset_regime:
                try:
                    regime_state = self._cross_asset_regime.get_state(refresh=False)
                    with open("data/cross_asset_regime.json.tmp", "w") as f:
                        json.dump({
                            "regime": regime_state.to_dict(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }, f, indent=2, default=str)
                    os.replace("data/cross_asset_regime.json.tmp", "data/cross_asset_regime.json")
                except Exception:
                    pass

            # Signal ensemble state
            if self._ensemble_blender:
                try:
                    with open("data/signal_ensemble.json.tmp", "w") as f:
                        json.dump({
                            "posteriors": self._ensemble_blender.to_dict(),
                            "top_strategies": self._ensemble_blender.top_strategies(n=10),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        }, f, indent=2, default=str)
                    os.replace("data/signal_ensemble.json.tmp", "data/signal_ensemble.json")
                except Exception:
                    pass

            # Parameter optimization results
            if self._param_opt_results:
                with open("data/param_opt_results.json.tmp", "w") as f:
                    json.dump({
                        "results": dict(self._param_opt_results),
                        "last_run": self._last_param_opt_ts,
                        "interval_days": self._param_opt_interval / 86400.0,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }, f, indent=2, default=str)
                os.replace("data/param_opt_results.json.tmp", "data/param_opt_results.json")

            # Wash-sale cooldown state
            if self._wash_sale_cooldown:
                with open("data/wash_sale_state.json.tmp", "w") as f:
                    json.dump({
                        "cooldowns": {k: v for k, v in self._wash_sale_cooldown.items()},
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }, f, indent=2, default=str)
                os.replace("data/wash_sale_state.json.tmp", "data/wash_sale_state.json")

            # Order flow signals
            if self._order_flow_engine:
                try:
                    of_signals = {}
                    if self.state:
                        for cur, h in self.state.holdings.items():
                            if cur in ("USDC", "USDT", "DAI"):
                                continue
                            pid = h.get("product_id", f"{cur}-USD")
                            sig = self._order_flow_engine.get_signal(pid)
                            if sig:
                                of_signals[pid] = {
                                    "action": sig.action,
                                    "confidence": sig.confidence,
                                    "spread_bps": sig.spread_bps,
                                    "spread_z": sig.spread_z,
                                    "spread_tight": sig.spread_tight,
                                }
                    if of_signals:
                        with open("data/order_flow_signals.json.tmp", "w") as f:
                            json.dump({
                                "signals": of_signals,
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                            }, f, indent=2)
                        os.replace("data/order_flow_signals.json.tmp", "data/order_flow_signals.json")
                except Exception:
                    pass

            # S/R levels for top tracked products (periodic, not every tick)
            if self._tick_count % 20 == 0 and self.state and self._feed_mgr:
                try:
                    top_currencies = sorted(
                        [h for h in self.state.holdings.values()
                         if h["currency"] not in ("USDC", "USDT", "DAI") and h["value"] >= self.min_value],
                        key=lambda x: x["value"], reverse=True,
                    )[:5]
                    top_pids = [h.get("product_id", f"{h['currency']}-USD") for h in top_currencies]
                    batched = self._feed_mgr.get_candles_batch(top_pids, granularity=3600, limit=100)
                    sr_data = {}
                    for pid, candles in batched.items():
                        if not candles or len(candles) < 40:
                            continue
                        closes = [to_float(c[4]) for c in candles]
                        highs = [to_float(c[2]) for c in candles]
                        lows = [to_float(c[3]) for c in candles]
                        levels, atr = self._detect_sr_levels_for_product(closes, highs, lows)
                        if levels:
                            regime = _detect_market_regime(highs, lows, closes)
                            sr_data[pid] = {
                                "levels": [
                                    {"price": round(L.price, 4), "kind": L.kind, "strength": L.strength}
                                    for L in levels
                                ],
                                "atr": round(atr, 4),
                                "regime": regime,
                                "current_price": closes[-1] if closes else 0,
                            }
                    if sr_data:
                        with open("data/sr_levels.json.tmp", "w") as f:
                            json.dump({
                                "products": sr_data,
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                            }, f, indent=2, default=str)
                        os.replace("data/sr_levels.json.tmp", "data/sr_levels.json")
                except Exception:
                    pass

        except Exception as e:
            logger.debug("Enhanced state write failed: %s", e)

    def _write_signal_cache(self, opportunities: List[Opportunity]):
        """Persist a lightweight signal cache for the dashboard signal feed."""
        style_map = {
            "TLH": "tax_loss",
            "FEE_TIER_VOLUME": "rebalance",
            "REBALANCE": "rebalance",
            "VOLUME_CYCLE": "cycle",
            "STOCK_SIGNAL": "equity_momentum",
            "EVENT_ARBITRAGE": "arbitrage",
            "EVENT_MARKET": "event",
            "NEW_LISTING_MOMENTUM": "new_listing",
            "ACCUMULATOR_SIGNAL": "event",
            "STRATEGY_SIGNAL": "momentum",
        }
        queue = []
        for opp in opportunities[:100]:
            item = asdict(opp)
            item.update({
                "action": opp.side,
                "symbol": f"{opp.currency}-USD" if opp.currency and not str(opp.currency).endswith("-USD") else opp.currency,
                "instrument": opp.product_id or opp.currency,
                "strategy_name": (
                    opp.meta.get("strategy_name")
                    or opp.meta.get("strategy")
                    or opp.meta.get("source")
                    or opp.opp_type.name
                ),
                "trade_style": opp.meta.get("trade_style", "") or style_map.get(opp.opp_type.name, ""),
                "signal_reason": opp.reason,
                "confidence": float(opp.meta.get("final_confidence", opp.priority) or opp.priority or 0),
                "opportunity_score": float(opp.meta.get("opportunity_score", opp.priority) or opp.priority or 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "portfolio_optimizer",
                "graph_score": opp.meta.get("graph_score"),
                "graph_overlay": opp.meta.get("graph_overlay"),
            })
            queue.append(item)

        payload = {
            "status": "ok",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "queue": queue,
            "signals": queue,
            "total_signals": len(queue),
            "buy_signals": sum(1 for s in queue if str(s.get("action", "")).upper() == "BUY"),
            "sell_signals": sum(1 for s in queue if str(s.get("action", "")).upper() == "SELL"),
            "quality_score": round(sum(float(s.get("opportunity_score", 0) or 0) for s in queue[:5]) / len(queue[:5]) if queue[:5] else 0, 3),
        }
        try:
            tmp_path = "data/.unified_signal_cache.json.tmp"
            os.makedirs("data", exist_ok=True)
            with open(tmp_path, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            os.replace(tmp_path, "data/.unified_signal_cache.json")
        except Exception as e:
            logger.debug("Failed to write unified_signal_cache.json: %s", e)

    _SYMBOL_MAP = [
        # Crypto direct
        ("bitcoin", "BTC-USD"), ("btc", "BTC-USD"),
        ("ethereum", "ETH-USD"), ("eth", "ETH-USD"),
        ("solana", "SOL-USD"), ("sol", "SOL-USD"),
        ("dogecoin", "DOGE-USD"), ("doge", "DOGE-USD"),
        ("xrp", "XRP-USD"), ("ripple", "XRP-USD"),
        ("cardano", "ADA-USD"), ("ada", "ADA-USD"),
        ("polkadot", "DOT-USD"), ("dot", "DOT-USD"),
        ("polygon", "POL-USD"), ("matic", "POL-USD"), ("pol", "POL-USD"),
        ("avalanche", "AVAX-USD"), ("avax", "AVAX-USD"),
        ("chainlink", "LINK-USD"), ("link", "LINK-USD"),
        ("uniswap", "UNI-USD"), ("uni", "UNI-USD"),
        ("cosmos", "ATOM-USD"), ("atom", "ATOM-USD"),
        ("litecoin", "LTC-USD"), ("ltc", "LTC-USD"),
        ("bitcoin cash", "BCH-USD"), ("bch", "BCH-USD"),
        ("near", "NEAR-USD"), ("aptos", "APT-USD"), ("apt", "APT-USD"),
        ("sui", "SUI-USD"), ("arbitrum", "ARB-USD"), ("arb", "ARB-USD"),
        ("optimism", "OP-USD"), ("op", "OP-USD"),
        ("filecoin", "FIL-USD"), ("injective", "INJ-USD"),
        ("sei", "SEI-USD"), ("celestia", "TIA-USD"), ("tia", "TIA-USD"),
        ("shiba", "SHIB-USD"), ("shib", "SHIB-USD"),
        ("pepe", "PEPE-USD"), ("bonk", "BONK-USD"),
        ("trump", "TRUMP-USD"), ("floki", "FLOKI-USD"),
        ("algorand", "ALGO-USD"),
        ("stellar", "XLM-USD"), ("stacks", "STX-USD"),
        ("hedera", "HBAR-USD"),
        ("internet computer", "ICP-USD"), ("grt", "GRT-USD"),
        # Sports → general market sentiment (speculative)
        ("super bowl", "BTC-USD"),
        ("world cup", "BTC-USD"),
        ("champions league", "BTC-USD"),
        ("nba champion", "BTC-USD"),
        ("nfl champion", "BTC-USD"),
        ("world series", "BTC-USD"),
        ("stanley cup", "BTC-USD"),
        ("olympics", "BTC-USD"),
        # Politics → regulatory/macro uncertainty
        ("president", "BTC-USD"),
        ("presidential", "BTC-USD"),
        ("election", "BTC-USD"),
        ("congress", "BTC-USD"),
        ("senate", "BTC-USD"),
        # Economics → direct macro impact
        ("fed", "BTC-USD"),
        ("federal reserve", "BTC-USD"),
        ("inflation", "BTC-USD"),
        ("interest rate", "BTC-USD"),
        ("cpi", "BTC-USD"),
        ("gdp", "BTC-USD"),
        ("recession", "BTC-USD"),
        ("unemployment", "BTC-USD"),
        # Technology → sector-specific
        ("ai", "NVDA"),
        ("artificial intelligence", "NVDA"),
        ("nvidia", "NVDA"),
        ("spacex", "TSLA"),
        ("starship", "TSLA"),
    ]

    def _event_signals_to_ops(self, signals) -> List[Opportunity]:
        """Convert legacy ComparisonEngine EventSignals to Opportunities."""
        ops = []
        for sig in signals[:5]:
            ops.append(Opportunity(
                opp_type=OpportunityType.EVENT_MARKET,
                currency=sig.outcome.split()[-1] if sig.outcome.split() else "?",
                side=sig.outcome.split()[0] if sig.outcome.split() else "NONE",
                size_usd=sig.position_size,
                reason=sig.reason,
                priority=min(sig.confidence, 0.7),
                product_id=f"{sig.platform}:{sig.market_ticker}",
                entry_price_est=sig.probability * 2,
                stop_loss_pct=10.0,
                take_profit_pct=20.0,
                holding_period_hours=48,
                expected_return_pct=max((sig.probability - 0.5) * 200, 0),
                risk_pct=5.0,
                meta={
                    "platform": sig.platform,
                    "market_question": sig.market_question,
                    "market_ticker": sig.market_ticker,
                    "outcome": sig.outcome,
                    "probability": sig.probability,
                    "signal_type": sig.signal_type,
                    "confidence": sig.confidence,
                },
            ))
        return ops

    # ── Execution ──────────────────────────────────────────────────

    def _record_trade(self, opp: Opportunity, total_fee: float = 0.0):
        self.last_execution[opp.opp_type.value] = time.time()
        if opp.opp_type == OpportunityType.VOLUME_CYCLE:
            self.position_ages[opp.currency] = time.time()
        elif opp.opp_type == OpportunityType.TLH:
            self.cost_bases.pop(opp.currency, None)  # reset cost basis after sale

        # Immediately update in-memory state to prevent over-allocation in the same tick
        if self.state:
            pid = self._normalize_product_id(opp.currency, opp.side, opp.product_id)
            if opp.side == "BUY":
                self.state.usdc_balance = max(0.0, self.state.usdc_balance - opp.size_usd)
                existing = self.state.holdings.get(opp.currency, {})
                existing["value"] = existing.get("value", 0) + opp.size_usd
                existing["balance"] = existing.get("balance", 0) + opp.size_usd / max(opp.entry_price_est, 0.01)
                existing["currency"] = opp.currency
                existing["product_id"] = pid
                self.state.holdings[opp.currency] = existing
            elif opp.side == "SELL":
                added_usd = opp.size_usd * 0.97  # estimate ~3% slippage + fee
                self.state.usdc_balance += added_usd
                existing = self.state.holdings.get(opp.currency, {})
                existing["value"] = max(0.0, existing.get("value", 0) - opp.size_usd)
                if existing["value"] <= 0:
                    self.state.holdings.pop(opp.currency, None)
                else:
                    self.state.holdings[opp.currency] = existing
            self.state.total_value = max(self.state.total_value, sum(
                h.get("value", 0) for h in self.state.holdings.values()
            ) + self.state.usdc_balance)

        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": opp.opp_type.value,
            "side": opp.side,
            "currency": opp.currency,
            "size_usd": round(opp.size_usd, 2),
            "fee": round(total_fee, 2),
            "reason": opp.reason,
            "order_id": opp.order_id,
            "dry_run": self.dry_run,
        }
        self.trade_log.append(entry)
        self.store.save_trade(entry)
        if self.neo4j_store:
            try:
                self.neo4j_store.save_trade(entry)
            except Exception as e:
                logger.warning("Neo4j trade save failed: %s", e)
        logger.info("  → Logged trade #%d (saved)", len(self.trade_log))

    def _normalize_product_id(self, currency: str, side: str, hint: str = "") -> str:
        """Return the correct product_id: prefer USDC pairs for clean settlement tracking."""
        try:
            best = self.cli.best_product(currency, "BUY")
            if best:
                return best
        except Exception:
            pass
        if hint and (hint.endswith("-USDC") or hint.endswith("-USD")):
            return hint
        return f"{currency}-USDC"

    def _execute_with_bracket(self, opp: Opportunity, base_qty: float, is_quote: bool):
        entry_price = opp.entry_price_est
        side = opp.side
        product_id = self._normalize_product_id(opp.currency, side, opp.product_id)
        opp.product_id = product_id
        base_size = base_qty if not is_quote else opp.size_usd / max(entry_price, 0.01)
        if base_size <= 0:
            logger.warning("  → Bracket invalid base_size=%.8f, skipping", base_size)
            return

        stop_pct = opp.stop_loss_pct / 100.0
        target_pct = opp.take_profit_pct / 100.0
        if side.upper() == "BUY":
            stop_price = entry_price * (1.0 - stop_pct)
            target_price = entry_price * (1.0 + target_pct)
        else:
            stop_price = entry_price * (1.0 + stop_pct)
            target_price = entry_price * (1.0 - target_pct)

        # Preview via execution engine
        if self._exec_engine:
            intent = _OrderIntent(
                side=side,
                product_id=product_id,
                order_type=_OrderType.MARKET,
                base_size=_fmt_base(base_size) if not is_quote else "",
                quote_size=_fmt_quote(opp.size_usd) if is_quote else "",
            )
            preview_result = self._exec_engine._preview(intent)
            if not preview_result.success:
                logger.warning("  → Bracket preview failed: %s", preview_result.error)
                return
            fee_est = float(preview_result.raw.get("preview", {}).get("total_fee", 0))
            if fee_est > opp.size_usd * 0.02:
                logger.warning("  → Bracket fee too high (%.2f%%), skipping", fee_est / opp.size_usd * 100)
                return
            opp.expected_fee = fee_est
        else:
            fee_est = 0.0

        # Approval gate
        if self.require_approval and not self.dry_run:
            token = str(uuid.uuid4())
            bucket = self._capital_bucket_for(opp)
            pending_entry = {
                "type": opp.opp_type.value,
                "side": opp.side,
                "currency": opp.currency,
                "size_usd": round(opp.size_usd, 2),
                "expected_fee": round(fee_est, 2),
                "product_id": opp.product_id,
                "reason": opp.reason,
                "priority": opp.priority,
                "capital_bucket": bucket,
                "status": "pending",
                "bracket": True,
                "stop_price": round(stop_price, 2),
                "target_price": round(target_price, 2),
                "entry_price_est": round(entry_price, 2) if entry_price > 0 else 0,
                "base_qty": round(base_size, 8) if base_size > 0 else 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            os.makedirs(os.path.dirname(self.pending_file) or ".", exist_ok=True)
            try:
                with open(self.pending_file, "r") as f:
                    fcntl.flock(f, fcntl.LOCK_SH)
                    pending = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                pending = {}
            pending[token] = pending_entry
            with open(self.pending_file, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(pending, f, indent=2, default=str)
            if self.notifier:
                self.notifier.send_trade_alert(opp=pending_entry,
                    state={"total_value": round(self.state.total_value, 2) if self.state else 0,
                           "usdc_balance": round(self.state.usdc_balance, 2) if self.state else 0},
                    token=token)
            logger.info("  → PENDING APPROVAL (bracket): %s", token)
            return

        # Dry-run
        if self.dry_run:
            logger.info("  → DRY-RUN bracket: %s %s size=%.6f entry=$%.2f stop=$%.2f target=$%.2f",
                         side, product_id, base_size, entry_price, stop_price, target_price)
            opp.executed = True
            opp.order_id = "dry-run-bracket"
            self._record_trade(opp, fee_est)
            return

        # Live bracket placement
        bracket = self._bracket_mgr.place_bracket(
            product_id=product_id,
            side=side.upper(),
            base_size=base_size,
            entry_price=entry_price,
            stop_price=stop_price,
            target_price=target_price,
            strategy_id=f"opt_{opp.opp_type.value}",
        )
        if bracket.get("status") == "OPEN":
            bid = bracket.get("bracket_id", bracket.get("entry_result", {}).get("client_order_id", "unknown"))
            logger.info("  → BRACKET PLACED id=%s entry=$%.2f stop=$%.2f target=$%.2f",
                         bid, entry_price, stop_price, target_price)
            opp.executed = True
            opp.order_id = bid
            opp.expected_fee = fee_est
            opp.meta["bracket"] = {
                "bracket_id": bid,
                "stop_price": stop_price,
                "target_price": target_price,
                "entry_result": bracket.get("entry_result", {}),
            }
            self._save_brackets()
            self._record_trade(opp, fee_est)
        else:
            err = bracket.get("entry_result", {}).get("error", "unknown")
            logger.warning("  → Bracket placement failed: %s", err)
            # If the entry order partially filled but stop/target failed, try to flatten
            entry_result = bracket.get("entry_result", {})
            if entry_result.get("success") and entry_result.get("order_id"):
                logger.warning("  → Entry filled but bracket incomplete – attempting flat close")
                try:
                    self._bracket_mgr.force_flatten_bracket(
                        bracket.get("bracket_id", ""), reason="bracket_setup_failed"
                    )
                except Exception as flatten_err:
                    logger.error("  → Flat close also failed: %s", flatten_err)

    def _process_opportunity(self, opp: Opportunity):
        logger.info("Processing [%.2f] %s %s $%.0f: %s",
                     opp.priority, opp.side, opp.currency, opp.size_usd, opp.reason)

        if self._is_static_currency(opp.currency):
            logger.info("  → Static long-term holding skipped: %s", opp.currency)
            return

        # Non-Coinbase opportunities are notify-only until we add execution support.
        if opp.opp_type in (OpportunityType.EVENT_MARKET, OpportunityType.EVENT_ARBITRAGE, OpportunityType.STOCK_SIGNAL):
            if opp.opp_type == OpportunityType.EVENT_ARBITRAGE:
                logger.info("  → Event arbitrage notification: %s", opp.reason)
            elif opp.opp_type == OpportunityType.STOCK_SIGNAL:
                logger.info("  → Equity screen notification: %s", opp.reason)
            else:
                logger.info("  → Event notification (non-tradeable): %s — %s",
                             opp.meta.get("platform", "?"), opp.meta.get("market_question", "")[:60])
            if self.notifier and not self.dry_run:
                state_summary = {
                    "total_value": round(self.state.total_value, 2) if self.state else 0,
                    "usdc_balance": round(self.state.usdc_balance, 2) if self.state else 0,
                }
                self.notifier.send_trade_alert(
                    opp={
                        "type": (
                            f"event_market:{opp.meta.get('platform')}"
                            if opp.opp_type == OpportunityType.EVENT_MARKET
                            else "event_arbitrage" if opp.opp_type == OpportunityType.EVENT_ARBITRAGE
                            else "stock_screen"
                        ),
                        "side": opp.side,
                        "currency": opp.currency,
                        "size_usd": opp.size_usd,
                        "expected_fee": 0,
                        "priority": opp.priority,
                        "reason": opp.reason,
                    },
                    state=state_summary,
                    verdict={
                        "strategy": opp.meta.get("signal_type"),
                        "currency": opp.currency,
                        "win_rate": opp.meta.get("confidence", 0),
                        "sharpe_ratio": 0,
                        "profit_factor": 0,
                        "max_drawdown_pct": 0,
                        "total_trades": 0,
                        "winning_trades": 0,
                        "losing_trades": 0,
                        "total_return_pct": 0,
                        "regime": "event",
                        "passed": opp.meta.get("confidence", 0) > 0.3,
                        "reason": opp.reason,
                    },
                    token=("arb-" if opp.opp_type == OpportunityType.EVENT_ARBITRAGE else "event-") + str(uuid.uuid4())[:8],
                )
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": opp.opp_type.value,
                "side": opp.side,
                "currency": opp.currency,
                "size_usd": round(opp.size_usd, 2),
                "fee": 0,
                "reason": opp.reason,
                "order_id": (
                    f"arb:{opp.meta.get('event_key', '')}"
                    if opp.opp_type == OpportunityType.EVENT_ARBITRAGE
                    else f"{opp.meta.get('platform')}:{opp.meta.get('market_ticker','')}"
                ),
                "strategy": opp.meta.get("signal_type", opp.opp_type.value),
                "dry_run": True,
            }
            self.trade_log.append(entry)
            self.store.save_trade(entry)
            if self.neo4j_store:
                try:
                    self.neo4j_store.save_trade(entry)
                except Exception:
                    pass
            return

        # 1. Compute base quantity from USD size
        is_quote = opp.side == "BUY"
        base_qty = 0.0
        if is_quote:
            bucket = self._capital_bucket_for(opp)
            bucket_limit = self._core_batch_cap() if bucket == "core" else self._opportunity_batch_cap()
            bucket_gap = self._bucket_gap(bucket)
            buy_capacity = min(self._buy_capacity(), bucket_gap, bucket_limit)
            if buy_capacity < self.min_value:
                logger.warning("  → Buy capacity below minimum, skipping")
                return
            opp.size_usd = min(opp.size_usd, buy_capacity)
            if opp.size_usd < self.min_value:
                logger.warning("  → Size below minimum after reserve clamp, skipping")
                return
            entry_price = opp.entry_price_est or self._current_price_for_symbol(opp.product_id)
            base_qty = opp.size_usd / entry_price if entry_price > 0 else 0
        else:
            holder = self.state.holdings.get(opp.currency, {})
            price = holder.get("price", 0) or 1
            base_qty = opp.size_usd / price if price > 0 else 0
            if base_qty <= 0:
                logger.warning("  → Cannot compute base quantity, skipping")
                return

        route_decision = self._best_route_decision_for_opportunity(opp)
        route_plan = getattr(route_decision, "plan", None) if route_decision else None
        use_route = bool(route_plan and getattr(route_plan, "steps", None) and len(route_plan.steps) > 1)

        if use_route:
            opp.meta["route_decision"] = {
                "source": route_plan.source,
                "target": route_plan.target,
                "effective_rate": route_plan.effective_rate,
                "score": float(getattr(route_decision, "score", 0.0)),
                "expected_tax_impact_usd": float(getattr(route_decision, "expected_tax_impact_usd", 0.0)),
                "opportunity_bonus": float(getattr(route_decision, "opportunity_bonus", 0.0)),
                "drawdown_bonus": float(getattr(route_decision, "drawdown_bonus", 0.0)),
                "regime_bonus": float(getattr(route_decision, "regime_bonus", 0.0)),
                "hop_penalty": float(getattr(route_decision, "hop_penalty", 0.0)),
                "liquidity_bonus": float(getattr(route_decision, "liquidity_bonus", 0.0)),
                "factor_breakdown": dict(getattr(route_decision, "factor_breakdown", {})),
                "steps": [
                    {
                        "product_id": s.product_id,
                        "from_currency": s.from_currency,
                        "to_currency": s.to_currency,
                        "direction": s.direction,
                        "price": s.price,
                        "effective_rate": s.effective_rate,
                    }
                    for s in route_plan.steps
                ],
            }

            if self.require_approval and not self.dry_run:
                token = str(uuid.uuid4())
                bucket = self._capital_bucket_for(opp)
                pending_entry = {
                    "type": opp.opp_type.value,
                    "side": opp.side,
                    "currency": opp.currency,
                    "size_usd": round(opp.size_usd, 2),
                    "expected_fee": 0.0,
                    "product_id": opp.product_id,
                    "reason": opp.reason,
                    "priority": opp.priority,
                    "capital_bucket": bucket,
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "route_decision": opp.meta["route_decision"],
                }
                os.makedirs(os.path.dirname(self.pending_file) or ".", exist_ok=True)
                try:
                    with open(self.pending_file, "r") as f:
                        fcntl.flock(f, fcntl.LOCK_SH)
                        pending = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    pending = {}
                pending[token] = pending_entry
                with open(self.pending_file, "w") as f:
                    fcntl.flock(f, fcntl.LOCK_EX)
                    json.dump(pending, f, indent=2, default=str)
                if self.notifier:
                    state_summary = {
                        "total_value": round(self.state.total_value, 2) if self.state else 0,
                        "usdc_balance": round(self.state.usdc_balance, 2) if self.state else 0,
                        "holdings_count": len(self.state.holdings) if self.state else 0,
                    }
                    self.notifier.send_trade_alert(opp=pending_entry, state=state_summary, token=token)
                logger.info("  → PENDING APPROVAL (route): %s", token)
                return

            if self._execute_route_decision(opp, route_decision):
                logger.info("  → ROUTE EXECUTED: %s", opp.meta.get("route_decision", {}).get("steps", []))
                entry = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "type": opp.opp_type.value,
                    "side": opp.side,
                    "currency": opp.currency,
                    "size_usd": round(opp.size_usd, 2),
                    "fee": 0.0,
                    "reason": f"{opp.reason} [route]",
                    "order_id": opp.order_id,
                    "dry_run": self.dry_run,
                    "route_decision": opp.meta["route_decision"],
                }
                self.trade_log.append(entry)
                self.store.save_trade(entry)
                if self.neo4j_store:
                    try:
                        self.neo4j_store.save_trade(entry)
                    except Exception as e:
                        logger.warning("Neo4j trade save failed: %s", e)
                return

            logger.warning("  → Route execution failed; skipping direct fallback to avoid partial overlap")
            return

        # 5. Execute — via bracket engine if available, fallback to direct CLI
        total_fee = 0.0
        use_bracket = (
            self._bracket_mgr is not None
            and opp.opp_type in (OpportunityType.STRATEGY_SIGNAL, OpportunityType.REBALANCE, OpportunityType.NEW_LISTING_MOMENTUM)
            and opp.stop_loss_pct > 0
            and opp.entry_price_est > 0
        )

        if use_bracket:
            self._execute_with_bracket(opp, base_qty, is_quote)
            return
        else:
            # Preview (dry-run / fee check for non-bracket types)
            if is_quote:
                preview = self.cli.preview_order(opp.product_id, opp.side, opp.size_usd, is_quote=True)
            else:
                preview = self.cli.preview_order(opp.product_id, opp.side, base_qty, is_quote=False)
            if not preview:
                logger.warning("  → Preview failed, skipping")
                return

            total_fee = to_float(preview.get("total_fee", 0))
            total_cost = to_float(preview.get("total_cost", 0))
            logger.info("  → Preview: fee=$%.2f, cost=$%.2f", total_fee, total_cost)
            opp.preview_passed = True
            opp.expected_fee = total_fee

            if total_fee > opp.size_usd * 0.02:
                logger.warning("  → Fee too high (%.2f%%), skipping", total_fee / opp.size_usd * 100)
                return

            if self.require_approval and not self.dry_run:
                token = str(uuid.uuid4())
                bucket = self._capital_bucket_for(opp)
                pending_entry = {
                    "type": opp.opp_type.value,
                    "side": opp.side,
                    "currency": opp.currency,
                    "size_usd": round(opp.size_usd, 2),
                    "expected_fee": round(total_fee, 2),
                    "product_id": opp.product_id,
                    "reason": opp.reason,
                    "priority": opp.priority,
                    "capital_bucket": bucket,
                    "status": "pending",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                os.makedirs(os.path.dirname(self.pending_file) or ".", exist_ok=True)
                try:
                    with open(self.pending_file, "r") as f:
                        fcntl.flock(f, fcntl.LOCK_SH)
                        pending = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    pending = {}
                pending[token] = pending_entry
                with open(self.pending_file, "w") as f:
                    fcntl.flock(f, fcntl.LOCK_EX)
                    json.dump(pending, f, indent=2, default=str)

                if self.notifier:
                    state_summary = {
                        "total_value": round(self.state.total_value, 2) if self.state else 0,
                        "usdc_balance": round(self.state.usdc_balance, 2) if self.state else 0,
                        "holdings_count": len(self.state.holdings) if self.state else 0,
                    }
                    self.notifier.send_trade_alert(
                        opp=pending_entry,
                        state=state_summary,
                        token=token,
                    )
                logger.info("  → PENDING APPROVAL: %s", token)
                return

            if self.dry_run:
                logger.info("  → DRY-RUN: would execute %s %s $%.0f (fee=$%.2f)",
                             opp.side, opp.product_id, opp.size_usd, total_fee)
                opp.executed = True
                opp.order_id = "dry-run"
            else:
                if is_quote:
                    order = self.cli.create_order(opp.product_id, opp.side, opp.size_usd, is_quote=True)
                else:
                    order = self.cli.create_order(opp.product_id, opp.side, base_qty, is_quote=False)
                if not order:
                    logger.error("  → Execution returned no order")
                    return
                oid = order.get("id", "unknown")
                logger.info("  → EXECUTED: %s order_id=%s", opp.side, oid)
                opp.executed = True
                opp.order_id = oid

        self._record_trade(opp, total_fee)

    def summary(self) -> dict:
        by_type = defaultdict(list)
        for t in self.trade_log:
            by_type[t["type"]].append(t)
        total_volume = sum(t["size_usd"] for t in self.trade_log)
        total_fees = sum(t["fee"] for t in self.trade_log)
        return {
            "total_trades": len(self.trade_log),
            "total_volume": round(total_volume, 2),
            "total_fees": round(total_fees, 2),
            "by_type": {k: {"count": len(v), "volume": round(sum(x["size_usd"] for x in v), 2)}
                        for k, v in by_type.items()},
            "trades": self.trade_log[-50:],
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Portfolio Optimizer Daemon – continuously improve your Coinbase portfolio",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--live", action="store_true", help="Execute real trades (default: dry-run)")
    parser.add_argument("--interval", type=int, default=60, help="Check interval in seconds (default: 60)")
    parser.add_argument("--min-value", type=float, default=10.0, help="Minimum position value to trade (default: $10)")
    parser.add_argument("--max-deployable-usd", type=float, default=100.0,
                        help="Hard cap on non-cash capital the optimizer may deploy (e.g. 100)")
    parser.add_argument("-e", "--environment", default="live", choices=["live", "sandbox"])
    parser.add_argument("--once", action="store_true", help="Run a single tick then exit")
    parser.add_argument("--summary", action="store_true", help="Print trade summary and exit (no tick)")
    parser.add_argument("--db", default="optimizer_state.db", help="SQLite DB path (default: optimizer_state.db)")
    parser.add_argument("--reset-db", action="store_true", help="Delete existing DB and start fresh")
    parser.add_argument("--neo4j-uri", default="",
                        help="Neo4j bolt URI (e.g. bolt://100.64.43.123:7687)")
    parser.add_argument("--neo4j-user", default="neo4j",
                        help="Neo4j username (default: neo4j)")
    parser.add_argument("--neo4j-password", default="",
                        help="Neo4j password")
    parser.add_argument("--neo4j-db", default="trading",
                        help="Neo4j database name (default: trading)")
    parser.add_argument("--require-approval", action="store_true",
                        help="Require email approval before executing live trades")
    parser.add_argument("--smtp-user", default="",
                        help="Gmail SMTP username (email address) for trade alerts")
    parser.add_argument("--smtp-password", default="",
                        help="Gmail App Password for SMTP")
    parser.add_argument("--from-addr", default="",
                        help="From email address (default: same as smtp-user)")
    parser.add_argument("--to-addr", default="",
                        help="To email address (default: same as smtp-user)")
    parser.add_argument("--approval-base-url", default="http://localhost:8080",
                        help="Base URL for approve/deny links (default: http://localhost:8080)")
    parser.add_argument("--pending-file", default="data/pending_approvals.json",
                        help="Path for pending approvals JSON (default: data/pending_approvals.json)")
    parser.add_argument("--polymarket", action="store_true",
                        help="Enable Polymarket event market monitoring (read-only)")
    parser.add_argument("--kalshi-email", default="",
                        help="Kalshi account email (enables Kalshi market monitoring)")
    parser.add_argument("--kalshi-password", default="",
                        help="Kalshi account password")
    args = parser.parse_args()

    if args.reset_db:
        if os.path.exists(args.db):
            os.remove(args.db)
            print(f"Removed existing DB: {args.db}")
        else:
            print(f"DB not found: {args.db}")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # Suppress noisy Neo4j schema notifications
    logging.getLogger("neo4j.notifications").setLevel(logging.WARNING)

    opt = PortfolioOptimizer(
        environment=args.environment,
        interval=args.interval,
        min_value=args.min_value,
        max_deployable_usd=args.max_deployable_usd,
        dry_run=not args.live,
        db_path=args.db,
        neo4j_uri=args.neo4j_uri,
        neo4j_user=args.neo4j_user,
        neo4j_password=args.neo4j_password,
        neo4j_db=args.neo4j_db,
        require_approval=args.require_approval,
        smtp_user=args.smtp_user,
        smtp_password=args.smtp_password,
        from_addr=args.from_addr,
        to_addr=args.to_addr,
        approval_base_url=args.approval_base_url,
        pending_file=args.pending_file,
        enable_polymarket=args.polymarket,
        kalshi_email=args.kalshi_email,
        kalshi_password=args.kalshi_password,
    )

    if args.summary:
        if not opt.trade_log:
            print("No trades recorded yet.")
        else:
            print(json.dumps(opt.summary(), indent=2))
        return

    if args.once:
        opt._tick()
        print(json.dumps(opt.summary(), indent=2))
        return

    try:
        opt.run()
    except KeyboardInterrupt:
        logger.info("Shutdown")
        print("\n" + json.dumps(opt.summary(), indent=2))


if __name__ == "__main__":
    main()
