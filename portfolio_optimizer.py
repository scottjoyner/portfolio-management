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
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("optimizer")

# Strategy engine
from strategy_engine import run_strategies as _run_strategies
from strategy_engine import Signal as StrategySignal
from strategy_engine import backtest_strategy as _backtest_strategy
from strategy_engine import BacktestVerdict

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
STATIC_LONG_TERM_ASSETS = {"BTC", "ETH"}
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
OP_COOLDOWN = {"tlh": 86400, "fee_tier": 3600, "rebalance": 43200, "strategy": 300, "cycle": 600, "accumulator": 120}

# Fee tier volume cycling
CYCLE_MIN_PROFIT_PCT = 0.0   # we'll break even or small loss for volume
CYCLE_MAX_HOLD_TICKS = 6     # force-close after this many checks

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class OpportunityType(Enum):
    TLH = "tlh"
    FEE_TIER_VOLUME = "fee_tier"
    REBALANCE = "rebalance"
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
        pending_file: str = "pending_approvals.json",
        enable_polymarket: bool = False,
        kalshi_email: str = "",
        kalshi_password: str = "",
    ):
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
        self.position_ages: Dict[str, int] = defaultdict(int)
        self.trade_log: List[dict] = []
        self.running = False
        self._bt_cache: Dict[str, BacktestVerdict] = {}
        self._bt_cache_ttl: float = 3600
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
                self._save_capital_policy()
                self.neo4j_store.prune_bt_cache()
            except Exception as e:
                logger.warning("Neo4j save failed: %s", e)

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
        return _clamp(size, min_notional, max_notional)

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

        return _clamp(max(atr_pct * 5.0, recent_move_pct * 1.2, 2.0), 2.0, 100.0)

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

    def _compute_exit_plan(
        self,
        currency: str,
        side: str,
        confidence: float,
        expected_return_pct: float = 0.0,
        *,
        trade_style: str = "momentum",
        volatility_pct: float = 60.0,
        spread_pct: float = 0.0,
        hold_hint_hours: Optional[float] = None,
    ) -> Dict[str, float]:
        """Compute a conservative execution plan for a candidate trade."""
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
        }

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
            try:
                self._tick()
            except KeyboardInterrupt:
                self.running = False
                logger.info("Shutdown requested")
                break
            except Exception as e:
                logger.error("Tick failed: %s", e, exc_info=True)
            logger.info("Sleeping %ds...", self.interval)
            time.sleep(self.interval)

    def stop(self):
        self.running = False
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
        opportunities = self._detect_opportunities()
        opportunities.sort(key=lambda o: o.priority, reverse=True)
        self._write_trade_plans(opportunities)
        self._write_signal_cache(opportunities)
        logger.info("Found %d opportunities", len(opportunities))
        for opp in opportunities[:5]:  # max 5 per tick
            self._process_opportunity(opp)
        self._save_state()

    # ── State ──────────────────────────────────────────────────────

    def _fetch_state(self):
        try:
            self.cli.get_products()  # warm the cache
            balances = self.cli.get_balances()
            fees_data = self.cli.get_fees()
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

    # ── Opportunity detection ─────────────────────────────────────

    def _detect_opportunities(self) -> List[Opportunity]:
        ops = []
        ops.extend(self._detect_tlh())
        ops.extend(self._detect_coinbase_universe_signals())
        ops.extend(self._detect_stock_opportunities())
        ops.extend(self._detect_fee_tier_volume())
        ops.extend(self._detect_rebalance())
        ops.extend(self._detect_strategy_signals())
        ops.extend(self._detect_volume_cycles())
        ops.extend(self._detect_accumulator_signals())
        ops.extend(self._detect_event_markets())
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
        for pid, p in rows[:25]:
            try:
                candles = self.cli.get_candles(pid, granularity="1h", limit=100)
                if len(candles) < 40:
                    continue
                closes, vols, highs, lows = [], [], [], []
                for c in reversed(candles):
                    closes.append(to_float(c.get("close", 0)))
                    vols.append(to_float(c.get("volume", 0)))
                    highs.append(to_float(c.get("high", 0)))
                    lows.append(to_float(c.get("low", 0)))
                if len(closes) < 40:
                    continue
                recent = closes[-10:]
                prior = closes[-40:-10]
                vol_recent = sum(vols[-10:]) / max(len(vols[-10:]), 1)
                vol_prior = sum(vols[-40:-10]) / max(len(vols[-40:-10]), 1)
                base = pid.split("-")[0]
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
                    pid.split("-")[0], side, score,
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
                    symbol, side, score,
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
                currency=cur,
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
                    best["currency"], "BUY", 0.65,
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
                    meta={"capital_bucket": bucket, "graph_multiplier": graph_multiplier, "graph_score": self._graph_score_for_product(best.get("product_id") or f"{best['currency']}-USD"), "exit_plan": exit_plan},
                ))
        if ops:
            logger.info("Rebalance: %d actions", len(ops))
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
            self.position_ages[cur] += 1
            if self.position_ages[cur] >= CYCLE_MAX_HOLD_TICKS:
                pid = self.cli.best_product(cur, "SELL")
                if not pid:
                    continue
                ops.append(Opportunity(
                    opp_type=OpportunityType.VOLUME_CYCLE,
                    currency=cur,
                    side="SELL",
                    size_usd=h["value"],
                    reason=f"Volume cycle: close after {self.position_ages[cur]} ticks",
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
            logger.info("Volume cycles: %d positions stale", len(ops))
        return ops

    def _detect_strategy_signals(self) -> List[Opportunity]:
        """Run 5 strategies on each meaningful holding; return top signals as opportunities."""
        if time.time() - self.last_execution.get("strategy", 0) < OP_COOLDOWN["strategy"]:
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
        for h in candidates:
            currency = h["currency"]
            buy_capacity = self._buy_capacity()
            pid = h.get("product_id", f"{currency}-USD")
            candles = self.cli.get_candles(pid, granularity="1h", limit=100)
            if len(candles) < 30:
                continue

            closes = []
            volumes = []
            highs = []
            lows = []
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

            signals = _run_strategies(
                currency=currency,
                asset_class=h["classification"],
                closes=closes,
                volumes=volumes,
                current_price=h["price"],
                highs=highs if highs else None,
                lows=lows if lows else None,
            )

            if len(signals) < 1:
                continue

            # Backtest all signals, filter passed
            passed_signals = []
            for sig in signals:
                if sig.action == "BUY" and buy_capacity < self.min_value:
                    continue
                if sig.action == "SELL" and h["value"] < self.min_value:
                    continue

                cache_key = f"{sig.strategy}/{currency}"
                cached = self._bt_cache.get(cache_key)
                if cached:
                    verdict = cached
                else:
                    verdict = _backtest_strategy(sig.strategy, currency, closes, volumes,
                                                  highs=highs if highs else None,
                                                  lows=lows if lows else None)
                    self._bt_cache[cache_key] = verdict
                    self.store.save_bt_cache(cache_key, verdict)
                    if self.neo4j_store:
                        try:
                            self.neo4j_store.save_bt_cache(cache_key, verdict)
                        except Exception as e:
                            logger.warning("Neo4j BT cache write failed: %s", e)

                logger.info("  BT %s/%s: trades=%d WR=%.0f%% Sharpe=%.1f Pf=%.1f dd=%.1f%% → %s (%s)",
                             sig.strategy, currency, verdict.total_trades,
                             verdict.win_rate * 100, verdict.sharpe_ratio,
                             verdict.profit_factor, verdict.max_drawdown_pct,
                             "PASS" if verdict.passed else "SKIP", verdict.reason)

                if verdict.passed:
                    passed_signals.append(sig)

            if not passed_signals:
                continue

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
                passed_signals,
                asset_class=h["classification"],
                currency=currency,
            )

            for agg in aggregated[:2]:
                side = agg.direction
                bucket = "core" if currency.upper().replace("-USD", "") in CORE_LONG_TERM_ASSETS else "opportunity"
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
                                "spread": h.get("spread", 0.001),
                                "volume": h.get("volume_24h", 0),
                                "price": h.get("price", 0),
                            },
                            regime=_detect_regime({"change_pct": h.get("change_24h", 0)}),
                            market_leaders=["BTC", "ETH"],
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

                exit_plan = self._compute_exit_plan(
                    currency, side, final_confidence,
                    expected_return_pct=max(final_confidence * 10.0, 0.5),
                    trade_style="momentum" if side == "BUY" else "mean_reversion",
                    volatility_pct=max(abs(to_float(h.get("change_24h", 0))) * 50, 30),
                )
                ops.append(Opportunity(
                    opp_type=OpportunityType.STRATEGY_SIGNAL,
                    currency=currency,
                    side=side,
                    size_usd=size,
                    reason=f"{agg.best_reason} (agg_conf={agg.confidence:.2f}, graph={graph_multiplier:.2f}, {agg.strategy_count} strats)",
                    priority=min(final_confidence * 0.8 + 0.1, 0.95),
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
                        "graph_multiplier": graph_multiplier,
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
                currency, sig.action, final_confidence,
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
                    "graph_multiplier": graph_multiplier,
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
                        "ARB", "PAIR", arb.confidence,
                        expected_return_pct=max(arb.edge_pct * 100.0, 0.5),
                        trade_style="arbitrage",
                        volatility_pct=max(arb.edge_pct * 100.0 * 6.0 + 2.0, 2.0),
                        spread_pct=arb.edge_pct * 100.0,
                        hold_hint_hours=6.0,
                    )
                    ops.append(Opportunity(
                        opp_type=OpportunityType.EVENT_ARBITRAGE,
                        currency="ARB",
                        side="PAIR",
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
                    crypto_symbol.replace("-USD", ""), side, conf,
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
                "strategy_name": opp.meta.get("strategy_name") or opp.opp_type.name,
                "trade_style": opp.meta.get("trade_style", "") or style_map.get(opp.opp_type.name, ""),
                "signal_reason": opp.reason,
                "confidence": float(opp.meta.get("final_confidence", opp.priority) or opp.priority or 0),
                "opportunity_score": float(opp.meta.get("opportunity_score", opp.priority) or opp.priority or 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "portfolio_optimizer",
                "graph_score": opp.meta.get("graph_score"),
                "graph_overlay": opp.meta.get("graph_multiplier"),
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

        # 1. For sells, convert USD amount to base quantity
        is_quote = opp.side == "BUY"
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
        if not is_quote:
            holder = self.state.holdings.get(opp.currency, {})
            price = holder.get("price", 0) or 1
            base_qty = opp.size_usd / price if price > 0 else 0
            if base_qty <= 0:
                logger.warning("  → Cannot compute base quantity, skipping")
                return

        # 2. Preview (dry-run)
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

        # 3. Risk check
        if total_fee > opp.size_usd * 0.02:
            logger.warning("  → Fee too high (%.2f%%), skipping", total_fee / opp.size_usd * 100)
            return

        # 4. If approval is required, send email and pend (skip execution)
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
                    pending = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                pending = {}
            pending[token] = pending_entry
            with open(self.pending_file, "w") as f:
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

        # 5. Execute or dry-run
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

        # 4. Record
        self.last_execution[opp.opp_type.value] = time.time()
        if opp.opp_type == OpportunityType.VOLUME_CYCLE:
            self.position_ages[opp.currency] = 0
        elif opp.opp_type == OpportunityType.TLH:
            self.cost_bases.pop(opp.currency, None)  # reset cost basis after sale

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
    parser.add_argument("--pending-file", default="pending_approvals.json",
                        help="Path for pending approvals JSON (default: pending_approvals.json)")
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
