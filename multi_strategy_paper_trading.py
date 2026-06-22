#!/usr/bin/env python3
"""
Multi-Strategy Paper Trading — holistic confidence, opportunity cost, fee-tier volume optimization.
Fetches ALL Coinbase USD pairs concurrently, scores by risk-adjusted opportunity, ranks cross-market,
and boosts trades that help reach the next Coinbase fee tier (lower maker/taker rates for more volume).
"""

import sys, asyncio, json, time, datetime, math
from pathlib import Path
from dataclasses import dataclass, field
from trading_system.signal_confidence import ConfidenceEngine
from enum import Enum
import statistics
from typing import Optional, List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

from state_store import StateStore
from market_universe import discover_coinbase_products

PM_ROOT = Path("/home/scott/git/portfolio-management")
if str(PM_ROOT) not in sys.path:
    sys.path.insert(0, str(PM_ROOT))


# ======================================================================
# Coinbase fee tier table (maker / taker by rolling 30d volume)
# ======================================================================

@dataclass
class FeeTier:
    min_volume: float
    maker_rate: float
    taker_rate: float


COINBASE_FEE_TIERS: List[FeeTier] = [
    FeeTier(0, 0.0060, 0.0120),
    FeeTier(1_000, 0.0035, 0.0075),
    FeeTier(10_000, 0.0025, 0.0040),
    FeeTier(50_000, 0.0015, 0.0025),
    FeeTier(100_000, 0.0010, 0.0020),
    FeeTier(1_000_000, 0.0008, 0.0018),
    FeeTier(20_000_000, 0.0005, 0.0015),
]


class FeeTierManager:
    """Tracks rolling 30d volume and computes current Coinbase fee tier."""

    def __init__(self, initial_volume_30d: float = 0.0):
        self._initial_volume = initial_volume_30d
        self._trades_30d: List[Tuple[float, float]] = []

    @property
    def rolling_30d_volume(self) -> float:
        return self._initial_volume + sum(v for _, v in self._trades_30d)

    def get_current_tier(self) -> FeeTier:
        vol = self.rolling_30d_volume
        for tier in reversed(COINBASE_FEE_TIERS):
            if vol >= tier.min_volume:
                return tier
        return COINBASE_FEE_TIERS[0]

    def get_next_tier(self) -> Optional[FeeTier]:
        current = self.get_current_tier()
        for tier in COINBASE_FEE_TIERS:
            if tier.min_volume > current.min_volume:
                return tier
        return None

    def volume_to_next_tier(self) -> float:
        current = self.get_current_tier()
        next_tier = self.get_next_tier()
        if next_tier:
            return max(0.0, next_tier.min_volume - self.rolling_30d_volume)
        return 0.0

    def record_trade(self, volume_usd: float, timestamp: Optional[float] = None):
        ts = timestamp or time.time()
        self._trades_30d.append((ts, volume_usd))
        self._prune()

    def to_state(self) -> dict:
        return {
            "initial_volume_30d": self._initial_volume,
            "trades_30d": self._trades_30d,
        }

    @classmethod
    def from_state(cls, state: dict | None) -> "FeeTierManager":
        state = state or {}
        mgr = cls(initial_volume_30d=float(state.get("initial_volume_30d", 0.0)))
        trades = state.get("trades_30d", []) or []
        mgr._trades_30d = [
            (float(ts), float(vol))
            for ts, vol in trades
            if isinstance(ts, (int, float)) and isinstance(vol, (int, float))
        ]
        mgr._prune()
        return mgr

    def _prune(self):
        cutoff = time.time() - 30 * 86400
        self._trades_30d = [(ts, v) for ts, v in self._trades_30d if ts > cutoff]

    def fee_cost(self, trade_volume: float, is_maker: bool) -> float:
        tier = self.get_current_tier()
        rate = tier.maker_rate if is_maker else tier.taker_rate
        return trade_volume * rate

    def maker_rate(self) -> float:
        return self.get_current_tier().maker_rate

    def taker_rate(self) -> float:
        return self.get_current_tier().taker_rate

    def tier_display(self) -> str:
        tier = self.get_current_tier()
        next_tier = self.get_next_tier()
        base = (f"Tier ${tier.min_volume:,.0f}+: "
                f"maker={tier.maker_rate*100:.2f}% taker={tier.taker_rate*100:.2f}%")
        if next_tier:
            needed = self.volume_to_next_tier()
            base += (f" | ${needed:,.0f} to next tier "
                     f"(maker={next_tier.maker_rate*100:.2f}%)")
        return base


class VolumeOptimizer:
    """Volume-aware opportunity scorer that rewards trades boosting fee tier."""

    def __init__(self, fee_manager: FeeTierManager):
        self.fee_manager = fee_manager

    def volume_boost(self, trade_volume: float) -> float:
        needed = self.fee_manager.volume_to_next_tier()
        if needed <= 0:
            return 1.0
        proximity = min(trade_volume / max(needed, 1), 1.0)
        return 1.0 + proximity * 0.4

    def adjusted_opportunity_score(self, base_score: float, trade_volume: float) -> float:
        return base_score * self.volume_boost(trade_volume)


# ======================================================================
# Signal types & base strategy
# ======================================================================

@dataclass
class Signal:
    symbol: str
    action: str
    strength: float
    reason: str
    strategy: str


class StrategyType(Enum):
    MOMENTUM = "momentum"
    MEAN_REVERSION = "mean_reversion"
    BREAKOUT = "breakout"
    RSI_OVERSOLD_OVERBOUGHT = "rsi_oscillator"
    VOLATILITY_BREAKOUT = "volatility_breakout"
    SCALPING = "scalping"


class Strategy:
    def __init__(self, name: str, strategy_type: StrategyType):
        self.name = name
        self.type = strategy_type
        self.signals: List[Signal] = []

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        raise NotImplementedError


# ======================================================================
# Strategy implementations
# ======================================================================

class MomentumStrategy(Strategy):
    def __init__(self, lookback_period: int = 10):
        super().__init__("Momentum", StrategyType.MOMENTUM)
        self.lookback = lookback_period

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.lookback:
            return None
        recent = [h for h in history[-self.lookback:] if isinstance(h, dict) and "close" in h]
        if not recent:
            return None
        change_pct = price_data.get("price_percentage_change_24h", 0)
        if change_pct > 2.5:
            strength = min(change_pct / 5.0, 1.0)
            return Signal(symbol, "BUY", strength, f"Momentum +{change_pct:.1f}%", "momentum")
        if change_pct < -2.5:
            strength = min(abs(change_pct) / 5.0, 1.0)
            return Signal(symbol, "SELL", strength, f"Momentum {change_pct:.1f}%", "momentum")
        return None


class MeanReversionStrategy(Strategy):
    def __init__(self, lookback_period: int = 20):
        super().__init__("Mean Reversion", StrategyType.MEAN_REVERSION)
        self.lookback = lookback_period

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.lookback:
            return None
        price = float(price_data.get("price", 0))
        vals = [h["close"] for h in history[-self.lookback:] if "close" in h]
        if not vals:
            return None
        mean = sum(vals) / len(vals)
        std = statistics.stdev(vals) if len(vals) > 1 else 0
        if std == 0:
            return None
        z = (price - mean) / std
        if z < -2.0:
            return Signal(symbol, "BUY", min(abs(z) / 4.0, 1.0), f"Reversion z={z:.2f}", "mean_reversion")
        if z > 2.0:
            return Signal(symbol, "SELL", min(z / 4.0, 1.0), f"Reversion z={z:.2f}", "mean_reversion")
        return None


class RSIStrategy(Strategy):
    def __init__(self, period: int = 14, oversold: int = 30, overbought: int = 70):
        super().__init__("RSI Oscillator", StrategyType.RSI_OVERSOLD_OVERBOUGHT)
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.period + 1:
            return None
        closes = [h["close"] for h in history[-(self.period + 1):] if "close" in h]
        if not closes or len(closes) < self.period + 1:
            return None
        gains, losses = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i - 1]
            gains.append(diff if diff > 0 else 0)
            losses.append(abs(diff) if diff < 0 else 0)
        avg_gain = sum(gains[-self.period:]) / self.period
        avg_loss = sum(losses[-self.period:]) / self.period
        if avg_loss == 0:
            return None
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        if rsi < self.oversold:
            return Signal(symbol, "BUY", min((self.oversold - rsi) / 30.0, 1.0), f"RSI={rsi:.0f} oversold", "rsi")
        if rsi > self.overbought:
            return Signal(symbol, "SELL", min((rsi - self.overbought) / 30.0, 1.0), f"RSI={rsi:.0f} overbought", "rsi")
        return None


class BreakoutStrategy(Strategy):
    def __init__(self, lookback_period: int = 50):
        super().__init__("Breakout", StrategyType.BREAKOUT)
        self.lookback = lookback_period

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.lookback:
            return None
        ohlvs = [h for h in history[-self.lookback:] if all(k in h for k in ["open", "high", "low", "close"])]
        if not ohlvs:
            return None
        resistance = max(o["high"] for o in ohlvs)
        support = min(o["low"] for o in ohlvs)
        current_price = float(price_data.get("price", 0))
        change_pct = price_data.get("price_percentage_change_24h", 0)
        if current_price > resistance * 1.005 and change_pct > 2:
            strength = min((current_price / resistance - 1) / 0.01, 1.0)
            return Signal(symbol, "BUY", strength, f"Breakout above ${resistance:.2f}", "breakout")
        if current_price < support * 0.995 and change_pct < -1:
            strength = min((support / current_price - 1) / 0.01, 1.0)
            return Signal(symbol, "SELL", strength, f"Breakdown below ${support:.2f}", "breakout")
        return None


class VolatilityStrategy(Strategy):
    def __init__(self, atr_period: int = 14):
        super().__init__("ATR Volatility", StrategyType.VOLATILITY_BREAKOUT)
        self.atr_period = atr_period

    def calculate_atr(self, history: list) -> float:
        if len(history) < self.atr_period + 1:
            return 0.0
        trs = []
        for i in range(1, min(len(history), self.atr_period + 2)):
            h, l, pc = history[i]["high"], history[i]["low"], history[i - 1].get("close", 0)
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        return statistics.mean(trs) if trs else 0.0

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        if len(history) < self.atr_period:
            return None
        current_price = float(price_data.get("price", 0))
        change_pct = price_data.get("price_percentage_change_24h", 0)
        atr = self.calculate_atr(history)
        if atr == 0:
            return None
        if change_pct < -3.0 and current_price > 0:
            strength = min(1.5 / max(3.0, abs(change_pct) / 2.0), 1.0)
            return Signal(symbol, "BUY", strength, f"Oversold bounce (Δ{change_pct:.1f}%, ATR={atr:.4f})", "volatility")
        if change_pct > 3.5:
            strength = min(1.5 / max(3.5, change_pct / 2.0), 1.0)
            return Signal(symbol, "SELL", strength, f"Overextended (Δ{change_pct:.1f}%)", "volatility")
        return None


class ScalpingStrategy(Strategy):
    """Short-term scalping: micro-moves (0.5-1.5%) for quick volume-generating trades.
    
    Designed to produce trade volume for Coinbase fee tier progression. Trades may
    only break even or take micro-profit — the primary goal is generating 30d volume.
    """

    def __init__(self):
        super().__init__("Scalper", StrategyType.SCALPING)

    def generate_signal(self, symbol: str, price_data: dict, history: list) -> Optional[Signal]:
        change_pct = price_data.get("price_percentage_change_24h", 0)
        if not isinstance(change_pct, (int, float)):
            return None
        spread = float(price_data.get("spread", 0.001))
        spread_cost = spread * 100
        min_move = max(spread_cost * 1.5, 0.4)

        if -1.5 < change_pct < -min_move:
            strength = min(abs(change_pct) / 3.0, 0.6)
            return Signal(symbol, "BUY", strength,
                          f"Scalp Δ={change_pct:.1f}% spread={spread_cost:.1f}%", "scalper")
        if min_move < change_pct < 1.5:
            strength = min(change_pct / 3.0, 0.6)
            return Signal(symbol, "SELL", strength,
                          f"Scalp Δ={change_pct:.1f}% spread={spread_cost:.1f}%", "scalper")
        return None


# ======================================================================
# Paper trading system — with volume tracking & fee-tier awareness
# ======================================================================

class PaperTradingSystem:
    def __init__(self, initial_capital: float = 10000.0, state_db: str = "paper_trading_state.db"):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions: Dict[str, dict] = {}
        self.trades: List[dict] = []
        self.snapshots: List[dict] = []
        self.win_rate_tracker: Dict[str, List[bool]] = {}
        self.volume_tracker = FeeTierManager(initial_volume_30d=0.0)
        self._connector = None
        self._state_store = StateStore(state_db)
        self._state_key = "multi_strategy_paper_state"
        self._load_state()
        self.trades = [self._normalize_trade(row) for row in self._state_store.load_trades(limit=5000)] + self.trades

    def _serialize_state(self) -> dict:
        return {
            "capital": self.capital,
            "positions": self.positions,
            "win_rate_tracker": self.win_rate_tracker,
            "volume_tracker": self.volume_tracker.to_state(),
        }

    def _load_state(self) -> None:
        raw = self._state_store.get_meta(self._state_key)
        if not raw:
            return
        try:
            data = json.loads(raw)
        except Exception:
            return
        self.capital = float(data.get("capital", self.initial_capital))
        self.positions = data.get("positions", {}) or {}
        self.win_rate_tracker = {
            key: [bool(v) for v in vals]
            for key, vals in (data.get("win_rate_tracker", {}) or {}).items()
        }
        self.volume_tracker = FeeTierManager.from_state(data.get("volume_tracker"))

    def _persist_state(self) -> None:
        self._state_store.set_meta(self._state_key, json.dumps(self._serialize_state(), default=str))

    def _normalize_trade(self, row: dict) -> dict:
        action = row.get("action", row.get("side", row.get("type", ""))).upper()
        symbol = row.get("symbol", row.get("currency", ""))
        return {
            "timestamp": row.get("timestamp", ""),
            "action": action,
            "symbol": symbol,
            "price": float(row.get("price", 0) or 0),
            "quantity": float(row.get("quantity", 0) or 0),
            "size_usd": float(row.get("size_usd", 0) or 0),
            "fee_usd": float(row.get("fee_usd", row.get("fee", 0)) or 0),
            "pnl_usd": float(row.get("pnl_usd", 0) or 0),
            "reason": row.get("reason", ""),
            "strategy": row.get("strategy", row.get("type", "")),
        }

    def get_connector(self):
        if self._connector is None:
            from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3
            self._connector = CoinbaseConnectorV3()
        return self._connector

    def mark_to_market(self, price_map: Dict[str, float]) -> None:
        for symbol, pos in self.positions.items():
            current_price = float(price_map.get(symbol, pos.get("current_price", pos.get("avg_cost", 0.0))))
            qty = float(pos.get("quantity", 0.0))
            avg_cost = float(pos.get("avg_cost", 0.0))
            pos["current_price"] = current_price
            pos["value"] = qty * current_price
            pos["unrealized_pnl"] = (current_price - avg_cost) * qty
            pos["unrealized_pnl_pct"] = ((current_price / avg_cost) - 1.0) * 100 if avg_cost > 0 else 0.0

    def record_trade_outcome(self, strategy_id: str, won: bool):
        self.win_rate_tracker.setdefault(strategy_id, []).append(won)

    def get_win_rate(self, strategy_id: str) -> float:
        outcomes = self.win_rate_tracker.get(strategy_id, [])
        if not outcomes:
            return 0.5
        return sum(outcomes) / len(outcomes)

    def get_win_rates_for_all(self) -> Dict[Tuple[str, str], float]:
        rates: Dict[Tuple[str, str], float] = {}
        grouped: Dict[Tuple[str, str], List[dict]] = {}
        for trade in self.trades:
            if trade.get("action") != "SELL":
                continue
            key = (trade.get("strategy", ""), trade.get("symbol", ""))
            grouped.setdefault(key, []).append(trade)
        for key, sells in grouped.items():
            wins = sum(1 for t in sells if float(t.get("pnl_usd", 0.0)) > 0)
            rates[key] = wins / len(sells) if sells else -1.0
        return {k: v for k, v in rates.items() if v >= 0}

    def calculate_portfolio_value(self) -> float:
        pv = sum(
            float(p.get("value", float(p.get("quantity", 0.0)) * float(p.get("current_price", p.get("avg_cost", 0.0)))))
            for p in self.positions.values()
        )
        return self.capital + pv

    def get_position_pnl(self, symbol: str) -> Tuple[float, float]:
        if symbol not in self.positions:
            return 0.0, 0.0
        pos = self.positions[symbol]
        cp = float(pos.get("current_price", pos.get("avg_cost", 0)))
        pnl = (cp - pos["avg_cost"]) * pos["quantity"]
        pnl_pct = (pnl / max(pos["avg_cost"] * pos["quantity"], 1)) * 100
        return pnl, pnl_pct

    def _log_trade(self, action: str, signal: Signal, price: float, qty: int,
                   size_usd: float, fee_usd: float, pnl_usd: float = 0.0):
        self.volume_tracker.record_trade(size_usd)
        trade = {
            "timestamp": datetime.datetime.now().isoformat(),
            "action": action, "symbol": signal.symbol, "price": price,
            "quantity": qty, "size_usd": round(size_usd, 2),
            "fee_usd": round(fee_usd, 2),
            "pnl_usd": round(pnl_usd, 2),
            "reason": signal.reason, "strategy": signal.strategy,
        }
        self.trades.append(trade)
        self._state_store.save_trade({
            "timestamp": trade["timestamp"],
            "action": action,
            "side": action,
            "currency": signal.symbol,
            "symbol": signal.symbol,
            "size_usd": round(size_usd, 2),
            "fee": round(fee_usd, 2),
            "price": price,
            "quantity": qty,
            "strategy": signal.strategy,
            "pnl_usd": round(pnl_usd, 2),
            "reason": signal.reason,
            "dry_run": True,
        })
        self._persist_state()

    def execute_buy(self, signal: Signal, current_price: float, size_usd: float):
        if signal.symbol in self.positions:
            return
        if current_price <= 0 or size_usd <= 0:
            return
        qty = round(size_usd / current_price, 8)
        if qty == 0:
            return
        actual_cost = round(qty * current_price, 2)
        fee = self.volume_tracker.fee_cost(actual_cost, is_maker=False)
        total_cost = actual_cost + fee
        if total_cost > self.capital:
            return
        self.positions[signal.symbol] = {
            "symbol": signal.symbol, "quantity": qty, "avg_cost": current_price,
            "value": actual_cost, "entry_reason": signal.reason,
            "entry_strategy": signal.strategy,
            "current_price": current_price,
            "entry_fee": fee,
        }
        self.capital -= total_cost
        self._log_trade("BUY", signal, current_price, qty, actual_cost, fee, pnl_usd=0.0)
        print(f"   BUY {signal.symbol} ${actual_cost:.0f} @ ${current_price:,.2f} fee=${fee:.2f} | {signal.reason}")

    def execute_sell(self, signal: Signal, current_price: float):
        if signal.symbol not in self.positions:
            return
        pos = self.positions[signal.symbol]
        qty = pos["quantity"]
        pnl, pnl_pct = self.get_position_pnl(signal.symbol)
        proceeds = round(qty * current_price, 2)
        fee = self.volume_tracker.fee_cost(proceeds, is_maker=False)
        net_proceeds = proceeds - fee
        self.record_trade_outcome(signal.strategy, pnl > 0)
        del self.positions[signal.symbol]
        self.capital += net_proceeds
        self._log_trade("SELL", signal, current_price, qty, proceeds, fee, pnl_usd=pnl - fee)
        print(f"   SELL {signal.symbol} @ ${current_price:,.2f} net=${net_proceeds:.2f} fee=${fee:.2f} | PnL: ${pnl:,.2f} | {signal.reason}")

    def cycle_position(self, symbol: str, current_price: float, reason: str = "volume cycle"):
        """Close regardless of PnL to generate volume for the fee tier."""
        if symbol not in self.positions:
            return False
        pos = self.positions[symbol]
        qty = pos["quantity"]
        pnl, _ = self.get_position_pnl(symbol)
        proceeds = round(qty * current_price, 2)
        fee = self.volume_tracker.fee_cost(proceeds, is_maker=False)
        net_proceeds = proceeds - fee
        sig = Signal(symbol, "SELL", 0.5, reason, pos.get("entry_strategy", "cycle"))
        self.record_trade_outcome(sig.strategy, pnl > 0)
        del self.positions[symbol]
        self.capital += net_proceeds
        self._log_trade("SELL", sig, current_price, qty, proceeds, fee, pnl_usd=pnl - fee)
        print(f"   CYCLE {symbol} @ ${current_price:,.2f} PnL=${pnl:,.2f} fee=${fee:.2f} | {reason}")
        return True


# ======================================================================
# Concurrent market data fetching — ALL Coinbase USD pairs
# ======================================================================

def _parse_market_data(price_data: dict, orderbook: dict) -> Optional[dict]:
    if isinstance(price_data, str):
        try:
            price_data = json.loads(price_data)
        except Exception:
            return None
    if not isinstance(price_data, dict) or "price" not in price_data:
        return None
    bids = orderbook.get("bids", [])
    asks = orderbook.get("asks", [])
    spread_frac = 0.0
    if bids and asks:
        bb = float(bids[0].get("price", 0))
        ba = float(asks[0].get("price", 0))
        mid = (bb + ba) / 2
        if mid > 0:
            spread_frac = (ba - bb) / mid
    return {
        "price": float(price_data["price"]),
        "change_pct": float(price_data.get("price_percentage_change_24h", 0)),
        "price_percentage_change_24h": float(price_data.get("price_percentage_change_24h", 0)),
        "volume": float(price_data.get("volume_24h", 0)),
        "volume_24h": float(price_data.get("volume_24h", 0)),
        "high_24h": float(price_data.get("high_24h", 0)),
        "low_24h": float(price_data.get("low_24h", 0)),
        "spread": spread_frac,
    }


def _fetch_single_sync(connector, pair: str) -> Tuple[str, Optional[dict]]:
    try:
        price = connector.get_price(pair)
        ob = connector.get_order_book(pair, level=1)
        parsed = _parse_market_data(price, ob)
        return pair, parsed
    except Exception:
        return pair, None


async def fetch_all_market_data(connector, max_pairs: int = 0) -> Dict[str, dict]:
    entries = discover_coinbase_products(connector, max_pairs=max_pairs)
    coinbase_pairs = [e.symbol for e in entries]
    if not coinbase_pairs:
        coinbase_pairs = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]
        print("   [warning] Coinbase discovery returned no active products, falling back to watchlist")

    print(f"   Discovered {len(coinbase_pairs)} active Coinbase pairs")
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(15)

    async def fetch_one(pair: str) -> Tuple[str, Optional[dict]]:
        async with sem:
            return await loop.run_in_executor(None, _fetch_single_sync, connector, pair)

    tasks = [fetch_one(p) for p in coinbase_pairs]
    results = await asyncio.gather(*tasks)
    data = {pair: md for pair, md in results if md is not None}
    print(f"   Fetched market data for {len(data)} pairs")
    return data


# ======================================================================
# Liquidity tiers — auto-assigned from 24h volume
# ======================================================================

def assign_liquidity_tiers(market_data: Dict[str, dict]) -> Dict[str, int]:
    volumes = [(sym, md.get("volume", 0)) for sym, md in market_data.items()]
    volumes.sort(key=lambda x: x[1], reverse=True)
    total = len(volumes)
    tiers: Dict[str, int] = {}
    for i, (sym, _) in enumerate(volumes):
        pct = i / total
        if pct < 0.1:
            tiers[sym] = 1
        elif pct < 0.25:
            tiers[sym] = 2
        elif pct < 0.50:
            tiers[sym] = 3
        elif pct < 0.75:
            tiers[sym] = 4
        else:
            tiers[sym] = 5
    return tiers


# ======================================================================
# Regime detection & consensus
# ======================================================================

def detect_regime(price_data: Dict[str, Any]) -> str:
    change_pct = abs(float(price_data.get("change_pct", 0)))
    if change_pct > 5.0:
        return "volatile"
    if change_pct > 2.0:
        return "trending"
    if change_pct < 0.5:
        return "quiet"
    return "neutral"


def compute_global_consensus(all_signals: List[Signal]) -> float:
    if not all_signals:
        return 0.0
    buys = sum(1 for s in all_signals if s.action == "BUY")
    sells = sum(1 for s in all_signals if s.action == "SELL")
    return max(buys, sells) / len(all_signals)


# ======================================================================
# Opportunity cost scoring & capital allocation
# ======================================================================

@dataclass
class ScoredSignal:
    signal: Signal
    market_data: dict
    modifiers_result: Any
    opportunity_score: float

    @property
    def net_confidence(self) -> float:
        return self.modifiers_result.modified_confidence

    @property
    def estimated_volume(self) -> float:
        price = float(self.market_data.get("price", 1))
        net_conf = self.net_confidence
        return net_conf * 1000 * (1 + price / 100_000)  # rough position size estimator


def score_opportunity(signal: Signal, market_data: dict, mod_result,
                      volume_optimizer: Optional["VolumeOptimizer"] = None) -> float:
    """Score a signal across all markets. Higher = better risk-adjusted opportunity.
    
    The base score considers net confidence, edge size, spread cost, and volume.
    If a VolumeOptimizer is provided, trades that help reach the next Coinbase fee
    tier receive a boost (up to +40%).
    """
    confidence = mod_result.modified_confidence
    spread = max(market_data.get("spread", 0.001), 0.0001)
    volume = float(market_data.get("volume", 1))

    edge = confidence * abs(signal.strength)
    vol_bonus = min(math.log10(max(volume / 100_000, 1)) + 0.5, 2.0)
    base_score = (edge / math.sqrt(spread)) * vol_bonus

    if volume_optimizer:
        est_vol = confidence * 1000
        base_score = volume_optimizer.adjusted_opportunity_score(base_score, est_vol)

    return round(base_score, 4)


def allocate_capital(
    scored: List[ScoredSignal],
    available_capital: float,
    max_positions: int = 10,
    max_risk_per_position: float = 0.20,
    min_allocate: float = 10.0,
) -> List[Dict]:
    scored.sort(key=lambda x: x.opportunity_score, reverse=True)
    top = scored[:max_positions]
    total_score = sum(s.opportunity_score for s in top) or 1.0
    defensive_capital = available_capital * 0.5

    allocations = []
    for s in top:
        fraction = s.opportunity_score / total_score
        allocated = defensive_capital * fraction
        allocated = min(allocated, available_capital * max_risk_per_position)
        if allocated < min_allocate:
            continue
        allocations.append({
            "signal": s.signal, "score": s.opportunity_score,
            "fraction": fraction, "allocated_usd": allocated,
            "net_confidence": s.net_confidence,
        })
    return allocations


# ======================================================================
# Main orchestrator
# ======================================================================

class MultiStrategyPaperTrading:
    def __init__(self, initial_capital: float = 10000.0, state_db: str = "paper_trading_state.db",
                 history_lookback_days: int = 10, max_pairs: int = 0, granularity: str = "ONE_HOUR"):
        self.system = PaperTradingSystem(initial_capital, state_db=state_db)
        self.volume_optimizer = VolumeOptimizer(self.system.volume_tracker)
        self.market_leaders: List[str] = ["BTC-USD", "ETH-USD"]
        self.confidence_engine: Optional[ConfidenceEngine] = None
        self.history_client = None
        self.history_lookback_days = history_lookback_days
        self.max_pairs = max_pairs
        self.granularity = granularity
        self.market_history: Dict[str, List[dict]] = {}
        self.strategies: List[Strategy] = [
            MomentumStrategy(),
            MeanReversionStrategy(lookback_period=20),
            RSIStrategy(period=14, oversold=30, overbought=70),
            BreakoutStrategy(lookback_period=50),
            VolatilityStrategy(atr_period=14),
            ScalpingStrategy(),
        ]
        # Auto-cycle: positions held this many ticks will be force-closed for volume
        self.auto_cycle_ticks = 6
        # Track entry ticks for each position to know when to cycle
        self._position_entry_tick: Dict[str, int] = {}

    def _normalize_history_frame(self, df) -> List[dict]:
        if df is None or getattr(df, "empty", True):
            return []
        history = []
        for _, row in df.tail(240).iterrows():
            history.append({
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0)),
            })
        return history

    def _seed_history(self, symbols: List[str]) -> None:
        if self.history_client is None:
            from coinbase.src.cb_client import CBClient
            try:
                self.history_client = CBClient()
            except Exception:
                self.history_client = None
                for sym in symbols:
                    self.market_history[sym] = []
                return
        try:
            from coinbase.src.data import fetch_candles_df
        except Exception:
            for sym in symbols:
                self.market_history[sym] = []
            return
        for sym in symbols:
            try:
                df = fetch_candles_df(
                    self.history_client,
                    sym,
                    lookback_days=self.history_lookback_days,
                    granularity=self.granularity,
                )
                self.market_history[sym] = self._normalize_history_frame(df)
            except Exception:
                self.market_history[sym] = []

    def _append_market_snapshot(self, market_data: Dict[str, dict]) -> None:
        for sym, pdata in market_data.items():
            price = float(pdata.get("price", 0.0))
            if price <= 0:
                continue
            history = self.market_history.setdefault(sym, [])
            prev_close = float(history[-1]["close"]) if history else price
            spread = max(float(pdata.get("spread", 0.001)), 0.001)
            volatility = max(abs(float(pdata.get("change_pct", 0.0))) / 100.0 * 0.02, spread * 1.5, 0.002)
            candle = {
                "open": prev_close,
                "high": max(price, prev_close) * (1 + volatility),
                "low": min(price, prev_close) * (1 - volatility),
                "close": price,
                "volume": float(pdata.get("volume", pdata.get("volume_24h", 0.0))),
            }
            history.append(candle)
            if len(history) > 300:
                del history[:-300]

    def _init_confidence_engine(self, market_data: Dict[str, dict]):
        tiers = assign_liquidity_tiers(market_data)
        self.confidence_engine = ConfidenceEngine(
            liquidity_tiers=tiers,
            regime_caps={"volatile": 0.4, "quiet": 0.6, "trending": 1.0, "neutral": 0.8},
        )
        print(f"   Assigned liquidity tiers for {len(tiers)} products")

    def _get_cycle_candidates(self, current_tick: int) -> List[Tuple[str, float]]:
        """Find positions that have been held long enough to force-close for volume."""
        candidates = []
        for sym, pos in list(self.system.positions.items()):
            entry_tick = self._position_entry_tick.get(sym, 0)
            held = current_tick - entry_tick
            if held >= self.auto_cycle_ticks and pos.get("value", 0) > 0:
                candidates.append((sym, pos["value"]))
        return candidates

    async def run_benchmark(self, duration_hours: int = 24, poll_interval: float = 60.0):
        print("=" * 80)
        print("HOLISTIC PAPER TRADING — FEE-TIER VOLUME OPTIMIZATION")
        print("=" * 80)
        print(f"Duration: {duration_hours}h | Poll: {poll_interval}s")
        print("=" * 80 + "\n")

        # Connector lazily initialized via get_connector() (CoinbaseConnectorV3)
        connector = self.system.get_connector()

        print("\n--- Market Scan ---")
        all_market_data = await fetch_all_market_data(connector, max_pairs=self.max_pairs)
        self._init_confidence_engine(all_market_data)

        symbols = sorted(all_market_data.keys(),
                         key=lambda s: all_market_data[s].get("volume", 0), reverse=True)
        print(f"   Tracking {len(symbols)} pairs\n")
        self._seed_history(symbols)

        tick_num = 0
        start_time = time.time()
        max_ticks = int(duration_hours * 3600 / poll_interval)

        while tick_num < max_ticks:
            try:
                tick_start = time.time()

                # --- 1. Concurrently refresh market data for ALL pairs ---
                market_data = await fetch_all_market_data(connector, max_pairs=self.max_pairs)
                if not market_data:
                    await asyncio.sleep(poll_interval)
                    tick_num += 1
                    continue

                self._append_market_snapshot(market_data)
                self.system.mark_to_market({sym: md.get("price", 0.0) for sym, md in market_data.items()})

                # --- 2. Detect regime ---
                btc_data = market_data.get("BTC-USD", {})
                regime = detect_regime(btc_data)

                # --- 3. Generate signals from ALL strategies on ALL pairs ---
                all_raw_signals: List[Tuple[Signal, dict]] = []
                for strategy in self.strategies:
                    for sym, pdata in market_data.items():
                        history = self.market_history.get(sym, [])
                        sig = strategy.generate_signal(sym, pdata, history)
                        if sig:
                            all_raw_signals.append((sig, pdata))

                buy_count = sum(1 for s, _ in all_raw_signals if s.action == "BUY")
                sell_count = sum(1 for s, _ in all_raw_signals if s.action == "SELL")

                print(f"\n   Tick #{tick_num} | {len(market_data)} pairs | "
                      f"{len(all_raw_signals)} signals ({buy_count} BUY, {sell_count} SELL) | "
                      f"Regime: {regime}")

                # --- 4. Apply confidence modifiers + opportunity scoring ---
                consensus = compute_global_consensus([s for s, _ in all_raw_signals])

                scored_signals: List[ScoredSignal] = []
                for sig, pdata in all_raw_signals:
                    try:
                        mod_result = self.confidence_engine.apply_modifiers(
                            signal=sig, market_data=pdata, regime=regime,
                            market_leaders=self.market_leaders,
                            sentiment_score=0.0, global_consensus=consensus,
                        )
                        sig.strength = mod_result.modified_confidence
                        opp_score = score_opportunity(sig, pdata, mod_result, self.volume_optimizer)
                        scored_signals.append(ScoredSignal(
                            signal=sig, market_data=pdata,
                            modifiers_result=mod_result, opportunity_score=opp_score,
                        ))
                    except Exception:
                        continue

                # --- 5. Rank & allocate for BUY signals ---
                buy_scored = [s for s in scored_signals if s.signal.action == "BUY"]
                sell_scored = [s for s in scored_signals if s.signal.action == "SELL"
                               and s.signal.symbol in self.system.positions]

                buy_allocations = allocate_capital(
                    buy_scored, self.system.capital,
                    max_positions=10, max_risk_per_position=0.15, min_allocate=10.0,
                )
                for alloc in buy_allocations:
                    sig = alloc["signal"]
                    price = market_data.get(sig.symbol, {}).get("price", 0)
                    if price > 0 and alloc["net_confidence"] > 0.1:
                        self.system.execute_buy(sig, price, alloc["allocated_usd"])
                        self._position_entry_tick[sig.symbol] = tick_num

                # --- 6. Execute sell signals ---
                for ss in sell_scored:
                    sig = ss.signal
                    price = market_data.get(sig.symbol, {}).get("price", 0)
                    if price > 0 and ss.net_confidence > 0.1:
                        self.system.execute_sell(sig, price)

                # --- 7. Auto-cycle stale positions for volume generation ---
                cycle_candidates = self._get_cycle_candidates(tick_num)
                for sym, val in cycle_candidates:
                    price = market_data.get(sym, {}).get("price", 0)
                    if price > 0:
                        self.system.cycle_position(sym, price,
                                                   f"auto-cycle after {self.auto_cycle_ticks} ticks")
                self.system._persist_state()

                # --- 8. Summary ---
                if scored_signals:
                    ranked = sorted(scored_signals, key=lambda x: x.opportunity_score, reverse=True)
                    best = ranked[0]
                    print(f"   Top: {best.signal.strategy} on {best.signal.symbol} "
                          f"score={best.opportunity_score:.2f} conf={best.net_confidence:.2f}")
                    for i, rs in enumerate(ranked[:3]):
                        vol = rs.estimated_volume
                        print(f"      #{i+1}: {rs.signal.symbol:>9s} {rs.signal.action:>4s} "
                              f"strat={rs.signal.strategy} score={rs.opportunity_score:.2f} "
                              f"conf={rs.net_confidence:.2f} est_vol=${vol:.0f}")

                # Fee tier & volume summary
                vt = self.system.volume_tracker
                print(f"   Volume: 30d=${vt.rolling_30d_volume:,.0f} | {vt.tier_display()}")

                elapsed_this = time.time() - tick_start
                if elapsed_this < poll_interval:
                    await asyncio.sleep(poll_interval - elapsed_this)

                tick_num += 1
                if tick_num % 10 == 0:
                    elapsed_total = time.time() - start_time
                    pv = self.system.calculate_portfolio_value()
                    fee_total = sum(t.get("fee_usd", 0) for t in self.system.trades)
                    print(f"\n   >>> Progress: {tick_num}/{max_ticks} ticks "
                          f"({elapsed_total/3600:.2f}h) | Portfolio: ${pv:,.2f} "
                          f"| Fees: ${fee_total:,.2f} | {vt.tier_display()} <<<\n")

            except Exception as e:
                print(f"   Error at tick {tick_num}: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(poll_interval)
                tick_num += 1

        # --- Final summary ---
        print("\n" + "=" * 80)
        print("BENCHMARK COMPLETE")
        print("=" * 72 + "\n")

        total_value = self.system.calculate_portfolio_value()
        pnl = total_value - self.system.initial_capital
        pnl_pct = (pnl / max(self.system.initial_capital, 1)) * 100
        fee_total = sum(t.get("fee_usd", 0) for t in self.system.trades)
        total_volume = sum(t.get("size_usd", 0) for t in self.system.trades)

        strategy_sigs: Dict[str, int] = {}
        for trade in self.system.trades:
            strat = trade.get("strategy", "unknown")
            action = trade["action"]
            strategy_sigs[f"{strat}:{action}"] = strategy_sigs.get(f"{strat}:{action}", 0) + 1

        vt = self.system.volume_tracker
        print(f"FINAL SUMMARY")
        print("-" * 40)
        print(f"   Duration:          {(time.time() - start_time) / 3600:.1f}h")
        print(f"   Total ticks:       {tick_num}")
        print(f"   Trades executed:   {len(self.system.trades)}")
        print(f"   Total volume:      ${total_volume:,.2f}")
        print(f"   Total fees paid:   ${fee_total:,.2f}")
        print(f"   Positions held:    {len(self.system.positions)}")
        print(f"\n   Performance:")
        print(f"   Initial capital:   ${self.system.initial_capital:,.2f}")
        print(f"   Final value:       ${total_value:,.2f}")
        print(f"   Absolute PnL:      ${pnl:,.2f} ({pnl_pct:.1f}%)")
        print(f"\n   Fee tier status:   {vt.tier_display()}")

        print(f"\nSignal distribution by strategy:")
        for key, count in sorted(strategy_sigs.items()):
            print(f"   {key}: {count}")

        print(f"\nWin rates by strategy:")
        for key, rate in sorted(self.system.get_win_rates_for_all().items()):
            print(f"   {key[0]} on {key[1]}: {rate:.1%}")

        results_file = Path("multi_strategy_results.json")
        with open(results_file, "w") as f:
            json.dump({
                "start_time": datetime.datetime.now().isoformat(),
                "end_time": datetime.datetime.now().isoformat(),
                "duration_hours": duration_hours,
                "initial_capital": self.system.initial_capital,
                "final_capital": self.system.capital,
                "total_trades": len(self.system.trades),
                "total_volume": round(total_volume, 2),
                "total_fees": round(fee_total, 2),
                "fee_tier_volume": vt.rolling_30d_volume,
                "fee_tier_maker": vt.maker_rate(),
                "fee_tier_taker": vt.taker_rate(),
                "positions_held": len(self.system.positions),
                "pnl_absolute": pnl,
                "pnl_percent": pnl_pct,
                "trades": self.system.trades[-200:],
                "signal_distribution": strategy_sigs,
                "tick_count": tick_num,
            }, f, indent=2)

        print(f"\nResults saved to {results_file}")
        return {
            "pnl_pct": pnl_pct, "total_trades": len(self.system.trades),
            "total_volume": total_volume, "total_fees": fee_total,
            "positions_held": len(self.system.positions),
            "signal_distribution": strategy_sigs,
        }


if __name__ == "__main__":
    async def main():
        bt = MultiStrategyPaperTrading(initial_capital=10000.0)
        results = await bt.run_benchmark(duration_hours=6, poll_interval=60.0)
        print("\nBenchmark complete!")

    asyncio.run(main())
