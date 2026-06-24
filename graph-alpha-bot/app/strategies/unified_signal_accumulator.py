#!/usr/bin/env python3
"""
Unified Signal Accumulator — aggregates ALL signal sources into a single ranked queue.

Integrates:
  1. 9 BTC-XXX volatility strategies (via unified_signal_generator)
  2. News sentiment analysis
  3. Multi-strategy paper trading signals (momentum, mean reversion, RSI, breakout, volatility, scalping)
  4. Fee-tier opportunity boosting

Outputs: single ordered list of ScoredSignal objects, ranked by opportunity_score.
"""

import sys, json, time, math, logging
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'graph-alpha-bot'))
sys.path.insert(0, str(ROOT / 'graph-alpha-bot' / 'app' / 'strategies'))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Canonical Coinbase spot universe
from coinbase_universe import COINBASE_SPOT_PAIRS, FEED_PRODUCTS


# ======================================================================
# Data structures
# ======================================================================

@dataclass
class AccumulatedSignal:
    """A fully scored, ranked signal ready for consumption by the trading engine."""

    symbol: str
    action: str  # BUY, SELL, CLOSE
    base_confidence: float  # 0.0–1.0 from strategy
    final_confidence: float  # after modifiers (regime, fee-tier, etc.)
    opportunity_score: float  # composite rank score
    strategy_name: str
    signal_reason: str
    estimated_volume_usd: float
    market_data: dict = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "action": self.action,
            "base_confidence": round(self.base_confidence, 3),
            "final_confidence": round(self.final_confidence, 3),
            "opportunity_score": round(self.opportunity_score, 4),
            "strategy_name": self.strategy_name,
            "signal_reason": self.signal_reason,
            "estimated_volume_usd": round(self.estimated_volume_usd, 2),
            "timestamp": self.timestamp.isoformat(),
        }


# ======================================================================
# Price / market-data provider (Coinbase via CLI)
# ======================================================================

class CoinbasePriceProvider:
    """Thin wrapper around Coinbase CLI for price + 24h stats."""

    def __init__(self):
        self._cache: Dict[str, dict] = {}
        self._cache_ts: float = 0
        self._cache_ttl = 15  # seconds

    def _is_cache_fresh(self) -> bool:
        return bool(self._cache) and (time.time() - self._cache_ts) < self._cache_ttl

    def get_price(self, symbol: str) -> Optional[float]:
        md = self.get_market_data([symbol])
        entry = md.get(symbol)
        return float(entry["price"]) if entry else None

    def get_market_data(self, symbols: List[str]) -> Dict[str, dict]:
        if self._is_cache_fresh() and symbols and all(s in self._cache for s in symbols):
            return {s: self._cache[s] for s in symbols if s in self._cache}

        import subprocess, re
        coinbase_cli = "/home/scott/.npm-global/bin/coinbase"

        results: Dict[str, dict] = {}
        for sym in symbols:
            try:
                result = subprocess.run(
                    [coinbase_cli, "products", "ticker", sym],
                    capture_output=True, text=True, timeout=10,
                )
                if result.returncode == 0:
                    output = result.stdout.strip()
                    # Parse JSON-like output from ticker
                    price_match = re.search(r'"price":\s*"([\d.]+)"', output)
                    change_match = re.search(r'"price_percentage_change_24h":\s*"([\d.-]+)"', output)
                    volume_match = re.search(r'"volume_24h":\s*"([\d.]+)"', output)

                    price = float(price_match.group(1)) if price_match else None
                    change_pct = float(change_match.group(1)) if change_match else 0.0
                    volume = float(volume_match.group(1)) if volume_match else 0.0

                    if price is None:
                        # Try parsing lines differently
                        for line in output.split("\n"):
                            line = line.strip()
                            if "price" in line.lower() and ":" in line:
                                try:
                                    price = float(line.split(":")[1].strip().replace('"', '').replace(",", ""))
                                except (ValueError, IndexError):
                                    pass
                                if price:
                                    break

                    if price:
                        results[sym] = {
                            "price": price,
                            "change_pct": change_pct,
                            "volume_24h": volume,
                            "high_24h": price * (1 + abs(change_pct) / 200),
                            "low_24h": price * (1 - abs(change_pct) / 200),
                        }
                else:
                    logger.warning(f"CLI error for {sym}: {result.stderr.strip()[:100]}")
            except Exception as e:
                logger.warning(f"Failed to fetch {sym}: {e}")

            if sym not in results:
                results[sym] = {"price": 0.0, "change_pct": 0.0, "volume_24h": 0}

        if results:
            self._cache.update(results)
            self._cache_ts = time.time()

        return results


# ======================================================================
# Strategy wrappers — normalise all signal sources to AccumulatedSignal
# ======================================================================

class StrategySignalAdapter:
    """Adapts the 9 BTC-XXX strategies from unified_signal_generator."""

    def __init__(self):
        from unified_signal_generator import (
            UnifiedSignalGenerator, UnifiedSignalConfig,
            StrategySignalGenerator as Gen,
        )
        self.generator = Gen()
        self.config = UnifiedSignalConfig()

    def get_signals(self, symbol: str, price: float, history: List[Dict]) -> List[AccumulatedSignal]:
        raw = self.generator.generate_strategy_signals(symbol, history or [])
        results = []
        for sig in raw:
            direction = {"LONG": "BUY", "SHORT": "SELL", "CLOSE": "CLOSE"}.get(sig.direction, "CLOSE")
            results.append(AccumulatedSignal(
                symbol=symbol, action=direction,
                base_confidence=sig.confidence,
                final_confidence=sig.confidence,
                opportunity_score=sig.confidence * 0.5,
                strategy_name=sig.strategy_name,
                signal_reason=sig.signal_reason,
                estimated_volume_usd=sig.confidence * 1000,
                market_data={"price": price, "change_pct": sig.price_change_pct},
            ))
        return results


class NewsSentimentAdapter:
    """Adapts news sentiment signals."""

    def __init__(self):
        from unified_signal_generator import NewsSentimentAnalyzer
        self.analyzer = NewsSentimentAnalyzer(list(COINBASE_SPOT_PAIRS))

    def get_signals(self, price_map: Dict[str, float]) -> List[AccumulatedSignal]:
        rich_data = self.analyzer.analyze_full()
        results = []
        SIGNAL_TEMPLATES = [
            ("NewsSentiment", "avg_sentiment", None,
             lambda info: abs(info["avg_sentiment"]) > 0.25 and info["count"] >= 2,
             lambda info: ("BUY" if info["avg_sentiment"] > 0 else "SELL", min(abs(info["avg_sentiment"]) * 0.8, 0.95)),
             lambda info: f"Sentiment {info['avg_sentiment']:.2f} ({info['count']} articles)"),
            ("NewsHackAlert", "hack_count", "SELL",
             lambda info: info["hack_count"] >= 1,
             lambda info: ("SELL", min(0.3 + info["hack_count"] * 0.05, 0.7)),
             lambda info: f"Security incident ({info['hack_count']} articles)"),
            ("NewsRegulationWatch", "regulation_count", "SELL",
             lambda info: info["regulation_count"] >= 2,
             lambda info: ("SELL", min(0.2 + info["regulation_count"] * 0.04, 0.6)),
             lambda info: f"Regulatory coverage ({info['regulation_count']} articles)"),
            ("NewsAdoptionSignal", "adoption_count", "BUY",
             lambda info: info["adoption_count"] >= 2,
             lambda info: ("BUY", min(0.2 + info["adoption_count"] * 0.05, 0.6)),
             lambda info: f"Adoption news ({info['adoption_count']} articles)"),
            ("NewsTechSignal", "technology_count", "BUY",
             lambda info: info["technology_count"] >= 2,
             lambda info: ("BUY", min(0.15 + info["technology_count"] * 0.03, 0.5)),
             lambda info: f"Technology upgrade ({info['technology_count']} articles)"),
        ]

        for symbol_base, info in rich_data.items():
            symbol = f"{symbol_base}-USD"
            price = price_map.get(symbol, 0)

            for sname, count_field, force_dir, pred_fn, conf_fn, reason_fn in SIGNAL_TEMPLATES:
                if not pred_fn(info):
                    continue
                action, confidence = conf_fn(info)
                if force_dir and action != force_dir:
                    continue
                reason = reason_fn(info)
                # Boost if breaking
                breaking = info.get("breaking_ratio", 0)
                if breaking > 0.3:
                    confidence = min(confidence * 1.3, 0.95)
                    reason += " [BREAKING]"
                results.append(AccumulatedSignal(
                    symbol=symbol, action=action,
                    base_confidence=confidence,
                    final_confidence=confidence,
                    opportunity_score=confidence * 0.5,
                    strategy_name=sname,
                    signal_reason=reason,
                    estimated_volume_usd=confidence * 800,
                    market_data={
                        "price": price, "change_pct": 0,
                        "topics": info.get("topics", []),
                        "breaking_ratio": breaking,
                    },
                ))

        # Deduplicate: for same (symbol, action), keep highest confidence
        seen: dict = {}
        for sig in results:
            key = (sig.symbol, sig.action)
            if key not in seen or sig.final_confidence > seen[key].final_confidence:
                seen[key] = sig
        return list(seen.values())


class MultiStrategyAdapter:
    """Adapts the 6 multi-strategy signals (momentum, mean-reversion, RSI, breakout, volatility, scalping)."""

    def __init__(self):
        from multi_strategy_paper_trading import (
            MomentumStrategy, MeanReversionStrategy, RSIStrategy,
            BreakoutStrategy, VolatilityStrategy, ScalpingStrategy,
        )
        self.strategies: List[Any] = [
            MomentumStrategy(lookback_period=10),
            MeanReversionStrategy(lookback_period=20),
            RSIStrategy(period=14, oversold=30, overbought=70),
            BreakoutStrategy(lookback_period=50),
            VolatilityStrategy(atr_period=14),
            ScalpingStrategy(),
        ]

    def get_signals(self, symbol: str, price_data: dict, history: List[Dict]) -> List[AccumulatedSignal]:
        results = []
        for strat in self.strategies:
            try:
                sig = strat.generate_signal(symbol, price_data, history or [])
                if sig:
                    estimated_vol = sig.strength * 1000
                    results.append(AccumulatedSignal(
                        symbol=symbol, action=sig.action,
                        base_confidence=sig.strength,
                        final_confidence=sig.strength,
                        opportunity_score=sig.strength * 0.5,
                        strategy_name=f"Multi:{strat.name}",
                        signal_reason=sig.reason,
                        estimated_volume_usd=estimated_vol,
                        market_data=price_data,
                    ))
            except Exception as e:
                logger.debug(f"Multi-strategy {strat.name} failed for {symbol}: {e}")
        return results


# ======================================================================
# Daemon snapshot adapter — feeds live daemon data into the signal pipeline
# ======================================================================

class DaemonSnapshotAdapter:
    """Reads the unified market daemon's operator-state.json snapshot and
    converts prediction markets, arbitrage opportunities, and Coinbase
    live price data into AccumulatedSignal objects.

    This bridges the always-on daemon pipeline into the single-ranked-queue
    opportunity feed so the dashboard shows everything on one page.
    """

    STATE_PATH = ROOT / "data" / "operator-state.json"

    # Symbol mapping — mirrors event_markets/signal_adapter.py
    QUESTION_SYMBOL_MAP: list[tuple[str, str]] = [
        ("bitcoin", "BTC-USD"), ("btc", "BTC-USD"),
        ("ethereum", "ETH-USD"), ("eth", "ETH-USD"),
        ("solana", "SOL-USD"), ("sol", "SOL-USD"),
        ("dogecoin", "DOGE-USD"), ("doge", "DOGE-USD"),
        ("xrp", "XRP-USD"), ("ripple", "XRP-USD"),
        ("cardano", "ADA-USD"), ("polkadot", "DOT-USD"),
        ("avalanche", "AVAX-USD"), ("chainlink", "LINK-USD"),
        ("uniswap", "UNI-USD"),
        ("polygon", "POL-USD"), ("matic", "POL-USD"), ("pol", "POL-USD"),
        ("cosmos", "ATOM-USD"), ("atom", "ATOM-USD"),
        ("litecoin", "LTC-USD"), ("ltc", "LTC-USD"),
        ("bitcoin cash", "BCH-USD"), ("bitcoincash", "BCH-USD"),
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
        ("internet computer", "ICP-USD"),
        ("the graph", "GRT-USD"), ("grt", "GRT-USD"),
        ("ai", "NVDA"), ("nvidia", "NVDA"),
        ("inflation", "BTC-USD"), ("fed", "BTC-USD"),
        ("interest rate", "BTC-USD"),
        ("president", "BTC-USD"), ("election", "BTC-USD"),
    ]

    CATEGORY_FALLBACK: dict[str, str] = {
        "crypto": "BTC-USD", "economics": "BTC-USD",
        "technology": "NVDA", "sports": "BTC-USD",
        "politics": "BTC-USD", "entertainment": "BTC-USD",
    }

    def __init__(self, state_path: str | Path | None = None):
        self.state_path = Path(state_path) if state_path else self.STATE_PATH

    # ── public entry point ────────────────────────────────────────

    def get_signals(self, price_map: dict[str, float] | None = None) -> list[AccumulatedSignal]:
        """Read daemon snapshot and build signals for every source."""
        snapshot = self._read_snapshot()
        if not snapshot:
            return []

        mi = snapshot.get("marketIntelligence") or {}
        signals: list[AccumulatedSignal] = []

        signals.extend(self._prediction_market_signals(mi.get("prediction_markets") or {}))
        signals.extend(self._arbitrage_signals(mi.get("arbitrage") or {}))
        signals.extend(self._coinbase_momentum_signals(mi.get("coinbase") or {}))

        return signals

    # ── read snapshot ─────────────────────────────────────────────

    def _read_snapshot(self) -> dict:
        if not self.state_path.exists():
            logger.debug("daemon snapshot not found at %s", self.state_path)
            return {}
        try:
            return json.loads(self.state_path.read_text())
        except Exception as e:
            logger.debug("failed to read daemon snapshot: %s", e)
            return {}

    # ── prediction market signals ─────────────────────────────────

    def _prediction_market_signals(self, pm_data: dict) -> list[AccumulatedSignal]:
        markets = pm_data.get("rankings") or pm_data.get("markets") or []
        signals: list[AccumulatedSignal] = []

        for m in markets[:25]:  # top 25
            mp = float(m.get("mid_price", 0) or 0)
            if mp <= 0 or mp >= 1:
                continue
            extremity = float(m.get("probability_extremity", 0) or 0)
            if extremity < 0.15:
                continue

            symbol = self._map_question(m.get("question", ""), m.get("category", ""))
            confidence = min(extremity * 0.9, 0.95)
            heat = float(m.get("heat_score", 0) or 0)

            if mp > 0.55:
                action = "BUY"
                score = confidence * 0.5 * (1 + extremity)
            elif mp < 0.45:
                action = "SELL"
                score = confidence * 0.5 * (1 + extremity)
            else:
                continue

            platform = m.get("platform", "unknown")
            category = m.get("category", "general")
            signals.append(AccumulatedSignal(
                symbol=symbol,
                action=action,
                base_confidence=round(confidence, 3),
                final_confidence=round(confidence, 3),
                opportunity_score=round(score, 4),
                strategy_name=f"Daemon:PM:{platform}:{category}",
                signal_reason=(
                    f"{platform} [{category}]: {m.get('question','')[:60]} → "
                    f"{mp*100:.0f}% YES (heat={heat:.0f})"
                ),
                estimated_volume_usd=max(round(confidence * float(m.get("volume", 0) or 0) * 0.05, 2), 1.0),
                market_data={
                    "platform": platform,
                    "category": category,
                    "question": m.get("question", ""),
                    "probability": mp,
                    "volume": m.get("volume", 0),
                    "spread": m.get("spread", 0),
                    "heat_score": heat,
                    "source": "daemon",
                },
            ))
        return signals

    # ── arbitrage signals ─────────────────────────────────────────

    def _arbitrage_signals(self, arb_data: dict) -> list[AccumulatedSignal]:
        opps = arb_data.get("opportunities") or []
        signals: list[AccumulatedSignal] = []

        for i, a in enumerate(opps[:20]):
            edge_pct = float(a.get("edge_pct", 0) or 0)
            confidence = float(a.get("confidence", 0) or 0)
            if edge_pct <= 0:
                continue

            total_cost = float(a.get("total_cost", 0) or 0)
            payout = float(a.get("guaranteed_payout", 0) or 0)
            profit = payout - total_cost
            notional = 1000.0
            expected_profit = round(notional * edge_pct, 2)

            score = min(edge_pct * 10, 0.95) * confidence
            event_key = a.get("event_key", "")
            cat = a.get("category", "general")

            signals.append(AccumulatedSignal(
                symbol=a.get("platform_buy", "EVENT"),
                action="BUY",
                base_confidence=round(confidence, 3),
                final_confidence=round(confidence, 3),
                opportunity_score=round(score, 4),
                strategy_name=f"Daemon:Arbitrage:{cat}:{i}",
                signal_reason=(
                    f"Arb {cat}: {a.get('platform_buy','')} YES→{a.get('platform_hedge','')} NO "
                    f"edge={edge_pct*100:.1f}% est_profit=${expected_profit:.0f}"
                ),
                estimated_volume_usd=round(total_cost, 2),
                market_data={
                    "event_key": event_key,
                    "category": cat,
                    "edge_pct": edge_pct,
                    "profit": profit,
                    "total_cost": total_cost,
                    "payout": payout,
                    "confidence": confidence,
                    "expected_profit": expected_profit,
                    "source": "daemon",
                    "arb_index": i,
                },
            ))
        return signals

    # ── Coinbase live momentum signals ────────────────────────────

    def _coinbase_momentum_signals(self, cb_data: dict) -> list[AccumulatedSignal]:
        updates = cb_data.get("last_updates") or {}
        signals: list[AccumulatedSignal] = []

        # Compare current price with a simple inferred recent low/high
        # from the websocket feed to detect short-term momentum
        for product_id, info in updates.items():
            price = float(info.get("price", 0) or 0)
            if price <= 0:
                continue

            # Mild momentum signal based on live feed presence alone
            # (actual candle-based signals handled by strategy adapters)
            timestamp = info.get("timestamp", "")
            channel = info.get("channel", "unknown")
            signals.append(AccumulatedSignal(
                symbol=product_id,
                action="HOLD",
                base_confidence=0.5,
                final_confidence=0.5,
                opportunity_score=0.3,
                strategy_name="Daemon:LiveFeed",
                signal_reason=f"Live price ${price:,.2f} via {channel} ws feed",
                estimated_volume_usd=0,
                market_data={
                    "price": price,
                    "channel": channel,
                    "timestamp": timestamp,
                    "source": "daemon",
                },
            ))
        return signals

    # ── symbol mapping ────────────────────────────────────────────

    def _map_question(self, question: str, category: str) -> str:
        q = question.lower()
        for kw, sym in self.QUESTION_SYMBOL_MAP:
            if kw in q:
                return sym
        return self.CATEGORY_FALLBACK.get(category, "BTC-USD")


# ======================================================================
# Core accumulator — merge, score, rank, de-duplicate
# ======================================================================

class UnifiedSignalAccumulator:
    """Central hub: gather signals from ALL sources, score, rank, and emit a single ordered queue."""

    def __init__(self, max_queue_size: int = 50):
        self.max_queue_size = max_queue_size
        self.price_provider = CoinbasePriceProvider()
        self.strategy_adapter = StrategySignalAdapter()
        self.news_adapter = NewsSentimentAdapter()
        self.multi_adapter = MultiStrategyAdapter()

        self._history_cache: Dict[str, List[Dict]] = {}
        self._last_fetch: Dict[str, float] = {}

        # Registered symbols — must be valid Coinbase Advanced Trade products
        self.symbols: List[str] = list(COINBASE_SPOT_PAIRS)

        self.prediction_market_adapter = self._init_pm_adapter()
        self.daemon_adapter = DaemonSnapshotAdapter()

    def _init_pm_adapter(self):
        try:
            from event_markets.signal_adapter import PredictionMarketAdapter
            return PredictionMarketAdapter(min_volume=2000, min_extremity=0.2)
        except Exception as e:
            logger.debug(f"Prediction market adapter unavailable: {e}")
            return None

    def _ensure_history(self, symbol: str) -> List[Dict]:
        now = time.time()
        if symbol in self._history_cache and (now - self._last_fetch.get(symbol, 0)) < 60:
            return self._history_cache[symbol]

        self._last_fetch[symbol] = now
        try:
            import subprocess, re
            result = subprocess.run(
                ["/home/scott/.npm-global/bin/coinbase", "products", "candles", symbol,
                 "granularity==1h", "limit==100"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                output = result.stdout.strip()
                # Try JSON format — CLI returns {"candles": [...]} or flat [...]
                try:
                    raw = json.loads(output)
                    if isinstance(raw, list):
                        entries = raw
                    elif isinstance(raw, dict):
                        entries = raw.get("candles") or raw.get("data") or raw.get("results") or []
                    else:
                        entries = []
                    if entries:
                        candles = []
                        for entry in entries:
                            if isinstance(entry, dict) and all(k in entry for k in ["open", "high", "low", "close"]):
                                candles.append({
                                    "time": entry.get("start", "") or entry.get("time", "") or "",
                                    "open": float(entry["open"]),
                                    "high": float(entry["high"]),
                                    "low": float(entry["low"]),
                                    "close": float(entry["close"]),
                                    "volume": float(entry.get("volume", 0)),
                                })
                        if candles:
                            self._history_cache[symbol] = candles
                            return candles
                except (json.JSONDecodeError, TypeError):
                    pass

                # Fallback: parse line-by-line
                lines = output.strip().split("\n")
                candles = []
                for line in lines[1:101]:
                    parts = line.strip().split()
                    if len(parts) >= 5:
                        try:
                            candles.append({
                                "time": parts[0],
                                "open": float(parts[1]),
                                "high": float(parts[2]),
                                "low": float(parts[3]),
                                "close": float(parts[4]),
                                "volume": float(parts[5]) if len(parts) > 5 else 0,
                            })
                        except (ValueError, IndexError):
                            continue
                if candles:
                    self._history_cache[symbol] = candles
                    return candles
        except Exception as e:
            logger.warning(f"Failed to fetch history for {symbol}: {e}")

        return self._history_cache.get(symbol, [])

    def _apply_cross_consensus(self, signals: List[AccumulatedSignal]) -> List[AccumulatedSignal]:
        """Boost signals that agree across multiple strategies on the same symbol."""
        symbol_actions: Dict[Tuple[str, str], List[AccumulatedSignal]] = {}
        for sig in signals:
            key = (sig.symbol, sig.action)
            symbol_actions.setdefault(key, []).append(sig)

        for key, group in symbol_actions.items():
            count = len(group)
            if count >= 2:
                boost = 1.0 + (count - 1) * 0.15
                for sig in group:
                    sig.final_confidence = min(sig.base_confidence * boost, 1.0)
                    sig.opportunity_score = sig.opportunity_score * boost

        return signals

    def _apply_fee_tier_boost(self, signals: List[AccumulatedSignal]) -> List[AccumulatedSignal]:
        """Boost signals that help reach next Coinbase fee tier."""
        try:
            from multi_strategy_paper_trading import FeeTierManager, VolumeOptimizer
            ftm = FeeTierManager()
            opt = VolumeOptimizer(ftm)
            needed = ftm.volume_to_next_tier()
            for sig in signals:
                boost = opt.volume_boost(sig.estimated_volume_usd)
                sig.final_confidence = min(sig.base_confidence * boost, 1.0)
                sig.opportunity_score = sig.opportunity_score * boost
        except Exception as e:
            logger.debug(f"Fee-tier boost skipped: {e}")
        return signals

    def accumulate(self) -> List[AccumulatedSignal]:
        """Run full accumulation: fetch data, run all strategies, merge, score, rank."""
        all_signals: List[AccumulatedSignal] = []

        # 1. Fetch current prices for all symbols
        market_data = self.price_provider.get_market_data(self.symbols)
        price_map = {sym: md["price"] for sym, md in market_data.items()}

        # 2. Gather signals from ALL sources in parallel
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = []

            for symbol in self.symbols:
                price = price_map.get(symbol, 0)
                history = self._ensure_history(symbol)

                # BTC-XXX strategies
                futures.append(pool.submit(
                    self.strategy_adapter.get_signals, symbol, price, history,
                ))
                # Multi-strategy signals
                pd = {"price": price, "price_percentage_change_24h": market_data.get(symbol, {}).get("change_pct", 0)}
                futures.append(pool.submit(
                    self.multi_adapter.get_signals, symbol, pd, history,
                ))

            # News sentiment (runs once, not per-symbol)
            futures.append(pool.submit(self.news_adapter.get_signals, price_map))

            # Prediction market signals (Kalshi / Polymarket)
            if self.prediction_market_adapter:
                futures.append(pool.submit(self.prediction_market_adapter.get_signals, price_map))

            # Daemon snapshot signals (prediction markets, arbitrage, Coinbase live feed)
            futures.append(pool.submit(self.daemon_adapter.get_signals, price_map))

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        if isinstance(result, list) and result and isinstance(result[0], dict):
                            for r in result:
                                all_signals.append(AccumulatedSignal(**r))
                        else:
                            all_signals.extend(result)
                except Exception as e:
                    logger.warning(f"Signal source failed: {e}")

        # 3. Apply cross-consensus boost (agreement across strategies)
        all_signals = self._apply_cross_consensus(all_signals)

        # 4. Apply fee-tier volume boost
        all_signals = self._apply_fee_tier_boost(all_signals)

        # 5. Deduplicate: keep highest scoring signal per (symbol, action, strategy)
        dedup: Dict[str, AccumulatedSignal] = {}
        for sig in all_signals:
            key = f"{sig.symbol}:{sig.action}:{sig.strategy_name}"
            if key not in dedup or sig.opportunity_score > dedup[key].opportunity_score:
                dedup[key] = sig

        # 6. Sort by opportunity_score descending
        ranked = sorted(dedup.values(), key=lambda s: s.opportunity_score, reverse=True)

        # 7. Trim to max queue size
        ranked = ranked[:self.max_queue_size]

        return ranked

    def accumulate_and_report(self) -> Dict:
        """Accumulate signals and return a JSON-serialisable report."""
        signals = self.accumulate()

        if not signals:
            return {
                "status": "no_signals",
                "timestamp": datetime.now().isoformat(),
                "total_signals": 0,
                "queue": [],
                "top_signal": None,
            }

        buy_count = sum(1 for s in signals if s.action == "BUY")
        sell_count = sum(1 for s in signals if s.action == "SELL")

        return {
            "status": "ok",
            "timestamp": datetime.now().isoformat(),
            "total_signals": len(signals),
            "buy_signals": buy_count,
            "sell_signals": sell_count,
            "top_signal": signals[0].to_dict() if signals else None,
            "queue": [s.to_dict() for s in signals],
            "strategy_breakdown": self._strategy_breakdown(signals),
        }

    def _strategy_breakdown(self, signals: List[AccumulatedSignal]) -> Dict[str, int]:
        breakdown: Dict[str, int] = {}
        for s in signals:
            breakdown[s.strategy_name] = breakdown.get(s.strategy_name, 0) + 1
        return breakdown


# ======================================================================
# CLI entry point
# ======================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Unified Signal Accumulator")
    parser.add_argument("--max-signals", type=int, default=50, help="Max signals in queue")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    acc = UnifiedSignalAccumulator(max_queue_size=args.max_signals)
    report = acc.accumulate_and_report()

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("=" * 72)
        print("UNIFIED SIGNAL ACCUMULATOR")
        print("=" * 72)
        print(f"Total signals: {report['total_signals']}")
        print(f"  BUY:  {report['buy_signals']}")
        print(f"  SELL: {report['sell_signals']}")
        if report.get("top_signal"):
            ts = report["top_signal"]
            print(f"\nTop signal:")
            print(f"  {ts['action']} {ts['symbol']} | "
                  f"score={ts['opportunity_score']:.3f} | "
                  f"conf={ts['final_confidence']:.3f} | "
                  f"strat={ts['strategy_name']} | "
                  f"{ts['signal_reason']}")
        print(f"\nStrategy breakdown:")
        for strat, count in sorted(report["strategy_breakdown"].items()):
            print(f"  {strat}: {count}")
        print(f"\nQueue ({len(report['queue'])} signals):")
        for i, sig in enumerate(report["queue"][:10]):
            print(f"  #{i+1}: {sig['action']:>4s} {sig['symbol']:>9s} | "
                  f"score={sig['opportunity_score']:.3f} | "
                  f"conf={sig['final_confidence']:.3f} | "
                  f"strat={sig['strategy_name'][:20]:20s} | "
                  f"{sig['signal_reason'][:40]}")
        if len(report["queue"]) > 10:
            print(f"  ... and {len(report['queue']) - 10} more")
        print("=" * 72)

    return report


if __name__ == "__main__":
    main()
