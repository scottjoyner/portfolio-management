"""Unified signal aggregator: runs all strategies on all products,
combines into a single priority score per product, ranks across products.

Architecture:
  Rust evaluate_all (25 strategies)  ──┐
  Backtest validator (rayon)         ──┤──→ SignalAggregator
  Long-term trend indicators         ──┘     ↓
                                       UnifiedSignal per product
                                          → rank by priority
                                          → top N = execution candidates
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# Lazy imports — these modules may not be loaded yet
_rust_core = None
_HAS_RUST = False
_batch_backtest_rust = None


def _ensure_rust():
    global _rust_core, _HAS_RUST, _batch_backtest_rust
    if _rust_core is not None:
        return
    try:
        import rust_core as r
        _rust_core = r
        _HAS_RUST = True
    except ImportError:
        _HAS_RUST = False
        return
    if _batch_backtest_rust is not None:
        return
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "coinbase" / "src"))
        from strategy_engine import batch_backtest_rust as b
        from strategy_engine import BacktestVerdict
        _batch_backtest_rust = b
    except ImportError:
        log.warning("batch_backtest_rust not available — backtest quality disabled")
        _batch_backtest_rust = None


@dataclass
class UnifiedSignal:
    """Aggregated signal for one product across all strategies.

    The unified_score is the key ranking metric:
      - > 0 = bullish consensus
      - < 0 = bearish consensus
      - magnitude = conviction strength
    """

    product_id: str
    base: str
    price: float
    unified_score: float        # -1 (strong SELL) to +1 (strong BUY)
    consensus_score: float       # weighted direction consensus
    backtest_quality: float      # average win_rate × sharpe of passing strats
    trend_score: float           # long-term trend (-1 to +1)
    conviction: float            # fraction of strategies in dominant direction
    active_buys: int
    active_sells: int
    total_signals: int
    top_strategies: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def priority(self) -> float:
        """Composite priority for cross-product ranking (always non-negative)."""
        return abs(self.unified_score) * (0.5 + self.conviction * 0.5) * (0.5 + self.backtest_quality * 0.5)

    @property
    def direction(self) -> str:
        if self.unified_score > 0.15:
            return "BUY"
        elif self.unified_score < -0.15:
            return "SELL"
        return "HOLD"

    def short_report(self) -> str:
        return (
            f"{self.product_id:12s} {self.direction:4s} "
            f"score={self.unified_score:+.2f} "
            f"buys={self.active_buys} sells={self.active_sells} "
            f"conv={self.conviction:.0%} "
            f"bt={self.backtest_quality:.2f} "
            f"trend={self.trend_score:+.2f} "
            f"top={','.join(self.top_strategies[:3])}"
        )


class SignalAggregator:
    """Aggregate all strategy signals per product into unified priority scores.

    Usage:
        agg = SignalAggregator()
        results = agg.scan_universe(products, closes_dict, volumes_dict, highs_dict, lows_dict)
        for r in results[:10]:
            print(r.short_report())
    """

    def __init__(self):
        _ensure_rust()
        self._bt_cache: Dict[str, Any] = {}
        self._bt_cache_lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="agg")

    def scan_universe(
        self,
        products: List[Tuple[str, str]],
        closes_dict: Dict[str, List[float]],
        volumes_dict: Dict[str, List[float]],
        highs_dict: Dict[str, List[float]],
        lows_dict: Dict[str, List[float]],
        min_candles: int = 60,
    ) -> List[UnifiedSignal]:
        """Run ALL strategies on ALL products → ranked unified signals.

        Args:
            products: list of (product_id, base_currency) tuples
            closes_dict/volumes_dict/highs_dict/lows_dict: per-product OHLCV
            min_candles: minimum bars required (products with less are skipped)

        Returns:
            List of UnifiedSignal sorted by priority descending.
        """
        if not _HAS_RUST or _rust_core is None:
            log.error("Rust core not available — cannot scan")
            return []

        log.info("Scanning %d products with %s candles...", len(products), min_candles)

        # Check for duplicate product IDs to avoid orphaned futures
        seen_pids = set()
        unique_products = []
        for pid, base in products:
            if pid in seen_pids:
                log.warning("Duplicate product_id in scan universe: %s (skipping duplicate)", pid)
                continue
            seen_pids.add(pid)
            unique_products.append((pid, base))

        # Evaluate all products in parallel
        futures = {}
        for pid, base in unique_products:
            closes = closes_dict.get(pid, [])
            if len(closes) < min_candles:
                continue
            volumes = volumes_dict.get(pid, [])
            highs = highs_dict.get(pid, [])
            lows = lows_dict.get(pid, [])
            futures[self._executor.submit(
                self._evaluate_one, pid, base, closes, volumes, highs, lows
            )] = pid

        results: List[UnifiedSignal] = []
        for fut in as_completed(futures):
            try:
                us = fut.result()
                if us is not None:
                    results.append(us)
            except Exception as e:
                pid = futures[fut]
                log.debug("Failed for %s: %s", pid, e)

        # Sort by priority descending
        results.sort(key=lambda r: r.priority, reverse=True)
        log.info("Scan complete: %d/%d products produced signals", len(results), len(products))
        return results

    def _evaluate_one(
        self,
        pid: str,
        base: str,
        closes: List[float],
        volumes: List[float],
        highs: List[float],
        lows: List[float],
    ) -> Optional[UnifiedSignal]:
        """Evaluate one product: run strategies, backtest, compute score."""
        try:
            opens = [closes[0]] + closes[:-1] if len(closes) > 1 else closes[:]
            raw = _rust_core.evaluate_all_opens_py(closes, opens, volumes, highs, lows)
        except Exception as e:
            log.debug("Rust evaluate_all failed for %s: %s", pid, e)
            return None

        if not raw:
            return None

        buys = [(n, c) for n, a, c, r in raw if a == "BUY"]
        sells = [(n, c) for n, a, c, r in raw if a == "SELL"]

        if not buys and not sells:
            return None

        # Backtest all signals
        bt_map = self._backtest_one(pid, base, closes, volumes, highs, lows, buys + sells)

        # Long-term trend
        trend = self._compute_trend(closes, volumes, highs, lows)

        # Compute unified score
        return self._compute_unified(pid, base, closes[-1], buys, sells, bt_map, trend)

    def _backtest_one(
        self,
        pid: str,
        base: str,
        closes: List[float],
        volumes: List[float],
        highs: List[float],
        lows: List[float],
        signals: List[Tuple[str, float]],
    ) -> Dict[str, Any]:
        """Backtest validate signals, return per-strategy verdict map."""
        if not _batch_backtest_rust or not signals:
            return {}

        bt_list = [(name, base, closes, volumes, highs, lows) for name, _ in signals]
        new_bts = _batch_backtest_rust(bt_list, warmup=30)
        with self._bt_cache_lock:
            for ck, v in new_bts.items():
                if ck not in self._bt_cache:
                    self._bt_cache[ck] = v

            bt_map = {}
            for name, _ in signals:
                ck = f"{name}/{base}"
                bt_map[name] = self._bt_cache.get(ck)
        return bt_map

    def _compute_trend(
        self,
        closes: List[float],
        volumes: List[float],
        highs: List[float],
        lows: List[float],
    ) -> float:
        """Compute long-term trend score (-1 bearish, +1 bullish).

        Components:
          - 50/200 SMA crossover (40% weight)
          - Price vs 200 SMA (30% weight)
          - ADX trend strength (20% weight)
          - Volume vs 50-bar avg (10% weight)
        """
        n = len(closes)
        if n < 200:
            return self._compute_trend_short(closes, n) if n >= 50 else 0.0

        sma_50 = _rust_core.sma_py(closes, 50)
        sma_200 = _rust_core.sma_py(closes, 200)
        current_price = closes[-1]

        # SMA trend: golden/death cross magnitude
        sma_trend = 0.0
        if sma_200 != 0.0:
            sma_diff = (sma_50 - sma_200) / sma_200
            sma_trend = max(min(sma_diff * 10.0, 1.0), -1.0)

        # Price vs 200 SMA
        price_trend = 0.0
        if sma_200 != 0.0:
            pct = (current_price - sma_200) / sma_200
            price_trend = max(min(pct * 5.0, 1.0), -1.0)

        # ADX trend strength
        adx_strength = 0.0
        try:
            adx_result = _rust_core.run_strategy_py("adx", closes, volumes, highs, lows)
            if adx_result is not None:
                # Confidence from ADX signal roughly indicates trend strength
                _, conf, _ = adx_result
                adx_strength = conf * 0.5  # scale down, ADX is secondary
        except Exception:
            pass

        # Volume trend
        vol_trend = 0.0
        if n >= 60:
            avg_vol_50 = sum(volumes[-50:]) / 50.0
            recent_vol = sum(volumes[-5:]) / 5.0
            if avg_vol_50 > 0:
                vol_ratio = recent_vol / avg_vol_50
                vol_trend = max(min((vol_ratio - 1.0) * 0.5, 0.5), -0.5)

        combined = sma_trend * 0.4 + price_trend * 0.3 + adx_strength * 0.2 + vol_trend * 0.1
        return max(min(combined, 1.0), -1.0)

    def _compute_trend_short(self, closes: List[float], n: int) -> float:
        """Trend score for short data (< 200 bars). Use 20/50 SMA instead."""
        sma_20 = _rust_core.sma_py(closes, 20)
        sma_50 = _rust_core.sma_py(closes, 50) if n >= 50 else sma_20
        if sma_50 == 0.0:
            return 0.0
        diff = (sma_20 - sma_50) / sma_50
        return max(min(diff * 10.0, 1.0), -1.0)

    def _backtest_quality(self, bt_map: Dict[str, Any], signals: List[Tuple[str, float]]) -> float:
        """Compute average backtest quality from passing strategies."""
        scores = []
        for name, _ in signals:
            v = bt_map.get(name)
            if v is None:
                continue
            # v is a BacktestVerdict object (has attributes, not dict keys)
            passed = getattr(v, "passed", False)
            trades = getattr(v, "total_trades", 0)
            if passed and trades >= 5:
                wr = getattr(v, "win_rate", 0)
                sh = getattr(v, "sharpe_ratio", 0)
                pf = getattr(v, "profit_factor", 1)
                q = wr * min(max(sh, 0) / 0.5, 1.0) * min(max(pf, 0) / 1.5, 1.0)
                scores.append(q)
        return sum(scores) / len(scores) if scores else 0.0

    def _compute_unified(
        self,
        pid: str,
        base: str,
        price: float,
        buys: List[Tuple[str, float]],
        sells: List[Tuple[str, float]],
        bt_map: Dict[str, Any],
        trend: float,
    ) -> UnifiedSignal:
        """Combine all signals into UnifiedSignal with priority score."""
        # Weighted consensus: sum of confidence × direction
        buy_conf = sum(c for _, c in buys)
        sell_conf = sum(c for _, c in sells)
        n_buys = len(buys)
        n_sells = len(sells)
        total = n_buys + n_sells

        # Consensus: net confidence normalized by total strategies
        max_possible = total * 1.0  # max confidence per strategy = 1.0
        consensus = (buy_conf - sell_conf) / max_possible if max_possible > 0 else 0.0
        consensus = max(min(consensus, 1.0), -1.0)

        # Backtest quality
        bt_quality = self._backtest_quality(bt_map, buys + sells)

        # Conviction: fraction of strategies in the dominant direction
        if consensus > 0 and total > 0:
            conviction = n_buys / total
        elif consensus < 0 and total > 0:
            conviction = n_sells / total
        else:
            conviction = 0.0

        # Top strategies by (confidence × backtest_quality)
        all_scored = []
        for name, conf in buys + sells:
            v = bt_map.get(name)
            q = 0.1
            if v is not None and getattr(v, "passed", False):
                trades = getattr(v, "total_trades", 0)
                if trades >= 5:
                    wr = getattr(v, "win_rate", 0)
                    sh = getattr(v, "sharpe_ratio", 0)
                    q = wr * max(sh, 0)
            all_scored.append((name, conf * max(q, 0.1)))
        all_scored.sort(key=lambda x: -x[1])
        top_strats = [n for n, _ in all_scored[:5]]

        # Unified score
        trend_mult = 1.0 + abs(trend) * 0.4
        quality_mult = 0.5 + bt_quality * 0.5
        conviction_boost = 1.0 + conviction * 0.3
        unified = consensus * quality_mult * trend_mult * conviction_boost
        unified = max(min(unified, 1.0), -1.0)

        return UnifiedSignal(
            product_id=pid,
            base=base,
            price=price,
            unified_score=unified,
            consensus_score=consensus,
            backtest_quality=bt_quality,
            trend_score=trend,
            conviction=conviction,
            active_buys=n_buys,
            active_sells=n_sells,
            total_signals=total,
            top_strategies=top_strats,
            details={
                "buy_strategies": [n for n, _ in buys],
                "sell_strategies": [n for n, _ in sells],
            },
        )

    def top_n(self, results: List[UnifiedSignal], n: int = 5) -> List[UnifiedSignal]:
        """Return top-N highest priority signals (both BUY and SELL sides)."""
        return results[:n]

    @staticmethod
    def print_report(results: List[UnifiedSignal], n: int = 20):
        """Print a ranked report to console."""
        if not results:
            print("No signals found.")
            return

        print(f"\n{'':12s} {'Dir':4s} {'Score':6s} {'Prio':6s} {'Buy':3s} {'Sell':3s} {'Conv':5s} {'BT':4s} {'Trend':6s}  Top Strategies")
        print(f"{'─'*80}")
        for r in results[:n]:
            print(
                f"{r.product_id:12s} {r.direction:4s} "
                f"{r.unified_score:+.2f}  {r.priority:.3f} "
                f"{r.active_buys:3d} {r.active_sells:3d} "
                f"{r.conviction:.0%}  {r.backtest_quality:.2f} "
                f"{r.trend_score:+.2f}  "
                f"{', '.join(r.top_strategies[:4]):40s}"
            )
        print(f"  ({len(results)} total, showing top {min(n, len(results))})")
