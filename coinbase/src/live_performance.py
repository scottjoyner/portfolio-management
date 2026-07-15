"""Live per-strategy per-product performance tracker.

Tracks real-time win/loss/PnL per (strategy, product_id) pair.
Used for:
  - Auto-disable underperforming strategy-product pairs
  - Multi-signal confluence (only count strategies with good track records)
  - Kelly-optimal position sizing
  - Dynamic cooldown tuning
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("live_perf")


@dataclass
class StrategyProductRecord:
    strategy: str
    product_id: str
    trades: int = 0
    wins: int = 0
    losses: int = 0
    current_streak: int = 0  # positive = wins, negative = losses
    best_streak: int = 0
    worst_streak: int = 0
    total_pnl: float = 0.0
    total_volume: float = 0.0
    total_fees: float = 0.0
    sum_wins: float = 0.0  # sum of positive PnLs
    sum_losses: float = 0.0  # sum of negative PnLs (absolute)
    last_trade_ts: float = 0.0
    last_trade_side: str = ""  # "LONG" | "SHORT"
    backtest_win_rate: float = 0.0  # backtest win rate reported at entry (for live-vs-bt divergence)
    disabled: bool = False
    disable_reason: str = ""

    @property
    def win_rate(self) -> float:
        if self.trades == 0:
            return 0.0
        return self.wins / self.trades

    @property
    def loss_rate(self) -> float:
        return 1.0 - self.win_rate

    @property
    def avg_win(self) -> float:
        if self.wins == 0:
            return 0.0
        return self.sum_wins / self.wins

    @property
    def avg_loss(self) -> float:
        if self.losses == 0:
            return 0.0
        return self.sum_losses / self.losses

    @property
    def profit_factor(self) -> float:
        if self.losses == 0:
            return 99.9 if self.wins > 0 else 0.0
        if self.sum_losses == 0:
            return 99.9
        return self.sum_wins / self.sum_losses

    @property
    def expectancy(self) -> float:
        """Expected return per trade in fraction of risk."""
        wr = self.win_rate
        if self.avg_loss == 0:
            return 0.0
        return (wr * self.avg_win / self.avg_loss) - ((1 - wr) * 1.0)

    @property
    def kelly_fraction(self) -> float:
        """Full Kelly fraction: (win_rate * avg_win/avg_loss - loss_rate) / (avg_win/avg_loss)."""
        if self.avg_loss == 0 or self.trades < 5:
            return 0.0
        r = self.avg_win / max(self.avg_loss, 1e-9)
        if r <= 0:
            return 0.0
        return max(0.0, (self.win_rate * r - self.loss_rate) / r)

    def record_trade(self, pnl: float, volume: float, fee: float, side: str,
                     backtest_win_rate: float = 0.0) -> None:
        self.trades += 1
        if backtest_win_rate > 0:
            # EMA of the backtest win rate seen at entry time
            self.backtest_win_rate = (
                backtest_win_rate if self.backtest_win_rate <= 0
                else 0.8 * self.backtest_win_rate + 0.2 * backtest_win_rate
            )
        self.total_pnl += pnl
        self.total_volume += volume
        self.total_fees += fee
        self.last_trade_ts = time.time()
        self.last_trade_side = side
        if pnl >= 0:
            self.wins += 1
            self.sum_wins += pnl
            if self.current_streak >= 0:
                self.current_streak += 1
            else:
                self.current_streak = 1
        else:
            self.losses += 1
            self.sum_losses += abs(pnl)
            if self.current_streak <= 0:
                self.current_streak -= 1
            else:
                self.current_streak = -1
        self.best_streak = max(self.best_streak, self.current_streak)
        self.worst_streak = min(self.worst_streak, self.current_streak)

    def disable(self, reason: str) -> None:
        self.disabled = True
        self.disable_reason = reason

    def enable(self) -> None:
        self.disabled = False
        self.disable_reason = ""


class LivePerformanceTracker:
    """Tracks live performance per (strategy, product_id).

    Thread-safe. Persists to JSON for crash recovery.
    """

    def __init__(self, path: str = "data/live_performance.json"):
        self._path = Path(path)
        self._lock = Lock()
        self._records: Dict[str, StrategyProductRecord] = {}
        self._product_regime_cache: Dict[str, str] = {}
        self._disabled_strategies: Dict[str, str] = {}  # strategy -> reason (global disable)
        self._load()

    def _key(self, strategy: str, product_id: str) -> str:
        return f"{strategy}/{product_id}"

    def get_or_create(self, strategy: str, product_id: str) -> StrategyProductRecord:
        k = self._key(strategy, product_id)
        with self._lock:
            if k not in self._records:
                self._records[k] = StrategyProductRecord(
                    strategy=strategy, product_id=product_id,
                )
            return self._records[k]

    def record_trade(self, strategy: str, product_id: str, pnl: float,
                     volume: float, fee: float, side: str,
                     backtest_win_rate: float = 0.0) -> StrategyProductRecord:
        rec = self.get_or_create(strategy, product_id)
        with self._lock:
            rec.record_trade(pnl, volume, fee, side, backtest_win_rate)
        return rec

    def get(self, strategy: str, product_id: str) -> Optional[StrategyProductRecord]:
        k = self._key(strategy, product_id)
        with self._lock:
            return self._records.get(k)

    def win_rate(self, strategy: str, product_id: str, min_trades: int = 0) -> float:
        rec = self.get(strategy, product_id)
        if rec is None or rec.trades < min_trades:
            return 0.0
        return rec.win_rate

    def kelly(self, strategy: str, product_id: str, min_trades: int = 5) -> float:
        rec = self.get(strategy, product_id)
        if rec is None or rec.trades < min_trades:
            return 0.0
        return rec.kelly_fraction

    def is_disabled(self, strategy: str, product_id: str) -> bool:
        rec = self.get(strategy, product_id)
        if rec is None:
            return False
        return rec.disabled

    def auto_disable(self, min_trades: int = 10, max_loss_streak: int = 5,
                     min_win_rate: float = 0.25) -> int:
        """Auto-disable strategies that are clearly failing.

        Disable if:
        - Enough trades AND win rate below threshold
        - OR consecutive losses exceed max streak
        """
        count = 0
        with self._lock:
            for rec in self._records.values():
                if rec.disabled:
                    continue
                if rec.trades >= min_trades and rec.win_rate < min_win_rate:
                    rec.disable(f"win_rate={rec.win_rate:.1%} < {min_win_rate:.0%} over {rec.trades} trades")
                    count += 1
                elif abs(rec.worst_streak) >= max_loss_streak:
                    rec.disable(f"worst_streak={rec.worst_streak} >= {max_loss_streak}")
                    count += 1
        return count

    def divergence_report(self, min_trades: int = 10, min_gap: float = 0.20) -> List[Dict[str, Any]]:
        """Flag strategies whose LIVE win rate falls far short of their BACKTEST
        win rate — the classic "looked great in backtest, loses live" failure.

        Aggregates per strategy across products; returns entries sorted by gap.
        """
        agg: Dict[str, Dict[str, float]] = {}
        with self._lock:
            for rec in self._records.values():
                a = agg.setdefault(rec.strategy, {"trades": 0, "wins": 0, "bt_sum": 0.0, "bt_n": 0, "pnl": 0.0})
                a["trades"] += rec.trades
                a["wins"] += rec.wins
                a["pnl"] += rec.total_pnl
                if rec.backtest_win_rate > 0:
                    a["bt_sum"] += rec.backtest_win_rate * rec.trades
                    a["bt_n"] += rec.trades
        out: List[Dict[str, Any]] = []
        for strat, a in agg.items():
            if a["trades"] < min_trades or a["bt_n"] <= 0:
                continue
            live_wr = a["wins"] / a["trades"]
            bt_wr = a["bt_sum"] / a["bt_n"]
            gap = bt_wr - live_wr
            if gap >= min_gap:
                out.append({
                    "strategy": strat,
                    "backtest_win_rate": round(bt_wr, 3),
                    "live_win_rate": round(live_wr, 3),
                    "gap": round(gap, 3),
                    "trades": int(a["trades"]),
                    "total_pnl": round(a["pnl"], 2),
                })
        out.sort(key=lambda x: -x["gap"])
        return out

    def strategy_aggregate(self, strategy: str) -> Dict[str, Any]:
        """Aggregate live stats for a strategy across ALL products."""
        trades = wins = 0
        pnl = 0.0
        with self._lock:
            for rec in self._records.values():
                if rec.strategy == strategy:
                    trades += rec.trades
                    wins += rec.wins
                    pnl += rec.total_pnl
        win_rate = (wins / trades) if trades else 0.0
        return {"trades": trades, "wins": wins, "win_rate": win_rate, "total_pnl": pnl}

    def is_strategy_disabled(self, strategy: str) -> bool:
        with self._lock:
            return strategy in self._disabled_strategies

    def expectancy_report(self, min_trades: int = 5) -> List[Dict[str, Any]]:
        """Per-strategy live expectancy summary (aggregated across products),
        sorted worst-first, for tuning decisions. Expectancy = avg PnL per trade.
        """
        agg: Dict[str, Dict[str, float]] = {}
        with self._lock:
            disabled = dict(self._disabled_strategies)
            for rec in self._records.values():
                a = agg.setdefault(rec.strategy, {
                    "trades": 0, "wins": 0, "pnl": 0.0, "fees": 0.0,
                    "sum_wins": 0.0, "sum_losses": 0.0,
                })
                a["trades"] += rec.trades
                a["wins"] += rec.wins
                a["pnl"] += rec.total_pnl
                a["fees"] += rec.total_fees
                a["sum_wins"] += rec.sum_wins
                a["sum_losses"] += rec.sum_losses
        out: List[Dict[str, Any]] = []
        for strat, a in agg.items():
            if a["trades"] < min_trades:
                continue
            trades = int(a["trades"])
            wr = a["wins"] / trades if trades else 0.0
            expectancy = a["pnl"] / trades if trades else 0.0
            losses = abs(a["sum_losses"])
            pf = (a["sum_wins"] / losses) if losses > 0 else float("inf")
            out.append({
                "strategy": strat,
                "trades": trades,
                "win_rate": round(wr, 3),
                "expectancy": round(expectancy, 4),
                "total_pnl": round(a["pnl"], 2),
                "total_fees": round(a["fees"], 2),
                "profit_factor": (round(pf, 2) if pf != float("inf") else None),
                "disabled": strat in disabled,
                "disable_reason": disabled.get(strat, ""),
            })
        out.sort(key=lambda x: x["expectancy"])
        return out

    def auto_disable_strategies(self, min_trades: int = 20,
                                min_win_rate: float = 0.30) -> int:
        """Globally disable a strategy whose AGGREGATE live win rate across all
        products is poor over a meaningful sample. Catches broadly-bad strategies
        that over-trade thinly across many products (never tripping per-product limits).
        """
        agg: Dict[str, Tuple[int, int, float]] = {}
        with self._lock:
            for rec in self._records.values():
                t, w, p = agg.get(rec.strategy, (0, 0, 0.0))
                agg[rec.strategy] = (t + rec.trades, w + rec.wins, p + rec.total_pnl)
        count = 0
        with self._lock:
            for strat, (trades, wins, pnl) in agg.items():
                if strat in self._disabled_strategies:
                    continue
                wr = (wins / trades) if trades else 0.0
                # P&L-aware: don't prune a low-win-rate strategy that is still net
                # profitable (e.g. trend strategies with few large winners). Only
                # disable when it's both losing money AND has a poor hit rate.
                if trades >= min_trades and wr < min_win_rate and pnl < 0:
                    self._disabled_strategies[strat] = (
                        f"aggregate win_rate={wr:.1%} < {min_win_rate:.0%} over {trades} trades, pnl={pnl:.2f}"
                    )
                    count += 1
        return count

    def auto_enable_strategies(self, min_trades: int = 10,
                               min_win_rate: float = 0.45) -> int:
        """Re-enable a globally-disabled strategy if its aggregate recovers."""
        agg: Dict[str, Tuple[int, int]] = {}
        with self._lock:
            for rec in self._records.values():
                t, w = agg.get(rec.strategy, (0, 0))
                agg[rec.strategy] = (t + rec.trades, w + rec.wins)
        count = 0
        with self._lock:
            for strat in list(self._disabled_strategies.keys()):
                trades, wins = agg.get(strat, (0, 0))
                wr = (wins / trades) if trades else 0.0
                if trades >= min_trades and wr >= min_win_rate:
                    del self._disabled_strategies[strat]
                    count += 1
        return count

    def auto_enable(self, min_trades: int = 5, min_win_rate: float = 0.5) -> int:
        """Re-enable previously disabled strategies that have recovered."""
        count = 0
        with self._lock:
            for rec in self._records.values():
                if not rec.disabled:
                    continue
                if rec.trades >= min_trades and rec.win_rate >= min_win_rate:
                    rec.enable()
                    count += 1
        return count

    def best_strategies(self, product_id: str, n: int = 5,
                        min_trades: int = 3) -> List[Tuple[str, float]]:
        """Return top-N strategies for a product by win rate."""
        candidates = []
        with self._lock:
            for rec in self._records.values():
                if rec.product_id == product_id and rec.trades >= min_trades and not rec.disabled:
                    candidates.append((rec.strategy, rec.win_rate))
        candidates.sort(key=lambda x: -x[1])
        return candidates[:n]

    def worst_strategies(self, product_id: str, n: int = 5,
                         min_trades: int = 3) -> List[Tuple[str, float]]:
        """Return bottom-N strategies for a product by win rate."""
        candidates = []
        with self._lock:
            for rec in self._records.values():
                if rec.product_id == product_id and rec.trades >= min_trades and not rec.disabled:
                    candidates.append((rec.strategy, rec.win_rate))
        candidates.sort(key=lambda x: x[1])
        return candidates[:n]

    def enabled_count(self) -> int:
        with self._lock:
            return sum(1 for r in self._records.values() if not r.disabled)

    def disabled_count(self) -> int:
        with self._lock:
            return sum(1 for r in self._records.values() if r.disabled)

    def summary(self, top_n: int = 10) -> Dict[str, Any]:
        with self._lock:
            all_recs = list(self._records.values())
        best = sorted(all_recs, key=lambda r: -r.win_rate)[:top_n]
        worst = sorted(all_recs, key=lambda r: r.win_rate)[:top_n]
        disabled = [r for r in all_recs if r.disabled]
        return {
            "total_records": len(all_recs),
            "total_trades": sum(r.trades for r in all_recs),
            "enabled": self.enabled_count(),
            "disabled": self.disabled_count(),
            "best": [{"key": f"{r.strategy}/{r.product_id}", "win_rate": r.win_rate,
                       "trades": r.trades, "pnl": round(r.total_pnl, 2)}
                      for r in best],
            "worst": [{"key": f"{r.strategy}/{r.product_id}", "win_rate": r.win_rate,
                        "trades": r.trades, "pnl": round(r.total_pnl, 2)}
                       for r in worst],
            "disabled_list": [{"key": f"{r.strategy}/{r.product_id}",
                                "reason": r.disable_reason, "trades": r.trades}
                               for r in disabled],
            "disabled_strategies": dict(self._disabled_strategies),
            "expectancy": self.expectancy_report(min_trades=5),
            "divergences": self.divergence_report(min_trades=10, min_gap=0.20),
        }

    def save(self) -> None:
        with self._lock:
            data = {k: asdict(v) for k, v in self._records.items()}
            disabled_strats = dict(self._disabled_strategies)
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(
                {"_records": data, "_disabled_strategies": disabled_strats},
                indent=2, default=str))
            tmp.replace(self._path)
        except Exception as e:
            log.debug("Failed to save live performance: %s", e)

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            # New format: {"_records": {...}, "_disabled_strategies": {...}}
            # Legacy format: flat {key: record}
            if isinstance(data, dict) and "_records" in data:
                records = data.get("_records", {})
                self._disabled_strategies = dict(data.get("_disabled_strategies", {}))
            else:
                records = data
            for k, v in records.items():
                self._records[k] = StrategyProductRecord(**v)
            log.info("Loaded %d live performance records (%d disabled strategies)",
                     len(self._records), len(self._disabled_strategies))
        except Exception as e:
            log.debug("Failed to load live performance: %s", e)
