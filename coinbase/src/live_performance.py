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

    def record_trade(self, pnl: float, volume: float, fee: float, side: str) -> None:
        self.trades += 1
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
                     volume: float, fee: float, side: str) -> StrategyProductRecord:
        rec = self.get_or_create(strategy, product_id)
        with self._lock:
            rec.record_trade(pnl, volume, fee, side)
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
        }

    def save(self) -> None:
        with self._lock:
            data = {k: asdict(v) for k, v in self._records.items()}
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, indent=2, default=str))
            tmp.replace(self._path)
        except Exception as e:
            log.debug("Failed to save live performance: %s", e)

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            data = json.loads(self._path.read_text())
            for k, v in data.items():
                self._records[k] = StrategyProductRecord(**v)
            log.info("Loaded %d live performance records", len(self._records))
        except Exception as e:
            log.debug("Failed to load live performance: %s", e)
