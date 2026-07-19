"""Experiment specification for the iterative backtesting framework.

An :class:`Experiment` is a single, reproducible backtest configuration: which
strategies, which universe / asset classes, which data window + granularity,
and the pass/fail thresholds. Experiments are stored as JSON so any run can be
re-executed and compared against others (see ``compare_experiments.py``).

This is deliberately decoupled from the *live* ``trading_system/configs/*.yaml``
modes: those drive the paper/live trader at runtime, while an Experiment drives
an offline evaluation. A passing experiment can later be promoted to a yaml mode.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


# Default pass/fail thresholds (mirror the paper trader's paper_min_* guards).
DEFAULT_THRESHOLDS: Dict[str, float] = {
    "min_win_rate": 0.40,
    "min_sharpe": 0.30,
    "min_profit_factor": 1.05,
    "min_trades": 5,
    "min_avg_trade_pct": 0.0,  # minimum average per-trade return pct (net of fees); gates out noise edges
    "max_drawdown_pct": 50.0,  # reject if worse than this
}

VALID_GRANULARITIES = (60, 300, 900, 3600, 21600, 86400)
VALID_ASSET_CLASSES = ("safe", "growth", "speculative")


@dataclass
class Experiment:
    name: str
    # Strategy selection: "all", "rust", or an explicit comma list of names.
    strategies: str = "rust"
    # Universe: "all-harvested" or a comma list of product ids (e.g. BTC-USD,ETH-USD).
    universe: str = "all-harvested"
    # Asset classes to include (matches portfolio_optimizer.classify_asset buckets).
    asset_classes: List[str] = field(default_factory=lambda: list(VALID_ASSET_CLASSES))
    granularity: int = 3600
    # Trailing window: number of bars (None = all harvested).
    window_bars: Optional[int] = 5000
    # Optional fixed date range (ISO); overrides window_bars if both set.
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None
    # Pass/fail thresholds (merged over DEFAULT_THRESHOLDS).
    thresholds: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_THRESHOLDS))
    # Walk-forward: run out-of-sample folds (v2). Number of folds (0 = off).
    walk_forward_folds: int = 0
    # Ensemble: compute independence-group consensus across strategies (v2).
    ensemble: bool = False
    ensemble_min_groups: int = 2
    # Regime-conditioned mode (E16): split each symbol's window by detected
    # regime and recompute ensemble consensus PER regime.
    regime: bool = False
    regime_min_bars: int = 60
    # Multi-timeframe confirmation: require strategy to pass on a higher
    # confirmation granularity too (0 = off). e.g. 3600 primary + 86400 confirm.
    confirm_granularity: int = 0
    # Optional reference to a live yaml mode for provenance only.
    config_ref: str = ""
    notes: str = ""

    def validate(self) -> None:
        if self.granularity not in VALID_GRANULARITIES:
            raise ValueError(f"granularity {self.granularity} not in {VALID_GRANULARITIES}")
        for ac in self.asset_classes:
            if ac not in VALID_ASSET_CLASSES:
                raise ValueError(f"asset_class {ac!r} not in {VALID_ASSET_CLASSES}")
        if self.strategies not in ("all", "rust") and not self.strategies:
            raise ValueError("strategies must be 'all', 'rust', or a comma list")

    def to_dict(self) -> dict:
        return asdict(self)

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str) -> "Experiment":
        with open(path) as f:
            data = json.load(f)
        exp = cls(**data)
        exp.thresholds = {**DEFAULT_THRESHOLDS, **exp.thresholds}
        exp.validate()
        return exp

    @classmethod
    def from_args(cls, args) -> "Experiment":
        """Build an Experiment from argparse args (used by the CLI)."""
        thresholds = dict(DEFAULT_THRESHOLDS)
        for key in ("min_win_rate", "min_sharpe", "min_profit_factor", "min_trades",
                    "max_drawdown_pct", "min_avg_trade_pct"):
            val = getattr(args, key, None)
            if val is not None:
                thresholds[key] = val
        return cls(
            name=args.name,
            strategies=getattr(args, "strategies", "rust") or "rust",
            universe=getattr(args, "universe", "all-harvested") or "all-harvested",
            asset_classes=[a.strip() for a in (args.asset_classes or "safe,growth,speculative").split(",")],
            granularity=getattr(args, "granularity", 3600),
            window_bars=getattr(args, "window_bars", 5000),
            start_ts=getattr(args, "start_ts", None),
            end_ts=getattr(args, "end_ts", None),
            thresholds=thresholds,
            walk_forward_folds=getattr(args, "walk_forward_folds", 0) or 0,
            ensemble=bool(getattr(args, "ensemble", False)),
            ensemble_min_groups=getattr(args, "ensemble_min_groups", 2) or 2,
            regime=bool(getattr(args, "regime", False)),
            regime_min_bars=getattr(args, "regime_min_bars", 60) or 60,
            confirm_granularity=getattr(args, "confirm_granularity", 0) or 0,
            config_ref=getattr(args, "config_ref", "") or "",
            notes=getattr(args, "notes", "") or "",
        )


def resolve_strategies(strategies: str) -> List[str]:
    """Return the concrete strategy name list for an Experiment.strategies spec."""
    import strategy_engine as S
    if strategies == "rust":
        return [s for s in S.ALL_STRATEGIES.keys() if s in S._RUST_STRATEGIES]
    if strategies == "all":
        return list(S.ALL_STRATEGIES.keys())
    return [s.strip() for s in strategies.split(",") if s.strip()]


def evaluate_verdict(v, thresholds: Dict[str, float]) -> bool:
    """Apply an Experiment's thresholds to a BacktestVerdict, returning pass/fail.

    Extends the engine's own ``passed`` (which only checks the engine's internal
    thresholds) with the experiment's stricter, config-specific gates.
    """
    if not getattr(v, "passed", False):
        return False
    if v.total_trades < thresholds.get("min_trades", 5):
        return False
    if v.win_rate < thresholds.get("min_win_rate", 0.40):
        return False
    if v.sharpe_ratio < thresholds.get("min_sharpe", 0.30):
        return False
    if v.profit_factor < thresholds.get("min_profit_factor", 1.05):
        return False
    if v.max_drawdown_pct > thresholds.get("max_drawdown_pct", 50.0):
        return False
    if getattr(v, "avg_trade_pct", 0.0) < thresholds.get("min_avg_trade_pct", 0.0):
        return False
    return True


def experiments_dir() -> str:
    d = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "experiments")
    os.makedirs(d, exist_ok=True)
    return d
