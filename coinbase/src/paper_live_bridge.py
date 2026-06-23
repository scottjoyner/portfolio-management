from __future__ import annotations
import json
import os
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from .protocols import Direction, Opportunity, InstrumentType
from .orchestrator import ExecutionOrchestrator, TradeMode, TradeSignal
from .risk_manager import RiskManager, RiskProfile, RiskLimit, PositionRisk, KellySizer

log = logging.getLogger(__name__)


@dataclass
class StrategyDeployment:
    strategy_name: str
    asset_class: str
    win_rate: float = 0.0
    sharpe: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    passed_backtest: bool = False
    paper_trades: int = 0
    paper_win_rate: float = 0.0
    paper_sharpe: float = 0.0
    deployed: bool = False
    deployed_at: Optional[float] = None
    weight: float = 1.0


class PerformanceTracker:
    def __init__(self, state_path: str = "strategy_performance.json"):
        self.state_path = state_path
        self._data: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path) as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save(self):
        with open(self.state_path, "w") as f:
            json.dump(self._data, f, indent=2)

    def record_trade(self, strategy: str, product_id: str, direction: str,
                     entry: float, exit: float, pnl: float, r_multiple: float,
                     fees: float = 0.0, mode: str = "paper"):
        key = f"{strategy}"
        perf = self._data.setdefault(key, {
            "strategy": strategy,
            "trades": 0, "wins": 0, "losses": 0,
            "total_pnl": 0.0, "total_r": 0.0, "total_fees": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "max_win": 0.0, "max_loss": 0.0,
            "win_rate": 0.0, "sharpe": 0.0, "profit_factor": 1.0,
            "products": {}, "mode": mode,
            "last_trade_ts": time.time(),
            "history": [],
        })
        perf["trades"] += 1
        perf["total_pnl"] += pnl
        perf["total_r"] += r_multiple
        perf["total_fees"] += fees
        perf["last_trade_ts"] = time.time()

        if pnl > 0:
            perf["wins"] += 1
            perf["avg_win"] = (perf["avg_win"] * (perf["wins"] - 1) + r_multiple) / perf["wins"]
            perf["max_win"] = max(perf["max_win"], r_multiple)
        else:
            perf["losses"] += 1
            perf["avg_loss"] = (perf["avg_loss"] * (perf["losses"] - 1) + abs(r_multiple)) / perf["losses"]
            perf["max_loss"] = min(perf["max_loss"], abs(r_multiple))

        perf["win_rate"] = perf["wins"] / max(perf["trades"], 1)

        prod_perf = perf["products"].setdefault(product_id, {
            "trades": 0, "wins": 0, "total_pnl": 0.0,
        })
        prod_perf["trades"] += 1
        if pnl > 0:
            prod_perf["wins"] += 1
        prod_perf["total_pnl"] += pnl

        if perf["trades"] >= 5:
            returns = perf.get("_returns", []) + [r_multiple]
            perf["_returns"] = returns[-100:]
            avg_r = sum(returns[-100:]) / len(returns[-100:])
            var_r = sum((r - avg_r) ** 2 for r in returns[-100:]) / len(returns[-100:])
            perf["sharpe"] = avg_r / max(var_r ** 0.5, 0.001) if var_r > 0 else 0.0
            gross_win = perf["avg_win"] * perf["wins"] if perf["wins"] > 0 else 0.0
            gross_loss = perf["avg_loss"] * perf["losses"] if perf["losses"] > 0 else 0.0
            perf["profit_factor"] = gross_win / max(gross_loss, 0.001)

        self._save()

    def get_performance(self, strategy: str) -> Dict[str, Any]:
        return self._data.get(strategy, {})

    def get_all_performance(self) -> Dict[str, Dict[str, Any]]:
        return self._data

    def best_strategies(self, min_trades: int = 10, top_n: int = 5) -> List[Dict[str, Any]]:
        candidates = []
        for key, perf in self._data.items():
            if perf.get("trades", 0) >= min_trades and perf.get("win_rate", 0) > 0.5:
                score = perf["win_rate"] * perf.get("sharpe", 0) * perf.get("profit_factor", 1)
                candidates.append({**perf, "score": score})
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:top_n]


class DeploymentPipeline:
    def __init__(self, tracker: Optional[PerformanceTracker] = None,
                 backtest_fn: Optional[Any] = None):
        self.tracker = tracker or PerformanceTracker()
        self.backtest_fn = backtest_fn
        self._deployments: Dict[str, StrategyDeployment] = {}

    def register_strategy(self, name: str, asset_class: str = "growth"):
        self._deployments[name] = StrategyDeployment(
            strategy_name=name, asset_class=asset_class,
        )

    def update_backtest_result(self, name: str, win_rate: float, sharpe: float,
                                profit_factor: float, trades: int, passed: bool):
        dep = self._deployments.get(name)
        if dep is None:
            return
        dep.win_rate = win_rate
        dep.sharpe = sharpe
        dep.profit_factor = profit_factor
        dep.total_trades = trades
        dep.passed_backtest = passed

    def check_deployment(self, name: str) -> bool:
        dep = self._deployments.get(name)
        if dep is None:
            return False
        perf = self.tracker.get_performance(name)
        paper_trades = perf.get("trades", 0)
        paper_win_rate = perf.get("win_rate", 0.0)
        paper_sharpe = perf.get("sharpe", 0.0)

        dep.paper_trades = paper_trades
        dep.paper_win_rate = paper_win_rate
        dep.paper_sharpe = paper_sharpe

        if dep.passed_backtest and paper_trades >= 20 and paper_win_rate >= 0.5:
            if not dep.deployed:
                dep.deployed = True
                dep.deployed_at = time.time()
                log.info(f"[DEPLOY] {name}: passed backtest + {paper_trades} paper trades @ {paper_win_rate:.0%} WR → LIVE")
                return True
        return False

    def get_deployment_status(self) -> Dict[str, Any]:
        return {
            name: {
                "passed_backtest": dep.passed_backtest,
                "paper_trades": dep.paper_trades,
                "paper_win_rate": dep.paper_win_rate,
                "deployed": dep.deployed,
                "asset_class": dep.asset_class,
            }
            for name, dep in self._deployments.items()
        }


class FeedbackLoop:
    def __init__(self, tracker: PerformanceTracker, pipeline: DeploymentPipeline,
                 orchestrator: ExecutionOrchestrator):
        self.tracker = tracker
        self.pipeline = pipeline
        self.orchestrator = orchestrator
        self._cycle_count = 0

    def on_trade_close(self, product_id: str, direction: str, entry: float,
                       exit: float, pnl: float, r_multiple: float,
                       strategy: str, mode: str = "paper"):
        self.tracker.record_trade(strategy, product_id, direction,
                                   entry, exit, pnl, r_multiple, mode=mode)
        self.pipeline.check_deployment(strategy)

    def cycle(self):
        self._cycle_count += 1
        perf = self.tracker.get_all_performance()
        for name, dep in self.pipeline._deployments.items():
            p = perf.get(name, {})
            if p.get("trades", 0) > 0:
                self.pipeline.update_backtest_result(
                    name,
                    win_rate=p.get("win_rate", 0),
                    sharpe=p.get("sharpe", 0),
                    profit_factor=p.get("profit_factor", 1),
                    trades=p.get("trades", 0),
                    passed=p.get("trades", 0) >= 10 and p.get("win_rate", 0) >= 0.4,
                )
                deployed = self.pipeline.check_deployment(name)
                if deployed:
                    weight = min(2.0, 0.5 + p.get("win_rate", 0) * 2)
                    dep = self.pipeline._deployments[name]
                    dep.weight = weight
                    self.orchestrator.update_strategy_performance(
                        name, p.get("wins", 0) > p.get("losses", 0), p.get("avg_win", 0)
                    )
