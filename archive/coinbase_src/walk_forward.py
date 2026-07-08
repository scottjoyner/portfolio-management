from __future__ import annotations
import math
import random
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Callable, Tuple
from enum import Enum


@dataclass
class ParamRange:
    name: str
    min_val: float
    max_val: float
    step: float
    is_int: bool = False

    def values(self) -> List[float]:
        vals = []
        v = self.min_val
        while v <= self.max_val:
            vals.append(round(v) if self.is_int else round(v, 4))
            v += self.step
        return vals


@dataclass
class TrialResult:
    params: Dict[str, float]
    metric: float
    win_rate: float = 0.0
    sharpe: float = 0.0
    profit_factor: float = 0.0
    max_dd: float = 0.0
    trades: int = 0

    @property
    def score(self) -> float:
        return self.metric


@dataclass
class WalkForwardWindow:
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    best_params: Dict[str, float] = field(default_factory=dict)
    train_score: float = 0.0
    test_score: float = 0.0
    decay: float = 0.0


class WalkForwardOptimizer:
    def __init__(self, n_windows: int = 5, train_pct: float = 0.7,
                 metric_fn: Optional[Callable] = None,
                 random_search_iters: int = 200):
        self.n_windows = n_windows
        self.train_pct = train_pct
        self.metric_fn = metric_fn or self._default_metric
        self.random_search_iters = random_search_iters
        self.windows: List[WalkForwardWindow] = []
        self._best_overall: Dict[str, float] = {}

    def optimize(self, data_length: int, param_ranges: List[ParamRange],
                 objective_fn: Callable[[Dict[str, float], int, int], TrialResult]
                 ) -> Dict[str, Any]:
        self.windows = self._build_windows(data_length)

        for window in self.windows:
            best_score = -float('inf')
            best_params = {}

            for _ in range(self.random_search_iters):
                params = {}
                for pr in param_ranges:
                    if pr.is_int:
                        params[pr.name] = random.randint(int(pr.min_val), int(pr.max_val))
                    else:
                        params[pr.name] = round(
                            random.uniform(pr.min_val, pr.max_val), 4
                        )

                trial = objective_fn(params, window.train_start, window.train_end)
                if trial.score > best_score:
                    best_score = trial.score
                    best_params = params.copy()

            test_trial = objective_fn(best_params, window.test_start, window.test_end)
            window.best_params = best_params
            window.train_score = best_score
            window.test_score = test_trial.score

        self._compute_decay()

        param_stability = self._param_stability(param_ranges)
        avg_test_score = sum(w.test_score for w in self.windows) / max(len(self.windows), 1)
        avg_train_score = sum(w.train_score for w in self.windows) / max(len(self.windows), 1)
        overfitting_ratio = (avg_train_score - avg_test_score) / max(abs(avg_train_score), 1e-9)

        consensus_params = self._consensus_params(param_ranges)

        return {
            "windows": self.windows,
            "param_stability": param_stability,
            "avg_test_score": round(avg_test_score, 4),
            "avg_train_score": round(avg_train_score, 4),
            "overfitting_ratio": round(overfitting_ratio, 4),
            "consensus_params": consensus_params,
            "decay_avg": round(sum(w.decay for w in self.windows) / max(len(self.windows), 1), 4),
        }

    def _build_windows(self, data_length: int) -> List[WalkForwardWindow]:
        windows = []
        step = data_length // max(self.n_windows, 1)
        for i in range(self.n_windows):
            train_start = i * step
            train_end = train_start + int(step * self.train_pct)
            test_start = train_end
            test_end = min(test_start + int(step * (1 - self.train_pct)), data_length)
            if test_end <= test_start or train_end <= train_start:
                continue
            windows.append(WalkForwardWindow(
                train_start=train_start, train_end=train_end,
                test_start=test_start, test_end=test_end,
            ))
        return windows

    def _compute_decay(self):
        if len(self.windows) < 2:
            return
        for i in range(1, len(self.windows)):
            prev = self.windows[i - 1].test_score
            curr = self.windows[i].test_score
            self.windows[i].decay = (curr - prev) / max(abs(prev), 1e-9)

    def _param_stability(self, param_ranges: List[ParamRange]) -> Dict[str, float]:
        stability = {}
        for pr in param_ranges:
            values = [w.best_params.get(pr.name, 0) for w in self.windows if w.best_params]
            if len(values) < 2:
                stability[pr.name] = 0.0
            else:
                mean_v = sum(values) / len(values)
                variance = sum((v - mean_v) ** 2 for v in values) / len(values)
                rng = pr.max_val - pr.min_val
                stability[pr.name] = 1.0 - math.sqrt(variance) / max(rng, 1e-9)
        return stability

    def _consensus_params(self, param_ranges: List[ParamRange]) -> Dict[str, float]:
        consensus = {}
        for pr in param_ranges:
            values = [w.best_params.get(pr.name, 0) for w in self.windows if w.best_params]
            if values:
                if pr.is_int:
                    consensus[pr.name] = round(sum(values) / len(values))
                else:
                    consensus[pr.name] = round(sum(values) / len(values), 4)
        return consensus

    @staticmethod
    def _default_metric(trial: TrialResult) -> float:
        if trial.trades < 5:
            return -float('inf')
        return trial.sharpe * trial.win_rate * math.sqrt(trial.trades) / max(trial.max_dd + 0.01, 0.01)


@dataclass
class Scenario:
    name: str
    price_shock_pct: float
    vol_multiplier: float
    correlation_shock: float = 0.0
    recovery_days: int = 30


DEFAULT_SCENARIOS = [
    Scenario("flash_crash_10pct", -0.10, 3.0, 0.3, 5),
    Scenario("flash_crash_20pct", -0.20, 5.0, 0.5, 10),
    Scenario("crypto_winter", -0.50, 2.0, 0.8, 365),
    Scenario("liquidity_crisis", -0.15, 4.0, 0.6, 30),
    Scenario("black_swan", -0.30, 6.0, 0.7, 60),
    Scenario("btc_rally", 0.25, 1.5, 0.2, 0),
    Scenario("btc_crash_30pct", -0.30, 4.0, 0.5, 45),
    Scenario("correlated_selloff", -0.15, 2.5, 0.9, 20),
    Scenario("volatility_spike", -0.05, 5.0, 0.3, 7),
    Scenario("slow_decline", -0.25, 1.2, 0.4, 180),
]


class StressTester:
    def __init__(self, scenarios: Optional[List[Scenario]] = None):
        self.scenarios = scenarios or DEFAULT_SCENARIOS

    def run_all(self, positions: List[Dict[str, Any]],
                current_prices: Dict[str, float],
                volatilities: Dict[str, float]) -> List[Dict[str, Any]]:
        results = []
        for scenario in self.scenarios:
            result = self._run_scenario(scenario, positions, current_prices, volatilities)
            results.append(result)
        return results

    def _run_scenario(self, scenario: Scenario, positions: List[Dict],
                       prices: Dict[str, float],
                       volatilities: Dict[str, float]) -> Dict[str, Any]:
        total_loss = 0.0
        max_loss = 0.0
        losses_by_product = {}

        for pos in positions:
            pid = pos["product_id"]
            current = prices.get(pid, 0.0)
            if current <= 0:
                continue

            shocked = current * (1.0 + scenario.price_shock_pct)

            vol = volatilities.get(pid, 0.02) * scenario.vol_multiplier

            if scenario.correlation_shock > 0:
                shocked = current * (1.0 + scenario.price_shock_pct * scenario.correlation_shock)

            size = pos.get("size", 0.0)
            side = pos.get("side", "long")
            entry = pos.get("entry_price", current)
            leverage = pos.get("leverage", 1.0)

            if side == "long":
                loss = (shocked - entry) * size / leverage
            else:
                loss = (entry - shocked) * size / leverage

            total_loss += loss
            max_loss = min(max_loss, loss)
            losses_by_product[pid] = round(loss, 2)

        recovery_value = total_loss * (1 + 0.02 * scenario.recovery_days / 30) if scenario.recovery_days > 0 else total_loss

        return {
            "scenario": scenario.name,
            "shock_pct": scenario.price_shock_pct * 100,
            "vol_multiplier": scenario.vol_multiplier,
            "total_loss": round(total_loss, 2),
            "max_loss": round(max_loss, 2),
            "losses_by_product": losses_by_product,
            "max_drawdown_estimate": round(min(-0.01, total_loss / max(sum(
                pos.get("size", 0) * prices.get(pos["product_id"], 1) / max(pos.get("leverage", 1), 1)
                for pos in positions
            ), 1)), 4),
            "estimated_recovery_value": round(recovery_value, 2),
            "risk_rating": self._risk_rating(total_loss),
        }

    def monte_carlo(self, positions: List[Dict], prices: Dict[str, float],
                     volatilities: Dict[str, float], n_sims: int = 1000,
                     horizon_days: int = 1) -> Dict[str, float]:
        total_notional = sum(
            pos.get("size", 0) * prices.get(pos["product_id"], 1) / max(pos.get("leverage", 1), 1)
            for pos in positions
        )
        if total_notional <= 0:
            return {"expected_loss": 0, "var_95": 0, "var_99": 0, "max_loss": 0}

        pnl_sims = []
        for _ in range(n_sims):
            daily_pnl = 0.0
            for pos in positions:
                pid = pos["product_id"]
                vol = volatilities.get(pid, 0.02)
                size = pos.get("size", 0)
                side = pos.get("side", "long")
                lev = pos.get("leverage", 1.0)
                ret = random.gauss(0, vol / math.sqrt(365 / max(horizon_days, 1)))
                if side == "long":
                    pnl = size * prices.get(pid, 1) * ret / lev
                else:
                    pnl = -size * prices.get(pid, 1) * ret / lev
                daily_pnl += pnl
            pnl_sims.append(daily_pnl)

        pnl_sims.sort()
        idx_95 = max(0, int(n_sims * 0.05) - 1)
        idx_99 = max(0, int(n_sims * 0.01) - 1)
        return {
            "expected_loss": round(sum(pnl_sims) / n_sims, 2),
            "var_95": round(pnl_sims[idx_95], 2),
            "var_99": round(pnl_sims[idx_99], 2),
            "max_loss": round(min(pnl_sims), 2),
            "n_simulations": n_sims,
        }

    @staticmethod
    def _risk_rating(total_loss: float) -> str:
        if total_loss >= 0:
            return "none"
        abs_loss = abs(total_loss)
        if abs_loss < 100:
            return "low"
        elif abs_loss < 1000:
            return "moderate"
        elif abs_loss < 10000:
            return "high"
        return "severe"

    @staticmethod
    def print_report(results: List[Dict]):
        print(f"{'Scenario':25s} {'Shock':>8s} {'Loss':>12s} {'DD Est':>8s} {'Rating':>10s}")
        print("-" * 70)
        for r in sorted(results, key=lambda x: x["total_loss"]):
            print(
                f"{r['scenario']:25s} "
                f"{r['shock_pct']:>7.1f}% "
                f"${r['total_loss']:>9.2f} "
                f"{r['max_drawdown_estimate']*100:>7.1f}% "
                f"{r['risk_rating']:>10s}"
            )
