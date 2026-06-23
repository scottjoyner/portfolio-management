from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from .protocols import Direction, Opportunity


@dataclass
class AssetRisk:
    product_id: str
    volatility: float
    weight: float = 0.0
    risk_contribution: float = 0.0
    marginal_risk: float = 0.0


@dataclass
class RiskParityResult:
    assets: List[AssetRisk]
    target_vol: float
    portfolio_vol: float = 0.0
    concentration: float = 1.0
    iterations: int = 0
    converged: bool = False


class RiskParityPortfolio:
    def __init__(self, target_vol: float = 0.12, max_iter: int = 100,
                 tol: float = 1e-6):
        self.target_vol = target_vol
        self.max_iter = max_iter
        self.tol = tol

    def optimize(self, product_ids: List[str],
                 volatilities: Dict[str, float],
                 correlations: Optional[Dict[Tuple[str, str], float]] = None
                 ) -> RiskParityResult:
        n = len(product_ids)
        if n == 0:
            return RiskParityResult(assets=[], target_vol=self.target_vol)
        if n == 1:
            vol = max(volatilities.get(product_ids[0], 0.2), 0.01)
            return RiskParityResult(
                assets=[AssetRisk(product_id=product_ids[0], volatility=vol, weight=1.0, risk_contribution=vol)],
                target_vol=self.target_vol, portfolio_vol=vol,
            )

        vols = [max(volatilities.get(p, 0.2), 0.01) for p in product_ids]
        corr = self._build_correlation(n, product_ids, correlations)

        w = [1.0 / n] * n
        for iteration in range(self.max_iter):
            sigma = self._portfolio_vol(w, vols, corr)
            if sigma <= 0:
                break
            mrc = [self._marginal_risk_contribution(i, w, vols, corr) for i in range(n)]
            rc = [w[i] * mrc[i] for i in range(n)]
            avg_rc = sum(rc) / n
            for i in range(n):
                if mrc[i] > 0:
                    w[i] = max(0.0, w[i] * (avg_rc / max(rc[i], 1e-9)))
            w_sum = sum(w)
            if w_sum > 0:
                w = [wi / w_sum for wi in w]
            max_diff = max(abs(rc[i] - avg_rc) for i in range(n))
            if max_diff < self.tol:
                break

        w = [wi / sum(w) for wi in w]
        final_vol = self._portfolio_vol(w, vols, corr)
        leverage = self.target_vol / max(final_vol, 0.001)
        w = [wi * leverage for wi in w]

        mrc_final = [self._marginal_risk_contribution(i, w, vols, corr) for i in range(n)]
        rc_final = [w[i] * mrc_final[i] for i in range(n)]
        total_rc = sum(rc_final)

        assets = [
            AssetRisk(
                product_id=product_ids[i],
                volatility=vols[i],
                weight=round(w[i], 6),
                risk_contribution=rc_final[i] / max(total_rc, 1e-9) if total_rc > 0 else 1.0 / n,
                marginal_risk=mrc_final[i],
            )
            for i in range(n)
        ]

        concentration = sum(r.risk_contribution ** 2 for r in assets)
        return RiskParityResult(
            assets=assets,
            target_vol=self.target_vol,
            portfolio_vol=round(final_vol * math.sqrt(leverage), 6),
            concentration=round(concentration, 4),
            iterations=iteration + 1,
            converged=iteration < self.max_iter - 1,
        )

    def allocate(self, equity: float, result: RiskParityResult,
                 prices: Dict[str, float]) -> Dict[str, float]:
        sizes = {}
        for asset in result.assets:
            target_notional = equity * asset.weight
            price = prices.get(asset.product_id, 1.0)
            if price > 0:
                sizes[asset.product_id] = round(target_notional / price, 8)
        return sizes

    def risk_budget_sizing(self, opportunities: List[Opportunity],
                           equity: float,
                           volatilities: Dict[str, float]) -> List[Opportunity]:
        if not opportunities:
            return []

        pids = list(set(o.product_id for o in opportunities))
        vols = {p: volatilities.get(p, 0.02) for p in pids}

        result = self.optimize(pids, vols)
        alloc = self.allocate(equity, result, {o.product_id: o.entry_price for o in opportunities})

        for opp in opportunities:
            target_notional = alloc.get(opp.product_id, 0.0)
            if target_notional <= 0:
                opp.base_size = 0.0
            else:
                opp.base_size = target_notional / max(opp.entry_price, 1e-9)
            opp.meta["risk_parity_weight"] = round(
                next((a.weight for a in result.assets if a.product_id == opp.product_id), 0), 4
            )
            opp.meta["risk_contribution"] = round(
                next((a.risk_contribution for a in result.assets if a.product_id == opp.product_id), 0), 4
            )

        return opportunities

    @staticmethod
    def _portfolio_vol(w: List[float], vols: List[float],
                       corr: List[List[float]]) -> float:
        n = len(w)
        variance = sum(
            w[i] * w[j] * vols[i] * vols[j] * corr[i][j]
            for i in range(n) for j in range(n)
        )
        return math.sqrt(max(variance, 0))

    @staticmethod
    def _marginal_risk_contribution(i: int, w: List[float], vols: List[float],
                                     corr: List[List[float]]) -> float:
        n = len(w)
        return sum(w[j] * vols[i] * vols[j] * corr[i][j] for j in range(n)) / max(
            RiskParityPortfolio._portfolio_vol(w, vols, corr), 1e-9
        )

    @staticmethod
    def _build_correlation(n: int, pids: List[str],
                           correlations: Optional[Dict[Tuple[str, str], float]]
                           ) -> List[List[float]]:
        corr = [[1.0] * n for _ in range(n)]
        if correlations:
            for i in range(n):
                for j in range(i + 1, n):
                    r = correlations.get((pids[i], pids[j]),
                                         correlations.get((pids[j], pids[i]), 0.3))
                    corr[i][j] = r
                    corr[j][i] = r
        else:
            for i in range(n):
                for j in range(i + 1, n):
                    r = 0.3
                    if pids[i].startswith("BTC") or pids[j].startswith("BTC"):
                        r = 0.5
                    corr[i][j] = r
                    corr[j][i] = r
        return corr
