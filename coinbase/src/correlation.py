from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import numpy as np


@dataclass
class CorrelationMatrix:
    products: List[str] = field(default_factory=list)
    matrix: Dict[Tuple[str, str], float] = field(default_factory=dict)

    def get(self, a: str, b: str) -> float:
        if a == b:
            return 1.0
        return self.matrix.get((a, b), self.matrix.get((b, a), 0.0))

    def set(self, a: str, b: str, r: float):
        self.matrix[(a, b)] = r
        self.matrix[(b, a)] = r

    def estimate_default(self, a: str, b: str) -> float:
        crypto_pairs = {
            ("BTC", "ETH"): 0.65,
            ("BTC", "SOL"): 0.55,
            ("BTC", "DOGE"): 0.40,
            ("BTC", "ADA"): 0.50,
            ("BTC", "XRP"): 0.45,
            ("ETH", "SOL"): 0.55,
            ("ETH", "LINK"): 0.50,
            ("ETH", "UNI"): 0.45,
            ("SOL", "AVAX"): 0.50,
            ("SOL", "DOGE"): 0.35,
        }
        base_a = a.split("-")[0] if "-" in a else a
        base_b = b.split("-")[0] if "-" in b else b
        if base_a == base_b:
            return 1.0
        return crypto_pairs.get((base_a, base_b),
                                crypto_pairs.get((base_b, base_a), 0.30))

    def compute_from_returns(self, returns_a: List[float], returns_b: List[float]) -> float:
        if len(returns_a) < 3 or len(returns_b) < 3:
            return 0.5
        n = min(len(returns_a), len(returns_b))
        ra = np.array(returns_a[-n:])
        rb = np.array(returns_b[-n:])
        if np.std(ra) == 0 or np.std(rb) == 0:
            return 0.0
        return float(np.corrcoef(ra, rb)[0, 1])


class CorrelationAwareSizer:
    def __init__(self, correlation: Optional[CorrelationMatrix] = None):
        self.corr = correlation or CorrelationMatrix()

    def diversify_multiplier(self, product_id: str,
                             existing_notionals: Dict[str, float]) -> float:
        if not existing_notionals:
            return 1.0
        weights = []
        for pid, notional in existing_notionals.items():
            r = self.corr.estimate_default(product_id, pid)
            weights.append(r * notional)
        total = sum(weights)
        if total <= 0:
            return 1.0
        avg_corr = total / sum(abs(w) for w in weights) if any(weights) else 0.5
        mult = 1.0 - avg_corr * 0.3
        return max(0.4, min(1.0, mult))

    def size_with_correlation(self, base_size: float, product_id: str,
                               existing_notionals: Dict[str, float]) -> float:
        mult = self.diversify_multiplier(product_id, existing_notionals)
        return base_size * mult

    def portfolio_heat(self, positions: List[Dict[str, float]]) -> Dict[str, float]:
        n = len(positions)
        if n < 2:
            return {"heat": 0.0, "diversification_score": 1.0}
        pids = [p["product_id"] for p in positions]
        weights = np.array([p["weight"] for p in positions])
        weights = weights / max(np.sum(weights), 1e-9)
        corr_matrix = np.ones((n, n))
        for i in range(n):
            for j in range(i + 1, n):
                r = self.corr.estimate_default(pids[i], pids[j])
                corr_matrix[i, j] = r
                corr_matrix[j, i] = r
        portfolio_variance = weights.T @ corr_matrix @ weights
        avg_corr = (np.sum(corr_matrix) - n) / max(n * (n - 1), 1)
        diversification = 1.0 - float(avg_corr)
        return {
            "heat": float(np.sqrt(portfolio_variance)),
            "diversification_score": float(max(0, min(1, diversification))),
            "avg_correlation": float(avg_corr),
        }
