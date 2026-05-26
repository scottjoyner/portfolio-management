from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class RouteSolution:
    path: list[str]
    estimated_output: Decimal
    estimated_gas: Decimal
    score: float = 0.0


@dataclass
class RouteSolver:
    def find_best_route(self, token_in: str, token_out: str, amount_in: Decimal, max_hops: int = 3) -> RouteSolution | None:
        return None
