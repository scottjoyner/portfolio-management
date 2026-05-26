from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum


class RouteStepProtocol(Enum):
    UNISWAP_V2 = "uniswap_v2"
    UNISWAP_V3 = "uniswap_v3"
    CURVE = "curve"
    BALANCER = "balancer"


@dataclass
class RouteStep:
    protocol: RouteStepProtocol
    pool: str
    token_in: str
    token_out: str
    expected_amount_out: Decimal = Decimal("0")


@dataclass
class Route:
    steps: list[RouteStep] = field(default_factory=list)
    total_expected_out: Decimal = Decimal("0")


@dataclass
class Router:
    def build_route(self, token_in: str, token_out: str, amount_in: Decimal, pools: list[str]) -> Route:
        return Route()
