"""Multi-hop Coinbase conversion routing.

Builds a currency graph from Coinbase products and finds the best conversion
path between two currencies using current ticker prices and an approximate fee
model. This is a planning utility, but it is designed to be wired into trade
execution for direct or bridged conversions.
"""

from __future__ import annotations

import heapq
import json
import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import urllib3

log = logging.getLogger(__name__)

_http = urllib3.PoolManager(maxsize=20)


@dataclass
class RouteStep:
    product_id: str
    from_currency: str
    to_currency: str
    direction: str  # BUY or SELL from the perspective of the product
    price: float
    effective_rate: float


@dataclass
class RoutePlan:
    source: str
    target: str
    steps: List[RouteStep] = field(default_factory=list)
    effective_rate: float = 0.0
    fee_bps: float = 10.0
    spread_bps: float = 5.0

    @property
    def hop_count(self) -> int:
        return len(self.steps)

    @property
    def path(self) -> List[str]:
        if not self.steps:
            return [self.source, self.target]
        nodes = [self.source]
        for step in self.steps:
            nodes.append(step.to_currency)
        return nodes


@dataclass
class RouteContext:
    """Decision inputs for ranking route candidates.

    The planner uses these factors to decide whether a route is actually worth
    taking, not merely whether it exists.
    """

    amount_in: float = 0.0
    candidate_targets: List[str] = field(default_factory=list)
    opportunities: List[Dict[str, Any]] = field(default_factory=list)
    holdings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    current_prices: Dict[str, float] = field(default_factory=dict)
    drawdown_pct: float = 0.0
    regime: str = "neutral"
    tax_rate_short_term: float = 0.20
    tax_rate_long_term: float = 0.15
    prefer_core_assets: bool = True
    core_assets: Tuple[str, ...] = ("BTC", "ETH", "SOL")
    stable_assets: Tuple[str, ...] = ("USD", "USDC", "USDT", "DAI", "USD1", "USDS")
    max_hops: int = 3


@dataclass
class RouteDecision:
    """A route plus the score and factor breakdown used to choose it."""

    plan: RoutePlan
    score: float
    expected_tax_impact_usd: float = 0.0
    opportunity_bonus: float = 0.0
    drawdown_bonus: float = 0.0
    regime_bonus: float = 0.0
    hop_penalty: float = 0.0
    liquidity_bonus: float = 0.0
    factor_breakdown: Dict[str, float] = field(default_factory=dict)

    @property
    def path(self) -> List[str]:
        return self.plan.path


def _normalize_products(products: Iterable[dict]) -> Dict[str, Tuple[str, str]]:
    mapping: Dict[str, Tuple[str, str]] = {}
    for p in products:
        pid = p.get("id") or p.get("product_id")
        base = p.get("base_currency") or p.get("base")
        quote = p.get("quote_currency") or p.get("quote")
        if pid and base and quote:
            mapping[str(pid)] = (str(base).upper(), str(quote).upper())
    return mapping


def _ticker_prices(product_ids: Iterable[str]) -> Dict[str, float]:
    prices: Dict[str, float] = {}
    for pid in product_ids:
        try:
            r = _http.request("GET", f"https://api.exchange.coinbase.com/products/{pid}/ticker", timeout=10)
            t = json.loads(r.data)
            px = float(t.get("price", 0) or 0)
            if px > 0:
                prices[pid] = px
        except Exception:
            continue
    return prices


def build_route_graph(products: Iterable[dict]) -> Dict[str, List[Tuple[str, RouteStep]]]:
    """Build a directed graph of currency conversion edges."""

    mapping = _normalize_products(products)
    prices = _ticker_prices(mapping.keys())
    graph: Dict[str, List[Tuple[str, RouteStep]]] = {}

    for pid, (base, quote) in mapping.items():
        px = prices.get(pid)
        if not px or px <= 0:
            continue
        fee_mult = 1.0 - 0.0015  # approx taker fee + spread cushion
        # quote -> base (buy base with quote)
        buy_rate = (1.0 / px) * fee_mult
        # base -> quote (sell base for quote)
        sell_rate = px * fee_mult
        graph.setdefault(quote, []).append((base, RouteStep(pid, quote, base, "BUY", px, buy_rate)))
        graph.setdefault(base, []).append((quote, RouteStep(pid, base, quote, "SELL", px, sell_rate)))

    return graph


def _is_core(currency: str, core_assets: Sequence[str]) -> bool:
    return str(currency).upper().replace("-USD", "") in {c.upper() for c in core_assets}


def _normalized_opportunity_bonus(plan: RoutePlan, ctx: RouteContext) -> float:
    if not ctx.opportunities:
        return 0.0

    path = {node.upper() for node in plan.path}
    bonus = 0.0
    for opp in ctx.opportunities:
        currency = str(opp.get("currency", "")).upper().replace("-USD", "")
        side = str(opp.get("side", "")).upper()
        priority = float(opp.get("priority", opp.get("confidence", 0.0)) or 0.0)
        if currency not in path:
            continue
        if side == "BUY" and currency == plan.target:
            bonus += priority
        elif side == "SELL" and currency == plan.source:
            bonus += priority * 0.75
        else:
            bonus += priority * 0.35
    return min(bonus, 1.5)


def _tax_impact_usd(plan: RoutePlan, ctx: RouteContext) -> float:
    """Estimate the tax benefit (positive) or cost (negative) of the source sale."""

    source = plan.source.upper().replace("-USD", "")
    holding = ctx.holdings.get(source, {})
    amount = float(ctx.amount_in or 0.0)
    if amount <= 0:
        amount = float(holding.get("value", 0.0) or 0.0)
    if amount <= 0:
        return 0.0

    cost_basis = float(holding.get("cost_basis", 0.0) or 0.0)
    price = float(holding.get("price", 0.0) or ctx.current_prices.get(source, 0.0) or 0.0)
    if cost_basis <= 0 or price <= 0:
        return 0.0

    pnl_pct = (price / cost_basis) - 1.0
    holding_days = float(holding.get("holding_days", 0.0) or 0.0)
    tax_rate = ctx.tax_rate_long_term if holding_days >= 365 else ctx.tax_rate_short_term
    notional = min(amount, float(holding.get("value", amount) or amount))
    pnl_usd = notional * pnl_pct

    if pnl_usd < 0:
        return abs(pnl_usd) * tax_rate
    return -pnl_usd * tax_rate


def _drawdown_bonus(plan: RoutePlan, ctx: RouteContext) -> float:
    dd = max(0.0, float(ctx.drawdown_pct))
    if dd <= 0:
        return 0.0
    target = plan.target.upper().replace("-USD", "")
    if _is_core(target, ctx.core_assets):
        return min(dd * 1.25, 0.75)
    if target in {a.upper() for a in ctx.stable_assets}:
        return min(dd * 0.75, 0.35)
    return min(dd * 0.25, 0.15)


def _regime_bonus(plan: RoutePlan, ctx: RouteContext) -> float:
    regime = (ctx.regime or "").lower()
    target = plan.target.upper().replace("-USD", "")
    source = plan.source.upper().replace("-USD", "")

    if "down" in regime or regime in {"bear", "bearish", "risk_off"}:
        if _is_core(target, ctx.core_assets):
            return 0.3
        if target in {a.upper() for a in ctx.stable_assets}:
            return 0.15
        return -0.1

    if "up" in regime or regime in {"bull", "bullish", "risk_on"}:
        if _is_core(target, ctx.core_assets):
            return 0.2
        if _is_core(source, ctx.core_assets):
            return 0.1
    return 0.0


def score_route_plan(plan: RoutePlan, ctx: Optional[RouteContext] = None) -> RouteDecision:
    """Score a route using efficiency, tax impact, opportunities, and regime."""

    ctx = ctx or RouteContext()
    hops = max(0, plan.hop_count - 1)
    hop_penalty = hops * 0.05
    liquidity_bonus = 0.0

    # Efficiency: log-scale so huge nominal exchange rates don't dominate.
    efficiency_score = 0.0
    if plan.effective_rate > 0:
        efficiency_score = max(-1.0, min(1.0, math.log1p(plan.effective_rate) / 10.0))

    opportunity_bonus = _normalized_opportunity_bonus(plan, ctx)
    tax_impact_usd = _tax_impact_usd(plan, ctx)
    # Convert tax impact into a bounded score component.
    tax_score = max(-0.5, min(0.75, tax_impact_usd / max(ctx.amount_in or 1.0, 1.0)))

    drawdown_bonus = _drawdown_bonus(plan, ctx)
    regime_bonus = _regime_bonus(plan, ctx)

    if ctx.prefer_core_assets and _is_core(plan.target, ctx.core_assets):
        liquidity_bonus += 0.1
    if plan.target.upper() in {a.upper() for a in ctx.stable_assets}:
        liquidity_bonus += 0.15

    # Reward direct routes but allow multi-hop if the net score is better.
    base_score = (
        efficiency_score
        + opportunity_bonus
        + tax_score
        + drawdown_bonus
        + regime_bonus
        + liquidity_bonus
        - hop_penalty
    )
    score = max(-1.0, min(1.5, base_score))
    return RouteDecision(
        plan=plan,
        score=score,
        expected_tax_impact_usd=tax_impact_usd,
        opportunity_bonus=opportunity_bonus,
        drawdown_bonus=drawdown_bonus,
        regime_bonus=regime_bonus,
        hop_penalty=hop_penalty,
        liquidity_bonus=liquidity_bonus,
        factor_breakdown={
            "efficiency": efficiency_score,
            "opportunity": opportunity_bonus,
            "tax": tax_score,
            "drawdown": drawdown_bonus,
            "regime": regime_bonus,
            "liquidity": liquidity_bonus,
            "hop_penalty": -hop_penalty,
        },
    )


def find_best_route(
    source: str,
    target: str,
    products: Iterable[dict],
    *,
    max_hops: int = 3,
) -> Optional[RoutePlan]:
    """Find the highest-effective conversion path from source to target.

    The objective maximizes the effective conversion rate from one unit of
    source currency to target currency.
    """

    source = source.upper()
    target = target.upper()
    if source == target:
        return RoutePlan(source=source, target=target, effective_rate=1.0)

    graph = build_route_graph(products)
    if source not in graph:
        return None

    # Priority queue stores (negative log gain, hops, current_currency, path_steps, effective_rate)
    pq: List[Tuple[float, int, str, List[RouteStep], float]] = [(0.0, 0, source, [], 1.0)]
    best: Dict[Tuple[str, int], float] = {(source, 0): 1.0}
    best_plan: Optional[RoutePlan] = None
    best_rate = 0.0

    while pq:
        cost, hops, cur, steps, rate = heapq.heappop(pq)
        if cur == target and rate > best_rate:
            best_rate = rate
            best_plan = RoutePlan(source=source, target=target, steps=steps, effective_rate=rate)
        if hops >= max_hops:
            continue
        for nxt, step in graph.get(cur, []):
            next_rate = rate * step.effective_rate
            next_hops = hops + 1
            state = (nxt, next_hops)
            if next_rate <= best.get(state, 0.0):
                continue
            best[state] = next_rate
            next_cost = -next_rate
            heapq.heappush(pq, (next_cost, next_hops, nxt, steps + [step], next_rate))

    return best_plan


def find_best_decision(
    source: str,
    target_candidates: Sequence[str],
    products: Iterable[dict],
    *,
    context: Optional[RouteContext] = None,
    max_hops: int = 3,
) -> Optional[RouteDecision]:
    """Find the best route across multiple target candidates.

    This is the decision layer: it considers route efficiency plus the market
    context (opportunities, tax, drawdown, regime, etc.).
    """

    context = context or RouteContext()
    candidates = [str(t).upper().replace("-USD", "") for t in target_candidates if str(t).strip()]
    if not candidates:
        candidates = ["USD"]

    best: Optional[RouteDecision] = None
    for target in candidates:
        plan = find_best_route(source, target, products, max_hops=max_hops)
        if plan is None:
            continue
        decision = score_route_plan(plan, context)
        if best is None or decision.score > best.score:
            best = decision
    return best


def describe_route(plan: Optional[RoutePlan]) -> str:
    if plan is None:
        return "no route"
    if not plan.steps:
        return f"{plan.source} -> {plan.target} (direct)"
    hops = " -> ".join(plan.path)
    return f"{hops} | rate={plan.effective_rate:.6f} | hops={plan.hop_count}"


def describe_decision(decision: Optional[RouteDecision]) -> str:
    if decision is None:
        return "no decision"
    plan = decision.plan
    return (
        f"{describe_route(plan)} | score={decision.score:+.3f} "
        f"tax={decision.expected_tax_impact_usd:+.2f} "
        f"opp={decision.opportunity_bonus:+.2f} dd={decision.drawdown_bonus:+.2f} "
        f"regime={decision.regime_bonus:+.2f} hop={decision.hop_penalty:+.2f}"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from coinbase.src.pair_discovery import get_all_coinbase_pairs

    products = get_all_coinbase_pairs(min_volume_usd=0)
    plan = find_best_route("BTC", "USD", products, max_hops=3)
    print(describe_route(plan))
