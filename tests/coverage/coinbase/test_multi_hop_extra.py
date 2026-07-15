"""Extra branch-coverage tests for coinbase/src/multi_hop.py (target >=90%)."""
from __future__ import annotations

from unittest.mock import Mock

import pytest

from coinbase.src import multi_hop as mh


def _products():
    return [
        {"id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD"},
        {"id": "ETH-USD", "base_currency": "ETH", "quote_currency": "USD"},
        {"id": "ETH-BTC", "base_currency": "ETH", "quote_currency": "BTC"},
        {"id": "SOL-USD", "base_currency": "SOL", "quote_currency": "USD"},
    ]


def _handler(prices):
    def handler(method, url, timeout=10):
        for pid, px in prices.items():
            if pid in url:
                if isinstance(px, Exception):
                    raise px
                return Mock(data=f'{{"price": {px}}}')
        return Mock(data='{"price": 0}')
    return handler


def test_normalize_products_missing_fields():
    # pid/base/quote missing -> `if` is falsy, continue (line 106->102)
    prods = _products() + [{"id": "BAD", "base_currency": "BAD"}]  # no quote
    m = mh._normalize_products(prods)
    assert "BAD" not in m
    assert "BTC-USD" in m


def test_ticker_prices_exception():
    # one ticker fetch raises -> except -> continue (lines 120-121)
    prices = {"BTC-USD": 50000, "ETH-USD": RuntimeError("boom"),
              "ETH-BTC": 0.06, "SOL-USD": 150}
    mh._http.request = _handler(prices)
    out = mh._ticker_prices(["BTC-USD", "ETH-USD", "ETH-BTC", "SOL-USD", "NOPE-USD"])
    assert "ETH-USD" not in out
    assert out["BTC-USD"] == 50000


def test_build_route_graph_skips_zero_price():
    # product with px <= 0 -> continue at line 135
    prices = {"BTC-USD": 50000, "ETH-USD": 3000, "ETH-BTC": 0.06, "SOL-USD": 0}
    mh._http.request = _handler(prices)
    g = mh.build_route_graph(_products())
    # SOL has zero price -> no SOL edges
    assert "SOL" not in g


def test_opportunity_bonus_else_branch():
    plan = mh.RoutePlan(source="USD", target="BTC")
    ctx = mh.RouteContext(opportunities=[
        {"currency": "BTC", "side": "SELL", "priority": 1.0},  # in path, not BUY-target/SELL-source
    ])
    b = mh._normalized_opportunity_bonus(plan, ctx)
    assert b > 0


def test_regime_bonus_bear_non_core():
    plan = mh.RoutePlan(source="USD", target="XRP")
    assert mh._regime_bonus(plan, mh.RouteContext(regime="bear")) == -0.1


def test_regime_bonus_bear_stable():
    # bear regime, stable target -> return 0.1 (line 220)
    plan = mh.RoutePlan(source="USD", target="USDC")
    assert mh._regime_bonus(plan, mh.RouteContext(regime="bear")) == 0.15


def test_regime_bonus_bull_source_core():
    plan = mh.RoutePlan(source="BTC", target="XRP")
    assert mh._regime_bonus(plan, mh.RouteContext(regime="bull")) == 0.1


def test_regime_bonus_bull_non_core():
    # bull regime, source & target non-core -> 226->228 returns 0.0
    plan = mh.RoutePlan(source="USD", target="XRP")
    assert mh._regime_bonus(plan, mh.RouteContext(regime="bull")) == 0.0


def test_score_route_plan_no_prefer_core():
    plan = mh.RoutePlan(source="USD", target="BTC", effective_rate=1.0, steps=[
        mh.RouteStep("BTC-USD", "USD", "BTC", "BUY", 50000, 1 / 50000)
    ])
    # prefer_core_assets=False -> skip the liquidity bonus branch (241->244)
    ctx = mh.RouteContext(prefer_core_assets=False)
    dec = mh.score_route_plan(plan, ctx)
    assert -1.0 <= dec.score <= 1.5
    # RouteDecision.path property (line 97)
    assert "BTC" in dec.path


def test_score_route_plan_zero_effective_rate():
    # effective_rate <= 0 -> efficiency_score branch skipped (241->244)
    plan = mh.RoutePlan(source="USD", target="BTC", effective_rate=0.0)
    dec = mh.score_route_plan(plan, mh.RouteContext())
    assert -1.0 <= dec.score <= 1.5


def test_score_route_plan_target_not_core():
    plan = mh.RoutePlan(source="USD", target="XRP", effective_rate=1.0, steps=[
        mh.RouteStep("XRP-USD", "USD", "XRP", "BUY", 1, 1.0)
    ])
    # target not core, prefer_core_assets=True -> liquidity bonus skip (226->228)
    dec = mh.score_route_plan(plan, mh.RouteContext(prefer_core_assets=True))
    assert -1.0 <= dec.score <= 1.5
