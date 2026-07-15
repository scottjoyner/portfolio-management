"""Coverage tests for coinbase/src/multi_hop.py"""
from __future__ import annotations

import math
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


def test_normalize_products():
    m = mh._normalize_products(_products())
    assert m["BTC-USD"] == ("BTC", "USD")


def test_ticker_prices():
    def handler(url, timeout=10):
        if "BTC-USD" in url:
            return b'{"price": 50000}'
        if "ETH-USD" in url:
            return b'{"price": 3000}'
        if "ETH-BTC" in url:
            return b'{"price": 0.06}'
        if "SOL-USD" in url:
            return b'{"price": 150}'
        return b'{"price": 0}'
    mh._http.request = lambda method, url, timeout=10: Mock(data=handler(url))
    prices = mh._ticker_prices(["BTC-USD", "ETH-USD", "ETH-BTC", "SOL-USD", "NOPE-USD"])
    assert prices["BTC-USD"] == 50000


def test_build_route_graph():
    def handler(url, timeout=10):
        if "BTC-USD" in url:
            return b'{"price": 50000}'
        if "ETH-USD" in url:
            return b'{"price": 3000}'
        if "ETH-BTC" in url:
            return b'{"price": 0.06}'
        if "SOL-USD" in url:
            return b'{"price": 150}'
        return b'{"price": 0}'
    mh._http.request = lambda method, url, timeout=10: Mock(data=handler(url))
    g = mh.build_route_graph(_products())
    assert "USD" in g
    assert "BTC" in g


def test_is_core():
    assert mh._is_core("BTC", ("BTC", "ETH")) is True
    assert mh._is_core("BTC-USD", ("BTC",)) is True
    assert mh._is_core("XRP", ("BTC",)) is False


def test_opportunity_bonus():
    plan = mh.RoutePlan(source="USD", target="BTC")
    # empty
    assert mh._normalized_opportunity_bonus(plan, mh.RouteContext()) == 0.0
    ctx = mh.RouteContext(opportunities=[
        {"currency": "BTC", "side": "BUY", "priority": 1.0},
        {"currency": "USD", "side": "SELL", "priority": 1.0},
        {"currency": "SOL", "side": "HOLD", "priority": 1.0},
    ])
    plan2 = mh.RoutePlan(source="USD", target="BTC")
    b = mh._normalized_opportunity_bonus(plan2, ctx)
    assert b > 0


def test_tax_impact():
    plan = mh.RoutePlan(source="BTC", target="USD")
    # no amount, no holding
    assert mh._tax_impact_usd(plan, mh.RouteContext()) == 0.0
    # holding with loss -> positive (benefit)
    ctx = mh.RouteContext(
        amount_in=1000.0,
        holdings={"BTC": {"value": 1000.0, "cost_basis": 2000.0, "price": 1000.0,
                          "holding_days": 10}},
    )
    assert mh._tax_impact_usd(plan, ctx) > 0
    # holding with gain, long term
    ctx2 = mh.RouteContext(
        amount_in=1000.0,
        holdings={"BTC": {"value": 1000.0, "cost_basis": 500.0, "price": 1000.0,
                          "holding_days": 400}},
    )
    assert mh._tax_impact_usd(plan, ctx2) < 0
    # cost basis <= 0
    ctx3 = mh.RouteContext(holdings={"BTC": {"cost_basis": 0}})
    assert mh._tax_impact_usd(plan, ctx3) == 0.0


def test_drawdown_bonus():
    plan = mh.RoutePlan(source="USD", target="BTC")
    assert mh._drawdown_bonus(plan, mh.RouteContext(drawdown_pct=0.0)) == 0.0
    core = mh.RoutePlan(source="USD", target="BTC")
    assert mh._drawdown_bonus(core, mh.RouteContext(drawdown_pct=0.5)) > 0
    stable = mh.RoutePlan(source="USD", target="USDC")
    assert mh._drawdown_bonus(stable, mh.RouteContext(drawdown_pct=0.5)) > 0
    other = mh.RoutePlan(source="USD", target="XRP")
    assert mh._drawdown_bonus(other, mh.RouteContext(drawdown_pct=0.5)) > 0


def test_regime_bonus():
    plan = mh.RoutePlan(source="USD", target="BTC")
    assert mh._regime_bonus(plan, mh.RouteContext(regime="bear")) > 0
    assert mh._regime_bonus(plan, mh.RouteContext(regime="bull")) > 0
    assert mh._regime_bonus(plan, mh.RouteContext(regime="neutral")) == 0.0


def test_score_route_plan():
    plan = mh.RoutePlan(source="USD", target="BTC", effective_rate=1.0, steps=[
        mh.RouteStep("BTC-USD", "USD", "BTC", "BUY", 50000, 1 / 50000)
    ])
    ctx = mh.RouteContext(
        amount_in=1000.0,
        opportunities=[{"currency": "BTC", "side": "BUY", "priority": 0.5}],
        holdings={"BTC": {"value": 1000.0, "cost_basis": 800.0, "price": 1000.0,
                          "holding_days": 10}},
        drawdown_pct=0.3, regime="bear",
    )
    dec = mh.score_route_plan(plan, ctx)
    assert -1.0 <= dec.score <= 1.5
    assert dec.factor_breakdown["opportunity"] == 0.5
    assert dec.factor_breakdown["regime"] == 0.3


def test_find_best_route():
    def handler(url, timeout=10):
        if "BTC-USD" in url:
            return b'{"price": 50000}'
        if "ETH-USD" in url:
            return b'{"price": 3000}'
        if "ETH-BTC" in url:
            return b'{"price": 0.06}'
        if "SOL-USD" in url:
            return b'{"price": 150}'
        return b'{"price": 0}'
    mh._http.request = lambda method, url, timeout=10: Mock(data=handler(url))
    plan = mh.find_best_route("ETH", "USD", _products(), max_hops=3)
    assert plan is not None
    assert plan.target == "USD"
    # same source
    assert mh.find_best_route("BTC", "BTC", _products()).effective_rate == 1.0
    # not in graph
    assert mh.find_best_route("XYZ", "USD", _products()) is None


def test_find_best_decision():
    def handler(url, timeout=10):
        if "BTC-USD" in url:
            return b'{"price": 50000}'
        if "ETH-USD" in url:
            return b'{"price": 3000}'
        if "ETH-BTC" in url:
            return b'{"price": 0.06}'
        if "SOL-USD" in url:
            return b'{"price": 150}'
        return b'{"price": 0}'
    mh._http.request = lambda method, url, timeout=10: Mock(data=handler(url))
    dec = mh.find_best_decision("ETH", ["USD", "BTC"], _products(), max_hops=3)
    assert dec is not None
    # empty candidates -> defaults to USD
    dec2 = mh.find_best_decision("ETH", [], _products(), max_hops=3)
    assert dec2 is not None
    # no route at all
    assert mh.find_best_decision("XYZ", ["ABC"], _products()) is None


def test_describe():
    plan = mh.RoutePlan(source="USD", target="BTC", effective_rate=1.0)
    assert mh.describe_route(None) == "no route"
    assert "direct" in mh.describe_route(plan)
    step = mh.RouteStep("BTC-USD", "USD", "BTC", "BUY", 50000, 1 / 50000)
    plan2 = mh.RoutePlan(source="USD", target="BTC", effective_rate=1.0, steps=[step])
    assert "->" in mh.describe_route(plan2)
    assert mh.describe_decision(None) == "no decision"
    dec = mh.score_route_plan(plan2, mh.RouteContext())
    assert "score" in mh.describe_decision(dec)
