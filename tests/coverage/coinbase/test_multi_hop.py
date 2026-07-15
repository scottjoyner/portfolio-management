import json
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

import coinbase.src.multi_hop as mh
from coinbase.src.multi_hop import (
    build_route_graph,
    find_best_route,
    find_best_decision,
    score_route_plan,
    describe_route,
    describe_decision,
    RoutePlan,
    RouteStep,
    RouteContext,
    RouteDecision,
    _is_core,
    _normalized_opportunity_bonus,
    _tax_impact_usd,
    _drawdown_bonus,
    _regime_bonus,
)


@contextmanager
def _noop_slot():
    yield


def _fake_response(payload):
    r = MagicMock()
    r.data = json.dumps(payload).encode()
    return r


_PRICES = {"BTC-USD": 50000.0, "ETH-USD": 3000.0}


@pytest.fixture
def fake_http(monkeypatch):
    http = MagicMock()

    def _side(method, url, timeout):
        for pid, px in _PRICES.items():
            if pid in url:
                return _fake_response({"price": str(px)})
        return _fake_response({"price": "1"})

    http.request.side_effect = _side
    monkeypatch.setattr(mh, "_http", http)
    return http


def _products():
    return [
        {"id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD"},
        {"id": "ETH-USD", "base_currency": "ETH", "quote_currency": "USD"},
    ]


def test_is_core():
    assert _is_core("BTC", ("BTC", "ETH")) is True
    assert _is_core("DOGE", ("BTC", "ETH")) is False


def test_build_route_graph(fake_http):
    g = build_route_graph(_products())
    assert "BTC" in g
    assert "USD" in g


def test_find_best_route_same():
    plan = find_best_route("BTC", "BTC", _products())
    assert plan.effective_rate == 1.0


def test_find_best_route_direct(fake_http):
    plan = find_best_route("BTC", "USD", _products())
    assert plan is not None
    assert plan.target == "USD"


def test_find_best_route_two_hop(fake_http):
    plan = find_best_route("BTC", "ETH", _products())
    assert plan is not None
    assert plan.target == "ETH"
    assert plan.hop_count >= 1


def test_find_best_route_no_source(fake_http):
    assert find_best_route("XYZ", "USD", _products()) is None


def test_find_best_decision(fake_http):
    ctx = RouteContext(candidate_targets=["USD", "ETH"])
    dec = find_best_decision("BTC", ["USD", "ETH"], _products(), context=ctx)
    assert isinstance(dec, RouteDecision)


def test_describe_route():
    plan = RoutePlan(source="BTC", target="BTC", effective_rate=1.0)
    assert "direct" in describe_route(plan)
    plan2 = RoutePlan(source="BTC", target="USD", effective_rate=2.0,
                      steps=[RouteStep("BTC-USD", "BTC", "USD", "SELL", 50000, 0.5)])
    assert "BTC" in describe_route(plan2)
    assert describe_route(None) == "no route"


def test_describe_decision():
    plan = RoutePlan(source="BTC", target="USD", effective_rate=2.0,
                     steps=[RouteStep("BTC-USD", "BTC", "USD", "SELL", 50000, 0.5)])
    dec = score_route_plan(plan, RouteContext())
    assert "score=" in describe_decision(dec)
    assert describe_decision(None) == "no decision"


def test_score_route_plan_basic():
    plan = RoutePlan(source="BTC", target="USD", effective_rate=2.0)
    dec = score_route_plan(plan, RouteContext())
    assert -1.0 <= dec.score <= 1.5


def test_opportunity_bonus_branches():
    plan = RoutePlan(source="BTC", target="USD", effective_rate=1.0)
    ctx = RouteContext(opportunities=[
        {"currency": "USD", "side": "BUY", "priority": 1.0},
        {"currency": "BTC", "side": "SELL", "priority": 1.0},
        {"currency": "SOL", "side": "BUY", "priority": 1.0},
    ])
    bonus = _normalized_opportunity_bonus(plan, ctx)
    assert 0.0 <= bonus <= 1.5


def test_opportunity_bonus_empty():
    plan = RoutePlan(source="BTC", target="USD", effective_rate=1.0)
    assert _normalized_opportunity_bonus(plan, RouteContext()) == 0.0


def test_tax_impact_branches():
    plan = RoutePlan(source="BTC", target="USD", effective_rate=1.0)
    ctx = RouteContext(amount_in=1000.0, holdings={
        "BTC": {"value": 1000, "cost_basis": 500, "price": 800, "holding_days": 400}
    })
    tax = _tax_impact_usd(plan, ctx)
    assert tax < 0  # gain (positive pnl) -> negative tax cost
    ctx2 = RouteContext(amount_in=1000.0, holdings={
        "BTC": {"value": 1000, "cost_basis": 500, "price": 800, "holding_days": 10}
    })
    tax2 = _tax_impact_usd(plan, ctx2)
    assert isinstance(tax2, float)
    # zero amount
    assert _tax_impact_usd(plan, RouteContext()) == 0.0


def test_tax_impact_loss():
    plan = RoutePlan(source="BTC", target="USD", effective_rate=1.0)
    ctx = RouteContext(amount_in=1000.0, holdings={
        "BTC": {"value": 1000, "cost_basis": 1500, "price": 1000, "holding_days": 400}
    })
    assert _tax_impact_usd(plan, ctx) > 0  # loss -> positive benefit


def test_drawdown_bonus_branches():
    plan = RoutePlan(source="BTC", target="ETH", effective_rate=1.0)
    ctx = RouteContext(drawdown_pct=0.5)
    assert _drawdown_bonus(plan, ctx) > 0
    plan2 = RoutePlan(source="BTC", target="USDC", effective_rate=1.0)
    assert _drawdown_bonus(plan2, ctx) > 0
    plan3 = RoutePlan(source="BTC", target="SOL", effective_rate=1.0)
    assert _drawdown_bonus(plan3, ctx) >= 0
    assert _drawdown_bonus(plan, RouteContext(drawdown_pct=0.0)) == 0.0


def test_regime_bonus_branches():
    plan = RoutePlan(source="BTC", target="ETH", effective_rate=1.0)
    assert _regime_bonus(plan, RouteContext(regime="bear")) > 0
    assert _regime_bonus(plan, RouteContext(regime="bull")) > 0
    assert _regime_bonus(plan, RouteContext(regime="neutral")) == 0.0


def test_score_route_plan_full_context():
    plan = RoutePlan(source="BTC", target="ETH", effective_rate=16.0,
                     steps=[RouteStep("BTC-USD", "BTC", "USD", "SELL", 50000, 0.5),
                            RouteStep("ETH-USD", "USD", "ETH", "BUY", 3000, 0.5)])
    ctx = RouteContext(
        amount_in=1000.0,
        candidate_targets=["ETH"],
        opportunities=[{"currency": "ETH", "side": "BUY", "priority": 0.8}],
        holdings={"BTC": {"value": 1000, "cost_basis": 500, "price": 800, "holding_days": 400}},
        current_prices={"BTC": 800},
        drawdown_pct=0.3,
        regime="risk_off",
        prefer_core_assets=True,
    )
    dec = score_route_plan(plan, ctx)
    assert dec.factor_breakdown["efficiency"] is not None
    assert dec.expected_tax_impact_usd != 0.0
