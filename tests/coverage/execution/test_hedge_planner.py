from decimal import Decimal

from onchain.dex.clmm.schemas import HedgeMode

from trading_system.execution.hybrid.hedge_planner import (
    build_coinbase_hedge_plan,
    estimate_coinbase_hedge_cost,
)


def test_build_plan_delta_neutral():
    plan = build_coinbase_hedge_plan(Decimal("-300"), HedgeMode.DELTA_NEUTRAL, Decimal("1"))
    assert plan.base_asset == "ETH"
    assert plan.quote_asset == "USD"
    assert plan.target_delta_usd == Decimal("-300")
    assert plan.hedge_mode == HedgeMode.DELTA_NEUTRAL
    assert plan.max_slippage_bps == Decimal("15")
    assert plan.cooldown_seconds == 30


def test_build_plan_none():
    plan = build_coinbase_hedge_plan(Decimal("100"), HedgeMode.NONE, Decimal("0"))
    assert plan.hedge_notional_usd == Decimal("0")


def test_build_plan_partial():
    plan = build_coinbase_hedge_plan(Decimal("200"), HedgeMode.PARTIAL_HEDGE, Decimal("0"))
    assert plan.hedge_notional_usd == Decimal("-200")


def test_build_plan_directional():
    plan = build_coinbase_hedge_plan(Decimal("200"), HedgeMode.DIRECTIONAL_BIAS, Decimal("0"))
    assert plan.hedge_notional_usd == Decimal("-200")


def test_build_plan_emergency():
    plan = build_coinbase_hedge_plan(Decimal("200"), HedgeMode.EMERGENCY_FLATTEN, Decimal("0"))
    assert plan.hedge_notional_usd == Decimal("-200")


def test_build_plan_band():
    plan = build_coinbase_hedge_plan(Decimal("200"), HedgeMode.BAND_HEDGE, Decimal("0"))
    assert plan.hedge_notional_usd >= Decimal("0")


def test_urgency_score_capped():
    plan = build_coinbase_hedge_plan(Decimal("1000000"), HedgeMode.DELTA_NEUTRAL, Decimal("0"))
    assert plan.urgency_score == Decimal("1")


def test_estimate_cost():
    cost = estimate_coinbase_hedge_cost(Decimal("10000"))
    # 10000 * (2+3)/10000 = 5
    assert cost == Decimal("5")
    cost2 = estimate_coinbase_hedge_cost(Decimal("10000"), Decimal("5"), Decimal("10"))
    assert cost2 == Decimal("15")
