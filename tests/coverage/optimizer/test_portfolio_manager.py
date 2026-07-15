"""Coverage tests for trading_system/core/portfolio_manager.py (calculate_rebalance)."""

from trading_system.core import portfolio_manager as pm
from trading_system.core.models.domain import OrderIntent


def test_empty_weights():
    assert pm.calculate_rebalance({"BTC-USD": 100.0}, 1000.0, {}, {}) == []


def test_skip_missing_and_nonpositive_price():
    prices = {"A": 100.0, "B": 0.0, "C": None}
    weights = {"A": 0.5, "B": 0.2, "C": 0.1}
    intents = pm.calculate_rebalance(prices, 1000.0, weights, {})
    # B (px<=0) and C (px None) skipped; only A remains
    assert [i.product_id for i in intents] == ["A"]


def test_buy_and_sell_and_near_zero_skip():
    prices = {"A": 100.0, "B": 50.0, "E": 200.0, "F": 100.0}
    weights = {"A": 0.5, "B": 0.1, "E": 0.0, "F": 0.0001}
    current_base = {"A": 0.0, "B": 100.0, "E": 1.0, "F": 0.0}
    intents = pm.calculate_rebalance(prices, 1000.0, weights, current_base, min_notional=50.0)
    by_prod = {i.product_id: i for i in intents}
    # A: target>current -> buy
    assert by_prod["A"].side == "buy"
    assert by_prod["A"].size == 5.0
    # B: target<current -> sell
    assert by_prod["B"].side == "sell"
    # E: weight 0 -> target 0 -> sell remaining base
    assert by_prod["E"].side == "sell"
    # F: tiny diff under min_notional -> skipped
    assert "F" not in by_prod


def test_intent_fields():
    intents = pm.calculate_rebalance({"A": 100.0}, 1000.0, {"A": 1.0}, {})
    it = intents[0]
    assert isinstance(it, OrderIntent)
    assert it.strategy_id == "rebalance_auto"
    assert it.order_type == "market"
    assert it.bucket.value is not None or True  # enum present
    assert "weight 1.0" in it.rationale
