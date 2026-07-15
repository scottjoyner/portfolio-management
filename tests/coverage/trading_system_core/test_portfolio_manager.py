"""Tests for trading_system.core.portfolio_manager (calculate_rebalance)."""

import math
import pytest

from trading_system.core.portfolio_manager import calculate_rebalance
from trading_system.core.models.domain import OrderIntent, CapitalBucketType, RiskMode


def _intent_map(results):
    return {r.product_id: r for r in results}


def test_basic_buy_and_sell():
    prices = {"BTC-USD": 100.0, "ETH-USD": 50.0}
    weights = {"BTC-USD": 0.5, "ETH-USD": 0.5}
    base = {"BTC-USD": 0.0, "ETH-USD": 1.0}
    results = calculate_rebalance(prices, 1000.0, weights, base)
    m = _intent_map(results)
    assert set(m) == {"BTC-USD", "ETH-USD"}
    # BTC needs to buy 5 base (500/100)
    assert m["BTC-USD"].side == "buy"
    assert math.isclose(m["BTC-USD"].size, 5.0)
    # ETH currently 1.0 base = 50 usd, target 500 -> need to buy 9 more
    assert m["ETH-USD"].side == "buy"
    assert math.isclose(m["ETH-USD"].size, 9.0)
    for r in results:
        assert isinstance(r, OrderIntent)
        assert r.bucket == CapitalBucketType.ACTIVE_TRADING
        assert r.risk_mode == RiskMode.NORMAL


def test_missing_price_skipped():
    prices = {"BTC-USD": 100.0}
    results = calculate_rebalance(prices, 1000.0, {"BTC-USD": 0.5, "ETH-USD": 0.5}, {})
    assert "ETH-USD" not in _intent_map(results)
    assert "BTC-USD" in _intent_map(results)


def test_nonpositive_price_skipped():
    prices = {"BTC-USD": 0.0, "ETH-USD": -5.0}
    results = calculate_rebalance(prices, 1000.0, {"BTC-USD": 0.5, "ETH-USD": 0.5}, {})
    assert results == []


def test_negative_weight_clamped_to_zero():
    prices = {"BTC-USD": 100.0}
    results = calculate_rebalance(prices, 1000.0, {"BTC-USD": -0.5}, {"BTC-USD": 10.0})
    # target_usd = 0, target_base = 0, diff = -10 -> sell, diff_usd = -1000 (>= min_notional)
    assert len(results) == 1
    assert results[0].side == "sell"


def test_below_min_notional_skipped():
    prices = {"BTC-USD": 100.0}
    # diff_usd well below default 50.0
    results = calculate_rebalance(prices, 10.0, {"BTC-USD": 0.5}, {"BTC-USD": 0.04})
    # target_base=0.05, cur=0.04 -> diff 0.01 -> diff_usd=1.0 -> below 50 -> skip
    assert results == []


def test_custom_min_notional():
    prices = {"BTC-USD": 100.0}
    results = calculate_rebalance(prices, 10.0, {"BTC-USD": 0.5}, {"BTC-USD": 0.04}, min_notional=0.5)
    assert len(results) == 1
