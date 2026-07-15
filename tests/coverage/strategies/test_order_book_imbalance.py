"""Coverage tests for the Order Book Imbalance market-making strategy."""
from __future__ import annotations

from trading_system.strategies.market_making.order_book_imbalance import (
    OrderBookImbalanceStrategy,
    OrderBookImbalanceConfig,
)


def _bars(n, base=100.0):
    out = []
    for i in range(n):
        c = base + i
        out.append({"timestamp": i, "open": c, "high": c * 1.01,
                    "low": c * 0.99, "close": c, "volume": 1000 + i})
    return out


def test_config_defaults():
    cfg = OrderBookImbalanceConfig()
    assert cfg.inventory_limit_pct == 0.05
    assert cfg.rebalancing_threshold_pct == 3.0
    s = OrderBookImbalanceStrategy(cfg)
    assert s.config is cfg


def test_init_empty_raises():
    s = OrderBookImbalanceStrategy()
    try:
        s.init([])
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_init_valid():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    assert s.current_position_value == 0.0
    assert s.inventory_limit_value == s.total_capital * s.config.inventory_limit_pct


def test_on_bar_missing_close():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    assert s.on_bar({"timestamp": 1, "volume": 10}) is None


def test_on_bar_nan_close():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    assert s.on_bar({"close": float("nan")}) is None


def test_on_bar_zero_close():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    assert s.on_bar({"close": 0.0}) is None


def test_on_bar_valid_close():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    # no signal emitted in prototype, just must not raise
    assert s.on_bar({"close": 100.0}) is None
    assert s.on_bar({"price": 100.0}) is None


def test_handle_signal_none():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    assert s.handle_signal(None) is None


def test_handle_signal_rebalance_long():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    s.current_position_value = 20000.0
    res = s.handle_signal({"action": "REBALANCE_LONG"})
    assert res == {"position_adjusted": True, "new_position_value": 15000.0}
    assert s.current_position_value == 15000.0


def test_handle_signal_rebalance_short():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    s.current_position_value = 20000.0
    res = s.handle_signal({"action": "REBALANCE_SHORT"})
    # short branch returns nothing explicit
    assert res is None
    assert s.current_position_value == 25000.0


def test_handle_signal_other_action():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    s.current_position_value = 20000.0
    res = s.handle_signal({"action": "HOLD"})
    assert res is None
    assert s.current_position_value == 20000.0


def test_performance_metrics_default():
    s = OrderBookImbalanceStrategy()
    s.init(_bars(60))
    s.current_position_value = 5000.0
    m = s.get_performance_metrics()
    assert m["total_rebalances"] == 0
    assert m["current_position_pct"] == 5.0
    assert m["inventory_utilization"] > 0


def test_performance_metrics_zero_limit():
    s = OrderBookImbalanceStrategy(OrderBookImbalanceConfig(inventory_limit_pct=0.0))
    s.init(_bars(60))
    s.current_position_value = 5000.0
    m = s.get_performance_metrics()
    # inventory_limit_value == 0 -> else branch -> 0.0
    assert m["inventory_utilization"] == 0.0
