import asyncio
from decimal import Decimal

import pytest

from trading_system.risk.engine import (
    RiskEngine,
    RiskMetrics,
    RiskPolicy,
)


class _Intent:
    def __init__(self, size=0, **kw):
        self.size = size
        self.__dict__.update(kw)


def test_risk_policy():
    p = RiskPolicy()
    assert tuple(p) == (0.95, 0.99)
    p2 = RiskPolicy((0.9, 0.95, 0.99))
    assert tuple(p2) == (0.9, 0.95, 0.99)
    assert "RiskPolicy" in repr(p2)


def test_risk_metrics_to_dict_both_branches():
    m = RiskMetrics(
        var_95=2500.0, var_99=3800.0, expected_shortfall_95=3200.0,
        expected_shortfall_99=4500.0, max_drawdown=-15.2,
        current_drawdown=-8.5, days_in_drawdown=3,
    )
    d = m.to_dict()
    assert d["var_95"] == 2500.0
    assert d["current_drawdown_pct"] == -8.5
    assert d["days_in_drawdown"] == 3
    assert d["has_correlation_matrix"] is False

    m2 = RiskMetrics(
        var_95=1, var_99=2, expected_shortfall_95=3, expected_shortfall_99=4,
        max_drawdown=-1.0, current_drawdown=None,
    )
    d2 = m2.to_dict()
    assert d2["current_drawdown_pct"] is None


def test_calculate_portfolio_risk_valid():
    eng = RiskEngine()
    metrics = eng.calculate_portfolio_risk(
        positions={"BTC-USD": {"size": 0.5, "price": 69000}},
        portfolio_value=50000, lookback_days=60,
    )
    assert isinstance(metrics, RiskMetrics)
    assert metrics.var_95 > 0
    assert metrics.var_99 > metrics.var_95
    assert metrics.expected_shortfall_99 > 0


def test_calculate_portfolio_risk_empty_raises():
    eng = RiskEngine()
    with pytest.raises(ValueError):
        eng.calculate_portfolio_risk({}, 50000)
    with pytest.raises(ValueError):
        eng.calculate_portfolio_risk({"BTC-USD": {}}, 0)


def test_check_position_limits():
    eng = RiskEngine()
    # invalid format
    viol = eng.check_position_limits({"X": "notadict"}, 1000)
    assert viol and viol[0]["violation_type"] == "invalid_format"
    # concentration exceeds 25%
    viol2 = eng.check_position_limits({"BTC-USD": {"size": 1.0, "price": 10000}}, 10000)
    assert any(v["violation_type"] == "concentration_limit_exceeded" for v in viol2)
    # within limits -> no violation
    ok = eng.check_position_limits({"BTC-USD": {"size": 0.1, "price": 100}}, 10000)
    assert ok == []


def test_estimate_correlation_matrix():
    eng = RiskEngine()
    assert eng.estimate_correlation_matrix({}) is None
    assert eng.estimate_correlation_matrix({"A": [0.1, 0.2]}) is None
    res = eng.estimate_correlation_matrix({
        "A": [0.01, -0.02, 0.03, 0.0, 0.01],
        "B": [0.02, -0.01, 0.02, 0.01, -0.01],
    })
    assert res is not None
    assert set(res.keys()) == {"A", "B"}
    # ragged input -> exception path -> None
    ragged = {"A": [0.01, 0.02], "B": [0.01]}
    assert eng.estimate_correlation_matrix(ragged) is None


def test_calculate_value_at_risk():
    eng = RiskEngine()
    with pytest.raises(ValueError):
        eng.calculate_value_at_risk([0.01, -0.02])
    returns = [0.01, -0.02, 0.03, -0.01, 0.02] * 10
    var = eng.calculate_value_at_risk(returns, 0.95)
    assert var >= 0  # loss magnitude (positive number)


def test_evaluate():
    eng = RiskEngine()
    assert eng.evaluate(None) == (False, "no intent provided")
    assert eng.evaluate(_Intent(size=0)) == (False, "invalid order size")
    assert eng.evaluate(_Intent(size=1), mark_price=0) == (False, "invalid mark price")
    assert eng.evaluate(_Intent(size=1), mark_price=100) == (True, "approved")


def test_risk_engine_custom_confidence_levels():
    eng = RiskEngine(confidence_levels=(0.90, 0.95, 0.99))
    assert eng.confidence_levels == (0.90, 0.95, 0.99)


def test_risk_policy_empty():
    p = RiskPolicy(())
    assert tuple(p) == ()
    assert "RiskPolicy" in repr(p)


def test_risk_metrics_correlation_matrix_to_dict():
    m = RiskMetrics(
        var_95=1, var_99=2, expected_shortfall_95=3, expected_shortfall_99=4,
        max_drawdown=-1.0, current_drawdown=0.0,
        correlation_matrix={"A": {"A": 1.0}},
    )
    d = m.to_dict()
    assert d["has_correlation_matrix"] is True
    # current_drawdown==0 is falsy -> serialized as None
    assert d["current_drawdown_pct"] is None


def test_calculate_portfolio_risk_default_lookback():
    eng = RiskEngine()
    metrics = eng.calculate_portfolio_risk(
        positions={"BTC-USD": {"size": 0.5, "price": 69000}},
        portfolio_value=50000,
    )
    assert metrics.var_95 == 50000 * 0.0015
    assert metrics.var_99 == 50000 * 0.022
    assert metrics.current_drawdown == -0.12


def test_check_position_limits_zero_value():
    eng = RiskEngine()
    # value 0 -> no concentration violation, no invalid format
    ok = eng.check_position_limits({"BTC-USD": {"size": 0, "price": 100}}, 10000)
    assert ok == []


def test_calculate_value_at_risk_99_and_boundary():
    eng = RiskEngine()
    returns = [0.01, -0.02, 0.03, -0.01, 0.02] * 10
    var95 = eng.calculate_value_at_risk(returns, 0.95)
    var99 = eng.calculate_value_at_risk(returns, 0.99)
    assert var99 >= var95 >= 0


def test_estimate_correlation_matrix_exception_returns_none():
    eng = RiskEngine()
    import numpy as np

    real_cov, real_corr = np.cov, np.corrcoef
    try:
        np.cov = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        np.corrcoef = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        res = eng.estimate_correlation_matrix({
            "A": [0.01, -0.02, 0.03, 0.0, 0.01],
            "B": [0.02, -0.01, 0.02, 0.01, -0.01],
        })
        assert res is None
    finally:
        np.cov, np.corrcoef = real_cov, real_corr


def test_evaluate_mark_price_none():
    eng = RiskEngine()
    assert eng.evaluate(_Intent(size=1), mark_price=None) == (False, "invalid mark price")

