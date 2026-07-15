"""Tests for trading_system.core.performance_model."""

import pytest

from trading_system.core import performance_model as pm
from trading_system.core.performance_model import (
    LatencyProfile,
    expected_detection_delay_ms,
    expected_round_trip_ms,
    expected_fill_delay_ms,
    expected_exit_delay_ms,
    strategy_horizon_minutes,
    latency_priority_multiplier,
    latency_tuned_priority,
    summarize_profile,
)


def test_latency_profile_defaults():
    p = LatencyProfile()
    assert p.network_rtt_ms == 75.0
    assert p.spread_bps == 10.0


def test_expected_detection_delay():
    assert expected_detection_delay_ms(1.0) == 500.0
    assert expected_detection_delay_ms(0.0) == 0.0
    assert expected_detection_delay_ms(-1.0) == 0.0


def test_expected_round_trip_ms():
    p = LatencyProfile()
    # products clamped to >=1, workers clamped to >=1
    assert expected_round_trip_ms(p, workers=0, products=0) > 0
    val = expected_round_trip_ms(p, workers=10, products=50)
    assert val > p.network_rtt_ms


def test_expected_fill_delay_ms():
    p = LatencyProfile()
    # crossing spread True path
    d1 = expected_fill_delay_ms(p, liquidity_score=1.0, crossing_spread=True)
    # liquidity clamped low
    d2 = expected_fill_delay_ms(p, liquidity_score=0.0, crossing_spread=True)
    assert d2 >= d1
    # crossing spread False path (smaller multiplier)
    d3 = expected_fill_delay_ms(p, liquidity_score=1.0, crossing_spread=False)
    assert d3 < d1


def test_expected_exit_delay_ms():
    p = LatencyProfile()
    assert expected_exit_delay_ms(p) > 0


@pytest.mark.parametrize(
    "style,name,expected",
    [
        ("arbitrage", "arb", 5.0),
        ("arbitrage", "cross_exchange_arb", 5.0),
        ("event", "", 30.0),
        ("mean_reversion", "", 120.0),
        ("cycle", "", 240.0),
        ("tax_loss", "", 1440.0),
        ("prediction_market", "", 240.0),
        ("momentum", "", 60.0),
        ("anything", "ema_cross", 60.0),
    ],
)
def test_strategy_horizon_minutes(style, name, expected):
    assert strategy_horizon_minutes(name, style) == expected


def test_latency_priority_multiplier():
    # within bounds
    m = latency_priority_multiplier(1.0, expected_delay_ms=100.0, horizon_minutes=60.0)
    assert 0.5 <= m <= 1.15
    # large delay + low horizon -> floored at 0.5
    m2 = latency_priority_multiplier(1.0, expected_delay_ms=1e9, horizon_minutes=1.0)
    assert m2 == 0.5
    # trivial delay -> ceiling 1.15
    m3 = latency_priority_multiplier(2.0, expected_delay_ms=0.0, horizon_minutes=100000.0)
    assert m3 == 1.15


def test_latency_tuned_priority():
    m = latency_tuned_priority(
        1.0, strategy_name="ema_cross", trade_style="momentum", expected_delay_ms=100.0
    )
    assert 0.5 <= m <= 1.15


def test_summarize_profile():
    p = LatencyProfile()
    s = summarize_profile(p, products=50, workers=12)
    assert s["round_trip_ms"] > 0
    assert s["fill_delay_ms"] > 0
    assert s["products"] == 50
    assert s["workers"] == 12


def test_main_invocation(capsys):
    rc = pm.main(
        [
            "--network-rtt-ms",
            "50",
            "--products",
            "10",
            "--workers",
            "4",
            "--strategy-name",
            "arb",
            "--trade-style",
            "arbitrage",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "round_trip_ms" in out
