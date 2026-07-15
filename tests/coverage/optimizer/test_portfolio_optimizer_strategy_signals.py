"""Coverage test for PortfolioOptimizer._detect_strategy_signals.

This is a large integration method; its external dependencies (candle feed,
batch signal generation, backtest cache, confidence matrix, pulse tracking,
Kelly sizing, cluster limits) are mocked so the full decision path is exercised
and its many branches are covered.
"""
from __future__ import annotations

import time
from unittest import mock

import pytest

import portfolio_optimizer as P
from confidence_matrix import AggregatedSignal
import strategy_engine as SE
from tests.coverage.optimizer.conftest import holding, make_state


@pytest.fixture
def o2(opt):
    o = opt
    o.last_execution = {}
    o.position_ages = {}
    o.confidence_engine = None
    o._signal_pulses = {}
    o._bt_cache = {}
    # representative passing backtest verdict
    o._bt_cache["ema_cross/XRP"] = SE.BacktestVerdict(
        strategy="ema_cross", currency="XRP", total_trades=20,
        winning_trades=14, losing_trades=6, win_rate=0.7,
        total_return_pct=10.0, sharpe_ratio=1.0, profit_factor=1.5,
        max_drawdown_pct=5.0, regime="neutral", passed=True, reason="ok",
    )
    # valid pulse so the quality gate passes
    o._signal_pulses[o._pulse_key("XRP-USD", "ema_cross", "BUY")] = {
        "strategy": "ema_cross", "direction": "BUY", "product_id": "XRP-USD",
        "pulse_count": 5, "first_ts": time.time(), "last_ts": time.time(),
        "avg_confidence": 0.8, "min_price": 0.5, "max_price": 0.5, "flip_count": 0,
    }
    return o


def _candles(n=40, start=0.5):
    return [
        {"open": start, "high": start * 1.02, "low": start * 0.98,
         "close": start, "volume": 1000.0}
        for start in [start] * n
    ]


def test_strategy_signals_emits(o2):
    o = o2
    o._feed_mgr = mock.MagicMock()
    o._feed_mgr.get_candles_batch.return_value = {"XRP-USD": _candles()}

    sig = SE.Signal(strategy="ema_cross", action="BUY", confidence=0.5, reason="batch:ema_cross")

    with mock.patch.object(P, "_batch_signals_fast", return_value={"XRP-USD": {"ema_cross": "BUY"}}), \
         mock.patch("portfolio_optimizer._detect_market_regime", return_value="neutral"), \
         mock.patch("confidence_matrix.ConfidenceMatrix.aggregate", return_value=[
             AggregatedSignal(
                 asset="XRP-USD", direction="BUY", confidence=0.8, raw_confidence=0.8,
                 agreeing_groups=1, total_groups=1, strategy_count=1,
                 strategies=["ema_cross"], best_reason="ema_cross bullish", asset_class="growth",
             )
         ]), \
         mock.patch.object(o, "_check_cluster_limit", return_value=True), \
         mock.patch.object(o, "_kelly_size", return_value=100.0), \
         mock.patch.object(o, "_risk_reward_size", return_value=100.0):
        o.state = make_state({
            "XRP": holding("XRP", 5000.0, "growth", price=0.5, product_id="XRP-USD",
                           volume_24h=2_000_000.0, change_24h=1.0, liquidity_score=0.8),
        }, total_value=100000.0)
        ops = o._detect_strategy_signals()

    assert len(ops) == 1
    assert ops[0].opp_type == P.OpportunityType.STRATEGY_SIGNAL
    assert ops[0].side == "BUY"
    assert ops[0].currency == "XRP"


def test_strategy_signals_cooldown(o2):
    o2.last_execution["strategy"] = time.time()
    o2.state = make_state({"XRP": holding("XRP", 5000.0, "growth", price=0.5, product_id="XRP-USD")})
    assert o2._detect_strategy_signals() == []


def test_strategy_signals_no_state(o2):
    o2.state = None
    assert o2._detect_strategy_signals() == []


def test_strategy_signals_no_candidates(o2):
    o2.state = make_state({"USDC": holding("USDC", 5000.0, "safe")}, total_value=100000.0)
    assert o2._detect_strategy_signals() == []


def test_strategy_signals_no_candles(o2):
    o2._feed_mgr = mock.MagicMock()
    o2._feed_mgr.get_candles_batch.return_value = {"XRP-USD": []}
    o2.state = make_state({
        "XRP": holding("XRP", 5000.0, "growth", price=0.5, product_id="XRP-USD"),
    }, total_value=100000.0)
    assert o2._detect_strategy_signals() == []


def test_strategy_signals_batch_fails(o2):
    # batch signal gen raises -> falls back to _run_strategies (empty here) -> no ops
    o2._feed_mgr = mock.MagicMock()
    o2._feed_mgr.get_candles_batch.return_value = {"XRP-USD": _candles()}
    with mock.patch.object(P, "_batch_signals_fast", side_effect=RuntimeError("boom")), \
         mock.patch("portfolio_optimizer._detect_market_regime", return_value="neutral"):
        o2.state = make_state({
            "XRP": holding("XRP", 5000.0, "growth", price=0.5, product_id="XRP-USD"),
        }, total_value=100000.0)
        assert o2._detect_strategy_signals() == []
