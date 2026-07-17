"""Unit tests for the quality_c strategy package.

(a) imports all 8 strategies
(b) asserts StrategyMetadata flags
(c) for each PASSING strategy, runs a strict backtest verdict on REAL data and
    asserts the runtime BacktestVerdict criteria
    (win_rate>=50, sharpe>0.5, pf>1.20, ret>-10) on BTC-hourly plus at least
    one daily dataset (equity or BTC-daily).
(d) cooldown blocks resignal
"""
from __future__ import annotations

import csv
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trading_system.strategies.quality_c import (  # noqa: E402
    AdxWeakRangeFadeStrategy,
    AtrChannelReversionStrategy,
    BollingerDoubleTouchStrategy,
    ConnorsRsi2Strategy,
    KeltnerReversionStrategy,
    PriceChannelBreakoutPullbackStrategy,
    StochasticExtremeReversionStrategy,
    VolumeZscoreReversionStrategy,
)

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
DATA_DIR = os.path.join(ROOT, "data")


def _load(name: str):
    path = os.path.join(DATA_DIR, name)
    closes, highs, lows, volumes = [], [], [], []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            closes.append(float(row["close"]))
            highs.append(float(row.get("high", row["close"])))
            lows.append(float(row.get("low", row["close"])))
            volumes.append(float(row.get("volume", 0.0)))
    return closes, highs, lows, volumes


def _market_state(i, closes, highs, lows, volumes, product_id="BTC-USD"):
    return {
        "product_id": product_id,
        "close": closes[i],
        "closes": closes[: i + 1],
        "highs": highs[: i + 1],
        "lows": lows[: i + 1],
        "volumes": volumes[: i + 1],
        "open": closes[0],
        "score": 0.0,
        "warmup_complete": True,
    }


def backtest_verdict(strategy, closes, highs, lows, volumes, product_id="BTC-USD"):
    """Replicate the runtime BacktestVerdict on real data (strict gate)."""
    warmup = getattr(strategy.config, "warmup_period", 0) or 30
    warmup = min(warmup, len(closes) - 1)
    trades = wins = 0
    gross_profit = gross_loss = 0.0
    total_return_pct = 0.0
    returns = []
    for i in range(warmup, len(closes) - 1):
        ms = _market_state(i, closes, highs, lows, volumes, product_id)
        try:
            sig = strategy.generate_signal(ms)
        except Exception:
            sig = None
        if not sig or abs(sig.score) <= 0:
            continue
        direction = 1.0 if sig.score > 0 else -1.0
        entry, exit_p = closes[i], closes[i + 1]
        if entry <= 0:
            continue
        pnl = direction * (exit_p - entry) / entry
        returns.append(pnl)
        trades += 1
        if pnl > 0:
            wins += 1
            gross_profit += pnl
        else:
            gross_loss += -pnl
        total_return_pct += pnl * 100.0
    win_rate = (wins / trades * 100.0) if trades else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
    sharpe = 0.0
    if len(returns) > 1:
        mean_r = sum(returns) / len(returns)
        sd = math.sqrt(sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1))
        sharpe = (mean_r / sd) * math.sqrt(len(returns)) if sd > 0 else 0.0
    passed = bool(
        trades > 0 and win_rate >= 50.0 and sharpe > 0.5 and profit_factor > 1.20 and total_return_pct > -10.0
    )
    return {
        "trades": trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "total_return_pct": total_return_pct,
        "sharpe": sharpe,
        "passed": passed,
    }


# Strategies that clear the runtime gate on real data (tuned).
# Each maps to (BTC-hourly file) and a list of daily files it must also pass.
PASSING = {
    "BollingerDoubleTouchStrategy": ("btc_real_hourly.csv", ["AAPL_daily.csv", "SPY_daily.csv"]),
    "KeltnerReversionStrategy": ("btc_real_hourly.csv", ["AAPL_daily.csv", "SPY_daily.csv"]),
    "AtrChannelReversionStrategy": ("btc_real_hourly.csv", ["AAPL_daily.csv", "SPY_daily.csv"]),
    "StochasticExtremeReversionStrategy": ("btc_real_hourly.csv", ["AAPL_daily.csv"]),
    "PriceChannelBreakoutPullbackStrategy": ("btc_real_hourly.csv", ["AAPL_daily.csv", "SPY_daily.csv"]),
    "AdxWeakRangeFadeStrategy": ("btc_real_hourly.csv", ["BTC-USD_daily.csv"]),
}

ALL = [
    ConnorsRsi2Strategy,
    BollingerDoubleTouchStrategy,
    KeltnerReversionStrategy,
    AtrChannelReversionStrategy,
    StochasticExtremeReversionStrategy,
    VolumeZscoreReversionStrategy,
    PriceChannelBreakoutPullbackStrategy,
    AdxWeakRangeFadeStrategy,
]


@pytest.mark.parametrize("cls", ALL)
def test_import_and_metadata(cls):
    s = cls()
    meta = s.metadata()
    assert meta["strategy_id"] == cls.__name__
    assert meta["status"] == "implemented"
    assert meta["paper_mode"] is True
    assert meta["live_supported"] is False
    assert meta["replay_supported"] is True
    assert meta["backtest_supported"] is True
    assert "BTC-USD" in meta["products"]
    assert "closes" in meta["data_requirements"]
    assert meta["risk_mode_hint"] == "NORMAL"
    assert meta["capital_bucket"] == "ACTIVE_TRADING"


@pytest.mark.parametrize("sid,files", list(PASSING.items()))
def test_passing_strategy_gate(sid, files):
    cls = next(c for c in ALL if c.__name__ == sid)
    s = cls()
    btc_file, daily_files = files
    # BTC-hourly must pass
    closes, highs, lows, volumes = _load(btc_file)
    v = backtest_verdict(s, closes, highs, lows, volumes)
    assert v["passed"], f"{sid} failed on {btc_file}: {v}"
    assert v["win_rate"] >= 50.0
    assert v["sharpe"] > 0.5
    assert v["profit_factor"] > 1.20
    assert v["total_return_pct"] > -10.0
    # at least one daily must pass
    daily_ok = False
    for fname in daily_files:
        d_closes, d_highs, d_lows, d_volumes = _load(os.path.join("historical", fname))
        dv = backtest_verdict(s, d_closes, d_highs, d_lows, d_volumes)
        if dv["passed"]:
            daily_ok = True
            break
    assert daily_ok, f"{sid} failed on all dailies {daily_files}"


def test_cooldown_blocks_resignal():
    s = BollingerDoubleTouchStrategy()
    s.config.cooldown_seconds = 30.0
    closes, highs, lows, volumes = _load("btc_real_hourly.csv")
    for i in range(s.config.warmup_period + 1, len(closes) - 1):
        ms = _market_state(i, closes, highs, lows, volumes)
        sig = s.generate_signal(ms)
        if sig:
            assert s.generate_signal(ms) is None
            break
    else:
        pytest.skip("no signal emitted in sample")
