"""Tests for trading_system.signal_confidence.ConfidenceEngine."""

import pytest

from trading_system.signal_confidence import ConfidenceEngine, ConfidenceModifierResult


class FakeSignal:
    def __init__(self, symbol, strategy, action, strength=0.5):
        self.symbol = symbol
        self.strategy = strategy
        self.action = action
        self.strength = strength


def mk_engine(**kw):
    return ConfidenceEngine(**kw)


def apply(engine, signal, market_data=None, **kw):
    return engine.apply_modifiers(signal, market_data or {}, **kw)


def test_base_confidence_default():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.9))
    assert isinstance(r, ConfidenceModifierResult)
    assert r.original_confidence == 0.9


def test_liquidity_tier_high():
    e = mk_engine(liquidity_tiers={"BTC-USD": 4})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0))
    assert "liquidity_tier" in r.modifiers_applied
    # tier 4 -> mult 0.85
    assert r.modified_confidence == pytest.approx(0.85)


def test_liquidity_tier_very_high():
    e = mk_engine(liquidity_tiers={"BTC-USD": 6})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0))
    # tier 6 -> 1.0 - 3*0.15 = 0.55
    assert r.modified_confidence == pytest.approx(0.55)


def test_liquidity_tier_default_no_penalty():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5))
    assert "liquidity_tier" not in r.modifiers_applied


def test_spread_adjustment_within_bounds():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0), {"spread": 0.001})
    # 1 - 0.1 = 0.9
    assert r.modified_confidence == pytest.approx(0.9)
    assert "spread_adj" in r.modifiers_applied


def test_spread_adjustment_floor():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0), {"spread": 0.01})
    # 1 - 1.0 = 0 -> max(0.7, 0) = 0.7
    assert r.modified_confidence == pytest.approx(0.7)


def test_spread_zero_no_adjustment():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5), {"spread": 0.0})
    assert "spread_adj" not in r.modifiers_applied


def test_consecutive_boost():
    e = mk_engine()
    s = FakeSignal("BTC-USD", "ema", "BUY", 0.9)
    apply(e, s)  # first sets state
    r = apply(e, s)  # same action -> boost 1.05
    assert "consecutive" in r.modifiers_applied
    assert r.modified_confidence == pytest.approx(0.945)


def test_consecutive_different_action():
    e = mk_engine()
    apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0))
    r = apply(e, FakeSignal("BTC-USD", "ema", "SELL", 1.0))
    assert "consecutive" not in r.modifiers_applied


def test_win_rate_applied():
    e = mk_engine(win_rates={("ema", "BTC-USD"): 0.7})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0))
    assert r.modified_confidence == pytest.approx(0.7)
    assert "win_rate" in r.modifiers_applied


def test_win_rate_floor():
    e = mk_engine(win_rates={("ema", "BTC-USD"): 0.1})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0))
    # max(0.3, 0.1) = 0.3
    assert r.modified_confidence == pytest.approx(0.3)


def test_win_rate_absent():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5))
    assert "win_rate" not in r.modifiers_applied


def test_win_rate_nonpositive_skipped():
    e = mk_engine(win_rates={("ema", "BTC-USD"): 0.0})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5))
    assert "win_rate" not in r.modifiers_applied


def test_sentiment_aligned_buy():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.9), sentiment_score=0.5)
    assert "sentiment" in r.modifiers_applied
    # 0.9 * (1 + min(0.5*0.2, 0.2)) = 0.9 * 1.1
    assert r.modified_confidence == pytest.approx(0.99)


def test_sentiment_aligned_sell():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "SELL", 0.9), sentiment_score=-0.5)
    assert "sentiment" in r.modifiers_applied
    assert r.modified_confidence == pytest.approx(0.99)


def test_sentiment_misaligned():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0), sentiment_score=-0.5)
    assert "sentiment_p" in r.modifiers_applied
    assert r.modified_confidence == pytest.approx(0.85)


def test_sentiment_zero():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5), sentiment_score=0.0)
    assert "sentiment" not in r.modifiers_applied


def test_global_consensus_boost():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.8), global_consensus=0.7)
    assert "consensus" in r.modifiers_applied
    # 0.8 * 1.15 = 0.92
    assert r.modified_confidence == pytest.approx(0.92)


def test_global_consensus_penalty():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0), global_consensus=0.3)
    assert "consensus_p" in r.modifiers_applied
    assert r.modified_confidence == pytest.approx(0.85)


def test_global_consensus_within_band_no_modifier():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5), global_consensus=0.5)
    assert "consensus" not in r.modifiers_applied
    assert "consensus_p" not in r.modifiers_applied


def test_global_consensus_negative_no_modifier():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5), global_consensus=-0.5)
    assert "consensus" not in r.modifiers_applied


def test_regime_cap_applied():
    e = mk_engine(regime_caps={"volatile": 0.4})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0), regime="volatile")
    assert r.modified_confidence <= 0.4


def test_regime_cap_default_one():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0), regime="unknown_regime")
    assert r.modified_confidence <= 1.0


def test_correlation_buy_leaders_dumping():
    e = mk_engine()
    md = {"BTC-USD": {"change_pct": -5.0}}
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0),
              md, market_leaders=["BTC-USD"])
    assert "correlation" in r.modifiers_applied
    assert r.modified_confidence == pytest.approx(0.85)


def test_correlation_sell_leaders_pumping():
    e = mk_engine()
    md = {"BTC-USD": {"change_pct": 5.0}}
    r = apply(e, FakeSignal("BTC-USD", "ema", "SELL", 1.0),
              md, market_leaders=["BTC-USD"])
    assert "correlation" in r.modifiers_applied
    assert r.modified_confidence == pytest.approx(0.85)


def test_correlation_normalized_when_large():
    e = mk_engine()
    md = {"BTC-USD": {"change_pct": -500.0}}
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0),
              md, market_leaders=["BTC-USD"])
    # -500 -> normalized to -5.0 -> BUY + dumping leaders -> penalty
    assert "correlation" in r.modifiers_applied


def test_correlation_no_penalty_when_aligned():
    e = mk_engine()
    md = {"BTC-USD": {"change_pct": 5.0}}
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0),
              md, market_leaders=["BTC-USD"])
    assert "correlation" not in r.modifiers_applied


def test_correlation_no_leaders():
    e = mk_engine()
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 0.5))
    assert "correlation" not in r.modifiers_applied


def test_correlation_small_change_no_normalization():
    e = mk_engine()
    # change_pct within [-1, 1] -> abs(leader_change) > 1.0 is False (no /100)
    md = {"BTC-USD": {"change_pct": -0.5}}
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0),
              md, market_leaders=["BTC-USD"])
    assert "correlation" in r.modifiers_applied


def test_final_clamp_upper():
    e = mk_engine(liquidity_tiers={"BTC-USD": 6})
    r = apply(e, FakeSignal("BTC-USD", "ema", "BUY", 1.0),
              {"spread": 0.0}, global_consensus=0.7, sentiment_score=0.5)
    # many boosts but capped at 1.0 (cap default 1.0)
    assert r.modified_confidence <= 1.0
