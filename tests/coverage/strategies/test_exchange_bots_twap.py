"""Tests for the TWAP execution algorithm (:mod:`trading_system.strategies.exchange_bots.twap`)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from trading_system.strategies.exchange_bots.twap import TwapConfig, TwapStrategy


def _ms(product="BTC-USD", price=100.0, timestamp=0.0, warmup=True):
    return {
        "product_id": product,
        "price": price,
        "timestamp": timestamp,
        "warmup_complete": warmup,
    }


def test_disabled():
    strat = TwapStrategy(bot_config=TwapConfig(enabled=False))
    assert strat.generate_signal(_ms()) is None


def test_warmup_not_complete():
    strat = TwapStrategy(bot_config=TwapConfig())
    assert strat.generate_signal(_ms(warmup=False)) is None


def test_invalid_slices():
    strat = TwapStrategy(bot_config=TwapConfig(slices=0))
    assert strat.generate_signal(_ms()) is None


def test_invalid_total():
    strat = TwapStrategy(bot_config=TwapConfig(total_usd=0.0))
    assert strat.generate_signal(_ms()) is None


def test_invalid_duration():
    strat = TwapStrategy(bot_config=TwapConfig(duration_seconds=0.0))
    assert strat.generate_signal(_ms()) is None


def test_invalid_side():
    strat = TwapStrategy(bot_config=TwapConfig(side="HOLD"))
    assert strat.generate_signal(_ms()) is None


def test_price_guard():
    strat = TwapStrategy(bot_config=TwapConfig())
    assert strat.generate_signal(_ms(price=0.0)) is None


def test_first_slice_emitted():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=10, side="BUY")
    strat = TwapStrategy(bot_config=cfg)
    sig = strat.generate_signal(_ms(price=100.0, timestamp=0.0))
    assert sig is not None
    assert sig.score > 0
    assert strat._slice_idx["BTC-USD"] == 1
    assert "BTC-USD" in strat._decisions
    intent = strat.order_intents(sig, _ms())
    assert intent and intent[0]["side"] == "BUY" and intent[0]["size_hint"] > 0


def test_subsequent_slice_after_interval():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=10, side="BUY")
    strat = TwapStrategy(bot_config=cfg)
    strat.generate_signal(_ms(price=100.0, timestamp=0.0))  # slice 0
    sig = strat.generate_signal(_ms(price=100.0, timestamp=10.0))  # slice 1
    assert sig is not None
    assert strat._slice_idx["BTC-USD"] == 2


def test_not_yet_time_for_next_slice():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=10, side="BUY")
    strat = TwapStrategy(bot_config=cfg)
    strat.generate_signal(_ms(price=100.0, timestamp=0.0))  # slice 0
    sig = strat.generate_signal(_ms(price=100.0, timestamp=5.0))  # not yet slice 1
    assert sig is None
    assert strat._slice_idx["BTC-USD"] == 1


def test_all_slices_done():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=3, side="BUY")
    strat = TwapStrategy(bot_config=cfg)
    assert strat.generate_signal(_ms(price=100.0, timestamp=0.0)) is not None
    assert strat.generate_signal(_ms(price=100.0, timestamp=40.0)) is not None
    assert strat.generate_signal(_ms(price=100.0, timestamp=80.0)) is not None
    # all slices consumed
    assert strat.generate_signal(_ms(price=100.0, timestamp=200.0)) is None


def test_sell_side():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=10, side="SELL")
    strat = TwapStrategy(bot_config=cfg)
    sig = strat.generate_signal(_ms(price=100.0, timestamp=0.0))
    assert sig is not None
    assert sig.score < 0
    intent = strat.order_intents(sig, _ms())
    assert intent[0]["side"] == "SELL"


def test_multiple_products_independent():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=10, side="BUY")
    strat = TwapStrategy(bot_config=cfg)
    # BTC slice 0
    assert strat.generate_signal(_ms(product="BTC-USD", price=100.0, timestamp=0.0)) is not None
    # ETH independent, also at slice 0
    assert strat.generate_signal(_ms(product="ETH-USD", price=50.0, timestamp=0.0)) is not None
    assert strat._slice_idx["BTC-USD"] == 1
    assert strat._slice_idx["ETH-USD"] == 1
    # BTC advances to slice 1, ETH still at 0
    assert strat.generate_signal(_ms(product="BTC-USD", price=100.0, timestamp=10.0)) is not None
    assert strat._slice_idx["BTC-USD"] == 2
    assert strat._slice_idx["ETH-USD"] == 1


def test_step_counter_fallback():
    cfg = TwapConfig(total_usd=1000.0, duration_seconds=100.0, slices=3, side="BUY")
    strat = TwapStrategy(bot_config=cfg)
    ms = {"product_id": "BTC-USD", "price": 100.0, "warmup_complete": True}
    # no timestamp -> step counter
    assert strat.generate_signal(ms) is not None  # slice 0
    assert strat.generate_signal(ms) is not None  # slice 1
    assert strat.generate_signal(ms) is not None  # slice 2
    assert strat.generate_signal(ms) is None       # all done
