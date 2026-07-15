"""Tests for coinbase/src/capital_buckets.py"""
from __future__ import annotations

import os
import json

import pytest

from coinbase.src.capital_buckets import (
    BucketPosition,
    CapitalBucket,
    CapitalBucketLedger,
    _bucket_template,
    preset_challenge,
    preset_challenge_amount,
    preset_core,
    preset_fee_tier,
    preset_challenge_core_fee_tier,
    BUCKET_PRESETS,
    bucket_preset_names,
    build_bucket_preset,
)


def test_bucket_position_props():
    p = BucketPosition("BTC-USD", "BUY", 1.0, 100.0, 110.0)
    assert p.market_value == 110.0
    assert p.cost_basis == 100.0
    assert p.unrealized_pnl == 10.0


def test_bucket_total_value_and_progress():
    b = CapitalBucket("b", "B", 1000.0, 900.0, target_volume_usd=1000.0, target_multiple=2.0)
    b.positions["X"] = BucketPosition("X", "BUY", 1.0, 100.0, 200.0)
    assert b.total_value() == 900.0 + 200.0
    assert b.progress_to_volume_target() == 0.0  # volume 0
    b.volume_30d_usd = 500.0
    assert b.progress_to_volume_target() == 0.5
    assert b.progress_to_equity_target() == min(1.0, b.total_value() / 2000.0)
    # zero targets
    b2 = CapitalBucket("b2", "B2", 0.0, 0.0, target_volume_usd=0.0, target_multiple=0.0)
    assert b2.progress_to_volume_target() == 1.0
    assert b2.progress_to_equity_target() == 1.0


def test_bucket_available_and_max_notional():
    b = CapitalBucket("b", "B", 1000.0, 800.0, max_position_pct=0.25)
    assert b.available_cash() == 800.0
    assert b.max_notional() == b.total_value() * 0.25


def test_bucket_can_trade_strategy():
    b = CapitalBucket("b", "B", 1000.0, 800.0, allowed_strategies=["ema"])
    assert b.can_trade_strategy("ema")
    assert not b.can_trade_strategy("other")
    b2 = CapitalBucket("b2", "B2", 1000.0, 800.0, allowed_strategies=[])
    assert b2.can_trade_strategy("any")


def test_bucket_mark_prices():
    b = CapitalBucket("b", "B", 1000.0, 800.0)
    b.positions["X"] = BucketPosition("X", "BUY", 1.0, 100.0, 100.0)
    b.mark_prices({"X": 150.0, "Y": 5.0})
    assert b.positions["X"].current_price == 150.0


def test_bucket_open_position():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    assert b.open_position("X", "BUY", 1.0, 100.0)
    assert b.cash_usd == 900.0
    assert b.volume_30d_usd == 100.0
    # too big
    assert not b.open_position("Y", "BUY", 100.0, 100.0)
    # exceed max notional with existing
    assert not b.open_position("X", "BUY", 100.0, 100.0)
    # opposite side
    assert not b.open_position("X", "SELL", 0.5, 100.0)


def test_bucket_open_position_zero_notional():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    assert not b.open_position("X", "BUY", 0.0, 0.0)


def test_bucket_open_position_avg_combine():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    assert b.open_position("X", "BUY", 1.0, 100.0)
    assert b.open_position("X", "BUY", 1.0, 200.0)
    pos = b.positions["X"]
    assert pos.size == 2.0
    assert abs(pos.entry_price - 150.0) < 1e-9


def test_bucket_open_position_combine_zero():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    assert b.open_position("X", "BUY", 1.0, 100.0)
    assert not b.open_position("X", "BUY", -1.0, 100.0)  # combined <= 0


def test_bucket_close_position():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    b.open_position("X", "BUY", 1.0, 100.0)
    pnl = b.close_position("X", 120.0)
    assert pnl == 20.0
    assert b.realized_pnl_usd == 20.0
    assert "X" not in b.positions
    # missing
    assert b.close_position("Z", 1.0) == 0.0


def test_bucket_to_from_dict():
    b = CapitalBucket("b", "B", 1000.0, 900.0, allowed_strategies=["ema"])
    b.open_position("X", "BUY", 1.0, 100.0)
    d = b.to_dict()
    b2 = CapitalBucket.from_dict(d)
    assert b2.bucket_id == "b"
    assert b2.cash_usd == 800.0
    assert "X" in b2.positions


def test_bucket_from_dict_defaults():
    b = CapitalBucket.from_dict({})
    assert b.bucket_id == "default"


# ── Ledger ──────────────────────────────────────────────────────────
def test_ledger_get_open_close():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    ledger = CapitalBucketLedger(buckets=[b])
    assert ledger.get("b") is b
    assert ledger.open_position("b", "X", "BUY", 1.0, 100.0)
    assert ledger.close_position("b", "X", 120.0) == 20.0
    assert ledger.get("nope") is None


def test_ledger_mark_and_summary():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    ledger = CapitalBucketLedger(buckets=[b])
    ledger.mark_prices({"X": 1.0})
    s = ledger.summary({"X": 5.0})
    assert "buckets" in s
    assert "total_value_usd" in s


def test_ledger_choose_and_allocate():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    ledger = CapitalBucketLedger(buckets=[b])
    chosen = ledger.choose_bucket("ema", "X", 100.0)
    assert chosen is b
    # no active / can't trade
    b.active = False
    assert ledger.choose_bucket("ema", "X", 100.0) is None
    b.active = True
    assert ledger.allocate("ema", "X", 100.0) == "b"


def test_ledger_apply_opportunity_limits():
    b = CapitalBucket("b", "B", 1000.0, 1000.0)
    ledger = CapitalBucketLedger(buckets=[b])
    size, bid = ledger.apply_opportunity_limits("ema", "X", 100.0, 2.0)
    assert bid == "b"
    assert size <= 2.0
    # not enough cash
    size2, bid2 = ledger.apply_opportunity_limits("ema", "X", 100.0, 100.0)
    assert size2 < 100.0


def test_ledger_apply_opportunity_limits_no_bucket():
    b = CapitalBucket("b", "B", 1000.0, 1000.0, allowed_strategies=["only"])
    ledger = CapitalBucketLedger(buckets=[b])
    size, bid = ledger.apply_opportunity_limits("other", "X", 100.0, 2.0)
    assert size == 0.0
    assert bid is None


def test_ledger_apply_opportunity_limits_zero_notional():
    b = CapitalBucket("b", "B", 1000.0, 0.0)
    ledger = CapitalBucketLedger(buckets=[b])
    size, bid = ledger.apply_opportunity_limits("ema", "X", 1.0, 0.0)
    assert size == 0.0
    assert bid == "b"


def test_ledger_save_load(tmp_path):
    p = tmp_path / "buckets.json"
    b = CapitalBucket("b", "B", 1000.0, 900.0)
    b.open_position("X", "BUY", 1.0, 100.0)
    ledger = CapitalBucketLedger(buckets=[b], state_path=str(p))
    ledger.save()
    assert p.exists()
    ledger2 = CapitalBucketLedger(buckets=[], state_path=str(p))
    ledger2.load()
    assert "b" in ledger2.buckets
    assert "X" in ledger2.buckets["b"].positions


def test_ledger_load_missing(tmp_path):
    ledger = CapitalBucketLedger(buckets=[], state_path=str(tmp_path / "missing.json"))
    ledger.load()  # no error


def test_ledger_load_corrupt(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text("{not json")
    ledger = CapitalBucketLedger(buckets=[], state_path=str(p))
    ledger.load()  # swallow


def test_ledger_from_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_BUCKET_STATE_PATH", str(tmp_path / "st.json"))
    monkeypatch.setenv("TRADER_CHALLENGE_CAPITAL_USDC", "250")
    monkeypatch.setenv("TRADER_CHALLENGE_VOLUME_TARGET_USD", "5000")
    monkeypatch.setenv("TRADER_CHALLENGE_TARGET_MULTIPLE", "4.0")
    monkeypatch.setenv("TRADER_CHALLENGE_MAX_POSITION_PCT", "0.5")
    ledger = CapitalBucketLedger.from_env()
    b = ledger.get("challenge")
    assert b is not None
    assert b.starting_balance_usd == 250.0
    assert b.target_volume_usd == 5000.0


def test_ledger_from_env_json(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_BUCKET_STATE_PATH", str(tmp_path / "st.json"))
    raw = json.dumps([{"bucket_id": "custom", "name": "C", "starting_balance_usd": 500.0, "cash_usd": 500.0}])
    monkeypatch.setenv("TRADER_BUCKETS_JSON", raw)
    ledger = CapitalBucketLedger.from_env()
    assert ledger.get("custom") is not None


def test_ledger_from_env_json_bad(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_BUCKET_STATE_PATH", str(tmp_path / "st.json"))
    monkeypatch.setenv("TRADER_BUCKETS_JSON", "not json{")
    ledger = CapitalBucketLedger.from_env()
    assert ledger.get("challenge") is not None  # fallback to default


# ── Presets / templates ─────────────────────────────────────────────
def test_bucket_template():
    t = _bucket_template("b", "B", 100.0)
    assert t["bucket_id"] == "b"
    assert t["cash_usd"] == 100.0


def test_preset_challenge():
    d = preset_challenge(100.0)
    assert d["buckets"][0]["bucket_id"] == "challenge"
    d2 = preset_challenge(99.5)
    assert "99.5" in d2["buckets"][0]["name"]


def test_preset_challenge_amount():
    d = preset_challenge_amount(50.0)
    assert d["buckets"][0]["bucket_id"] == "challenge_50"
    d2 = preset_challenge_amount(99.5)
    assert "99_5" in d2["buckets"][0]["bucket_id"]


def test_preset_core():
    d = preset_core(1000.0)
    ids = [b["bucket_id"] for b in d["buckets"]]
    assert ids == ["core", "reserve", "opportunity"]
    reserve = d["buckets"][1]
    assert reserve["active"] is False


def test_preset_fee_tier():
    d = preset_fee_tier(1000.0)
    assert d["buckets"][0]["bucket_id"] == "fee_tier"


def test_preset_challenge_core_fee_tier():
    d = preset_challenge_core_fee_tier(100.0, 800.0, 100.0)
    ids = [b["bucket_id"] for b in d["buckets"]]
    assert ids == ["challenge", "core", "fee_tier"]


def test_bucket_preset_names_and_build():
    names = bucket_preset_names()
    assert "challenge" in names
    d = build_bucket_preset("core", starting_balance_usd=2000.0)
    assert d["buckets"][0]["bucket_id"] == "core"
    with pytest.raises(KeyError):
        build_bucket_preset("nonexistent")


def test_build_bucket_preset_challenge_amount():
    d = build_bucket_preset("challenge_10")
    assert d["buckets"][0]["bucket_id"] == "challenge_10"
    d2 = build_bucket_preset("challenge_10", starting_balance_usd=25.0)
    assert d2["buckets"][0]["bucket_id"] == "challenge_25"
