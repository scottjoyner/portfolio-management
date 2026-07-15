"""Coverage tests for coinbase/src/capital_buckets.py"""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

import pytest

from coinbase.src import capital_buckets as cb


def test_bucket_position_props():
    p = cb.BucketPosition(
        product_id="BTC-USD", side="long", size=2.0, entry_price=100.0,
        current_price=150.0, strategy="x", opened_at=1.0, bucket_id="b",
    )
    assert p.market_value == 300.0
    assert p.cost_basis == 200.0
    assert p.unrealized_pnl == 100.0


def test_capital_bucket_basics():
    b = cb.CapitalBucket(
        bucket_id="b", name="n", starting_balance_usd=1000.0, cash_usd=1000.0,
        target_volume_usd=10000.0, target_multiple=2.0, max_position_pct=0.25,
    )
    assert b.total_value() == 1000.0
    assert b.progress_to_volume_target() == 0.0
    assert b.progress_to_equity_target() == pytest.approx(1000.0 / 2000.0)
    assert b.available_cash() == 1000.0
    assert b.max_notional() == 250.0
    assert b.can_trade_strategy("") is True
    b.allowed_strategies = ["a"]
    assert b.can_trade_strategy("a") is True
    assert b.can_trade_strategy("z") is False


def test_progress_zero_targets():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=100.0,
                         cash_usd=100.0, target_volume_usd=0.0, target_multiple=0.0)
    assert b.progress_to_volume_target() == 1.0
    assert b.progress_to_equity_target() == 1.0


def test_mark_prices():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=100.0,
                         cash_usd=100.0)
    b.open_position("BTC-USD", "long", 1.0, 100.0)
    b.mark_prices({"BTC-USD": 200.0, "ETH-USD": 5.0})
    assert b.positions["BTC-USD"].current_price == 200.0
    assert b.total_value() == 200.0


def test_open_position_branches():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=1000.0,
                         cash_usd=1000.0, max_position_pct=0.5)
    # notional <= 0
    assert b.open_position("X", "long", 0.0, 100.0) is False
    # notional > available cash
    assert b.open_position("X", "long", 100.0, 1000.0) is False  # 100*1000 > 1000
    # valid new position
    assert b.open_position("BTC-USD", "long", 1.0, 100.0) is True
    assert b.cash_usd == 900.0
    assert b.volume_30d_usd == 100.0
    # add to same side -> combine
    assert b.open_position("BTC-USD", "long", 1.0, 300.0) is True
    pos = b.positions["BTC-USD"]
    assert pos.size == 2.0
    assert pos.entry_price == 200.0
    # opposite side -> rejected
    assert b.open_position("BTC-USD", "short", 1.0, 100.0) is False
    # combined size <= 0 -> rejected
    b2 = cb.CapitalBucket(bucket_id="b2", name="n", starting_balance_usd=100.0,
                          cash_usd=100.0, max_position_pct=1.0)
    b2.open_position("Z", "long", 2.0, 10.0)
    assert b2.open_position("Z", "long", -2.0, 10.0) is False


def test_open_position_over_max_notional_when_positions_exist():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=1000.0,
                         cash_usd=1000.0, max_position_pct=0.1)
    b.open_position("BTC-USD", "long", 1.0, 50.0)  # notional 50, max_notional=100
    # new different product exceeding max_notional and positions exist
    assert b.open_position("ETH-USD", "long", 1.0, 200.0) is False


def test_close_position():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=1000.0,
                         cash_usd=1000.0)
    b.open_position("BTC-USD", "long", 1.0, 100.0)
    pnl = b.close_position("BTC-USD", 150.0)
    assert pnl == 50.0
    assert b.cash_usd == 1050.0
    assert b.realized_pnl_usd == 50.0
    # closing missing
    assert b.close_position("NOPE", 1.0) == 0.0


def test_to_dict_from_dict_roundtrip():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=1000.0,
                         cash_usd=800.0)
    b.open_position("BTC-USD", "long", 2.0, 100.0)
    d = b.to_dict()
    b2 = cb.CapitalBucket.from_dict(d)
    assert b2.bucket_id == "b"
    assert b2.cash_usd == 600.0
    assert "BTC-USD" in b2.positions
    assert b2.positions["BTC-USD"].size == 2.0
    # missing optional fields
    b3 = cb.CapitalBucket.from_dict({})
    assert b3.bucket_id == "default"


def test_ledger_init_and_ops(tmp_path):
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=1000.0,
                         cash_usd=1000.0)
    ledger = cb.CapitalBucketLedger(buckets=[b], state_path=str(tmp_path / "x.json"))
    assert ledger.get("b") is b
    assert ledger.get("missing") is None
    assert ledger.open_position("b", "BTC-USD", "long", 1.0, 100.0) is True
    assert ledger.close_position("b", "BTC-USD", 150.0) == 50.0
    ledger.mark_prices({"BTC-USD": 150.0})
    # choose/allocate
    assert ledger.choose_bucket("any", "BTC-USD", 50.0).bucket_id == "b"
    assert ledger.allocate("any", "BTC-USD", 50.0) == "b"
    # apply_opportunity_limits
    size, bid = ledger.apply_opportunity_limits("any", "BTC-USD", 100.0, 1.0)
    assert bid == "b"
    assert size > 0
    # save/load
    ledger.save()
    ledger2 = cb.CapitalBucketLedger(state_path=str(tmp_path / "x.json"))
    ledger2.load()
    assert "b" in ledger2.buckets


def test_ledger_from_env_default(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_BUCKET_STATE_PATH", str(tmp_path / "s.json"))
    monkeypatch.delenv("TRADER_BUCKETS_JSON", raising=False)
    monkeypatch.setenv("TRADER_CHALLENGE_CAPITAL_USDC", "250")
    ledger = cb.CapitalBucketLedger.from_env()
    assert "challenge" in ledger.buckets
    assert ledger.buckets["challenge"].cash_usd == 250.0


def test_ledger_from_env_json(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_BUCKET_STATE_PATH", str(tmp_path / "s.json"))
    data = [{"bucket_id": "custom", "name": "C", "starting_balance_usd": 50.0,
             "cash_usd": 50.0}]
    monkeypatch.setenv("TRADER_BUCKETS_JSON", json.dumps(data))
    ledger = cb.CapitalBucketLedger.from_env()
    assert "custom" in ledger.buckets


def test_ledger_from_env_json_error(tmp_path, monkeypatch):
    monkeypatch.setenv("TRADER_BUCKET_STATE_PATH", str(tmp_path / "s.json"))
    monkeypatch.setenv("TRADER_BUCKETS_JSON", "{not valid")
    ledger = cb.CapitalBucketLedger.from_env()
    assert "challenge" in ledger.buckets


def test_ledger_load_file_formats(tmp_path):
    # list payload
    p = tmp_path / "a.json"
    p.write_text(json.dumps({"buckets": [{"bucket_id": "x", "name": "X",
                              "starting_balance_usd": 10.0, "cash_usd": 10.0}]}))
    ledger = cb.CapitalBucketLedger(state_path=str(p))
    ledger.load()
    assert "x" in ledger.buckets
    # dict payload with buckets key
    p2 = tmp_path / "b.json"
    p2.write_text(json.dumps({"buckets": [{"bucket_id": "y", "name": "Y",
                                            "starting_balance_usd": 5.0, "cash_usd": 5.0}]}))
    ledger2 = cb.CapitalBucketLedger(state_path=str(p2))
    ledger2.load()
    assert "y" in ledger2.buckets
    # missing file
    ledger3 = cb.CapitalBucketLedger(state_path=str(tmp_path / "missing.json"))
    assert ledger3.buckets == {}
    # corrupt file
    p3 = tmp_path / "c.json"
    p3.write_text("garbage")
    ledger4 = cb.CapitalBucketLedger(state_path=str(p3))
    ledger4.load()
    assert ledger4.buckets == {}


def test_ledger_open_close_missing_bucket(tmp_path):
    ledger = cb.CapitalBucketLedger(buckets=[], state_path=str(tmp_path / "x.json"))
    assert ledger.open_position("nope", "BTC-USD", "long", 1.0, 1.0) is False
    assert ledger.close_position("nope", "BTC-USD", 1.0) == 0.0


def test_ledger_apply_opportunity_limits_none():
    ledger = cb.CapitalBucketLedger(buckets=[])
    size, bid = ledger.apply_opportunity_limits("s", "P", 1.0, 1.0)
    assert size == 0.0 and bid is None


def test_ledger_summary():
    b = cb.CapitalBucket(bucket_id="b", name="n", starting_balance_usd=1000.0,
                         cash_usd=1000.0)
    ledger = cb.CapitalBucketLedger(buckets=[b])
    out = ledger.summary({"BTC-USD": 1.0})
    assert out["total_value_usd"] == 1000.0
    out2 = ledger.summary()
    assert "buckets" in out2


def test_presets_and_build():
    t = cb._bucket_template("id", "Name", 100.0, active=False)
    assert t["active"] is False
    assert cb.preset_challenge(100.0)["buckets"][0]["bucket_id"] == "challenge"
    assert cb.preset_challenge_amount(50.0)["buckets"][0]["bucket_id"] == "challenge_50"
    assert cb.preset_challenge_amount(99.5)["buckets"][0]["bucket_id"] == "challenge_99_5"
    core = cb.preset_core(1000.0)
    assert len(core["buckets"]) == 3
    assert cb.preset_fee_tier(500.0)["buckets"][0]["bucket_id"] == "fee_tier"
    combo = cb.preset_challenge_core_fee_tier(100, 800, 100)
    assert len(combo["buckets"]) == 3
    assert set(cb.bucket_preset_names()) >= {"challenge", "core", "fee_tier"}
    # build known preset
    out = cb.build_bucket_preset("core", starting_balance_usd=2000.0)
    assert len(out["buckets"]) == 3
    # build challenge_N
    out2 = cb.build_bucket_preset("challenge_10")
    assert out2["buckets"][0]["bucket_id"] == "challenge_10"
    # build unknown -> KeyError
    with pytest.raises(KeyError):
        cb.build_bucket_preset("does_not_exist")
