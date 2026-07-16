"""Tests for bug-fix campaign on coinbase execution/config/approval paths.

Pure tests — no network, no Coinbase CLI. Run with:
    .venv/bin/python -m pytest tests/coverage/coinbase/ -q
"""

import json
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from coinbase.src.run_trader_v4 import EventTraderV4  # noqa: E402
from coinbase.src import config as cfg  # noqa: E402
from coinbase.src.execution_v2 import (  # noqa: E402
    BracketManager,
    NativeExecutionEngine,
    compute_trailing_stop,
    _age_tighten_mult,
)
import approval_server  # noqa: E402


# ── P0-1: fee tier must descend to lower-fee (higher-volume) tiers ──────────

def _make_trader():
    t = object.__new__(EventTraderV4)
    t.paper_monthly_volume = 1000.0
    t.paper_month_ts = time.time()
    t.paper_trailing_volume_30d = 0.0
    return t


def test_fee_tier_t1_at_low_volume():
    t = _make_trader()
    t.paper_trailing_volume_30d = 0.0
    tier, taker, maker = t._fee_tier()
    assert tier == 1
    assert taker == 60.0 and maker == 40.0


def test_fee_tier_descends_with_volume():
    t = _make_trader()
    results = []
    for vol in (0, 5_000, 25_000, 75_000, 250_000, 2_000_000, 12_000_000):
        t.paper_trailing_volume_30d = float(vol)
        tier, taker, maker = t._fee_tier()
        results.append((tier, taker))
    # taker bps must be non-increasing as volume grows
    takers = [r[1] for r in results]
    assert takers == sorted(takers, reverse=True), takers
    # highest volume qualifies for a low-fee tier
    assert results[-1][1] < 60.0
    # monotonic tier progression
    tiers = [r[0] for r in results]
    assert tiers == sorted(tiers)


def test_fee_tier_top_tier_reached():
    t = _make_trader()
    t.paper_trailing_volume_30d = 600_000_000.0
    tier, taker, maker = t._fee_tier()
    assert tier == len(EventTraderV4.FEE_TIERS)
    assert taker == 0.0


def test_fee_tier_waiver_below_500():
    t = _make_trader()
    t.paper_monthly_volume = 100.0
    t.paper_trailing_volume_30d = 5_000_000.0
    tier, taker, maker = t._fee_tier()
    assert tier == 0 and taker == 0.0 and maker == 0.0


# ── P0-3: state save atomicity + lock scope under concurrency ──────────────

def _fake_snapshot(self):
    return {
        "paper_cash": 1000.0,
        "paper_positions": [],
        "paper_trades": [],
        "paper_starting_capital": 1000.0,
        "paper_realized_pnl": 0.0,
        "paper_fees_paid": 0.0,
        "paper_wins": 0,
        "paper_losses": 0,
        "paper_last_trade_ts": {},
        "paper_peak_equity": 1000.0,
        "paper_equity_curve": [1000.0],
        "paper_equity_tss": [time.time()],
    }


def _build_paper_trader(tmp_path):
    t = object.__new__(EventTraderV4)
    t.mode = "paper"
    t._paper_lock = threading.Lock()
    t._paper_state_path = tmp_path / "paper_state.json"
    t.health_status = {}
    t._paper_state_snapshot = _fake_snapshot.__get__(t, EventTraderV4)
    t._core_holdings = {}
    return t


def test_save_paper_state_valid_json():
    tmp = Path(tempfile.mkdtemp())
    t = _build_paper_trader(tmp)
    t._save_paper_state()
    data = json.loads(t._paper_state_path.read_text())
    assert "paper_cash" in data and "paper_positions" in data


def test_save_paper_state_concurrent_no_corruption():
    tmp = Path(tempfile.mkdtemp())
    t = _build_paper_trader(tmp)

    def worker():
        for _ in range(50):
            t._save_paper_state()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    # File must be valid JSON after concurrent writes
    raw = t._paper_state_path.read_text()
    data = json.loads(raw)
    assert "paper_cash" in data


def test_save_core_holdings_atomic():
    tmp = Path(tempfile.mkdtemp())
    t = object.__new__(EventTraderV4)
    t._core_holdings = {}

    class FakeHolding:
        product_id = "BTC-USD"
        qty = 1.0
        cost_basis = 100.0
        total_cost = 100.0
        total_qty = 1.0
        trades = []
        last_buy_ts = 0.0
        created_ts = 0.0
        target_value = 100.0
        drift_pct = 0.0
        rebalance_action = "hold"

    t._core_holdings["BTC-USD"] = FakeHolding()
    path = tmp / "core_holdings.json"

    import coinbase.src.run_trader_v4 as mod
    orig_path_class = Path

    class _P(Path):
        def __new__(cls, *a, **k):
            p = orig_path_class(*a, **k)
            if p.name == "core_holdings.json":
                return orig_path_class(str(path))
            return p

    mod.Path = _P
    try:
        t._save_core_holdings_state()
    finally:
        mod.Path = orig_path_class
    data = json.loads(path.read_text())
    assert isinstance(data, list) and data[0]["product_id"] == "BTC-USD"


# ── P0-2: approval atomicity + TTL ─────────────────────────────────────────

def _make_handler(pf):
    h = approval_server.ApprovalHandler.__new__(approval_server.ApprovalHandler)
    h.pending_file = str(pf)
    return h


def test_approval_atomic_write(tmp_path):
    pf = tmp_path / "pending.json"
    pf.write_text(json.dumps({"a": {"status": "pending"}}))

    h = _make_handler(pf)
    data = h._read_pending()
    data["b"] = {"status": "pending", "created_at": time.time()}
    approval_server.stamp_approval_timestamps(data["b"])
    h._write_pending(data)
    assert json.loads(pf.read_text()) == data
    assert "expiry_ts" in data["b"]


def test_approval_is_expired_explicit_ts():
    rec = {"created_at": time.time(), "expiry_ts": time.time() - 10}
    assert approval_server.is_expired(rec) is True
    rec2 = {"created_at": time.time(), "expiry_ts": time.time() + 10}
    assert approval_server.is_expired(rec2) is False


def test_approval_is_expired_fallback_ttl():
    rec = {"created_at": time.time() - (25 * 3600)}
    assert approval_server.is_expired(rec) is True
    rec2 = {"created_at": time.time() - 100}
    assert approval_server.is_expired(rec2) is False


def test_approval_is_expired_legacy_no_timestamp():
    assert approval_server.is_expired({"status": "pending"}) is False


def test_resolve_rejects_expired(tmp_path):
    pf = tmp_path / "pending.json"
    old = time.time() - (25 * 3600)
    pf.write_text(json.dumps({"tok": {"status": "pending", "created_at": old}}))

    h = _make_handler(pf)
    entry, ok = h._resolve_and_update("tok", "approved")
    assert ok is False and entry is None


# ── P1-4: REQUIRE_APPROVAL env enables approvals ───────────────────────────

def test_require_approval_env_true(monkeypatch):
    monkeypatch.setenv("REQUIRE_APPROVAL", "true")
    monkeypatch.setenv("REQUIRE_APPROVALS", "false")
    c = cfg.TradingConfig.from_env()
    assert c.require_approvals is True


def test_require_approval_env_false_default(monkeypatch):
    monkeypatch.delenv("REQUIRE_APPROVAL", raising=False)
    monkeypatch.setenv("REQUIRE_APPROVALS", "true")
    c = cfg.TradingConfig.from_env()
    assert c.require_approvals is True


# ── P1-5: kill switch hard block in LIVE ───────────────────────────────────

def test_kill_switch_blocks_live(monkeypatch):
    monkeypatch.setattr(cfg.KillSwitch, "is_active", staticmethod(lambda: True))
    c = cfg.TradingConfig(mode="live", dry_run=False, kill_switch=True)
    with pytest.raises(RuntimeError):
        cfg.LiveSafetyValidator.assert_kill_switch_resolved(c)


def test_kill_switch_ok_when_disengaged_live(monkeypatch):
    monkeypatch.setattr(cfg.KillSwitch, "is_active", staticmethod(lambda: False))
    c = cfg.TradingConfig(mode="live", dry_run=False, kill_switch=False)
    assert cfg.LiveSafetyValidator.assert_kill_switch_resolved(c) is None


def test_kill_switch_no_block_dry_run(monkeypatch):
    monkeypatch.setattr(cfg.KillSwitch, "is_active", staticmethod(lambda: True))
    c = cfg.TradingConfig(mode="live", dry_run=True, kill_switch=True)
    assert cfg.LiveSafetyValidator.assert_kill_switch_resolved(c) is None


def test_kill_switch_no_block_non_live(monkeypatch):
    monkeypatch.setattr(cfg.KillSwitch, "is_active", staticmethod(lambda: True))
    c = cfg.TradingConfig(mode="paper", dry_run=True, kill_switch=True)
    assert cfg.LiveSafetyValidator.assert_kill_switch_resolved(c) is None


# ── P1-6: poll loop surfaces after N consecutive failures ──────────────────

class _FailingEngine:
    def __init__(self):
        self.calls = 0

    def poll_status(self, oid):
        self.calls += 1
        raise RuntimeError("boom")


def test_poll_failure_counter_increments_and_raises():
    eng = _FailingEngine()
    mgr = BracketManager(eng)
    mgr._brackets["x"] = {"status": "OPEN", "stop_order_id": "oid", "target_order_id": "tid"}
    mgr.max_consecutive_poll_failures = 5
    # Each poll_brackets(poll_secs=0) runs one iteration then breaks; drive 5
    # iterations to reach the threshold and surface.
    with pytest.raises(RuntimeError):
        for _ in range(5):
            try:
                mgr.poll_brackets(poll_secs=0)
            except RuntimeError:
                raise
    assert mgr.poll_failures >= 5


def test_poll_succeeds_resets_counter():
    eng = _FailingEngine()
    mgr = BracketManager(eng)
    mgr._brackets["x"] = {"status": "CLOSED"}
    mgr.poll_brackets(poll_secs=0)
    assert mgr.poll_failures == 0


# ── P1-7: shared trailing-stop helper ──────────────────────────────────────

def test_age_tighten_mult_bounds():
    assert _age_tighten_mult(0, 100) == 1.0
    assert _age_tighten_mult(95, 100) == 0.2
    assert _age_tighten_mult(80, 100) == 0.4
    assert _age_tighten_mult(60, 100) == 0.6
    assert _age_tighten_mult(30, 100) == 0.8


def test_compute_trailing_stop_long_breakeven_and_age():
    new_stop, be = compute_trailing_stop(
        side="BUY",
        entry_price=100.0,
        current_stop=90.0,
        highest_price=120.0,
        lowest_price=0.0,
        initial_stop_dist=10.0,
        r_multiple=2.0,
        max_hold_s=100,
        age_s=95,
        regime="unknown",
        breakeven_set=False,
    )
    assert be is True
    assert new_stop >= 100.0
    assert new_stop >= 118.0


def test_compute_trailing_stop_short_symmetric():
    new_stop, be = compute_trailing_stop(
        side="SELL",
        entry_price=100.0,
        current_stop=110.0,
        highest_price=0.0,
        lowest_price=80.0,
        initial_stop_dist=10.0,
        r_multiple=2.0,
        max_hold_s=100,
        age_s=95,
        regime="unknown",
        breakeven_set=False,
    )
    assert be is True
    assert new_stop <= 100.0
    assert new_stop <= 82.0


def test_compute_trailing_stop_does_not_loosen():
    new_stop, _ = compute_trailing_stop(
        side="BUY",
        entry_price=100.0,
        current_stop=118.0,
        highest_price=120.0,
        lowest_price=0.0,
        initial_stop_dist=10.0,
        r_multiple=0.0,
        max_hold_s=100,
        age_s=0,
        regime="unknown",
        breakeven_set=False,
    )
    assert new_stop >= 118.0
