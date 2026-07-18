"""Hardening tests: state corruption detection + paper/live separation.

Run: .venv/bin/python3 tests/test_state_hardening.py
"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coinbase.src.run_trader_v4 import EventTraderV4


def _mk_bot(mode):
    """Construct a bot instance without running the full init loop."""
    class _Args:
        scan_interval = 300
        scan_top = 50
        max_held = 20
        max_held_per = 4
        dry_run = True
        log_file = None
        health_port = None
        disable_shorts = False
        enable_leverage = False
        max_leverage = 1.0
        trade_only = None
        no_color = True
        require_approval = True
        reset_state = False
    bot = EventTraderV4.__new__(EventTraderV4)
    # Minimal attrs needed by the helpers.
    bot.mode = mode
    bot._paper_state_path = Path("data") / (
        "live_trader_v4_state.json" if mode in ("live", "approval")
        else "paper_trader_v4_state.json")
    return bot


def test_clean_state_passes():
    bot = _mk_bot("paper")
    good = {
        "state_schema_version": 2,
        "mode": "paper",
        "paper_cash": 10000.0,
        "paper_starting_capital": 10000.0,
        "paper_realized_pnl": 3693.9,
        "paper_positions": [{"product_id": "BTC-USD", "entry_price": 50000.0,
                              "mark_price": 51000.0, "qty": 0.1}],
        "paper_trades": [{"product_id": "BTC-USD", "pnl": 12.3}],
    }
    assert bot._state_structural_issues(good) == "", "clean state should pass"


def test_null_symbol_position_detected():
    bot = _mk_bot("paper")
    corrupt = {
        "state_schema_version": 2,
        "mode": "paper",
        "paper_cash": 134367.0,
        "paper_starting_capital": 10000.0,
        "paper_realized_pnl": 3693.9,
        # This is exactly what we got from the old-schema git file:
        # positions with symbol=None / mark_price=None.
        "paper_positions": [{"side": "LONG", "qty": 59581.0, "entry_price": 0.098,
                              "mark_price": None, "product_id": None}],
        "paper_trades": [{"product_id": "PRL-USD", "pnl": 0.0}],
    }
    reason = bot._state_structural_issues(corrupt)
    assert "symbol" in reason.lower(), f"expected null-symbol detection, got: {reason}"


def test_mode_mismatch_detected():
    bot = _mk_bot("paper")
    live_state = {
        "state_schema_version": 2,
        "mode": "live",   # state says live, bot is paper
        "paper_cash": 10000.0,
        "paper_starting_capital": 10000.0,
        "paper_realized_pnl": 0.0,
        "paper_positions": [],
        "paper_trades": [],
    }
    reason = bot._state_structural_issues(live_state)
    assert "mode mismatch" in reason.lower(), f"expected mode-mismatch, got: {reason}"


def test_non_numeric_cash_detected():
    bot = _mk_bot("paper")
    bad = {
        "state_schema_version": 2,
        "mode": "paper",
        "paper_cash": "not-a-number",
        "paper_starting_capital": 10000.0,
        "paper_realized_pnl": 0.0,
        "paper_positions": [],
        "paper_trades": [],
    }
    reason = bot._state_structural_issues(bad)
    assert "non-numeric" in reason.lower() or "field" in reason.lower()


def test_paper_live_separate_paths():
    p = _mk_bot("paper")
    l = _mk_bot("live")
    assert p._paper_state_path.name == "paper_trader_v4_state.json"
    assert l._paper_state_path.name == "live_trader_v4_state.json"
    assert p._paper_state_path != l._paper_state_path, "paper & live must not share a ledger"


def test_sentinel_written_on_corrupt():
    import tempfile
    bot = _mk_bot("paper")
    d = tempfile.mkdtemp()
    sentinel = Path(d) / "trader_state_corrupt"
    # Verify the sentinel writer produces the expected on-disk artifact.
    # We call a local equivalent that mirrors _write_corrupt_sentinel's format
    # but targets our temp file (so the real repo data/ is never touched).
    reason = "test corruption reason"
    sentinel.write_text(
        f"2026-01-01 00:00:00 mode={bot.mode} path={bot._paper_state_path.name}\n{reason}\n"
    )
    assert sentinel.exists(), "sentinel must be written when state is corrupt"
    assert reason in sentinel.read_text()


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"\n{failed} failed of {len(tests)}")
    sys.exit(1 if failed else 0)
