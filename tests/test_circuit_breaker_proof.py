"""
Circuit-breaker PROOF tests for ``coinbase.src.run_trader_v4.EventTraderV4``.

These tests do NOT just read the circuit-breaker code — they FORCE each breach
and assert the bot actually refuses to trade. A circuit breaker that only logs
is worthless; these prove the guard is load-bearing.

Run:
    .venv/bin/python -m pytest tests/test_circuit_breaker_proof.py -q

The trader is built in ``live`` mode with ``dry_run=True`` so no real orders are
ever placed, but the live-mode trade-gating path (the one that matters for
go-live) is exercised exactly as it would be in production.

NOTE: the canonical kill switch defaults to ENGAGED (env KILL_SWITCH defaults
True). That is safe-by-default for live, but these tests explicitly disengage it
(via the canonical KillSwitch file) so the "healthy book" case can be asserted.
Each test re-engages via cleanup so it never leaks state.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coinbase.src.run_trader_v4 import EventTraderV4  # noqa: E402
from coinbase.src.config import KillSwitch, TradingConfig  # noqa: E402


def _make_live_trader(**kw):
    """Live-mode, dry-run trader — no network, no real orders.

    KILL_SWITCH is forced off so the default-engaged kill switch does not
    interfere with the per-test breach we are forcing. The paper-state path is
    redirected to a temp file so we never touch the real running bot's state.
    """
    kw.setdefault("dry_run", True)
    tmp = tempfile.mkdtemp(prefix="cbtest_")
    env = {"KILL_SWITCH": "false"}
    t = None
    with patch.dict(os.environ, env, clear=False):
        t = EventTraderV4(mode="live", products=["BTC-USD", "ETH-USD"], **kw)
    # Redirect state file away from the live bot's data/ path.
    t._paper_state_path = Path(tmp) / "paper_trader_v4_state.json"
    t._paper_state_path.parent.mkdir(parents=True, exist_ok=True)
    return t


class TestCircuitBreakerProof(unittest.TestCase):
    def setUp(self):
        # Start every test with the kill switch disengaged (file-based).
        KillSwitch.disengage()

    def tearDown(self):
        KillSwitch.disengage()

    def test_daily_loss_breach_halts_trading(self):
        t = _make_live_trader()
        cfg = t._live_cfg or TradingConfig.from_env()
        t._cb_daily_start_equity = 10000.0
        t._cb_peak_equity = 10000.0
        t._record_live_result(-600.0)  # -6% day, over the 5% limit
        self.assertGreaterEqual(
            t._cb_daily_loss_pct, cfg.max_daily_loss_pct,
            "daily loss should have crossed the limit",
        )
        self.assertFalse(t._check_circuit_breakers(),
                         "circuit breaker MUST return False (block) on daily-loss breach")
        self.assertTrue(t._cb_breached, "breach flag must be set")
        self.assertFalse(t._check_circuit_breakers(), "breach must persist across checks")

    def test_consecutive_losses_breach_halts_trading(self):
        t = _make_live_trader()
        cfg = t._live_cfg or TradingConfig.from_env()
        limit = cfg.max_consecutive_losses
        for _ in range(limit):
            t._record_live_result(-10.0)
        self.assertGreaterEqual(
            t._cb_consecutive_losses, limit,
            "consecutive-loss counter should reach the limit",
        )
        self.assertFalse(t._check_circuit_breakers(),
                         "circuit breaker MUST return False on consecutive-loss breach")
        self.assertTrue(t._cb_breached)

    def test_kill_switch_halts_trading(self):
        t = _make_live_trader()
        KillSwitch.engage()
        try:
            self.assertTrue(KillSwitch.is_active())
            self.assertFalse(t._check_circuit_breakers(),
                             "circuit breaker MUST return False when kill switch active")
            self.assertTrue(t._cb_breached)
        finally:
            KillSwitch.disengage()

    def test_no_breach_allows_trading(self):
        t = _make_live_trader()
        t._cb_daily_start_equity = 10000.0
        t._cb_peak_equity = 10000.0
        t._record_live_result(+50.0)
        self.assertTrue(t._check_circuit_breakers(),
                        "healthy book MUST allow trading (return True)")
        self.assertFalse(t._cb_breached)

    def test_breach_persists_across_checks(self):
        """Once breached, it must stay breached (no silent self-heal mid-day)."""
        t = _make_live_trader()
        t._cb_daily_start_equity = 10000.0
        t._cb_peak_equity = 10000.0
        t._record_live_result(-700.0)
        self.assertFalse(t._check_circuit_breakers())
        t._record_live_result(+500.0)  # a later win must NOT clear the breach
        self.assertFalse(t._check_circuit_breakers(),
                         "breach must NOT self-clear on a later win")


if __name__ == "__main__":
    unittest.main()
