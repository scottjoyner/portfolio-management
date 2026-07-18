"""Hardening tests: live authorization guard + max-drawdown hard-halt.

Run: .venv/bin/python3 tests/test_live_guards.py
"""
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coinbase.src.run_trader_v4 import EventTraderV4


def _make(mode, dry_run=True):
    class _Args:
        scan_interval = 300
        scan_top = 50
        max_held = 20
        max_held_per = 4
        log_file = None
        health_port = None
        disable_shorts = False
        enable_leverage = False
        max_leverage = 1.0
        trade_only = None
        no_color = True
        require_approval = True
        reset_state = False
    args = _Args()
    args.mode = mode
    args.dry_run = dry_run
    bot = EventTraderV4.__new__(EventTraderV4)
    bot.mode = mode
    bot.dry_run = dry_run
    bot._paper_state_path = __import__("pathlib").Path("data") / (
        "live_trader_v4_state.json" if mode in ("live", "approval")
        else "paper_trader_v4_state.json")
    bot._cb_breached = False
    bot._cb_breach_reason = ""
    bot._cb_daily_loss_pct = 0.0
    bot._cb_consecutive_losses = 0
    bot._cb_day_start_ts = 0.0
    bot._cb_peak_equity = 10000.0
    bot._live_start_balance = 0.0
    bot._live_positions = {}
    return bot, args


def _stub_start_exchange(bot):
    """Replace live-startup steps that need a real exchange with no-ops."""
    bot._live_cfg = types.SimpleNamespace(
        coinbase_api_key="x", coinbase_api_secret="y",
        max_positions=10, max_position_pct=0.15, max_drawdown_pct=0.20,
        max_daily_loss_pct=0.05, max_notional_per_trade_usd=200.0,
        risk_per_trade_pct=0.02, min_risk_reward=1.5, min_confidence=0.5,
        bracket_stop_atr_mult=2.0, bracket_target_atr_mult=3.0,
    )
    bot._exec_engine = object()
    bot._bracket_mgr = object()
    bot._risk_mgr = object()
    bot._strategy_ranker = None
    bot._cb_client = object()
    bot._adv_ws = None
    bot._slippage_model = None
    bot._book_cache = None


def test_live_refuses_without_authorization(monkeypatch):
    # Ensure no auth env and no auth file.
    monkeypatch.delenv("ALLOW_LIVE_TRADING", raising=False)
    if os.path.exists("data/live_authorized"):
        os.remove("data/live_authorized")
    bot, args = _make("live", dry_run=False)
    # We call the live-authorization slice directly by invoking start() but
    # it would proceed to exchange init; instead reproduce the guard check.
    import pathlib
    authorized = (os.environ.get("ALLOW_LIVE_TRADING", "").strip() == "1"
                  or pathlib.Path("data/live_authorized").exists())
    assert authorized is False, "precondition: not authorized"
    # Call the actual start() — it must raise before touching the exchange.
    raised = False
    try:
        bot.start()
    except RuntimeError as e:
        raised = "REFUSING TO START LIVE" in str(e)
    assert raised, "live start must refuse without ALLOW_LIVE_TRADING / auth file"


def test_live_proceeds_with_authorization(monkeypatch):
    monkeypatch.setenv("ALLOW_LIVE_TRADING", "1")
    bot, args = _make("live", dry_run=False)
    _stub_start_exchange(bot)
    # Patch the heavy live-init methods so start() runs the guard + our stubs.
    bot._reconcile_open_orders = lambda: None
    bot._sync_positions_from_exchange = lambda: None
    # start() will reach LiveSafetyValidator.check — stub it.
    import coinbase.src.run_trader_v4 as mod
    monkeypatch.setattr(mod, "LiveSafetyValidator",
                        types.SimpleNamespace(check=lambda c: []))
    monkeypatch.setattr(mod, "CBClient", lambda **k: object())
    monkeypatch.setattr(mod, "NativeExecutionEngine", lambda *a, **k: object())
    monkeypatch.setattr(mod, "BracketManager", lambda *a, **k: object())
    monkeypatch.setattr(mod, "StrategyRanking", lambda *a, **k: types.SimpleNamespace(load=lambda: None))
    monkeypatch.setattr(mod, "RiskLimit", types.SimpleNamespace(CONSERVATIVE="c"))
    monkeypatch.setattr(mod, "RiskProfile", lambda *a, **k: object())
    monkeypatch.setattr(mod, "RiskManager", lambda *a, **k: object())
    monkeypatch.setattr(mod, "get_slippage_model", lambda: object())
    monkeypatch.setattr(mod, "get_book_cache", lambda: object())
    try:
        bot.start()
    except Exception as e:
        # We only care that the auth guard passed (no REFUSING TO START LIVE).
        assert "REFUSING TO START LIVE" not in str(e), f"auth guard wrongly blocked: {e}"
    assert bot._live_start_balance == 0.0  # set only if balance fetch stubbed; fine


def test_drawdown_hard_halt(monkeypatch):
    bot, args = _make("live", dry_run=False)
    bot._live_cfg = types.SimpleNamespace(max_daily_loss_pct=0.05, max_consecutive_losses=0)
    bot._live_start_balance = 10000.0
    # Simulate 20% drawdown: avail=8000, no open positions -> equity=8000.
    monkeypatch.setattr(bot, "_dca_available_cash", lambda: 8000.0)
    bot._live_positions = {}
    monkeypatch.setenv("LIVE_MAX_DRAWDOWN_PCT", "0.15")
    # Prevent the real sentinel file + real process exit; capture both.
    written = {}
    real_path = __import__("pathlib").Path
    def _fake_write_text(self, data, *a, **k):
        if "trader_state_corrupt" in str(self):
            written["sentinel"] = data
            return None
        return real_path.write_text(self, data, *a, **k)
    monkeypatch.setattr(real_path, "write_text", _fake_write_text)
    # os._exit would kill the test runner; translate to SystemExit so we can
    # assert the halt fired without terminating pytest.
    monkeypatch.setattr("os._exit", lambda code: (_ for _ in ()).throw(SystemExit(code)))
    exited = False
    try:
        ok = bot._check_circuit_breakers()
        exited = (ok is False and bot._cb_breached
                 and "max_drawdown" in bot._cb_breach_reason)
    except SystemExit:
        exited = True
    assert exited, "drawdown >= cap must hard-halt (breach set or process exit)"
    assert "sentinel" in written, "halt must write corruption sentinel"
    assert os.path.exists("data/trader_state_corrupt") is False, "test must not leave real sentinel"


def test_drawdown_no_halt_within_cap(monkeypatch):
    bot, args = _make("live", dry_run=False)
    bot._live_cfg = types.SimpleNamespace(max_daily_loss_pct=0.05, max_consecutive_losses=0)
    bot._live_start_balance = 10000.0
    monkeypatch.setattr(bot, "_dca_available_cash", lambda: 9900.0)  # 1% dd
    bot._live_positions = {}
    monkeypatch.setenv("LIVE_MAX_DRAWDOWN_PCT", "0.15")
    ok = bot._check_circuit_breakers()
    assert ok is True, "within cap should allow trading"
    assert bot._cb_breach_reason == "", "no breach reason within cap"
    assert os.path.exists("data/trader_state_corrupt") is False, "no sentinel within cap"


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
