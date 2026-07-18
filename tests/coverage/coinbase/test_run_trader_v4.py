"""
Coverage tests for ``coinbase.src.run_trader_v4.EventTraderV4`` (and ``HealthServer``).

The module is a large event-driven trading daemon. These tests instantiate the
trader in ``paper`` mode (no network) and exercise its methods / branches
directly with ``unittest.mock``. Infinite loops (``start``, scan loops, watchdog)
are NEVER started — only their single-pass bodies / helper methods are invoked.

Run with the per-dir measurement command from the task:
    rm -f .coverage
    .venv/bin/python -m coverage run --source=coinbase.src.run_trader_v4 \
        -m pytest tests/coverage/coinbase/ \
        --ignore=tests/coverage/coinbase/test_config_manager.py \
        --ignore=tests/coverage/coinbase/test_smart_feed.py -q
    .venv/bin/python -m coverage report
"""
import argparse
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

logging = __import__("logging")
logging.disable(logging.CRITICAL)

from coinbase.src.run_trader_v4 import EventTraderV4, PaperPosition, CoreHolding, PulseRecord, HealthServer  # noqa: E402
from strategy_engine import BacktestVerdict  # noqa: E402


def _make_trader(products=("BTC-USD", "ETH-USD", "SOL-USD"), **kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    return EventTraderV4(mode=mode, products=list(products), **kw)


class _FakeBuf:
    """Mimics a StreamingIndicators buffer: has .closes/.volumes/etc with to_list()."""

    def __init__(self, closes=None, volumes=None, highs=None, lows=None):
        self._closes = list(closes if closes is not None else [100.0 + i for i in range(60)])
        self._volumes = list(volumes if volumes is not None else [1.0] * len(self._closes))
        self._highs = list(highs if highs is not None else [c + 0.5 for c in self._closes])
        self._lows = list(lows if lows is not None else [c - 0.5 for c in self._closes])

    def to_list(self):
        return list(self._closes)

    def __len__(self):
        return len(self._closes)


class _FakeStreaming:
    def __init__(self, buf=None):
        self.buf = buf or _FakeBuf()

    def try_get(self, pid):
        return self

    @property
    def closes(self):
        return self.buf

    @property
    def volumes(self):
        return self.buf

    @property
    def highs(self):
        return self.buf

    @property
    def lows(self):
        return self.buf


def _make_streaming(closes=None, volumes=None, highs=None, lows=None):
    return _FakeStreaming(_FakeBuf(closes, volumes, highs, lows))

class BaseV4(unittest.TestCase):
    """Isolates each test by wiping persistent trader state files in ``data/``
    before construction so leftover paper/core/bt/hot state does not leak
    between tests (the trader loads these on __init__)."""

    _STATE_FILES = [
        "data/paper_trader_v4_state.json",
        "data/core_holdings.json",
        "data/bt_cache_v4.json",
        "data/hot_scores_v4.json",
        "data/tuner_state_v4.json",
        "data/strategy_analytics.json",
        "data/equity_summary.json",
        "data/experiment_proposals.json",
    ]

    def setUp(self):
        for f in self._STATE_FILES:
            for path in __import__("pathlib").Path("data").glob(
                __import__("os").path.basename(f) + ("*" if f.endswith("state.json") else "")
            ):
                try:
                    path.unlink()
                except Exception:
                    pass
        super().setUp()

    def tearDown(self):
        super().tearDown()
        for f in self._STATE_FILES:
            for path in __import__("pathlib").Path("data").glob(
                __import__("os").path.basename(f) + ("*" if f.endswith("state.json") else "")
            ):
                try:
                    path.unlink()
                except Exception:
                    pass


# ───────────────────────── Dataclasses ─────────────────────────


class TestPaperPosition(BaseV4):
    def test_mark_and_signals(self):
        p = PaperPosition(product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
                          entry_ts=time.time(), strategy="ema_cross", confidence=0.6,
                          win_rate=0.6, sharpe=1.0, initial_stop_dist=5.0)
        p.mark(110.0)
        p.mark(90.0)
        self.assertEqual(p.highest_price, 110.0)
        self.assertEqual(p.lowest_price, 90.0)
        self.assertTrue(p.is_long)
        self.assertFalse(p.is_short)
        self.assertGreater(p.current_r_multiple, 0)

        s = PaperPosition(product_id="ETH-USD", side="SHORT", qty=1.0, entry_price=100.0,
                          entry_ts=time.time(), strategy="ema_cross", confidence=0.6,
                          win_rate=0.6, sharpe=1.0, initial_stop_dist=5.0)
        s.mark(90.0)
        s.mark(110.0)
        self.assertTrue(s.is_short)
        self.assertFalse(s.is_long)
        self.assertGreater(s.current_r_multiple, 0)

    def test_break_even_price(self):
        p = PaperPosition(product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
                          entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                          sharpe=0.5, entry_notional=100.0, fees_paid=2.0)
        self.assertAlmostEqual(p.break_even_price, 102.0)
        sh = PaperPosition(product_id="ETH-USD", side="SHORT", qty=1.0, entry_price=100.0,
                           entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                           sharpe=0.5, entry_notional=100.0, fees_paid=2.0)
        self.assertAlmostEqual(sh.break_even_price, 98.0)

    def test_break_even_zero_notional(self):
        p = PaperPosition(product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
                          entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                          sharpe=0.5, entry_notional=0.0)
        self.assertEqual(p.break_even_price, 100.0)

    def test_age_and_exposure_and_liq(self):
        p = PaperPosition(product_id="BTC-USD", side="LONG", qty=2.0, entry_price=100.0,
                          entry_ts=time.time() - 5, strategy="x", confidence=0.5, win_rate=0.5,
                          sharpe=0.5, entry_notional=200.0, leverage=2.0)
        self.assertAlmostEqual(p.age_s, 5, delta=2)
        self.assertAlmostEqual(p.notional_exposure, 400.0)
        # liq distance
        p.liq_price = 50.0
        self.assertGreater(p.liq_distance_pct, 0)


class TestCoreHolding(BaseV4):
    def test_avg_price_no_cost(self):
        h = CoreHolding(product_id="BTC-USD")
        self.assertEqual(h.avg_price, 0.0)

    def test_current_value(self):
        h = CoreHolding(product_id="BTC-USD", qty=2.0)
        self.assertEqual(h.current_value(100.0), 200.0)

    def test_add_buy(self):
        h = CoreHolding(product_id="BTC-USD")
        h.add_buy(1.0, 100.0, 1.0)
        h.add_buy(1.0, 200.0, 1.0)
        self.assertAlmostEqual(h.cost_basis, 151.0)
        self.assertEqual(h.total_qty, 2.0)
        self.assertEqual(h.qty, 2.0)
        self.assertEqual(h.trades, 2)

    def test_trim_sell(self):
        h = CoreHolding(product_id="BTC-USD")
        h.add_buy(2.0, 100.0, 0.0)
        realized = h.trim_sell(1.0, 150.0, 1.0)
        self.assertEqual(h.total_qty, 1.0)
        self.assertEqual(h.qty, 1.0)
        self.assertAlmostEqual(realized, 149.0)
        # trim more than held -> clamp
        r2 = h.trim_sell(5.0, 150.0, 0.0)
        self.assertEqual(h.total_qty, 0.0)
        self.assertGreaterEqual(r2, 0.0)

    def test_trim_zero_qty(self):
        h = CoreHolding(product_id="BTC-USD")
        self.assertEqual(h.trim_sell(1.0, 100.0, 0.0), 0.0)


class TestPulseRecord(BaseV4):
    def test_update_and_hot(self):
        pr = PulseRecord(strategy="ema_cross", direction="BUY", product_id="BTC-USD",
                        min_price=100.0, max_price=100.0)
        for _ in range(3):
            pr.update(0.7, 101.0)
        self.assertTrue(pr.is_hot)
        self.assertGreater(pr.avg_confidence, 0)
        self.assertEqual(pr.min_price, 100.0)
        self.assertEqual(pr.max_price, 101.0)

    def test_not_hot_when_few(self):
        pr = PulseRecord(strategy="ema_cross", direction="BUY", product_id="BTC-USD")
        pr.update(0.7, 100.0)
        self.assertFalse(pr.is_hot)


# ───────────────────────── Tunables / Knobs ─────────────────────────


class TestTunables(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_get_tunables(self):
        out = self.t.get_tunables()
        self.assertIn("paper_min_confidence", out)
        self.assertEqual(out["paper_min_confidence"]["value"], 0.55)

    def test_set_tunable_unknown(self):
        ok, msg = self.t.set_tunable("nope", 1)
        self.assertFalse(ok)
        self.assertIn("Unknown knob", msg)

    def test_set_tunable_int_and_float(self):
        ok, _ = self.t.set_tunable("paper_product_cooldown_s", 100)
        self.assertTrue(ok)
        self.assertEqual(self.t.paper_product_cooldown_s, 100)
        ok, _ = self.t.set_tunable("paper_min_confidence", 0.7)
        self.assertTrue(ok)
        self.assertAlmostEqual(self.t.paper_min_confidence, 0.7)

    def test_set_tunable_type_error(self):
        ok, msg = self.t.set_tunable("paper_product_cooldown_s", "abc")
        self.assertFalse(ok)
        self.assertIn("cannot convert", msg)

    def test_set_tunable_unsupported_type(self):
        # All knobs are int/float; int("10.5") -> 10 succeeds. Verifying int coercion.
        ok, _ = self.t.set_tunable("paper_min_hold_s", 10.5)
        self.assertTrue(ok)
        self.assertEqual(self.t.paper_min_hold_s, 10)

    def test_set_tunable_out_of_range(self):
        ok, msg = self.t.set_tunable("paper_min_confidence", 5.0)
        self.assertFalse(ok)
        self.assertIn("out of range", msg)

    def test_persist_and_load_knobs(self):
        self.t.set_tunable("paper_min_confidence", 0.42)
        self.t._persist_knobs()
        new_t = _make_trader()
        # load_knobs is invoked in start(), not __init__; call manually
        new_t._load_knobs()
        self.assertAlmostEqual(new_t.paper_min_confidence, 0.42)
        # cleanup persisted file
        import pathlib
        pathlib.Path("data/tuner_state_v4.json").unlink(missing_ok=True)

    def test_load_knobs_no_file(self):
        import pathlib
        p = pathlib.Path("data/tuner_state_v4.json")
        existed = p.exists()
        if existed:
            p.unlink()
        self.t._load_knobs()  # should no-op gracefully
        if existed:
            pass


# ───────────────────────── Fee Tier Math ─────────────────────────


class TestFeeTier(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_waiver_under_500(self):
        self.t.paper_monthly_volume = 100.0
        self.t.paper_trailing_volume_30d = 0.0
        self.assertEqual(self.t._fee_tier(), (0, 0.0, 0.0))

    def test_tier_selection_by_volume(self):
        self.t.paper_monthly_volume = 600.0  # above waiver
        self.t.paper_trailing_volume_30d = 1_000_000.0
        tier, taker, maker = self.t._fee_tier()
        self.assertEqual(tier, 6)
        self.assertEqual(taker, 10.0)
        self.assertEqual(maker, 0.0)

    def test_tier_lowest(self):
        self.t.paper_monthly_volume = 600.0
        self.t.paper_trailing_volume_30d = 1.0
        tier, taker, maker = self.t._fee_tier()
        self.assertEqual(tier, 1)
        self.assertEqual((taker, maker), (60.0, 40.0))

    def test_highest_tier(self):
        self.t.paper_monthly_volume = 600.0
        self.t.paper_trailing_volume_30d = 1_000_000_000.0
        tier, taker, maker = self.t._fee_tier()
        self.assertEqual(tier, 11)
        self.assertEqual((taker, maker), (0.0, 0.0))

    def test_effective_fee_bps(self):
        self.t.paper_monthly_volume = 600.0
        self.t.paper_trailing_volume_30d = 1_000_000.0
        self.t.paper_maker_pct = 0.5
        # tier6 taker=10 maker=0 -> 0.5*0 + 0.5*10 = 5
        self.assertAlmostEqual(self.t._effective_fee_bps(), 5.0)

    def test_update_trailing_volume_decay_and_accrue(self):
        self.t.paper_trailing_volume_30d = 0.0
        self.t._trailing_vol_last_ts = time.time() - 2_592_000.0  # 30d ago -> decay ~0
        self.t._update_trailing_volume(1000.0)
        self.assertAlmostEqual(self.t.paper_trailing_volume_30d, 1000.0, delta=50)
        self.assertAlmostEqual(self.t.paper_monthly_volume, 1000.0, delta=50)

    def test_reset_monthly_volume_new_attr(self):
        if hasattr(self.t, "paper_monthly_volume"):
            del self.t.paper_monthly_volume
        self.t._reset_monthly_volume_if_needed()
        self.assertEqual(self.t.paper_monthly_volume, 0.0)

    def test_reset_monthly_volume_month_change(self):
        self.t.paper_monthly_volume = 5000.0
        # last ts in a different year/month
        self.t.paper_month_ts = time.mktime((2020, 1, 1, 0, 0, 0, 0, 0, 0))
        self.t._reset_monthly_volume_if_needed()
        self.assertEqual(self.t.paper_monthly_volume, 0.0)


# ───────────────────────── Regime / Cross-asset ─────────────────────────


class TestRegimeHelpers(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_regime_to_cmatrix(self):
        self.assertEqual(self.t._regime_to_cmatrix("strong_uptrend"), "trending")
        self.assertEqual(self.t._regime_to_cmatrix("ranging"), "ranging")
        self.assertEqual(self.t._regime_to_cmatrix("high_volatility"), "volatile")
        self.assertEqual(self.t._regime_to_cmatrix("weird"), "")

    def test_detect_regime_success(self):
        closes = [float(100 + i) for i in range(60)]
        highs = [c + 1 for c in closes]
        lows = [c - 1 for c in closes]
        vols = [1.0] * 60
        with patch("rust_core.detect_regime_py", return_value=("strong_uptrend", 30.0, 0.8, 0.1, 0.5)), \
             patch("rust_core.atr_py", return_value=2.0), \
             patch("rust_core.regime_recommended_strategies_py", return_value=["ema_cross"]):
            res = self.t._detect_regime("BTC-USD", closes, highs, lows, vols)
        self.assertEqual(res["regime"], "strong_uptrend")
        self.assertEqual(res["atr_14"], 2.0)
        self.assertEqual(res["regime_conf"], 0.8)
        self.assertEqual(res["recommended"], {"ema_cross"})

    def test_detect_regime_atr_fails(self):
        closes = [100.0] * 60
        highs = lows = vols = [1.0] * 60
        with patch("rust_core.detect_regime_py", return_value=("ranging", 0.0, 0.0, 0.0, 0.0)), \
             patch("rust_core.atr_py", side_effect=Exception("boom")), \
             patch("rust_core.regime_recommended_strategies_py", side_effect=Exception("x")):
            res = self.t._detect_regime("BTC-USD", closes, highs, lows, vols)
        self.assertEqual(res["regime"], "ranging")
        self.assertEqual(res["atr_14"], 0.0)
        self.assertEqual(res["recommended"], set())

    def test_detect_regime_total_fail(self):
        closes = [100.0] * 60
        with patch("rust_core.detect_regime_py", side_effect=RuntimeError("no rust")):
            res = self.t._detect_regime("BTC-USD", closes, [1] * 60, [1] * 60, [1] * 60)
        self.assertEqual(res["regime"], "unknown")

    def test_cross_asset_state_none_engine(self):
        self.t._cross_asset_regime = None
        out = self.t._cross_asset_regime_snapshot()
        self.assertIn("regime", out)

    def test_cross_asset_state_exception_fallback(self):
        eng = MagicMock()
        eng.get_state.side_effect = Exception("x")
        eng.snapshot.side_effect = Exception("y")
        self.t._cross_asset_regime = eng
        out = self.t._cross_asset_regime_snapshot()
        self.assertIn("regime", out)

    def test_cross_asset_risk_multiplier(self):
        self.t._cross_asset_regime = None
        self.assertAlmostEqual(self.t._cross_asset_risk_multiplier(), 0.75)

    def test_cross_asset_risk_multiplier_from_state(self):
        eng = MagicMock()
        eng.get_state.return_value = SimpleNamespace(to_dict=lambda: {"risk_multiplier": 0.8})
        self.t._cross_asset_regime = eng
        self.assertAlmostEqual(self.t._cross_asset_risk_multiplier(), 0.8)

    def test_push_notification(self):
        before = len(self.t._notifications)
        self.t._push_notification("trade", "title", "msg", {"a": 1})
        self.assertEqual(len(self.t._notifications), before + 1)

    def test_btc_momentum_multiplier_no_regime(self):
        self.t._cross_asset_regime = None
        self.t._last_price["BTC-USD"] = 50000.0
        self.assertEqual(self.t._btc_momentum_multiplier(), 1.0)

    def test_btc_momentum_multiplier_boost(self):
        eng = MagicMock()
        eng.last_daily_close.return_value = 100.0
        self.t._cross_asset_regime = eng
        self.t._last_price["BTC-USD"] = 120.0  # +20% > 0.02 -> boost
        mult = self.t._btc_momentum_multiplier()
        self.assertGreater(mult, 1.0)
        self.assertLessEqual(mult, 1.3)

    def test_btc_momentum_multiplier_no_boost(self):
        eng = MagicMock()
        eng.last_daily_close.return_value = 100.0
        self.t._cross_asset_regime = eng
        self.t._last_price["BTC-USD"] = 100.5  # flat
        self.assertEqual(self.t._btc_momentum_multiplier(), 1.0)


# ───────────────────────── Evaluate pipeline ─────────────────────────


def _verdict(passed=True, win_rate=0.6, sharpe=1.0):
    return BacktestVerdict(
        strategy="ema_cross", currency="BTC", total_trades=10, winning_trades=6,
        losing_trades=4, win_rate=win_rate, total_return_pct=5.0, sharpe_ratio=sharpe,
        profit_factor=1.5, max_drawdown_pct=2.0, regime="trending", passed=passed, reason="ok",
    )


class TestEvaluatePipeline(BaseV4):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])
        self.t.streaming = _make_streaming(closes=[100.0 + i for i in range(60)])
        self.t._slice_cache = {}

    def _patch_rust(self, raw_signals):
        return patch("rust_core.evaluate_all_opens_py", return_value=raw_signals)

    def test_evaluate_impl_basic_buy(self):
        raw = [("ema_cross", "BUY", 0.8, "r")]
        with self._patch_rust(raw), \
             patch("coinbase.src.run_trader_v4.batch_backtest_rust",
                   return_value={"ema_cross/BTC": _verdict()}):
            with patch.object(self.t, "_paper_execute") as pe:
                self.t._evaluate_impl("BTC-USD")
                pe.assert_called_once()

    def test_evaluate_impl_short_slices_returns(self):
        self.t.streaming = _make_streaming(closes=[1.0, 2.0])
        self.t._evaluate_impl("BTC-USD")
        self.assertEqual(len(self.t.paper_positions), 0)

    def test_evaluate_impl_rust_fails(self):
        with patch("rust_core.evaluate_all_opens_py", side_effect=RuntimeError("x")):
            # should not raise; just returns after empty raw_signals
            self.t._evaluate_impl("BTC-USD")

    def test_evaluate_impl_no_signals(self):
        with self._patch_rust([]):
            self.t._evaluate_impl("BTC-USD")  # no crash
        self.assertEqual(self.t._tick_count, 0)

    def test_regime_gating_filters(self):
        raw = [("ema_cross", "BUY", 0.8, "r"), ("rsi_revert", "SELL", 0.9, "r")]
        with patch("rust_core.detect_regime_py", return_value=("strong_uptrend", 30.0, 0.8, 0.1, 0.5)), \
             patch("rust_core.atr_py", return_value=2.0), \
             patch("rust_core.regime_recommended_strategies_py", return_value=["ema_cross"]), \
             self._patch_rust(raw), \
             patch("coinbase.src.run_trader_v4.batch_backtest_rust",
                   return_value={"ema_cross/BTC": _verdict(), "rsi_revert/BTC": _verdict()}):
            with patch.object(self.t, "_paper_execute") as pe:
                self.t._evaluate_impl("BTC-USD")
                opps = pe.call_args[0][2]
                names = [o["strategy"] for o in opps]
                self.assertIn("ema_cross", names)
                self.assertNotIn("rsi_revert", names)

    def test_fingerprint_duplicate_boost(self):
        raw = [("ema_cross", "BUY", 0.8, "r")]
        with self._patch_rust(raw), \
             patch("coinbase.src.run_trader_v4.batch_backtest_rust",
                   return_value={"ema_cross/BTC": _verdict()}):
            with patch.object(self.t, "_paper_execute") as pe:
                self.t._evaluate_impl("BTC-USD")
                conf1 = pe.call_args[0][2][0]["confidence"]
                # second pass: duplicates
                self.t._evaluate_impl("BTC-USD")
                conf2 = pe.call_args[0][2][0]["confidence"]
                self.assertGreaterEqual(conf2, conf1)

    def test_macro_boost_and_leverage(self):
        raw = [("ema_cross", "BUY", 0.8, "r")]
        macro = SimpleNamespace(bias="bullish", confidence=0.6, risk_multiplier=1.0,
                                allows_new_longs=True, allows_new_shorts=True)
        self.t._last_macro_signal = macro
        self.t.enable_leverage = True
        with patch("rust_core.detect_regime_py", return_value=("strong_uptrend", 30.0, 0.8, 0.1, 0.5)), \
             patch("rust_core.atr_py", return_value=2.0), \
             patch("rust_core.regime_recommended_strategies_py", return_value=["ema_cross"]), \
             self._patch_rust(raw), \
             patch("coinbase.src.run_trader_v4.batch_backtest_rust", return_value={"ema_cross/BTC": _verdict()}), \
             patch.object(self.t, "_vol_scaled_leverage", return_value=1.5):
            with patch.object(self.t, "_paper_execute") as pe:
                self.t._evaluate_impl("BTC-USD")
                opp = pe.call_args[0][2][0]
                self.assertGreater(opp["confidence"], 0.8)
                self.assertEqual(opp["leverage"], 1.5)
                self.assertTrue(opp["is_long_horizon"])

    def test_evaluate_wrapper_consecutive_fail_circuit_breaker(self):
        # Force many consecutive failures via exception in impl
        with patch.object(self.t, "_evaluate_impl", side_effect=ValueError("fail")):
            key = "eval_fail_BTC-USD"
            for _ in range(5):
                self.t._evaluate("BTC-USD")
            self.assertEqual(getattr(self.t, key), 5)
            # 6th call should short-circuit (no new setattr exception)
            self.t._evaluate("BTC-USD")
            self.assertEqual(getattr(self.t, key), 5)

    def test_evaluate_wrapper_success_resets(self):
        with patch.object(self.t, "_evaluate_impl", side_effect=ValueError("fail")):
            self.t._evaluate("BTC-USD")
        key = "eval_fail_BTC-USD"
        self.assertEqual(getattr(self.t, key), 1)
        with patch.object(self.t, "_evaluate_impl"):
            self.t._evaluate("BTC-USD")
            self.assertEqual(getattr(self.t, key), 0)


# ───────────────────────── Paper equity / drawdown / scoring ─────────────────────────


class TestPaperAccounting(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_paper_equity_long(self):
        pos = PaperPosition(product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
                           entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                           sharpe=0.5, entry_notional=100.0, leverage=1.0)
        self.t.paper_positions["BTC-USD"] = pos
        # Honest equity: cash + unrealized P&L (mark - entry) for the long.
        eq = self.t._paper_equity({"BTC-USD": 120.0})
        self.assertAlmostEqual(eq, self.t.paper_cash + 20.0)  # (120-100)*1

    def test_paper_equity_short(self):
        pos = PaperPosition(product_id="BTC-USD", side="SHORT", qty=1.0, entry_price=100.0,
                           entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                           sharpe=0.5, entry_notional=100.0, leverage=1.0)
        self.t.paper_positions["BTC-USD"] = pos
        # Honest equity: cash + unrealized P&L (entry - mark) for the short.
        eq = self.t._paper_equity({"BTC-USD": 80.0})
        self.assertAlmostEqual(eq, self.t.paper_cash + 20.0)  # (100-80)*1

    def test_paper_equity_with_core_holdings(self):
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                        cost_basis=100.0, total_cost=100.0,
                                                        total_qty=1.0)
        eq = self.t._paper_equity({"BTC-USD": 110.0})
        self.assertAlmostEqual(eq, self.t.paper_cash + 110.0)

    def test_paper_drawdown(self):
        self.t.paper_peak_equity = 12000.0
        dd = self.t._paper_drawdown(10000.0)
        self.assertAlmostEqual(dd, 2000 / 12000)

    def test_paper_drawdown_peak_zero(self):
        self.t.paper_peak_equity = 0.0
        self.assertEqual(self.t._paper_drawdown(0.0), 0.0)

    def test_paper_score_multiplier(self):
        self.t._core_holdings = {}
        self.t._paper_drawdown = lambda e=None: 0.0
        s = self.t._paper_score_multiplier(0.8, 0.7, 1.0)
        self.assertAlmostEqual(s, 0.8)
        s2 = self.t._paper_score_multiplier(0.8, 0.3, 1.0)  # low win rate -> 0.7x
        self.assertLess(s2, s)
        s3 = self.t._paper_score_multiplier(0.8, 0.7, 0.1)  # low sharpe -> 0.5x
        self.assertLess(s3, s)

    def test_paper_trade_notional(self):
        self.t.paper_cash = 10000.0
        n = self.t._paper_trade_notional(0.8)
        self.assertGreaterEqual(n, self.t.paper_min_trade_usd)
        self.assertLessEqual(n, 10000.0 * self.t.paper_max_position_pct)

    def test_paper_edge_model(self):
        with patch.object(self.t, "_effective_fee_bps", return_value=10.0):
            edge = self.t._paper_edge_model(0.8, 0.7, 1.0)
        self.assertIn("net_bps", edge)
        self.assertGreater(edge["gross_bps"], 0)

    def test_paper_signal_score_buy(self):
        opp = {"action": "BUY", "confidence": 0.8, "win_rate": 0.7, "sharpe": 1.0}
        self.t._last_macro_signal = None
        self.t._btc_momentum_multiplier = lambda: 1.2
        score = self.t._paper_signal_score(opp)
        self.assertGreater(score, 0)

    def test_paper_signal_score_sell_macro(self):
        opp = {"action": "SELL", "confidence": 0.8, "win_rate": 0.7, "sharpe": 1.0}
        macro = SimpleNamespace(allows_new_shorts=True)
        self.t._last_macro_signal = macro
        s_yes = self.t._paper_signal_score(opp)
        macro2 = SimpleNamespace(allows_new_shorts=False)
        self.t._last_macro_signal = macro2
        s_no = self.t._paper_signal_score(opp)
        self.assertGreater(s_yes, s_no)


# ───────────────────────── State snapshot / load / save ─────────────────────────


class TestStatePersistence(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_paper_state_snapshot_shape(self):
        self.t.paper_positions["BTC-USD"] = PaperPosition(
            product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
            entry_ts=time.time(), strategy="ema_cross", confidence=0.6, win_rate=0.6,
            sharpe=1.0)
        snap = self.t._paper_state_snapshot()
        self.assertIn("paper_cash", snap)
        self.assertEqual(len(snap["paper_positions"]), 1)
        self.assertIn("strategy_stats", snap)

    def test_save_and_load_paper_state(self):
        self.t.paper_cash = 9000.0
        self.t.paper_positions["BTC-USD"] = PaperPosition(
            product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
            entry_ts=time.time(), strategy="ema_cross", confidence=0.6, win_rate=0.6, sharpe=1.0)
        self.t._save_paper_state()
        new_t = _make_trader()
        self.assertAlmostEqual(new_t.paper_cash, 9000.0, delta=1)
        self.assertIn("BTC-USD", new_t.paper_positions)
        # cleanup
        for p in list(new_t._paper_state_path.parent.glob("paper_trader_v4_state.json*")):
            try:
                p.unlink()
            except Exception:
                pass

    def test_load_paper_state_corrupt(self):
        self.t._paper_state_path.write_text("{not valid json")
        self.t._load_paper_state()  # should warn and return

    def test_load_paper_state_missing_fields(self):
        self.t._paper_state_path.write_text(json.dumps({"paper_cash": 5}))
        # missing paper_positions -> state None -> returns
        self.t._load_paper_state()
        for p in list(self.t._paper_state_path.parent.glob("paper_trader_v4_state.json*")):
            try:
                p.unlink()
            except Exception:
                pass

    def test_core_holdings_save_load(self):
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                        cost_basis=100.0, total_cost=100.0,
                                                        total_qty=1.0, target_value=100.0,
                                                        drift_pct=2.0, rebalance_action="buy")
        self.t._save_core_holdings_state()
        new_t = _make_trader(mode="approval")
        self.assertIn("BTC-USD", new_t._core_holdings)
        p = "data/core_holdings.json"
        if os.path.exists(p):
            os.remove(p)

    def test_core_holdings_load_invalid(self):
        self.t._core_holdings = {}
        import pathlib
        pathlib.Path("data/core_holdings.json").write_text("garbage")
        self.t._load_core_holdings_state()
        pathlib.Path("data/core_holdings.json").unlink(missing_ok=True)

    def test_bt_cache_serializable_and_load(self):
        self.t._bt_cache["ema_cross/BTC"] = _verdict()
        ser = self.t._bt_cache_serializable()
        self.assertIn("ema_cross/BTC", ser)
        # round-trip
        self.t._bt_cache = {}
        self.t._bt_cache_path.write_text(json.dumps(ser))
        self.t._load_bt_cache()
        self.assertIn("ema_cross/BTC", self.t._bt_cache)
        self.t._bt_cache_path.unlink(missing_ok=True)

    def test_bt_cache_expired(self):
        old = {"x/y": {**{k: 0 for k in ["total_trades", "winning_trades", "losing_trades"]},
                       "strategy": "s", "currency": "x", "win_rate": 0.0,
                       "total_return_pct": 0.0, "sharpe_ratio": 0.0, "profit_factor": 0.0,
                       "max_drawdown_pct": 0.0, "regime": "", "passed": False, "reason": "",
                       "_ts": time.time() - 100000.0}}
        self.t._bt_cache_path.write_text(json.dumps(old))
        self.t._load_bt_cache()
        self.assertEqual(len(self.t._bt_cache), 0)
        self.t._bt_cache_path.unlink(missing_ok=True)

    def test_bt_cache_path_missing(self):
        if self.t._bt_cache_path.exists():
            self.t._bt_cache_path.unlink()
        self.t._load_bt_cache()  # no-op

    def test_hot_scores_save_load(self):
        self.t._hot_scores["BTC-USD"] = 5.0
        self.t._save_hot_scores()
        new_t = _make_trader()
        self.assertGreater(new_t._hot_scores.get("BTC-USD", 0), 0)
        p = "data/hot_scores_v4.json"
        if os.path.exists(p):
            os.remove(p)

    def test_hot_scores_path_missing(self):
        if self.t._hot_scores_path.exists():
            self.t._hot_scores_path.unlink()
        self.t._load_hot_scores()


# ───────────────────────── Fingerprint / Pulse / Hot ─────────────────────────


class TestSignalDedupPulse(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_fingerprint_key(self):
        k = self.t._fingerprint_key("BTC-USD", "ema_cross", "BUY", 123.456)
        self.assertTrue(k.startswith("BTC-USD:ema_cross:BUY:"))

    def test_fingerprint_duplicate_second_time(self):
        k = "BTC-USD:ema_cross:BUY:100.0"
        self.assertFalse(self.t._is_fingerprint_duplicate(k))  # first -> not dup
        self.assertTrue(self.t._is_fingerprint_duplicate(k))   # second within ttl -> dup

    def test_pulse_key(self):
        self.assertEqual(self.t._pulse_key("BTC-USD", "ema_cross", "BUY"),
                         "BTC-USD:ema_cross:BUY")

    def test_record_pulse_new_and_update(self):
        rec = self.t._record_pulse("BTC-USD", "ema_cross", "BUY", 0.7, 100.0)
        self.assertEqual(rec.pulse_count, 1)
        rec2 = self.t._record_pulse("BTC-USD", "ema_cross", "BUY", 0.9, 101.0)
        self.assertEqual(rec2.pulse_count, 2)
        # Seed an opposite-direction "ANY" pulse so the flip counter branch runs.
        any_pulse = PulseRecord(strategy="ANY", direction="SELL", product_id="BTC-USD",
                               last_ts=time.time())
        self.t._signal_pulses["BTC-USD:ANY:SELL"] = any_pulse
        self.t._record_pulse("BTC-USD", "ema_cross", "SELL", 0.8, 99.0)
        self.assertEqual(any_pulse.flip_count, 1)

    def test_record_pulse_window_reset(self):
        rec = self.t._record_pulse("BTC-USD", "ema_cross", "BUY", 0.7, 100.0)
        rec.last_ts = time.time() - self.t._pulse_window_s - 10
        rec2 = self.t._record_pulse("BTC-USD", "ema_cross", "BUY", 0.9, 101.0)
        self.assertEqual(rec2.pulse_count, 1)

    def test_pulse_summary_and_prune(self):
        self.t._record_pulse("BTC-USD", "ema_cross", "BUY", 0.7, 100.0)
        summary = self.t._pulse_summary_for_pid("BTC-USD")
        self.assertEqual(len(summary), 1)
        # prune: set stale
        for r in self.t._signal_pulses.values():
            r.last_ts = time.time() - self.t._pulse_window_s * 8
        self.t._prune_pulses()
        self.assertEqual(len(self.t._signal_pulses), 0)

    def test_record_hotness_zero(self):
        self.t._record_hotness("BTC-USD", 0.0)  # no-op
        self.assertEqual(self.t._hot_scores.get("BTC-USD", 0), 0)

    def test_record_hotness_accumulates(self):
        self.t._record_hotness("BTC-USD", 0.5)
        self.assertGreater(self.t._hot_scores["BTC-USD"], 0)

    def test_adaptive_minute_top_n_no_hotset(self):
        self.t.minute_scan_use_hotset = False
        self.assertEqual(self.t._adaptive_minute_top_n(), self.t.minute_scan_top_n)

    def test_adaptive_minute_top_n_hotset(self):
        self.t.minute_scan_use_hotset = True
        self.t.minute_scan_top_n = 20
        self.t._hot_scores["BTC-USD"] = 10.0
        self.t._hot_scores["ETH-USD"] = 10.0
        self.t._hot_scores["SOL-USD"] = 10.0
        self.t._hot_scores["ADA-USD"] = 10.0
        n = self.t._adaptive_minute_top_n()
        self.assertGreater(n, self.t.minute_scan_top_n)

    def test_minute_scan_products_hotset(self):
        self.t.minute_scan_use_hotset = True
        self.t._hot_scores["BTC-USD"] = 1.0
        res = self.t._minute_scan_products(10)
        self.assertEqual(res[0][0], "BTC-USD")

    def test_minute_scan_products_fallback(self):
        self.t.minute_scan_use_hotset = False
        with patch("coinbase.src.run_trader_v4.top_coinbase_pairs",
                   return_value=[{"id": "BTC-USD", "base": "BTC"}]):
            res = self.t._minute_scan_products(10)
        self.assertEqual(res, [("BTC-USD", "BTC")])


# ───────────────────────── Paper execution ─────────────────────────


class TestPaperExecute(BaseV4):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])
        self.t.streaming = _make_streaming(closes=[float(100 + i) for i in range(60)])
        # Seed state the anti-fragility guards read (concentration + depth).
        self.t._last_price = {"BTC-USD": 100.0}
        # Give the test strategy/product enough live trades that the depth
        # guard (min_trades_for_full_sizing=20) does not scale confidence down,
        # and a tiny pnl so the concentration cap (30% of equity) never trips.
        for _ in range(25):
            self.t._perf_tracker.record_trade("ema_cross", "BTC-USD", 1.0, 100.0, 0.01, "LONG")

    def _opp(self, action="BUY", conf=0.8, wr=0.7, sharpe=1.0, atr=2.0, regime="strong_uptrend"):
        return {
            "action": action, "confidence": conf, "win_rate": wr, "sharpe": sharpe,
            "strategy": "ema_cross", "atr_14": atr, "regime": regime,
            "is_long_horizon": False, "leverage": 1.0, "stop_dist": atr * 2.5,
        }

    def test_paper_execute_price_le_zero(self):
        self.t._paper_execute("BTC-USD", 0.0, [self._opp()])

    def test_paper_execute_impl_drawdown_guard(self):
        self.t._paper_drawdown = lambda e=None: 0.9
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        self.assertEqual(len(self.t.paper_positions), 0)

    def test_paper_execute_impl_unfavorable_regime_skip(self):
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp(regime="ranging")])
        self.assertEqual(len(self.t.paper_positions), 0)

    def test_paper_execute_impl_opens_long(self):
        self.t._perf_tracker.is_disabled = lambda *a, **k: False
        self.t._perf_tracker.is_strategy_disabled = lambda *a, **k: False
        opp = self._opp()
        self.t._paper_execute_impl("BTC-USD", 100.0, [opp])
        self.assertIn("BTC-USD", self.t.paper_positions)

    def test_paper_execute_impl_short_when_enabled(self):
        self.t.enable_shorts = True
        self.t._perf_tracker.is_disabled = lambda *a, **k: False
        self.t._perf_tracker.is_strategy_disabled = lambda *a, **k: False
        opp = self._opp(action="SELL")
        self.t._paper_execute_impl("BTC-USD", 100.0, [opp])
        self.assertIn("BTC-USD", self.t.paper_positions)
        self.assertTrue(self.t.paper_positions["BTC-USD"].is_short)

    def test_paper_execute_impl_trailing_stop_exit(self):
        # open first
        self.t._perf_tracker.is_disabled = lambda *a, **k: False
        self.t._perf_tracker.is_strategy_disabled = lambda *a, **k: False
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        pos = self.t.paper_positions["BTC-USD"]
        pos.entry_ts = time.time() - 100000  # old -> min_hold ok
        pos.highest_price = 120.0
        pos.stop_price = 90.0
        pos.initial_stop_dist = 5.0
        # signal SELL consensus to trigger exit
        sell_opp = self._opp(action="SELL")
        # many sell signals
        opps = [sell_opp] * 5
        self.t._paper_execute_impl("BTC-USD", 80.0, opps)
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_paper_execute_impl_scale_in_long(self):
        self.t._perf_tracker.is_disabled = lambda *a, **k: False
        self.t._perf_tracker.is_strategy_disabled = lambda *a, **k: False
        self.t._paper_execute_impl("BTC-USD", 100.0, [self._opp()])
        pos = self.t.paper_positions["BTC-USD"]
        pos.entry_ts = time.time() - 10
        pos.trades = 1
        # price rose -> r_multiple high
        pos.highest_price = 130.0
        self.t._paper_execute_impl("BTC-USD", 130.0, [self._opp()])
        self.assertEqual(pos.trades, 2)


class TestPaperClose(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def _pos(self, side="LONG"):
        return PaperPosition(product_id="BTC-USD", side=side, qty=1.0, entry_price=100.0,
                             entry_ts=time.time(), strategy="ema_cross", confidence=0.6,
                             win_rate=0.6, sharpe=1.0, entry_notional=100.0, leverage=1.0,
                             fees_paid=1.0)

    def test_close_long_profit(self):
        p = self._pos("LONG")
        self.t._last_volume_24h["BTC-USD"] = 1e9
        self.t._fill_model.estimate = lambda *a, **k: SimpleNamespace(exit_price=120.0)
        self.t._fill_model.is_maker = lambda *a, **k: False
        before = self.t.paper_cash
        self.t._paper_close_position(p, 120.0, "test")
        self.assertGreater(self.t.paper_cash, before)
        self.assertEqual(self.t.paper_wins, 1)

    def test_close_short_profit(self):
        p = self._pos("SHORT")
        self.t._last_volume_24h["BTC-USD"] = 1e9
        self.t._fill_model.estimate = lambda *a, **k: SimpleNamespace(exit_price=80.0)
        self.t._fill_model.is_maker = lambda *a, **k: False
        self.t._paper_close_position(p, 80.0, "test")
        self.assertEqual(self.t.paper_wins, 1)
        self.assertGreater(self.t.paper_realized_pnl, 0)

    def test_close_records_and_notifies(self):
        p = self._pos("LONG")
        self.t._last_volume_24h["BTC-USD"] = 1e9
        self.t._fill_model.estimate = lambda *a, **k: SimpleNamespace(exit_price=90.0)
        self.t._fill_model.is_maker = lambda *a, **k: False
        self.t._paper_close_position(p, 90.0, "loss")
        self.assertEqual(self.t.paper_losses, 1)


# ───────────────────────── DCA / Rebalance ─────────────────────────


class TestDcaRebalance(BaseV4):
    def setUp(self):
        self.t = _make_trader()
        self.t._core_holdings_enabled = True

    def test_dca_core_holdings_disabled(self):
        self.t._core_holdings_enabled = False
        self.t._dca_core_holdings()  # returns immediately

    def test_dca_eval_no_price(self):
        self.t._last_price.pop("BTC-USD", None)
        self.t._dca_eval_asset("BTC-USD")  # no price -> return

    def test_dca_eval_insufficient_streaming(self):
        self.t._last_price["BTC-USD"] = 50000.0
        self.t.streaming = _make_streaming(closes=[100.0] * 10)
        self.t._dca_eval_asset("BTC-USD")  # < 50 closes -> return

    def test_dca_eval_buys_on_dip(self):
        self.t._last_price["BTC-USD"] = 90.0
        closes = [100.0] * 60
        closes[-1] = 90.0
        self.t.streaming = _make_streaming(closes=closes)
        self.t.paper_cash = 10000.0
        self.t._dca_eval_asset("BTC-USD")
        self.assertIn("BTC-USD", self.t._core_holdings)

    def test_dca_execute_buy_paper(self):
        self.t.paper_cash = 10000.0
        ok = self.t._dca_execute_buy("BTC-USD", 100.0, 25.0, 3.0)
        self.assertTrue(ok)
        self.assertIn("BTC-USD", self.t._core_holdings)

    def test_dca_execute_buy_insufficient_cash(self):
        self.t.paper_cash = 1.0
        ok = self.t._dca_execute_buy("BTC-USD", 100.0, 25.0, 3.0)
        self.assertFalse(ok)

    def test_dca_available_cash_no_client(self):
        self.t._cb_client = None
        self.assertEqual(self.t._dca_available_cash(), 0.0)

    def test_dca_available_cash_with_client(self):
        client = MagicMock()
        client.list_accounts.return_value = [
            {"currency": "USD", "available_balance": {"value": "500.0"}},
        ]
        self.t._cb_client = client
        self.assertAlmostEqual(self.t._dca_available_cash(), 500.0)

    def test_dca_execute_buy_live_no_engine(self):
        self.t.mode = "live"
        self.t._exec_engine = None
        ok = self.t._dca_execute_buy("BTC-USD", 100.0, 25.0, 3.0)
        self.assertFalse(ok)
        self.t.mode = "paper"

    def test_dca_execute_buy_live_circuit_breaker(self):
        self.t.mode = "live"
        self.t._exec_engine = MagicMock()
        self.t._cb_breached = True
        ok = self.t._dca_execute_buy("BTC-USD", 100.0, 25.0, 3.0)
        self.assertFalse(ok)
        self.t.mode = "paper"
        self.t._cb_breached = False

    def test_dca_execute_buy_live_success(self):
        self.t.mode = "live"
        self.t._exec_engine = MagicMock()
        client = MagicMock()
        client.preview_order.return_value = {"order_id": "p1"}
        client.market_order.return_value = {"status": "DONE", "avg_price": "101.0",
                                            "filled_size": "0.2", "fees": 0.5}
        self.t._cb_client = client
        ok = self.t._dca_execute_buy("BTC-USD", 100.0, 25.0, 3.0)
        self.assertTrue(ok)
        self.t.mode = "paper"

    def test_dca_execute_buy_live_failure(self):
        self.t.mode = "live"
        self.t._exec_engine = MagicMock()
        client = MagicMock()
        client.preview_order.return_value = {"order_id": "p1"}
        client.market_order.return_value = {"status": "FAILED"}
        self.t._cb_client = client
        ok = self.t._dca_execute_buy("BTC-USD", 100.0, 25.0, 3.0)
        self.assertFalse(ok)
        self.t.mode = "paper"

    def test_rebalance_core_holdings(self):
        self.t._last_price["BTC-USD"] = 100.0
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                        cost_basis=100.0, total_cost=100.0,
                                                        total_qty=1.0)
        # force rebalance interval
        self.t._core_rebalance_last_ts["stable"] = 0.0
        self.t.paper_cash = 10000.0
        self.t._rebalance_core_holdings()
        # Should not raise; bucket processed

    def test_rebalance_core_holdings_disabled_bucket(self):
        self.t._core_buckets_config["stable"]["enabled"] = False
        self.t._rebalance_core_holdings()  # skip stable

    def test_rebalance_buy(self):
        self.t._last_price["BTC-USD"] = 100.0
        self.t.paper_cash = 10000.0
        self.t._rebalance_buy("BTC-USD", 50.0)
        self.assertIn("BTC-USD", self.t._core_holdings)

    def test_rebalance_buy_no_price(self):
        self.t._last_price.pop("BTC-USD", None)
        self.t._rebalance_buy("BTC-USD", 50.0)  # return

    def test_rebalance_trim(self):
        self.t._last_price["BTC-USD"] = 100.0
        self.t.paper_cash = 5000.0
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=2.0,
                                                        cost_basis=100.0, total_cost=200.0,
                                                        total_qty=2.0, last_buy_ts=0.0)
        before = self.t._core_holdings["BTC-USD"].total_qty
        self.t._rebalance_trim("BTC-USD", 50.0)
        self.assertLess(self.t._core_holdings["BTC-USD"].total_qty, before)

    def test_rebalance_trim_no_holding(self):
        self.t._rebalance_trim("DOGE-USD", 50.0)  # no holding -> return

    def test_core_holdings_value(self):
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                        cost_basis=100.0, total_cost=100.0,
                                                        total_qty=1.0)
        self.assertAlmostEqual(self.t._core_holdings_value({"BTC-USD": 110.0}), 110.0)

    def test_core_holdings_value_fallback_cost(self):
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                        cost_basis=100.0, total_cost=100.0,
                                                        total_qty=1.0)
        self.assertAlmostEqual(self.t._core_holdings_value({}), 100.0)

    def test_core_holdings_to_dict(self):
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                        cost_basis=100.0, total_cost=100.0,
                                                        total_qty=1.0)
        self.t._core_target_weights = {"BTC-USD": 0.5}
        out = self.t._core_holdings_to_dict()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["product_id"], "BTC-USD")


class TestTightenStops(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_tighten_no_prices(self):
        self.t._last_price = {}
        self.t._tighten_all_position_stops()  # return no crash

    def test_tighten_closes_on_stop(self):
        pos = PaperPosition(product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
                           entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                           sharpe=0.5, entry_notional=100.0, initial_stop_dist=5.0,
                           stop_price=95.0, highest_price=110.0)
        self.t.paper_positions["BTC-USD"] = pos
        self.t._last_price["BTC-USD"] = 90.0
        self.t._fill_model.estimate = lambda *a, **k: SimpleNamespace(exit_price=90.0)
        self.t._fill_model.is_maker = lambda *a, **k: False
        self.t._tighten_all_position_stops()
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_tighten_short_age_tightening(self):
        pos = PaperPosition(product_id="BTC-USD", side="SHORT", qty=1.0, entry_price=100.0,
                           entry_ts=time.time() - self.t.max_hold_s * 0.95, strategy="x",
                           confidence=0.5, win_rate=0.5, sharpe=0.5, entry_notional=100.0,
                           initial_stop_dist=5.0, stop_price=105.0, lowest_price=90.0)
        self.t.paper_positions["BTC-USD"] = pos
        self.t._last_price["BTC-USD"] = 100.0
        self.t._fill_model.estimate = lambda *a, **k: SimpleNamespace(exit_price=100.0)
        self.t._fill_model.is_maker = lambda *a, **k: False
        self.t._tighten_all_position_stops()
        self.assertNotIn("BTC-USD", self.t.paper_positions)


# ───────────────────────── Open / Circuit Breakers ─────────────────────────


class TestOpenPosition(BaseV4):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])

    def _open(self, conf=0.8, wr=0.7, sharpe=1.0):
        return {
            "action": "BUY", "confidence": conf, "win_rate": wr, "sharpe": sharpe,
            "strategy": "ema_cross", "atr_14": 2.0, "regime": "strong_uptrend",
            "leverage": 1.0, "stop_dist": 5.0,
        }

    def test_open_basic(self):
        self.t._last_price["BTC-USD"] = 100.0
        self.t._perf_tracker.get = lambda *a, **k: None
        self.t._perf_tracker.strategy_aggregate = lambda *a, **k: {"trades": 0, "win_rate": 0.0}
        self.t._perf_tracker.kelly = lambda *a, **k: self.t.paper_max_position_pct
        with patch.object(self.t, "_btc_momentum_multiplier", return_value=1.0):
            self.t._paper_open_position("BTC-USD", 100.0, self._open())
        self.assertIn("BTC-USD", self.t.paper_positions)

    def test_open_price_le_zero(self):
        self.t._paper_open_position("BTC-USD", 0.0, self._open())

    def test_open_max_positions(self):
        self.t.paper_max_new_positions = 0
        self.t._paper_open_position("BTC-USD", 100.0, self._open())

    def test_open_cooldown(self):
        self.t.paper_last_trade_ts["BTC-USD"] = time.time()
        self.t._paper_open_position("BTC-USD", 100.0, self._open())

    def test_open_kelly_negative(self):
        self.t._last_price["BTC-USD"] = 100.0
        self.t._perf_tracker.get = lambda *a, **k: None
        self.t._perf_tracker.strategy_aggregate = lambda *a, **k: {"trades": 0, "win_rate": 0.0}
        self.t._perf_tracker.kelly = lambda *a, **k: -0.5
        self.t._paper_open_position("BTC-USD", 100.0, self._open())
        self.assertNotIn("BTC-USD", self.t.paper_positions)

    def test_open_low_confidence(self):
        self.t._last_price["BTC-USD"] = 100.0
        self.t._perf_tracker.get = lambda *a, **k: None
        self.t._perf_tracker.strategy_aggregate = lambda *a, **k: {"trades": 0, "win_rate": 0.0}
        self.t._perf_tracker.kelly = lambda *a, **k: self.t.paper_max_position_pct
        self.t._paper_open_position("BTC-USD", 100.0, self._open(conf=0.1))
        self.assertNotIn("BTC-USD", self.t.paper_positions)


class TestCircuitBreakers(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def _cfg(self):
        return SimpleNamespace(max_daily_loss_pct=0.05, max_consecutive_losses=3,
                               min_confidence=0.4, bracket_stop_atr_mult=2.5,
                               bracket_target_atr_mult=4.0, risk_per_trade_pct=0.02,
                               max_notional_per_trade_usd=1000.0)

    def test_ok(self):
        self.t._live_cfg = self._cfg()
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.assertTrue(self.t._check_circuit_breakers())

    def test_daily_loss_breach(self):
        self.t._live_cfg = self._cfg()
        self.t._cb_daily_loss_pct = 0.10
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.assertFalse(self.t._check_circuit_breakers())
        self.assertTrue(self.t._cb_breached)

    def test_consecutive_losses_breach(self):
        self.t._live_cfg = self._cfg()
        self.t._cb_consecutive_losses = 5
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.assertFalse(self.t._check_circuit_breakers())

    def test_kill_switch_breach(self):
        self.t._live_cfg = self._cfg()
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=True):
            self.assertFalse(self.t._check_circuit_breakers())

    def test_already_breached(self):
        self.t._cb_breached = True
        self.assertFalse(self.t._check_circuit_breakers())

    def test_daily_reset(self):
        self.t._live_cfg = self._cfg()
        self.t._cb_day_start_ts = time.time() - 100000
        self.t._cb_daily_loss_pct = 0.10
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.assertTrue(self.t._check_circuit_breakers())
        self.assertEqual(self.t._cb_daily_loss_pct, 0.0)

    def test_record_live_result_loss(self):
        self.t._cb_daily_start_equity = 10000.0
        self.t._cb_peak_equity = 10000.0
        self.t._record_live_result(-500.0)
        self.assertEqual(self.t._cb_consecutive_losses, 1)
        self.assertGreater(self.t._cb_daily_loss_pct, 0)

    def test_record_live_result_win(self):
        self.t._cb_consecutive_losses = 2
        self.t._record_live_result(100.0)
        self.assertEqual(self.t._cb_consecutive_losses, 0)


# ───────────────────────── Live execution helpers ─────────────────────────


class TestLiveHelpers(BaseV4):
    def setUp(self):
        self.t = _make_trader(["BTC-USD"])

    def _opp(self, action="BUY", confidence=0.8, win_rate=0.7, sharpe=1.0, regime="strong_uptrend"):
        return {"action": action, "confidence": confidence, "win_rate": win_rate, "sharpe": sharpe,
                "strategy": "ema_cross", "atr_14": 2.0, "regime": regime,
                "is_long_horizon": False, "leverage": 1.0}

    def test_live_execute_no_engines(self):
        self.t._exec_engine = None
        self.t._live_execute("BTC-USD", 100.0, [self._opp()])  # warn + return

    def test_live_execute_circuit_breaker(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._risk_mgr = MagicMock()
        self.t._cb_breached = True
        self.t._live_execute("BTC-USD", 100.0, [self._opp()])

    def test_live_execute_low_confidence(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._risk_mgr = MagicMock()
        self.t._live_cfg = SimpleNamespace(min_confidence=0.95, max_daily_loss_pct=0.05, max_consecutive_losses=3, bracket_stop_atr_mult=2.5, bracket_target_atr_mult=4.0, risk_per_trade_pct=0.02, max_notional_per_trade_usd=10000.0)
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.t._live_execute("BTC-USD", 100.0, [self._opp(confidence=0.5)])

    def test_live_execute_unfavorable_regime(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._risk_mgr = MagicMock()
        self.t._live_cfg = SimpleNamespace(min_confidence=0.4, max_daily_loss_pct=0.05, max_consecutive_losses=3, bracket_stop_atr_mult=2.5, bracket_target_atr_mult=4.0, risk_per_trade_pct=0.02, max_notional_per_trade_usd=10000.0)
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.t._live_execute("BTC-USD", 100.0, [self._opp()])
        # default opp regime strong_uptrend ok; use ranging:
        self.t._live_execute("BTC-USD", 100.0, [dict(self._opp(), regime="ranging")])

    def test_live_execute_sell_skipped(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._risk_mgr = MagicMock()
        self.t._live_cfg = SimpleNamespace(min_confidence=0.4, max_daily_loss_pct=0.05, max_consecutive_losses=3, bracket_stop_atr_mult=2.5, bracket_target_atr_mult=4.0, risk_per_trade_pct=0.02, max_notional_per_trade_usd=10000.0)
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.t._live_execute("BTC-USD", 100.0, [self._opp(action="SELL")])

    def test_live_execute_buy_success(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._bracket_mgr.place_bracket.return_value = {
            "entry_order": SimpleNamespace(success=True), "status": "ok",
        }
        self.t._risk_mgr = MagicMock()
        self.t._risk_mgr.check_trade.return_value = (True, "ok")
        self.t._live_cfg = SimpleNamespace(min_confidence=0.4, max_daily_loss_pct=0.05,
                                           max_consecutive_losses=3, bracket_stop_atr_mult=2.5,
                                           bracket_target_atr_mult=4.0,
                                           risk_per_trade_pct=0.02,
                                           max_notional_per_trade_usd=10000.0)
        self.t._cb_peak_equity = 10000.0
        self.t._strategy_ranker = None
        self.t._btc_momentum_multiplier = lambda: 1.0
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.t._live_execute("BTC-USD", 100.0, [self._opp()])

    def test_live_execute_risk_fail(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._risk_mgr = MagicMock()
        self.t._risk_mgr.check_trade.return_value = (False, "too risky")
        self.t._live_cfg = SimpleNamespace(min_confidence=0.4, max_daily_loss_pct=0.05, max_consecutive_losses=3, bracket_stop_atr_mult=2.5, bracket_target_atr_mult=4.0, risk_per_trade_pct=0.02, max_notional_per_trade_usd=10000.0)
        self.t._strategy_ranker = None
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.t._live_execute("BTC-USD", 100.0, [self._opp()])

    def test_live_execute_strategy_ranker_gate(self):
        self.t._exec_engine = MagicMock()
        self.t._bracket_mgr = MagicMock()
        self.t._risk_mgr = MagicMock()
        self.t._live_cfg = SimpleNamespace(min_confidence=0.4, max_daily_loss_pct=0.05, max_consecutive_losses=3, bracket_stop_atr_mult=2.5, bracket_target_atr_mult=4.0, risk_per_trade_pct=0.02, max_notional_per_trade_usd=10000.0)
        ranker = MagicMock()
        ranker.get_rank.return_value = 200  # bottom -> skip
        self.t._strategy_ranker = ranker
        with patch("coinbase.src.run_trader_v4.KillSwitch.is_active", return_value=False):
            self.t._live_execute("BTC-USD", 100.0, [self._opp()])

    def test_minute_live_trailing_no_brackets(self):
        self.t._bracket_mgr = None
        self.t._minute_live_trailing()  # return
        self.t._bracket_mgr = MagicMock()
        self.t._bracket_mgr._brackets = {}
        self.t._minute_live_trailing()  # return

    def test_minute_live_exit_check(self):
        self.t._bracket_mgr = MagicMock()
        self.t._bracket_mgr._brackets = {}
        self.t._minute_live_exit_check([self._opp()])

    def test_minute_live_exit_timeout(self):
        self.t._bracket_mgr = MagicMock()
        self.t._bracket_mgr._brackets = {
            "b1": {"product_id": "BTC-USD", "timestamp": time.time() - 100000},
        }
        self.t._bracket_mgr.active_brackets.return_value = self.t._bracket_mgr._brackets
        self.t._live_positions = {"BTC-USD": {"entry_price": 100.0, "size": 1.0, "side": "LONG"}}
        self.t._bracket_mgr.force_flatten_bracket = MagicMock()
        self.t._minute_live_exit_check([self._opp()])
        self.t._bracket_mgr.force_flatten_bracket.assert_called()


# ───────────────────────── Scans / macro / funding / onchain ─────────────────────────


class TestScansAndAux(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def _fake_unified(self, direction="BUY", score=0.7):
        return SimpleNamespace(
            direction=direction, product_id="BTC-USD", price=100.0,
            unified_score=score, backtest_quality=0.6, top_strategies=["ema_cross"],
            short_report=lambda: "r",
        )

    def test_run_scan_full(self):
        self.t._feed_mgr = MagicMock()
        self.t._feed_mgr.get_candles_batch.return_value = {
            "BTC-USD": [[0, 0, 101, 99, 100, 1] for _ in range(80)],
        }
        self.t._aggregator.scan_universe = MagicMock(return_value=[self._fake_unified()])
        with patch("coinbase.src.run_trader_v4.get_all_coinbase_pairs", return_value=[{"id": "BTC-USD", "base": "BTC"}]):
            with patch.object(self.t, "_paper_execute") as pe:
                self.t._run_scan(full=True, granularity=3600, label="FULL SCAN")
                pe.assert_called()

    def test_run_scan_already_running(self):
        self.t._scan_lock.acquire()
        try:
            self.t._run_scan(full=False)
        finally:
            self.t._scan_lock.release()

    def test_run_scan_no_pairs(self):
        self.t._feed_mgr = MagicMock()
        self.t._feed_mgr.get_candles_batch.return_value = {}
        self.t._aggregator.scan_universe = MagicMock(return_value=[])
        with patch("coinbase.src.run_trader_v4.top_coinbase_pairs", return_value=[]):
            self.t._run_scan(full=False)  # warning, returns

    def test_news_sentiment_scan_no_signals(self):
        self.t._news_sentiment.get_signals = MagicMock(return_value=[])
        self.t._news_sentiment_scan()  # no-op

    def test_news_sentiment_scan_with_signal(self):
        sig = MagicMock()
        sig.to_opportunity.return_value = {"product_id": "BTC-USD", "action": "BUY",
                                           "confidence": 0.8, "win_rate": 0.7, "sharpe": 1.0,
                                           "strategy": "news", "atr_14": 0.0, "regime": ""}
        self.t._news_sentiment.get_signals = MagicMock(return_value=[sig])
        self.t._last_price["BTC-USD"] = 100.0
        with patch.object(self.t, "_paper_execute") as pe:
            self.t._news_sentiment_scan()
            pe.assert_called()

    def test_macro_risk_scan(self):
        sig = MagicMock()
        sig.to_opportunity.return_value = {"product_id": "BTC-USD", "action": "BUY",
                                           "confidence": 0.8, "win_rate": 0.7, "sharpe": 1.0,
                                           "strategy": "macro", "atr_14": 0.0, "regime": ""}
        self.t._macro_risk.get_signal = MagicMock(return_value=sig)
        self.t._last_price["BTC-USD"] = 100.0
        with patch.object(self.t, "_paper_execute") as pe:
            self.t._macro_risk_scan()
            pe.assert_called()

    def test_macro_risk_scan_none(self):
        self.t._macro_risk.get_signal = MagicMock(return_value=None)
        self.t._macro_risk_scan()

    def test_macro_leverage_cap(self):
        self.t._core_holdings = {}
        self.t._paper_drawdown = lambda e=None: 0.0
        self.t._last_macro_signal = None
        self.assertEqual(self.t._macro_leverage_cap(), self.t.max_leverage)

    def test_macro_leverage_cap_high_dd(self):
        self.t._core_holdings = {}
        self.t._paper_drawdown = lambda e=None: 0.2
        self.assertEqual(self.t._macro_leverage_cap(), 1.0)

    def test_vol_scaled_leverage(self):
        self.t.enable_leverage = True
        lev = self.t._vol_scaled_leverage("BTC-USD", 100.0, 1.0)
        self.assertGreaterEqual(lev, 1.0)

    def test_vol_scaled_leverage_disabled(self):
        self.t.enable_leverage = False
        self.assertEqual(self.t._vol_scaled_leverage("BTC-USD", 100.0, 1.0), 1.0)

    def test_vol_scaled_leverage_no_price(self):
        self.assertEqual(self.t._vol_scaled_leverage("BTC-USD", 0.0, 1.0), 1.0)

    def test_macro_tf_scan(self):
        sig = SimpleNamespace(bias="bullish", confidence=0.6, risk_multiplier=1.0,
                              allows_new_longs=True, allows_new_shorts=True,
                              cycle_phase="accumulation", reason="x", btc_price=100.0)
        self.t._macro_tf_analyzer.analyze = MagicMock(return_value=sig)
        self.t._last_price["BTC-USD"] = 100.0
        self.t._macro_tf_scan()
        self.assertEqual(self.t._last_macro_signal, sig)

    def test_pair_trade_scan(self):
        sig = {"product_id": "BTC-USD", "action": "BUY", "confidence": 0.8,
               "win_rate": 0.7, "sharpe": 1.0, "strategy": "pair", "atr_14": 0.0, "regime": ""}
        self.t._pair_trading.on_prices = MagicMock(return_value=[sig])
        self.t._last_price["BTC-USD"] = 100.0
        with patch.object(self.t, "_paper_execute") as pe:
            self.t._pair_trade_scan()
            pe.assert_called()

    def test_onchain_flow_scan(self):
        sig = {"product_id": "BTC-USD", "action": "BUY", "confidence": 0.8,
               "win_rate": 0.7, "sharpe": 1.0, "strategy": "onchain", "atr_14": 0.0, "regime": ""}
        self.t._onchain_flow.get_signals = MagicMock(return_value=[sig])
        self.t._last_price["BTC-USD"] = 100.0
        with patch.object(self.t, "_paper_execute") as pe:
            self.t._onchain_flow_scan()
            pe.assert_called()

    def test_funding_scan_bullish(self):
        self.t._last_macro_signal = SimpleNamespace(bias="bullish", confidence=0.7,
                                                    allows_new_longs=True, allows_new_shorts=True)
        self.t.streaming = _make_streaming(closes=[float(100 + i) for i in range(60)])
        self.t._last_price["BTC-USD"] = 160.0
        self.t._last_price["ETH-USD"] = 160.0
        self.t._last_price["SOL-USD"] = 160.0
        self.t.enable_leverage = False
        with patch.object(self.t, "_paper_execute") as pe:
            self.t._funding_scan()
            self.assertTrue(pe.called)

    def test_funding_scan_no_macro(self):
        self.t._last_macro_signal = None
        self.t._funding_scan()  # return

    def test_funding_scan_neutral(self):
        self.t._last_macro_signal = SimpleNamespace(bias="neutral", confidence=0.1)
        self.t.streaming = _make_streaming(closes=[100.0] * 60)
        self.t._last_price["BTC-USD"] = 100.0
        self.t._funding_scan()  # returns (no action)


# ───────────────────────── Live callbacks / reconcile / sync ─────────────────────────


class TestLiveCallbacks(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_on_fill_long_add(self):
        self.t._bracket_mgr = MagicMock()
        self.t._live_positions = {"BTC-USD": {"side": "LONG", "size": 1.0, "entry_price": 100.0}}
        self.t._on_fill({"order_id": "o1", "product_id": "BTC-USD", "side": "BUY",
                         "size": "0.5", "price": "110", "fee": "0"})
        self.assertEqual(self.t._live_positions["BTC-USD"]["size"], 1.5)

    def test_on_fill_long_sell_closes(self):
        self.t._bracket_mgr = MagicMock()
        self.t._live_positions = {"BTC-USD": {"side": "LONG", "size": 0.5, "entry_price": 100.0}}
        self.t._on_fill({"product_id": "BTC-USD", "side": "SELL", "size": "0.5", "price": "110", "fee": "0"})
        self.assertNotIn("BTC-USD", self.t._live_positions)

    def test_on_fill_short_sell_add(self):
        self.t._bracket_mgr = MagicMock()
        self.t._live_positions = {"BTC-USD": {"side": "SHORT", "size": 1.0, "entry_price": 100.0}}
        self.t._on_fill({"product_id": "BTC-USD", "side": "SELL", "size": "0.5", "price": "90", "fee": "0"})
        self.assertEqual(self.t._live_positions["BTC-USD"]["size"], 1.5)

    def test_on_fill_short_buy_closes(self):
        self.t._bracket_mgr = MagicMock()
        self.t._live_positions = {"BTC-USD": {"side": "SHORT", "size": 0.5, "entry_price": 100.0}}
        self.t._on_fill({"product_id": "BTC-USD", "side": "BUY", "size": "0.5", "price": "90", "fee": "0"})
        self.assertNotIn("BTC-USD", self.t._live_positions)

    def test_on_order_update(self):
        self.t._on_order_update({"order_id": "o1", "status": "FILLED", "product_id": "BTC-USD"})

    def test_on_account_update(self):
        self.t._on_account_update({"accounts": [{"currency": "USD", "available_balance": 5, "hold": 0}]})

    def test_reconcile_no_client(self):
        self.t._cb_client = None
        self.t._reconcile_open_orders()  # return

    def test_reconcile_with_orders(self):
        self.t._cb_client = MagicMock()
        self.t._cb_client.list_orders.return_value = [
            {"order_id": "o1", "client_order_id": "c1", "product_id": "BTC-USD",
             "side": "BUY", "filled_size": "0.1", "status": "OPEN"},
        ]
        self.t._bracket_mgr = MagicMock()
        self.t._bracket_mgr._brackets = {"c1": {"status": "OPEN"}}
        self.t._reconcile_open_orders()

    def test_sync_no_client(self):
        self.t._cb_client = None
        self.t._sync_positions_from_exchange()  # return

    def test_sync_live(self):
        self.t.mode = "live"
        self.t._cb_client = MagicMock()
        self.t._cb_client.get_positions.return_value = [
            {"product_id": "BTC-USD", "side": "LONG", "size": "1.0", "entry_price": "100"},
        ]
        self.t._sync_positions_from_exchange()
        self.assertIn("BTC-USD", self.t._live_positions)


# ───────────────────────── Analytics / cleanup ─────────────────────────


class TestAnalyticsAndCleanup(BaseV4):
    def setUp(self):
        self.t = _make_trader()

    def test_compute_strategy_analytics(self):
        self.t.strategy_stats["ema_cross"] = {
            "trades": 10, "wins": 6, "losses": 4, "volume": 1000.0, "pnl": 50.0,
            "exit_reasons": {"signal": 2}, "entry_confidences": [0.6, 0.7],
            "hold_times": [10.0, 20.0],
        }
        self.t._signal_type_counts["ema_cross"] = {"BUY": 6, "SELL": 4}
        out = self.t._compute_strategy_analytics()
        self.assertIn("ema_cross", out)
        self.assertEqual(out["ema_cross"]["trades"], 10)
        self.assertGreater(out["ema_cross"]["profit_factor"], 0)

    def test_compute_strategy_analytics_bad_entry(self):
        self.t.strategy_stats["bad"] = {"foo": 1}
        out = self.t._compute_strategy_analytics()
        self.assertNotIn("bad", out)

    def test_analytics_review_prompt_empty(self):
        self.t.strategy_stats = {}
        self.assertEqual(self.t._analytics_review_prompt(), "No strategy data yet.")

    def test_analytics_review_prompt(self):
        self.t.strategy_stats["ema_cross"] = {
            "trades": 10, "wins": 6, "losses": 4, "volume": 1000.0, "pnl": 50.0,
            "exit_reasons": {}, "entry_confidences": [0.6], "hold_times": [10.0],
        }
        self.t._signal_type_counts["ema_cross"] = {"BUY": 6, "SELL": 4}
        self.t.paper_wins = 6
        self.t.paper_losses = 4
        prompt = self.t._analytics_review_prompt()
        self.assertIn("ema_cross", prompt)

    def test_save_analytics(self):
        self.t._save_analytics()
        p = "data/strategy_analytics.json"
        self.assertTrue(os.path.exists(p))
        os.remove(p)

    def test_analytics_loop(self):
        # run a single body iteration without sleeping loop forever -> patch sleep to break
        with patch("time.sleep", side_effect=InterruptedError):
            try:
                self.t._analytics_loop()
            except InterruptedError:
                pass

    def test_experiment_backtest(self):
        out = self.t._experiment_backtest("ema_cross", {})
        self.assertEqual(out["win_rate"], 0.0)

    def test_llm_review_failure(self):
        with patch("urllib.request.urlopen", side_effect=Exception("no net")):
            self.assertEqual(self.t._llm_review("p", "orinth"), "")

    def test_experiment_review_no_data(self):
        self.t.strategy_stats = {}
        self.assertEqual(self.t._experiment_review(), {})

    def test_experiment_review(self):
        self.t.paper_wins = 10
        self.t.paper_losses = 0
        self.t.strategy_stats["ema_cross"] = {
            "trades": 10, "wins": 10, "losses": 0, "volume": 1000.0, "pnl": 50.0,
            "exit_reasons": {}, "entry_confidences": [0.6], "hold_times": [10.0],
        }
        self.t._signal_type_counts["ema_cross"] = {"BUY": 10, "SELL": 0}
        with patch("urllib.request.urlopen", side_effect=Exception("no net")):
            prop = self.t._experiment_review()
        self.assertIn("orinth", prop)

    def test_experiment_loop(self):
        with patch("time.sleep", side_effect=InterruptedError):
            try:
                self.t._experiment_loop()
            except InterruptedError:
                pass

    def test_cleanup(self):
        # ensure no background threads started; just call cleanup
        self.t._feed_mgr = None
        self.t._ws_feed = None
        self.t._cleanup()
        self.assertTrue(self.t._shutdown)
        self.assertEqual(self.t.health_status["status"], "stopped")

    def test_flatten_all_positions(self):
        pos = PaperPosition(product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
                           entry_ts=time.time(), strategy="x", confidence=0.5, win_rate=0.5,
                           sharpe=0.5, entry_notional=100.0)
        self.t.paper_positions["BTC-USD"] = pos
        self.t._last_price["BTC-USD"] = 100.0
        self.t._fill_model.estimate = lambda *a, **k: SimpleNamespace(exit_price=100.0)
        self.t._fill_model.is_maker = lambda *a, **k: False
        closed = self.t.flatten_all_positions()
        self.assertEqual(closed, 1)


# ───────────────────────── Health refresh ─────────────────────────


class TestHealthRefresh(BaseV4):
    def setUp(self):
        self.t = _make_trader()
        self.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                       cost_basis=100.0, total_cost=100.0,
                                                       total_qty=1.0)
        self.t._last_price["BTC-USD"] = 110.0
        self.t._paper_drawdown = lambda e=None: 0.0

    def test_refresh_populates(self):
        self.t._paper_refresh_health()
        paper = self.t.health_status["paper"]
        self.assertIn("equity", paper)
        self.assertIn("fee_tier", paper)
        self.assertIn("effective_fee_bps", paper)
        self.assertEqual(paper["maker_pct"], self.t.paper_maker_pct)
        self.assertEqual(paper["trailing_volume_30d"], round(self.t.paper_trailing_volume_30d, 2))


# ───────────────────────── from_cli ─────────────────────────


class TestFromCli(BaseV4):
    def test_from_cli_paper(self):
        with patch.object(sys, "argv", ["prog", "--mode", "paper", "--products", "BTC-USD", "ETH-USD"]):
            t = EventTraderV4.from_cli()
        self.assertEqual(t.mode, "paper")
        self.assertIn("BTC-USD", t.products)

    def test_from_cli_reset_paper(self):
        path = "data/paper_trader_v4_state.json"
        open(path, "w").write("{}")
        try:
            with patch.object(sys, "argv", ["prog", "--mode", "paper", "--reset-paper", "--products", "BTC-USD"]):
                t = EventTraderV4.from_cli()
            self.assertIn("BTC-USD", t.products)
        finally:
            import pathlib
            for p in pathlib.Path("data").glob("paper_trader_v4_state.json*"):
                try:
                    p.unlink()
                except Exception:
                    pass


# ───────────────────────── HealthServer (HTTP endpoints) ─────────────────────────


class TestHealthServer(BaseV4):
    @classmethod
    def setUpClass(cls):
        import socket as _sock
        _s = _sock.socket()
        _s.bind(("127.0.0.1", 0))
        free_port = _s.getsockname()[1]
        _s.close()
        os.environ["APPROVAL_TOKEN"] = "secret"
        cls.t = _make_trader(["BTC-USD"], health_port=free_port)
        cls.t._core_holdings["BTC-USD"] = CoreHolding(product_id="BTC-USD", qty=1.0,
                                                       cost_basis=100.0, total_cost=100.0,
                                                       total_qty=1.0)
        cls.t._last_price["BTC-USD"] = 110.0
        cls.t.streaming = _make_streaming(closes=[float(100 + i) for i in range(60)])
        cls.t._paper_refresh_health()
        cls.t.health_status["latency"] = {"rust_signals": 0.04}
        cls.t.health_server.start()
        # wait for server bind
        deadline = time.time() + 10
        while time.time() < deadline and getattr(cls.t.health_server, "_server", None) is None:
            time.sleep(0.05)
        cls.port = cls.t.health_server._server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        try:
            cls.t.health_server._server.shutdown()
        except Exception:
            pass

    def _get(self, path):
        import urllib.request
        import urllib.error
        url = f"http://127.0.0.1:{self.port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return resp.read().decode(), resp.status
        except urllib.error.HTTPError as e:
            return e.read().decode(), e.code

    def test_root(self):
        body, status = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("health_ok", body)

    def test_health(self):
        body, _ = self._get("/health")
        self.assertIn("tick_count", body)

    def test_ping(self):
        with patch("coinbase.src.run_trader_v4.measure_coinbase_latency",
                   return_value={"latency_ms": 12.0}):
            body, status = self._get("/ping")
        self.assertEqual(status, 200)
        self.assertIn("latency_ms", body)

    def test_latency(self):
        body, _ = self._get("/latency")
        self.assertIn("rust_signals", body)

    def test_strategies(self):
        body, _ = self._get("/strategies")
        self.assertIn("total", body)

    def test_pulses(self):
        body, _ = self._get("/pulses")
        self.assertIn("hot", body)

    def test_scan(self):
        body, _ = self._get("/scan")
        import json as _json
        self.assertIsInstance(_json.loads(body), dict)

    def test_metrics(self):
        body, _ = self._get("/metrics")
        self.assertIn("trader_tick_count", body)

    def test_paper_history(self):
        body, _ = self._get("/paper/history")
        self.assertIn("equity_curve", body)

    def test_paper_positions(self):
        body, _ = self._get("/paper/positions")
        self.assertIn("positions", body)

    def test_holdings(self):
        body, _ = self._get("/holdings")
        self.assertIn("holdings", body)

    def test_config_get(self):
        body, _ = self._get("/config")
        self.assertIn("paper_min_confidence", body)

    def test_paper_status(self):
        body, _ = self._get("/paper-status")
        self.assertIn("paper", body)

    def test_notifications(self):
        body, _ = self._get("/notifications")
        self.assertIn("notifications", body)

    def test_experiments(self):
        body, _ = self._get("/experiments")
        self.assertIn("[", body)

    def test_analytics(self):
        body, _ = self._get("/analytics")
        self.assertIn("strategies", body)

    def test_debug(self):
        body, _ = self._get("/debug")
        self.assertIn("ok", body)

    def test_config_post(self):
        import urllib.request
        import urllib.error
        url = f"http://127.0.0.1:{self.port}/config"
        data = json.dumps({"paper_min_confidence": 0.5}).encode()
        req = urllib.request.Request(url, data=data, method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
        self.assertIn("results", body)
        self.t.set_tunable("paper_min_confidence", 0.55)  # restore

    def test_post_empty_body(self):
        import urllib.request
        url = f"http://127.0.0.1:{self.port}/config"
        req = urllib.request.Request(url, data=b"", method="POST")
        import urllib.error
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(ctx.exception.code, 400)

    def test_post_invalid_json(self):
        import urllib.request
        url = f"http://127.0.0.1:{self.port}/config"
        req = urllib.request.Request(url, data=b"{bad", method="POST",
                                     headers={"Content-Type": "application/json"})
        import urllib.error
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(ctx.exception.code, 400)

    def test_flatten_no_auth(self):
        body, status = self._get("/flatten")
        self.assertEqual(status, 403)

    def test_flatten_with_auth(self):
        import urllib.request
        url = f"http://127.0.0.1:{self.port}/flatten"
        req = urllib.request.Request(url, headers={"Authorization": "Bearer secret"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode()
        self.assertIn("flatten", body)


if __name__ == "__main__":
    unittest.main()
