"""Tests for coinbase/src/strategy_registry.py"""
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from coinbase.src import strategy_registry as sr


class TestStrategyPerf(unittest.TestCase):
    def test_is_ready_for_live_true(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=30, win_rate=0.5, sharpe_ratio=0.6, profit_factor=1.2)
        self.assertTrue(p.is_ready_for_live)

    def test_is_ready_for_live_false_trades(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=5, win_rate=0.9, sharpe_ratio=2.0, profit_factor=3.0)
        self.assertFalse(p.is_ready_for_live)

    def test_is_ready_for_live_false_wr(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=30, win_rate=0.3, sharpe_ratio=0.6, profit_factor=1.2)
        self.assertFalse(p.is_ready_for_live)

    def test_is_ready_for_live_false_sharpe(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=30, win_rate=0.5, sharpe_ratio=0.1, profit_factor=1.2)
        self.assertFalse(p.is_ready_for_live)

    def test_is_ready_for_live_false_pf(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=30, win_rate=0.5, sharpe_ratio=0.6, profit_factor=0.9)
        self.assertFalse(p.is_ready_for_live)

    def test_quality_score_no_trades(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=2)
        self.assertEqual(p.quality_score, 0.0)

    def test_quality_score_full(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=100, win_rate=0.6, sharpe_ratio=1.5,
                            profit_factor=2.0)
        self.assertAlmostEqual(p.quality_score, 1.0, places=5)

    def test_quality_score_partial(self):
        p = sr.StrategyPerf(strategy_name="s", product_id="BTC-USD", asset_class="growth",
                            total_trades=50, win_rate=0.3, sharpe_ratio=0.0,
                            profit_factor=0.0)
        s = p.quality_score
        self.assertGreaterEqual(s, 0.0)
        self.assertLessEqual(s, 1.0)


class TestStrategyRegistry(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = Path(self.tmp) / "perf.db"
        self.reg = sr.StrategyRegistry(db_path=self.db)

    def _perf(self, **kw):
        base = dict(strategy_name="ema", product_id="BTC-USD", asset_class="growth",
                    total_trades=30, win_rate=0.5, sharpe_ratio=0.6, profit_factor=1.2)
        base.update(kw)
        return sr.StrategyPerf(**base)

    def test_init_creates_tables(self):
        self.assertTrue(self.db.exists())

    def test_get_missing(self):
        self.assertIsNone(self.reg.get("nope", "X"))

    def test_update_and_get(self):
        p = self._perf()
        self.reg.update_backtest(p)
        got = self.reg.get("ema", "BTC-USD")
        self.assertIsNotNone(got)
        self.assertEqual(got.win_rate, 0.5)

    def test_update_conflict(self):
        self.reg.update_backtest(self._perf(win_rate=0.5))
        self.reg.update_backtest(self._perf(win_rate=0.9))
        self.assertEqual(self.reg.get("ema", "BTC-USD").win_rate, 0.9)

    def test_get_top_strategies_ready(self):
        self.reg.update_backtest(self._perf(strategy_name="a", win_rate=0.6, profit_factor=2.0))
        top = self.reg.get_top_strategies("growth", limit=10)
        self.assertEqual(len(top), 1)

    def test_get_top_strategies_not_ready(self):
        self.reg.update_backtest(self._perf(strategy_name="a", total_trades=5))
        top = self.reg.get_top_strategies("growth", limit=10)
        self.assertEqual(len(top), 0)

    def test_record_live_trade_missing(self):
        self.reg.record_live_trade("nope", "X", 1.0, True)  # returns early

    def test_record_live_trade_win(self):
        self.reg.update_backtest(self._perf())
        self.reg.record_live_trade("ema", "BTC-USD", 0.05, True)
        got = self.reg.get("ema", "BTC-USD")
        self.assertEqual(got.live_trades, 1)
        self.assertAlmostEqual(got.live_win_rate, 1.0)
        self.assertAlmostEqual(got.live_pnl, 0.05)

    def test_record_live_trade_loss(self):
        self.reg.update_backtest(self._perf())
        self.reg.record_live_trade("ema", "BTC-USD", -0.03, False)
        got = self.reg.get("ema", "BTC-USD")
        self.assertEqual(got.live_trades, 1)
        self.assertAlmostEqual(got.live_win_rate, 0.0)

    def test_calibrate_no_perf(self):
        self.assertEqual(self.reg.calibrate_confidence("x", "y", 0.7), 0.7)

    def test_calibrate_low_samples(self):
        self.reg.update_backtest(self._perf(calibration_samples=5,
                                            calibration_slope=1.0, calibration_intercept=0.0))
        self.assertEqual(self.reg.calibrate_confidence("ema", "BTC-USD", 0.7), 0.7)

    def test_calibrate_platt(self):
        self.reg.update_backtest(self._perf(calibration_samples=20,
                                            calibration_slope=2.0, calibration_intercept=0.0))
        c = self.reg.calibrate_confidence("ema", "BTC-USD", 0.5)
        self.assertGreaterEqual(c, 0.0)
        self.assertLessEqual(c, 1.0)
        # raw 0 -> calibrated 0.5
        self.assertAlmostEqual(self.reg.calibrate_confidence("ema", "BTC-USD", 0.0), 0.5)

    def test_deactivate(self):
        self.reg.update_backtest(self._perf())
        self.reg.deactivate_strategy("ema", "BTC-USD")
        got = self.reg.get("ema", "BTC-USD")
        self.assertFalse(got.is_active)
        self.assertNotIn(got, self.reg.get_all_active())

    def test_get_all_active(self):
        self.reg.update_backtest(self._perf(strategy_name="a"))
        self.assertEqual(len(self.reg.get_all_active()), 1)


class TestGetRegistry(unittest.TestCase):
    def test_global(self):
        # patch the global registry path so we don't touch the real data dir
        with mock.patch.object(sr, "_REGISTRY", None):
            r = sr.get_registry()
            self.assertIsInstance(r, sr.StrategyRegistry)
            # second call returns the same cached instance (already-set branch)
            r2 = sr.get_registry()
            self.assertIs(r, r2)


class TestLoadCache(unittest.TestCase):
    def test_load_from_existing_db(self):
        tmp = tempfile.mkdtemp()
        db = Path(tmp) / "perf.db"
        reg1 = sr.StrategyRegistry(db_path=db)
        reg1.update_backtest(sr.StrategyPerf(strategy_name="ema", product_id="BTC-USD",
                                             asset_class="growth", total_trades=30,
                                             win_rate=0.5, sharpe_ratio=0.6,
                                             profit_factor=1.2))
        # New instance reads the persisted active rows -> exercises _load_cache loop
        reg2 = sr.StrategyRegistry(db_path=db)
        self.assertIsNotNone(reg2.get("ema", "BTC-USD"))

    def test_deactivate_absent_key(self):
        reg = sr.StrategyRegistry(db_path=Path(tempfile.mkdtemp()) / "p.db")
        # key not in cache -> `if key in self._cache` False branch
        reg.deactivate_strategy("absent", "X")


if __name__ == "__main__":
    unittest.main()
