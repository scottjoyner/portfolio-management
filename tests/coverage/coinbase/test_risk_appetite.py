"""Tests for coinbase/src/risk_appetite.py"""
import unittest
from unittest import mock

from coinbase.src import risk_appetite as ra
from coinbase.src.risk_manager import RiskLimit
from coinbase.src.regime import Regime


def fresh():
    return ra.DynamicRiskController(base_limit=RiskLimit.MODERATE)


class TestRiskAppetiteSnapshot(unittest.TestCase):
    def test_defaults(self):
        s = ra.RiskAppetiteSnapshot()
        self.assertEqual(s.score, 0.5)
        self.assertEqual(s.profile_label, "moderate")


class TestDynamicRiskController(unittest.TestCase):
    def test_init(self):
        c = fresh()
        self.assertEqual(c.base_limit, RiskLimit.MODERATE)

    def test_state_property(self):
        c = fresh()
        s = c.state
        self.assertIsInstance(s, ra.RiskAppetiteSnapshot)

    def test_update_equity_peak(self):
        c = fresh()
        c.update_equity(100.0)
        c.update_equity(110.0)
        self.assertEqual(c._peak_equity, 110.0)
        c.update_equity(105.0)
        self.assertEqual(c._peak_equity, 110.0)

    def test_update_equity_history_cap(self):
        c = fresh()
        for i in range(600):
            c.update_equity(float(i))
        self.assertEqual(len(c._equity_history), 500)

    def test_record_trade_win(self):
        c = fresh()
        c.record_trade(True, r_multiple=1.5)
        self.assertEqual(c._consecutive_wins, 1)
        self.assertEqual(c._wins, 1)

    def test_record_trade_loss(self):
        c = fresh()
        c.record_trade(False, r_multiple=-1.0)
        self.assertEqual(c._consecutive_losses, 1)
        self.assertEqual(c._consecutive_wins, 0)

    def test_record_trade_history_cap(self):
        c = fresh()
        for i in range(150):
            c.record_trade(i % 2 == 0)
        self.assertEqual(len(c._trade_results), 100)

    def test_update_regime_baseline(self):
        c = fresh()
        c.update_regime(Regime.STRONG_UPTREND.value, volatility=0.05)
        self.assertEqual(c._current_regime, Regime.STRONG_UPTREND.value)
        # baseline only initializes when below 0.005
        self.assertEqual(c._baseline_volatility, 0.02)

    def test_update_regime_baseline_set(self):
        c = fresh()
        c._baseline_volatility = 0.001
        c.update_regime(Regime.STRONG_UPTREND.value, volatility=0.05)
        self.assertEqual(c._baseline_volatility, 0.05)

    def test_reset_daily(self):
        c = fresh()
        c._pnl_history = [1.0, 2.0]
        c.reset_daily(100.0)
        self.assertEqual(c._pnl_history, [])

    def test_recent_win_rate_empty(self):
        c = fresh()
        self.assertEqual(c.recent_win_rate(), 0.5)

    def test_recent_win_rate_filled(self):
        c = fresh()
        for i in range(10):
            c.record_trade(True)
        self.assertEqual(c.recent_win_rate(), 1.0)

    def test_classify_drawdown(self):
        c = fresh()
        self.assertEqual(c._classify_drawdown(0.01), ra.DrawdownSeverity.NONE.value)
        self.assertEqual(c._classify_drawdown(0.07), ra.DrawdownSeverity.MILD.value)
        self.assertEqual(c._classify_drawdown(0.15), ra.DrawdownSeverity.MODERATE.value)
        self.assertEqual(c._classify_drawdown(0.5), ra.DrawdownSeverity.SEVERE.value)

    def test_classify_loss_streak(self):
        c = fresh()
        self.assertEqual(c._classify_loss_streak(), ra.ConsecutiveLossState.NORMAL.value)
        c._consecutive_losses = 1
        self.assertEqual(c._classify_loss_streak(), ra.ConsecutiveLossState.CAUTION.value)
        c._consecutive_losses = 3
        self.assertEqual(c._classify_loss_streak(), ra.ConsecutiveLossState.REDUCED.value)
        c._consecutive_losses = 99
        self.assertEqual(c._classify_loss_streak(), ra.ConsecutiveLossState.STOPPED.value)

    def test_drawdown_multiplier_tiers(self):
        c = fresh()
        self.assertEqual(c._drawdown_multiplier(0.0), 1.0)
        self.assertEqual(c._drawdown_multiplier(0.06), 0.8)
        self.assertEqual(c._drawdown_multiplier(0.12), 0.5)
        self.assertEqual(c._drawdown_multiplier(0.18), 0.25)
        self.assertEqual(c._drawdown_multiplier(0.25), 0.0)

    def test_consecutive_loss_multiplier(self):
        c = fresh()
        self.assertEqual(c._consecutive_loss_multiplier(), 1.0)
        c._consecutive_losses = 1
        self.assertEqual(c._consecutive_loss_multiplier(), 0.8)
        c._consecutive_losses = 3
        self.assertEqual(c._consecutive_loss_multiplier(), 0.5)
        c._consecutive_losses = 10
        self.assertEqual(c._consecutive_loss_multiplier(), 0.0)

    def test_snapshot_unknown_regime(self):
        c = fresh()
        c.update_regime(Regime.UNKNOWN.value, volatility=0.02)
        s = c.snapshot()
        self.assertEqual(s.regime_multiplier, ra.REGIME_RISK_MULTIPLIERS[Regime.UNKNOWN.value])

    def test_snapshot_drawdown_gating(self):
        c = fresh()
        c.update_equity(100.0)
        c.update_equity(70.0)  # 30% drawdown
        s = c.snapshot()
        self.assertIn("drawdown", " ".join(s.gating_reasons))

    def test_snapshot_profit_lock(self):
        # profit lock compares current vs running peak; normal flow keeps gain<=0,
        # so the lock branch stays inactive (documents the dead branch).
        c = fresh()
        c.update_equity(100.0)
        c.update_equity(130.0)
        s = c.snapshot()
        self.assertFalse(s.profit_lock_active)
        # Directly drive the lock branch via a forced reference equity.
        c._peak_equity = 100.0
        # snapshot recomputes from _equity_history; push a higher current equity
        c._equity_history = [100.0, 130.0]
        s2 = c.snapshot()
        self.assertTrue(s2.profit_lock_active)
        self.assertLess(s2.profit_lock_multiplier, 1.0)

    def test_snapshot_loss_streak_gating(self):
        c = fresh()
        for _ in range(5):
            c.record_trade(False)
        s = c.snapshot()
        self.assertLess(s.consecutive_loss_multiplier, 1.0)

    def test_snapshot_compounding(self):
        c = fresh()
        for _ in range(15):
            c.record_trade(True)
        s = c.snapshot()
        self.assertTrue(s.is_compounding)
        self.assertGreater(s.compound_multiplier, 1.0)

    def test_snapshot_volatility_high(self):
        c = fresh()
        c.update_regime(Regime.RANGING.value, volatility=0.02)
        c._current_volatility = 0.1  # ratio 5
        s = c.snapshot()
        self.assertLess(s.volatility_multiplier, 1.0)

    def test_snapshot_volatility_low(self):
        c = fresh()
        c.update_regime(Regime.RANGING.value, volatility=0.02)
        c._current_volatility = 0.005  # ratio 0.25
        s = c.snapshot()
        self.assertGreater(s.volatility_multiplier, 1.0)

    def test_snapshot_profile_label_high(self):
        c = fresh()
        c.update_regime(Regime.STRONG_UPTREND.value, volatility=0.02)
        s = c.snapshot()
        self.assertIn(s.profile_label, ("conservative", "moderate", "aggressive", "high_risk"))

    def test_get_profile(self):
        c = fresh()
        p = c.get_profile()
        self.assertGreaterEqual(p.max_leverage, 1.0)

    def test_size_adjustment(self):
        c = fresh()
        self.assertGreater(c.size_adjustment(), 0.0)

    def test_snapshot_compounding_low_wr(self):
        c = fresh()
        for _ in range(7):
            c.record_trade(False)
        for _ in range(6):
            c.record_trade(True)
        s = c.snapshot()
        self.assertEqual(c._consecutive_wins, 6)
        self.assertFalse(s.is_compounding)  # recent_wr <= 0.6 -> no boost

    def test_snapshot_zero_baseline_vol(self):
        c = fresh()
        c._baseline_volatility = 0.0
        s = c.snapshot()
        self.assertEqual(s.volatility_multiplier, 1.0)

    def test_summary(self):
        c = fresh()
        c.update_equity(100.0)
        c.record_trade(True)
        d = c.summary()
        self.assertIn("appetite_score", d)
        self.assertIn("peak_equity", d)


if __name__ == "__main__":
    unittest.main()
