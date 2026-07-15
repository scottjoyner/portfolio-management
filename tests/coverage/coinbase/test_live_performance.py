import unittest
from pathlib import Path
from unittest import mock

from coinbase.src import live_performance as lp


def rec(strategy="s1", product="P", trades=0, wins=0, losses=0, pnl=0.0,
        sum_wins=0.0, sum_losses=0.0, btwr=0.0, **kw):
    r = lp.StrategyProductRecord(strategy=strategy, product_id=product, **kw)
    r.trades = trades
    r.wins = wins
    r.losses = losses
    r.total_pnl = pnl
    r.sum_wins = sum_wins
    r.sum_losses = sum_losses
    r.backtest_win_rate = btwr
    return r


class TestStrategyProductRecord(unittest.TestCase):
    def test_win_rate(self):
        self.assertEqual(rec(trades=0).win_rate, 0.0)
        self.assertEqual(rec(trades=4, wins=2).win_rate, 0.5)

    def test_loss_rate(self):
        self.assertEqual(rec(trades=4, wins=1).loss_rate, 0.75)

    def test_avg_win(self):
        self.assertEqual(rec(wins=0).avg_win, 0.0)
        self.assertEqual(rec(wins=2, sum_wins=10.0).avg_win, 5.0)

    def test_avg_loss(self):
        self.assertEqual(rec(losses=0).avg_loss, 0.0)
        self.assertEqual(rec(losses=2, sum_losses=4.0).avg_loss, 2.0)

    def test_profit_factor(self):
        self.assertEqual(rec(wins=0, losses=0).profit_factor, 0.0)
        self.assertEqual(rec(wins=1, losses=0).profit_factor, 99.9)
        self.assertEqual(rec(wins=0, losses=1, sum_losses=0.0).profit_factor, 99.9)
        self.assertAlmostEqual(rec(wins=2, sum_wins=10.0, losses=2, sum_losses=4.0).profit_factor, 2.5)

    def test_expectancy_avg_loss_zero(self):
        self.assertEqual(rec(wins=1, sum_wins=5.0, losses=0).expectancy, 0.0)

    def test_expectancy(self):
        r = rec(trades=2, wins=1, sum_wins=10.0, losses=1, sum_losses=4.0)
        # wr=0.5, avg_win=10, avg_loss=4 => 0.5*10/4 - 0.5 = 1.25-0.5=0.75
        self.assertAlmostEqual(r.expectancy, 0.75)

    def test_kelly_trades_lt5(self):
        self.assertEqual(rec(trades=4, wins=2, sum_wins=10, sum_losses=4).kelly_fraction, 0.0)

    def test_kelly_avg_loss_zero(self):
        r = rec(trades=10, wins=5, sum_wins=10, losses=0, sum_losses=0)
        self.assertEqual(r.kelly_fraction, 0.0)

    def test_kelly_r_le_zero(self):
        r = rec(trades=10, wins=1, sum_wins=1.0, losses=9, sum_losses=20.0)
        # r=0.1/20=0.05<=0? 0.05>0 ... need r<=0: avg_win tiny
        r.sum_wins = 0.0
        self.assertEqual(r.kelly_fraction, 0.0)

    def test_kelly_computed(self):
        r = rec(trades=10, wins=6, sum_wins=60.0, losses=4, sum_losses=20.0)
        # avg_win=10, avg_loss=5, r=2, wr=0.6 => (0.6*2 - 0.4)/2 = 0.4
        self.assertAlmostEqual(r.kelly_fraction, 0.4, places=6)

    def test_record_trade_win(self):
        r = rec()
        r.record_trade(5.0, 100.0, 0.1, "LONG", backtest_win_rate=0.7)
        self.assertEqual(r.trades, 1)
        self.assertEqual(r.wins, 1)
        self.assertEqual(r.losses, 0)
        self.assertEqual(r.sum_wins, 5.0)
        self.assertEqual(r.current_streak, 1)
        self.assertEqual(r.best_streak, 1)
        self.assertAlmostEqual(r.backtest_win_rate, 0.7)
        self.assertEqual(r.last_trade_side, "LONG")

    def test_record_trade_loss(self):
        r = rec()
        r.record_trade(-3.0, 50.0, 0.1, "SHORT")
        self.assertEqual(r.losses, 1)
        self.assertEqual(r.sum_losses, 3.0)
        self.assertEqual(r.current_streak, -1)
        self.assertEqual(r.worst_streak, -1)

    def test_record_trade_streak_transitions(self):
        r = rec()
        r.record_trade(1.0, 1.0, 0.0, "LONG")  # streak 1
        r.record_trade(-1.0, 1.0, 0.0, "LONG")  # streak -1
        self.assertEqual(r.current_streak, -1)
        r.record_trade(-1.0, 1.0, 0.0, "LONG")  # streak -2
        self.assertEqual(r.current_streak, -2)
        self.assertEqual(r.worst_streak, -2)
        r.record_trade(1.0, 1.0, 0.0, "LONG")  # streak 1
        self.assertEqual(r.current_streak, 1)
        self.assertEqual(r.best_streak, 1)

    def test_record_trade_backtest_ema(self):
        r = rec(btwr=0.5)
        r.record_trade(1.0, 1.0, 0.0, "LONG", backtest_win_rate=0.9)
        # 0.8*0.5 + 0.2*0.9 = 0.58
        self.assertAlmostEqual(r.backtest_win_rate, 0.58)

    def test_disable_enable(self):
        r = rec()
        r.disable("because")
        self.assertTrue(r.disabled)
        self.assertEqual(r.disable_reason, "because")
        r.enable()
        self.assertFalse(r.disabled)


class TestLivePerformanceTracker(unittest.TestCase):
    def setUp(self):
        self.path = Path(mock.MagicMock())
        # use a real temp file via tmp handled per test; default path uses MagicMock parent
        self.tracker = lp.LivePerformanceTracker(path="data/_test_live_perf.json")

    def test_key_and_get_or_create(self):
        r = self.tracker.get_or_create("s", "P")
        self.assertEqual(r.strategy, "s")
        r2 = self.tracker.get_or_create("s", "P")
        self.assertIs(r, r2)

    def test_record_trade(self):
        r = self.tracker.record_trade("s", "P", 5.0, 100.0, 0.1, "LONG", 0.7)
        self.assertEqual(r.trades, 1)

    def test_get_missing(self):
        self.assertIsNone(self.tracker.get("nope", "X"))

    def test_win_rate_none(self):
        self.assertEqual(self.tracker.win_rate("nope", "X"), 0.0)

    def test_win_rate_min_trades(self):
        self.tracker.record_trade("s", "P", 1.0, 1.0, 0.0, "LONG")
        self.assertEqual(self.tracker.win_rate("s", "P", min_trades=5), 0.0)
        self.assertEqual(self.tracker.win_rate("s", "P", min_trades=0), 1.0)

    def test_kelly_none(self):
        self.assertEqual(self.tracker.kelly("nope", "X"), 0.0)

    def test_kelly_min_trades(self):
        for _ in range(6):
            self.tracker.record_trade("s", "P", 1.0, 1.0, 0.0, "LONG")
        self.assertGreaterEqual(self.tracker.kelly("s", "P", min_trades=5), 0.0)

    def test_is_disabled(self):
        self.assertFalse(self.tracker.is_disabled("s", "P"))

    def test_auto_disable_win_rate(self):
        t = lp.LivePerformanceTracker(path="data/_t2.json")
        for _ in range(12):
            t.record_trade("bad", "P", -1.0, 1.0, 0.0, "LONG")
        n = t.auto_disable(min_trades=10, max_loss_streak=50, min_win_rate=0.25)
        self.assertEqual(n, 1)
        self.assertTrue(t.is_disabled("bad", "P"))

    def test_auto_disable_streak(self):
        t = lp.LivePerformanceTracker(path="data/_t3.json")
        for _ in range(6):
            t.record_trade("streak", "P", -1.0, 1.0, 0.0, "LONG")
        n = t.auto_disable(min_trades=100, max_loss_streak=5, min_win_rate=0.0)
        self.assertEqual(n, 1)

    def test_auto_disable_skip_already_disabled(self):
        t = lp.LivePerformanceTracker(path="data/_t4.json")
        for _ in range(12):
            t.record_trade("bad", "P", -1.0, 1.0, 0.0, "LONG")
        t.auto_disable(min_trades=10)
        rec = t.get("bad", "P")
        rec.disabled = True
        n = t.auto_disable(min_trades=10)
        self.assertEqual(n, 0)

    def test_divergence_report(self):
        t = lp.LivePerformanceTracker(path="data/_t5.json")
        for _ in range(12):
            t.record_trade("div", "P", 1.0, 1.0, 0.0, "LONG", backtest_win_rate=0.9)
        for _ in range(12):
            t.record_trade("div", "P", -0.5, 1.0, 0.0, "LONG", backtest_win_rate=0.9)
        # live wr ~0.5, bt 0.9 -> gap 0.4 >= 0.2
        rep = t.divergence_report(min_trades=10, min_gap=0.20)
        self.assertEqual(len(rep), 1)
        self.assertEqual(rep[0]["strategy"], "div")
        self.assertGreaterEqual(rep[0]["gap"], 0.2)

    def test_divergence_no_backtest(self):
        t = lp.LivePerformanceTracker(path="data/_t6.json")
        for _ in range(12):
            t.record_trade("nobt", "P", 1.0, 1.0, 0.0, "LONG")
        self.assertEqual(t.divergence_report(min_trades=10), [])

    def test_divergence_small_gap(self):
        t = lp.LivePerformanceTracker(path="data/_t7.json")
        for _ in range(12):
            t.record_trade("ok", "P", 1.0, 1.0, 0.0, "LONG", backtest_win_rate=0.55)
        for _ in range(12):
            t.record_trade("ok", "P", 1.0, 1.0, 0.0, "LONG", backtest_win_rate=0.55)
        self.assertEqual(t.divergence_report(min_trades=10, min_gap=0.20), [])

    def test_strategy_aggregate(self):
        t = lp.LivePerformanceTracker(path="data/_t8.json")
        t.record_trade("agg", "P1", 1.0, 1.0, 0.0, "LONG")
        t.record_trade("agg", "P2", 2.0, 1.0, 0.0, "LONG")
        out = t.strategy_aggregate("agg")
        self.assertEqual(out["trades"], 2)
        self.assertEqual(out["wins"], 2)
        self.assertAlmostEqual(out["total_pnl"], 3.0)

    def test_is_strategy_disabled(self):
        self.tracker._disabled_strategies["x"] = "reason"
        self.assertTrue(self.tracker.is_strategy_disabled("x"))
        self.assertFalse(self.tracker.is_strategy_disabled("y"))

    def test_expectancy_report(self):
        t = lp.LivePerformanceTracker(path="data/_t9.json")
        t.record_trade("e", "P", 2.0, 1.0, 0.1, "LONG")   # win
        t.record_trade("e", "P", -1.0, 1.0, 0.1, "LONG")  # loss
        rep = t.expectancy_report(min_trades=1)
        self.assertEqual(len(rep), 1)
        self.assertEqual(rep[0]["strategy"], "e")
        self.assertAlmostEqual(rep[0]["profit_factor"], 2.0, places=6)

    def test_expectancy_report_inf_pf(self):
        t = lp.LivePerformanceTracker(path="data/_t10.json")
        t.record_trade("winner", "P", 3.0, 1.0, 0.0, "LONG")
        rep = t.expectancy_report(min_trades=1)
        self.assertEqual(rep[0]["profit_factor"], None)

    def test_expectency_report_min_trades(self):
        t = lp.LivePerformanceTracker(path="data/_t11.json")
        t.record_trade("low", "P", 1.0, 1.0, 0.0, "LONG")
        self.assertEqual(t.expectancy_report(min_trades=5), [])

    def test_expectancy_report_disabled_flag(self):
        t = lp.LivePerformanceTracker(path="data/_t12.json")
        t.record_trade("d", "P", 1.0, 1.0, 0.0, "LONG")
        t._disabled_strategies["d"] = "r"
        rep = t.expectancy_report(min_trades=1)
        self.assertTrue(rep[0]["disabled"])

    def test_auto_disable_strategies(self):
        t = lp.LivePerformanceTracker(path="data/_t13.json")
        for _ in range(25):
            t.record_trade("loser", "P", -1.0, 1.0, 0.0, "LONG")
        n = t.auto_disable_strategies(min_trades=20, min_win_rate=0.30)
        self.assertEqual(n, 1)
        self.assertTrue(t.is_strategy_disabled("loser"))

    def test_auto_disable_strategies_profitable_skipped(self):
        t = lp.LivePerformanceTracker(path="data/_t14.json")
        for _ in range(25):
            t.record_trade("prof", "P", 1.0, 1.0, 0.0, "LONG")  # low win rate? all wins
        # all wins => wr=1 => not disabled
        n = t.auto_disable_strategies(min_trades=20, min_win_rate=0.30)
        self.assertEqual(n, 0)

    def test_auto_disable_strategies_already_disabled(self):
        t = lp.LivePerformanceTracker(path="data/_t15.json")
        t._disabled_strategies["already"] = "r"
        n = t.auto_disable_strategies(min_trades=1, min_win_rate=0.0)
        self.assertEqual(n, 0)

    def test_auto_enable_strategies(self):
        t = lp.LivePerformanceTracker(path="data/_t16.json")
        t._disabled_strategies["rec"] = "r"
        for _ in range(12):
            t.record_trade("rec", "P", 1.0, 1.0, 0.0, "LONG")
        n = t.auto_enable_strategies(min_trades=10, min_win_rate=0.45)
        self.assertEqual(n, 1)
        self.assertFalse(t.is_strategy_disabled("rec"))

    def test_auto_enable(self):
        t = lp.LivePerformanceTracker(path="data/_t17.json")
        r = t.get_or_create("a", "P")
        r.record_trade(-1.0, 1.0, 0.0, "LONG")
        r.disable("x")
        for _ in range(6):
            r.record_trade(1.0, 1.0, 0.0, "LONG")
        n = t.auto_enable(min_trades=5, min_win_rate=0.5)
        self.assertEqual(n, 1)
        self.assertFalse(r.disabled)

    def test_auto_enable_skip_enabled(self):
        t = lp.LivePerformanceTracker(path="data/_t18.json")
        r = t.get_or_create("b", "P")
        for _ in range(6):
            r.record_trade(1.0, 1.0, 0.0, "LONG")
        n = t.auto_enable(min_trades=5, min_win_rate=0.5)
        self.assertEqual(n, 0)

    def test_best_worst_strategies(self):
        t = lp.LivePerformanceTracker(path="data/_t19.json")
        ra = t.get_or_create("a", "P")
        rb = t.get_or_create("b", "P")
        for _ in range(3):
            ra.record_trade(1.0, 1.0, 0.0, "LONG")
        for _ in range(3):
            rb.record_trade(-1.0, 1.0, 0.0, "LONG")
        best = t.best_strategies("P", n=5, min_trades=3)
        worst = t.worst_strategies("P", n=5, min_trades=3)
        self.assertEqual(best[0][0], "a")
        self.assertEqual(worst[0][0], "b")

    def test_best_worst_min_trades(self):
        t = lp.LivePerformanceTracker(path="data/_t20.json")
        t.record_trade("a", "P", 1.0, 1.0, 0.0, "LONG")
        self.assertEqual(t.best_strategies("P", min_trades=5), [])

    def test_enabled_disabled_count(self):
        t = lp.LivePerformanceTracker(path="data/_t21.json")
        r = t.get_or_create("a", "P")
        r.disable("x")
        self.assertEqual(t.enabled_count(), 0)
        self.assertEqual(t.disabled_count(), 1)

    def test_summary(self):
        t = lp.LivePerformanceTracker(path="data/_t22.json")
        t.record_trade("a", "P", 1.0, 1.0, 0.0, "LONG", backtest_win_rate=0.8)
        t.record_trade("a", "P", -1.0, 1.0, 0.0, "LONG", backtest_win_rate=0.8)
        s = t.summary(top_n=5)
        self.assertIn("total_records", s)
        self.assertGreaterEqual(s["total_trades"], 2)
        self.assertIn("expectancy", s)
        self.assertIn("divergences", s)

    def test_save_and_load_new_format(self):
        import tempfile, os
        d = tempfile.mkdtemp()
        p = os.path.join(d, "lp.json")
        t = lp.LivePerformanceTracker(path=p)
        t.record_trade("a", "P", 1.0, 1.0, 0.0, "LONG")
        t._disabled_strategies["x"] = "r"
        t.save()
        t2 = lp.LivePerformanceTracker(path=p)
        self.assertEqual(t2.get("a", "P").trades, 1)
        self.assertIn("x", t2._disabled_strategies)

    def test_load_legacy_format(self):
        import tempfile, os, json
        d = tempfile.mkdtemp()
        p = os.path.join(d, "lp_legacy.json")
        r = rec(trades=2, wins=1, pnl=0.5)
        json.dump({"a/P": r.__dict__}, open(p, "w"))
        t = lp.LivePerformanceTracker(path=p)
        self.assertEqual(t.get("a", "P").trades, 2)

    def test_load_missing_file(self):
        t = lp.LivePerformanceTracker(path="data/_does_not_exist_xyz.json")
        self.assertEqual(len(t._records), 0)

    def test_load_corrupt(self):
        import tempfile, os
        d = tempfile.mkdtemp()
        p = os.path.join(d, "corrupt.json")
        open(p, "w").write("{not valid json")
        t = lp.LivePerformanceTracker(path=p)
        self.assertEqual(len(t._records), 0)

    def test_save_failure(self):
        t = lp.LivePerformanceTracker(path="/nonexistent_dir_xyz/lp.json")
        # parent mkdir may fail; ensure no exception propagates
        try:
            t.save()
        except Exception:
            self.fail("save should swallow exceptions")


if __name__ == "__main__":
    unittest.main()
