import json
import os
import unittest
from unittest import mock

from coinbase.src.ranking import (
    StrategyRanking, StrategyStats, TopRankedStrategyWrapper, StrategyRankingFilter,
)
from coinbase.src.protocols import Direction, Opportunity, BaseStrategy, BracketSetup


class InnerStrat(BaseStrategy):
    def __init__(self, name="ema_cross"):
        self._n = name

    def name(self):
        return self._n

    def on_bar(self, bar, history):
        return "bar"


def make_opp(strategy_name, conf=0.5):
    return Opportunity(product_id="BTC-USD", direction=Direction.LONG,
                       instrument_type=None, entry_price=100, stop_price=90,
                       target_price=110, risk_reward=2, confidence=conf,
                       reason="r", strategy_name=strategy_name)


class TestStrategyRanking(unittest.TestCase):
    def test_record_trade_and_sharpe(self):
        r = StrategyRanking(min_trades=3)
        for pnl in [0.1, -0.05, 0.2, 0.05, 0.15]:
            r.record_trade("s1", pnl, 0.6)
        stat = r._stats["s1"]
        self.assertEqual(stat.trades, 5)
        self.assertGreater(stat.win_rate, 0)
        self.assertNotEqual(stat.sharpe, 0.0)
        self.assertGreaterEqual(stat.max_drawdown, 0)

    def test_record_trade_drawdown(self):
        r = StrategyRanking(min_trades=2)
        r.record_trade("s", 1.0, 0.5)
        r.record_trade("s", -2.0, 0.5)
        self.assertGreater(r._stats["s"].max_drawdown, 0)

    def test_rank_all(self):
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1, 0.2, 0.15, 0.05, 0.2]:
            r.record_trade("good", pnl, 0.6)
        for pnl in [-0.1, -0.2]:
            r.record_trade("bad", pnl, 0.4)
        ranked = r.rank_all()
        self.assertTrue(any(n == "good" for n, _ in ranked))
        self.assertTrue(all(-100 <= score <= 100 for _, score in ranked))

    def test_rank_all_below_min_trades_skipped(self):
        r = StrategyRanking(min_trades=5)
        r.record_trade("few", 0.1, 0.5)
        self.assertEqual(r.rank_all(), [])

    def test_should_rebalance(self):
        r = StrategyRanking(rebalance_bars=3)
        self.assertFalse(r.should_rebalance())
        self.assertFalse(r.should_rebalance())
        self.assertTrue(r.should_rebalance())
        self.assertFalse(r.should_rebalance())

    def test_rebalance_weights(self):
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1, 0.2, 0.15, 0.05, 0.2]:
            r.record_trade("good", pnl, 0.6)
        for pnl in [0.05, 0.1]:
            r.record_trade("ok", pnl, 0.5)
        r.rank_all()
        weights = r.rebalance_weights()
        self.assertIn("good", weights)
        self.assertGreater(sum(weights.values()), 0)

    def test_rebalance_weights_empty(self):
        r = StrategyRanking()
        self.assertEqual(r.rebalance_weights(), {})

    def test_rebalance_weights_total_zero(self):
        r = StrategyRanking(min_trades=1)
        r._ranked = [("x", -100.0), ("y", -100.0)]
        r._stats["x"] = StrategyStats(name="x")
        r._stats["y"] = StrategyStats(name="y")
        self.assertEqual(r.rebalance_weights(), {})

    def test_top_strategies(self):
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1, 0.2, 0.15, 0.05, 0.2]:
            r.record_trade("good", pnl, 0.6)
        r.rank_all()
        self.assertIn("good", r.top_strategies())

    def test_to_from_dict(self):
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1, 0.2, 0.15, 0.05, 0.2]:
            r.record_trade("good", pnl, 0.6)
        r.rank_all()
        d = r.to_dict()
        r2 = StrategyRanking.from_dict(d)
        self.assertIn("good", r2._stats)

    def test_save_load(self, tmp_path=None):
        import tempfile
        d = tempfile.mkdtemp()
        path = os.path.join(d, "rank.json")
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1, 0.2, 0.15, 0.05, 0.2]:
            r.record_trade("good", pnl, 0.6)
        r.rank_all()
        r.save(path)
        self.assertTrue(os.path.exists(path))
        r2 = StrategyRanking()
        r2.load(path)
        self.assertIn("good", r2._stats)

    def test_save_exception(self):
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1] * 5:
            r.record_trade("good", pnl, 0.6)
        with mock.patch("coinbase.src.ranking.json.dump", side_effect=OSError("disk")):
            with self.assertLogs("coinbase.src.ranking", level="WARNING"):
                r.save("/tmp/x.json")

    def test_load_exception(self, tmp_path=None):
        import tempfile
        d = tempfile.mkdtemp()
        path = os.path.join(d, "rank.json")
        with open(path, "w") as f:
            f.write("{}")
        r = StrategyRanking()
        with mock.patch("coinbase.src.ranking.json.load", side_effect=ValueError("bad")):
            with self.assertLogs("coinbase.src.ranking", level="WARNING"):
                r.load(path)

    def test_summary(self):
        r = StrategyRanking(min_trades=2)
        for pnl in [0.1] * 5:
            r.record_trade("good", pnl, 0.6)
        r.rank_all()
        s = r.summary()
        self.assertEqual(s["min_trades_threshold"], 2)
        self.assertGreaterEqual(s["total_tracked"], 1)


class TestWrappers(unittest.TestCase):
    def test_top_ranked_wrapper(self):
        ranking = StrategyRanking(min_trades=2)
        for pnl in [0.1] * 5:
            ranking.record_trade("ema_cross", pnl, 0.6)
        ranking.rank_all()
        inner = InnerStrat("ema_cross")
        w = TopRankedStrategyWrapper(inner, ranking)
        self.assertEqual(w.name(), "ranked_ema_cross")
        self.assertIsNotNone(w.on_bar(None, []))
        # not ranked -> None
        ranking2 = StrategyRanking(min_trades=2)
        w2 = TopRankedStrategyWrapper(InnerStrat("other"), ranking2)
        self.assertIsNone(w2.on_bar(None, []))

    def test_top_ranked_wrapper_set_product_id(self):
        w = TopRankedStrategyWrapper(InnerStrat(), StrategyRanking())
        w.set_product_id("ETH-USD")

    def test_ranking_filter(self):
        ranking = StrategyRanking(min_trades=2)
        for pnl in [0.1] * 5:
            ranking.record_trade("ema_cross", pnl, 0.6)
        ranking.rank_all()
        f = StrategyRankingFilter(ranking)
        opps = [make_opp("ema_cross"), make_opp("unknown")]
        filtered = f.filter_opportunities(opps)
        self.assertEqual(len(filtered), 1)
        weighted = f.weight_opportunities(opps)
        self.assertEqual(len(weighted), 2)
        self.assertLessEqual(weighted[0].confidence, 0.99)


class TestRankingEdgeCases(unittest.TestCase):
    def test_load_nonexistent_path_is_noop(self):
        r = StrategyRanking()
        r.load("/tmp/does_not_exist_xyz.json")  # should not raise
        self.assertEqual(r._stats, {})

    def test_wrapper_ranked_but_name_not_present(self):
        ranking = StrategyRanking(min_trades=2)
        for pnl in [0.1] * 5:
            ranking.record_trade("good", pnl, 0.6)
        ranking.rank_all()
        w = TopRankedStrategyWrapper(InnerStrat("other"), ranking)
        self.assertIsNone(w.on_bar(None, []))

    def test_record_trade_truncates_recent_returns(self):
        r = StrategyRanking(lookback_max=10)
        for i in range(25):
            r.record_trade("s", 0.1, 0.5)
        self.assertLessEqual(len(r._stats["s"].recent_returns), 10)

    def test_record_trade_fewer_than_5_returns_no_sharpe(self):
        r = StrategyRanking(min_trades=1)
        for pnl in [0.1, -0.05, 0.2]:
            r.record_trade("s", pnl, 0.5)
        self.assertEqual(r._stats["s"].sharpe, 0.0)


if __name__ == "__main__":
    unittest.main()
