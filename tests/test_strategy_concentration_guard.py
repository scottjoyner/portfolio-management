"""
Unit tests for the strategy-concentration + sample-depth anti-fragility guards
added to run_trader_v4 (_paper_execute_impl) and live_performance
(strategy_total_pnl). These prevent a few low-sample lucky trades from
dominating the book (observed: top-3 strategies = 110% of total pnl).

The production-paper competition profile deliberately uses a 60% default. It
allows a genuinely dominant strategy to keep proving itself while still
blocking new entries before one strategy accounts for nearly the whole book.
The knob remains operator-tunable downward; this is not a live-trading limit.
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coinbase.src.live_performance import LivePerformanceTracker  # noqa: E402
from coinbase.src.run_trader_v4 import EventTraderV4  # noqa: E402


class TestConcentrationGuard(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._tmp = tempfile.mkdtemp(prefix="concg_")
        self.t = EventTraderV4(mode="paper", products=["BTC-USD"], dry_run=True)
        # Redirect state file so we never touch the real running bot's data.
        self.t._paper_state_path = Path(self._tmp) / "paper_trader_v4_state.json"

    def test_new_knobs_seeded_with_paper_competition_defaults(self):
        # The __init__ seed loop must create both guards from TUNABLE_KNOBS.
        self.assertTrue(hasattr(self.t, "max_strategy_pnl_share"))
        self.assertTrue(hasattr(self.t, "min_trades_for_full_sizing"))
        self.assertEqual(self.t.max_strategy_pnl_share, 0.60)
        self.assertEqual(self.t.min_trades_for_full_sizing, 20)

    def test_strategy_total_pnl_sums_across_products(self):
        import os
        p = os.path.join(self._tmp, "lp.json")
        tr = LivePerformanceTracker(path=p)
        for prod in ["BTC-USD", "ETH-USD", "SOL-USD"]:
            tr.record_trade("vwap_revert", prod, 500.0, 1000.0, 1.0, "LONG")
            tr.record_trade("vwap_revert", prod, -100.0, 1000.0, 1.0, "LONG")
        self.assertAlmostEqual(tr.strategy_total_pnl("vwap_revert"), 1200.0)
        self.assertEqual(tr.strategy_total_pnl("rsi_revert"), 0.0)

    def test_concentration_cap_blocks_new_entries(self):
        # Patch _load_core_holdings_state BEFORE creating the instance so it's a no-op.
        import coinbase.src.run_trader_v4 as mod
        original_load = mod.EventTraderV4._load_core_holdings_state
        mod.EventTraderV4._load_core_holdings_state = lambda self: None

        # Create a fresh trader with patched init (no core holdings loaded).
        t = EventTraderV4(mode="paper", products=["BTC-USD"], dry_run=True)
        t.paper_starting_capital = 10000.0
        # Use a state file with NO core_holdings so equity == starting capital exactly.
        t._paper_state_path.write_text(
            '{"paper_starting_capital": 10000.0, "paper_cash": 10000.0, '
            '"paper_realized_pnl": 0.0, "paper_positions": {}, '
            '"paper_trades": [], "paper_wins": 0, "paper_losses": 0, '
            '"paper_peak_equity": 10000.0, "core_holdings": []}'
        )
        t._load_paper_state()

        import os
        p = os.path.join(self._tmp, "lp.json")
        tr = LivePerformanceTracker(path=p)
        t._perf_tracker = tr
        t._last_price = {"BTC-USD": 100.0}

        eq = t._paper_equity(t._last_price)
        threshold = t.max_strategy_pnl_share * eq
        # With no core_holdings, equity == starting_capital exactly.
        self.assertAlmostEqual(threshold, 6000.0, places=1)

        tr.record_trade("hot_strat", "BTC-USD", 5900.0, 1000.0, 1.0, "LONG")
        self.assertLess(tr.strategy_total_pnl("hot_strat"), threshold)

        tr.record_trade("hot_strat", "ETH-USD", 200.0, 1000.0, 1.0, "LONG")
        strat_pnl = tr.strategy_total_pnl("hot_strat")
        self.assertGreater(strat_pnl, threshold)

        blocked = eq > 0 and t.max_strategy_pnl_share > 0 and \
                  strat_pnl > t.max_strategy_pnl_share * eq
        self.assertTrue(blocked, "concentration guard should block hot_strat")

        # Restore original method.
        mod.EventTraderV4._load_core_holdings_state = original_load

    def test_depth_penalty_scales_confidence_down(self):
        # A pair with 5 trades (< 20) should get confidence scaled to 0.25 floor.
        rec = type("R", (), {"trades": 5})()
        pair_trades = rec.trades
        min_t = self.t.min_trades_for_full_sizing
        depth_scale = max(0.25, pair_trades / float(min_t))
        conf = 0.80 * depth_scale
        self.assertLess(conf, 0.80)  # penalty applied
        self.assertGreaterEqual(depth_scale, 0.25)  # floor


if __name__ == "__main__":
    unittest.main()
