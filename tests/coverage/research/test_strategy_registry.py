import io
import runpy
import unittest
from contextlib import redirect_stdout

import trading_system.catalog.strategy_registry as reg
from trading_system.catalog.strategy_registry import (
    StrategyRegistry,
    StrategyMetadata,
    list_all_phase1_strategies,
)


class TestStrategyRegistry(unittest.TestCase):
    def test_strategy_metadata(self):
        m = StrategyMetadata(
            name="x",
            category="trend_following",
            description="d",
            expected_win_rate_min=40,
            expected_win_rate_max=55,
            target_profit_factor=1.3,
            regime_classifications=["TRENDED_REGIME"],
            implementation_status="production",
        )
        self.assertEqual(m.name, "x")

    def test_register_and_query(self):
        r = StrategyRegistry()
        r.register_strategy("foo", StrategyMetadata(
            name="foo", category="arbitrage", description="d",
            expected_win_rate_min=1, expected_win_rate_max=2,
            target_profit_factor=1.0, regime_classifications=[], implementation_status="dev"))
        self.assertEqual(r.get_strategies_by_category("arbitrage")[0].name, "foo")
        self.assertEqual(r.get_all_strategy_names(), ["foo"])
        # Empty category
        self.assertEqual(r.get_strategies_by_category("mean_reversion"), [])

    def test_list_all_phase1(self):
        results = list_all_phase1_strategies()
        names = {r["name"] for r in results}
        self.assertIn("macdsignalcrossover", names)
        self.assertIn("zscorearb", names)
        self.assertIn("spotfuturesbasisarb", names)

    def test_list_all_with_empty_registry(self):
        orig = reg.registry
        reg.registry = StrategyRegistry()
        try:
            self.assertEqual(list_all_phase1_strategies(), [])
        finally:
            reg.registry = orig

    def test_main_block(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            runpy.run_path(reg.__file__, run_name="__main__")
        self.assertIn("PHASE 1", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
