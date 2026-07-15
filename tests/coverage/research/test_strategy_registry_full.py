import io
import json
import runpy
import unittest
from contextlib import redirect_stdout

import trading_system.catalog.strategy_registry as reg
from trading_system.catalog.strategy_registry import (
    StrategyRegistry,
    StrategyMetadata,
    list_all_phase1_strategies,
)


def _make(category, name="s"):
    return StrategyMetadata(
        name=name, category=category, description="d",
        expected_win_rate_min=40, expected_win_rate_max=55,
        target_profit_factor=1.3, regime_classifications=["TRENDED_REGIME"],
        implementation_status="production",
    )


class TestStrategyRegistryFull(unittest.TestCase):
    def test_register_multiple(self):
        r = StrategyRegistry()
        r.register_strategy("a", _make("trend_following", "a"))
        r.register_strategy("b", _make("mean_reversion", "b"))
        r.register_strategy("c", _make("arbitrage", "c"))
        self.assertEqual(set(r.get_all_strategy_names()), {"a", "b", "c"})
        self.assertEqual(len(r.get_strategies_by_category("trend_following")), 1)
        self.assertEqual(len(r.get_strategies_by_category("mean_reversion")), 1)
        self.assertEqual(len(r.get_strategies_by_category("arbitrage")), 1)

    def test_duplicate_registration_overwrites(self):
        r = StrategyRegistry()
        r.register_strategy("a", _make("trend_following", "a"))
        r.register_strategy("a", _make("arbitrage", "a"))
        self.assertEqual(r.get_all_strategy_names(), ["a"])
        self.assertEqual(r.get_strategies_by_category("trend_following"), [])
        self.assertEqual(r.get_strategies_by_category("arbitrage")[0].name, "a")

    def test_get_by_missing_category(self):
        r = StrategyRegistry()
        r.register_strategy("a", _make("trend_following", "a"))
        self.assertEqual(r.get_strategies_by_category("volatility"), [])
        self.assertEqual(r.get_strategies_by_category("nonexistent"), [])

    def test_register_then_lookup_miss(self):
        r = StrategyRegistry()
        r.register_strategy("a", _make("mean_reversion", "a"))
        self.assertEqual(r.get_strategies_by_category("trend_following"), [])

    def test_empty_registry_list_names(self):
        r = StrategyRegistry()
        self.assertEqual(r.get_all_strategy_names(), [])
        self.assertEqual(r.get_strategies_by_category("arbitrage"), [])

    def test_list_all_phase1_all_categories(self):
        results = list_all_phase1_strategies()
        cats = {r["category"] for r in results}
        self.assertEqual(cats, {"trend_following", "mean_reversion", "arbitrage"})
        for r in results:
            self.assertIn("expected_win_rate_min", r)
            self.assertIn("target_profit_factor", r)
            self.assertIn("regimes", r)
            self.assertIn("status", r)

    def test_metadata_fields(self):
        m = _make("volatility", "vol")
        self.assertEqual(m.category, "volatility")
        self.assertEqual(m.expected_win_rate_min, 40)
        self.assertEqual(m.regime_classifications, ["TRENDED_REGIME"])

    def test_main_block_output(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            runpy.run_path(reg.__file__, run_name="__main__")
        out = buf.getvalue()
        self.assertIn("PHASE 1", out)
        self.assertIn("macdsignalcrossover", out)
        self.assertIn("SCALING PATH", out)

    def test_json_serializable(self):
        m = _make("trend_following", "a")
        payload = json.dumps({
            "name": m.name, "category": m.category,
            "regimes": m.regime_classifications,
        })
        self.assertIsInstance(payload, str)


if __name__ == "__main__":
    unittest.main()
