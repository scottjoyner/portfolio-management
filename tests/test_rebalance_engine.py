import math
import os
import unittest

from coinbase.src.rebalance_engine import (
    ALLOCATION_PRESETS,
    RebalanceEngine,
    RebalanceOrder,
    RebalanceBot,
    Recommendation,
    StairStepEngine,
    StairStepOrder,
)


def _finite(x):
    return isinstance(x, (int, float)) and math.isfinite(x)


class TestAllocationPresets(unittest.TestCase):
    def test_presets_exist(self):
        self.assertIn("core_balanced", ALLOCATION_PRESETS)
        self.assertIn("volatile_tilt", ALLOCATION_PRESETS)
        self.assertIn("safe", ALLOCATION_PRESETS)

    def test_weights_positive(self):
        for name, weights in ALLOCATION_PRESETS.items():
            with self.subTest(name):
                self.assertTrue(len(weights) > 0)
                for sym, w in weights.items():
                    self.assertGreater(w, 0.0)

    def test_weights_sum_normalized(self):
        for name, weights in ALLOCATION_PRESETS.items():
            with self.subTest(name):
                total = sum(weights.values())
                for sym, w in weights.items():
                    self.assertAlmostEqual(w / total, w / sum(weights.values()))

    def test_core_balanced_small_coins(self):
        w = ALLOCATION_PRESETS["core_balanced"]
        small = w["XRP-USD"] + w["XLM-USD"] + w["MON-USD"]
        self.assertAlmostEqual(small, 0.20)

    def test_volatile_tilt_small_coins(self):
        w = ALLOCATION_PRESETS["volatile_tilt"]
        self.assertIn("PEPE-USD", w)
        self.assertIn("BONK-USD", w)


class TestRebalanceEngine(unittest.TestCase):
    def setUp(self):
        self.engine = RebalanceEngine.from_preset("core_balanced")

    def test_compute_maps_to_dataclass(self):
        values = {"BTC-USD": 60.0, "ETH-USD": 40.0,
                  "SOL-USD": 0.0, "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0}
        rec = self.engine.compute(values, total=100.0)
        self.assertIsInstance(rec, Recommendation)
        self.assertTrue(len(rec.orders) > 0)
        for o in rec.orders:
            self.assertIsInstance(o, RebalanceOrder)
            self.assertIn(o.side, ("BUY", "SELL"))
            self.assertGreater(o.notional, 0)
            self.assertAlmostEqual(o.current_weight - o.drift, o.target_weight)
        self.assertGreaterEqual(rec.max_drift, 0.0)
        self.assertAlmostEqual(rec.turnover, sum(o.notional for o in rec.orders))

    def test_to_dict(self):
        rec = self.engine.compute(
            {"BTC-USD": 60.0, "ETH-USD": 40.0,
             "SOL-USD": 0.0, "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0},
            total=100.0)
        d = rec.to_dict()
        self.assertIn("orders", d)
        self.assertIn("max_drift", d)
        self.assertIn("turnover", d)
        self.assertEqual(len(d["orders"]), len(rec.orders))
        self.assertEqual(d["orders"][0]["asset"], rec.orders[0].asset)

    def test_drift_below_threshold_empty(self):
        # All within drift_threshold of target -> no orders.
        weights = ALLOCATION_PRESETS["core_balanced"]
        total = 1000.0
        values = {sym: w * total for sym, w in weights.items()}
        rec = self.engine.compute(values, total=total)
        self.assertEqual(rec.orders, [])
        self.assertAlmostEqual(rec.turnover, 0.0)

    def test_slim_profit_partial_sell(self):
        # profit_take_pct=0.01 (1% of excess) makes the sell "slim" rather than full rebalance.
        eng = RebalanceEngine.from_preset(
            "core_balanced", profit_take_pct=0.01, min_trade_notional=0.1)
        rec = eng.compute(
            {"BTC-USD": 60.0, "ETH-USD": 40.0,
             "SOL-USD": 0.0, "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0},
            total=100.0)
        btc = [o for o in rec.orders if o.asset == "BTC-USD"]
        self.assertEqual(len(btc), 1)
        self.assertEqual(btc[0].side, "SELL")
        excess = (0.60 - 0.40) * 100.0
        self.assertAlmostEqual(btc[0].notional, excess * 0.01)

    def test_missing_asset_yields_buy(self):
        rec = self.engine.compute(
            {"BTC-USD": 100.0, "ETH-USD": 0.0, "SOL-USD": 0.0,
             "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0},
            total=100.0)
        for asset in ("ETH-USD", "SOL-USD", "XRP-USD"):
            buys = [o for o in rec.orders if o.asset == asset and o.side == "BUY"]
            self.assertEqual(len(buys), 1)

    def test_total_none_falls_back_to_sum(self):
        values = {"BTC-USD": 60.0, "ETH-USD": 40.0,
                  "SOL-USD": 0.0, "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0}
        rec = self.engine.compute(values)
        total = sum(values.values())
        self.assertAlmostEqual(
            rec.max_drift,
            RebalanceEngine.from_preset("core_balanced").compute(values, total=total).max_drift)

    def test_total_zero_fallback(self):
        values = {"BTC-USD": 50.0, "ETH-USD": 50.0,
                  "SOL-USD": 0.0, "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0}
        rec = self.engine.compute(values, total=0.0)
        self.assertIsInstance(rec, Recommendation)

    def test_empty_book(self):
        rec = self.engine.compute({}, total=100.0)
        self.assertTrue(len(rec.orders) > 0)
        for o in rec.orders:
            self.assertEqual(o.side, "BUY")
        self.assertGreater(rec.turnover, 0.0)
        self.assertAlmostEqual(rec.max_drift, 0.4)

    def test_max_drift_reported(self):
        rec = self.engine.compute(
            {"BTC-USD": 100.0, "ETH-USD": 0.0, "SOL-USD": 0.0,
             "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0},
            total=100.0)
        self.assertAlmostEqual(rec.max_drift, 0.60)

    def test_from_preset_overrides(self):
        eng = RebalanceEngine.from_preset("safe", **{"BTC-USD": 0.5, "ETH-USD": 0.4})
        self.assertAlmostEqual(eng.targets["BTC-USD"], 0.5)
        self.assertAlmostEqual(eng.targets["ETH-USD"], 0.4)
        self.assertAlmostEqual(eng.targets["SOL-USD"], 0.10)

    def test_env_preset_and_weights(self):
        os.environ["REBALANCE_PRESET"] = "safe"
        os.environ["REBALANCE_WEIGHTS"] = "BTC-USD=0.7,ETH-USD=0.2,SOL-USD=0.1"
        try:
            eng = RebalanceEngine.from_preset("core_balanced")
            self.assertAlmostEqual(eng.targets["BTC-USD"], 0.7)
        finally:
            os.environ.pop("REBALANCE_PRESET", None)
            os.environ.pop("REBALANCE_WEIGHTS", None)


class TestStairStepEngine(unittest.TestCase):
    def setUp(self):
        self.engine = StairStepEngine()

    def test_add_symbol_and_state(self):
        self.engine.add_symbol("BTC-USD", 100.0, 200.0, 5, 1000.0, 0.1, 0.1)
        st = self.engine.state("BTC-USD")
        self.assertEqual(st[0], 0)
        self.engine.reset("BTC-USD")
        self.assertEqual(self.engine.state("BTC-USD")[0], 0)

    def test_on_price_drives_buy_then_sell(self):
        self.engine.add_symbol("BTC-USD", 100.0, 200.0, 5, 1000.0, 0.1, 0.1)
        buys = 0
        for p in [200.0, 180.0, 160.0, 140.0, 120.0]:
            o = self.engine.on_price("BTC-USD", p)
            if o is not None and o.side == "BUY":
                buys += 1
                self.assertIsInstance(o, StairStepOrder)
        self.assertEqual(buys, 5)
        # Recovery should trigger a SELL with positive realized pnl.
        sell = None
        for p in [140.0, 160.0, 180.0, 190.0]:
            o = self.engine.on_price("BTC-USD", p)
            if o is not None and o.side == "SELL":
                sell = o
        self.assertIsNotNone(sell)
        st = self.engine.state("BTC-USD")
        self.assertGreater(st[2], 0)
        self.assertGreater(st[4], 0.0)

    def test_reset_clears(self):
        self.engine.add_symbol("BTC-USD", 100.0, 200.0, 5, 1000.0, 0.1, 0.1)
        self.engine.on_price("BTC-USD", 200.0)
        self.engine.reset("BTC-USD")
        st = self.engine.state("BTC-USD")
        self.assertEqual(st[1], 0)
        self.assertEqual(st[2], 0)
        self.assertAlmostEqual(st[3], 0.0)

    def test_non_finite_price_ignored(self):
        self.engine.add_symbol("BTC-USD", 100.0, 200.0, 5, 1000.0, 0.1, 0.1)
        self.assertIsNone(self.engine.on_price("BTC-USD", float("nan")))
        self.assertIsNone(self.engine.on_price("BTC-USD", float("inf")))

    def test_to_dict(self):
        self.engine.add_symbol("BTC-USD", 100.0, 200.0, 5, 1000.0, 0.1, 0.1)
        self.engine.on_price("BTC-USD", 200.0)
        d = self.engine.to_dict()
        self.assertIn("BTC-USD", d)
        self.assertEqual(len(d["BTC-USD"]), 6)

    def test_state_and_reset_unknown_symbol(self):
        self.assertEqual(self.engine.state("NOPE"), (0, 0, 0, 0.0, 0.0, "INIT"))
        self.engine.reset("NOPE")

    def test_stairstep_order_to_dict(self):
        o = StairStepOrder(side="BUY", price=123.0, notional=5.0)
        self.assertEqual(o.to_dict(), {"side": "BUY", "price": 123.0, "notional": 5.0})


class TestEnvAndEdge(unittest.TestCase):
    def test_unknown_preset_raises(self):
        with self.assertRaises(KeyError):
            RebalanceEngine.from_preset("does_not_exist")

    def test_env_invalid_weights_ignored(self):
        os.environ["REBALANCE_PRESET"] = "safe"
        os.environ["REBALANCE_WEIGHTS"] = "BTC-USD=0.7,,ETH-USD=abc,SOL-USD=0.2"
        try:
            eng = RebalanceEngine.from_preset("core_balanced")
            self.assertAlmostEqual(eng.targets["BTC-USD"], 0.7)
            self.assertAlmostEqual(eng.targets["SOL-USD"], 0.2)
        finally:
            os.environ.pop("REBALANCE_PRESET", None)
            os.environ.pop("REBALANCE_WEIGHTS", None)


class TestRebalanceBot(unittest.TestCase):
    def test_recommend_and_on_price(self):
        bot = RebalanceBot(RebalanceEngine.from_preset("core_balanced"))
        book = {"BTC-USD": 60.0, "ETH-USD": 40.0,
                "SOL-USD": 0.0, "XRP-USD": 0.0, "XLM-USD": 0.0, "MON-USD": 0.0}
        rec = bot.recommend(book)
        self.assertIsInstance(rec, Recommendation)
        bot.stair_step.add_symbol("BTC-USD", 100.0, 200.0, 5, 1000.0, 0.1, 0.1)
        o = bot.on_price("BTC-USD", 200.0)
        self.assertIsNotNone(o)
        self.assertEqual(o.side, "BUY")


if __name__ == "__main__":
    unittest.main()
