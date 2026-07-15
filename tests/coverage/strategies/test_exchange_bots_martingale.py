"""Coverage tests for trading_system.strategies.exchange_bots.spot_martingale.

Drives :class:`SpotMartingaleStrategy` through every public branch: warmup /
disabled guards, config validation, down-cross buys (with layer-scaled sizing),
up-cross sells, max-layers capping, out-of-bounds prices, multiplier behaviour,
and independent multi-product state.
"""
import unittest

from pydantic import ValidationError

from trading_system.strategies.exchange_bots.spot_martingale import (
    SpotMartingaleConfig,
    SpotMartingaleStrategy,
)


def make_strategy(**overrides) -> SpotMartingaleStrategy:
    cfg = SpotMartingaleConfig(**overrides)
    return SpotMartingaleStrategy(bot_config=cfg)


def ms(product_id, price, warmup_complete=True, enabled=True):
    return {
        "product_id": product_id,
        "price": price,
        "warmup_complete": warmup_complete,
        "enabled": enabled,
    }


class TestConfigValidation(unittest.TestCase):
    def test_valid_default(self):
        cfg = SpotMartingaleConfig()
        self.assertEqual(cfg.grids, 10)
        self.assertEqual(cfg.multiplier, 1.5)
        self.assertEqual(cfg.max_layers, 0)

    def test_grids_too_small(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(grids=1)

    def test_grids_zero(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(grids=0)

    def test_initial_size_non_positive(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(initial_size=0.0)
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(initial_size=-1.0)

    def test_multiplier_non_positive(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(multiplier=0.0)
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(multiplier=-0.5)

    def test_max_layers_negative(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(max_layers=-1)

    def test_bounds_invalid(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(lower=200.0, upper=200.0)
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(lower=300.0, upper=100.0)

    def test_lower_non_positive(self):
        with self.assertRaises(ValidationError):
            SpotMartingaleConfig(lower=0.0)


class TestGuards(unittest.TestCase):
    def test_warmup_incomplete(self):
        s = make_strategy(lower=100, upper=200, grids=5)
        self.assertIsNone(s.generate_signal(ms("BTC-USD", 150, warmup_complete=False)))

    def test_disabled(self):
        s = make_strategy(lower=100, upper=200, grids=5, enabled=False)
        sig = s.generate_signal(ms("BTC-USD", 150))
        self.assertIsNone(sig)

    def test_price_missing(self):
        s = make_strategy(lower=100, upper=200, grids=5)
        self.assertIsNone(s.generate_signal({"product_id": "BTC-USD"}))


class TestDownCrossBuy(unittest.TestCase):
    def test_buy_top_grid(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0)
        # lines: 200, 175, 150, 125, 100 ; drop to 200 -> buy grid 0
        sig = s.generate_signal(ms("BTC-USD", 200.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.score, 0.8)
        intents = s.order_intents(sig, ms("BTC-USD", 200.0))
        self.assertEqual(intents[0]["side"], "BUY")
        self.assertAlmostEqual(intents[0]["price"], 200.0)
        # layer 0 -> size = initial * mult**0 = 1.0
        self.assertAlmostEqual(intents[0]["size_hint"], 1.0)
        self.assertIn(0, s._filled["BTC-USD"])

    def test_buy_deeper_grid_scales(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0, multiplier=2.0)
        # drop to bottom grid line 4 (price 100) -> buy layer 4
        sig = s.generate_signal(ms("BTC-USD", 100.0))
        intents = s.order_intents(sig, ms("BTC-USD", 100.0))
        self.assertEqual(intents[0]["side"], "BUY")
        self.assertAlmostEqual(intents[0]["price"], 100.0)
        # size = 1.0 * 2**4 = 16.0
        self.assertAlmostEqual(intents[0]["size_hint"], 16.0)
        self.assertIn(4, s._filled["BTC-USD"])
        self.assertEqual(s._layer["BTC-USD"], 4)

    def test_buy_intermediate_grid(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0, multiplier=1.5)
        # price between line1(175) and line2(150): drop to 150 -> buy grid 2
        sig = s.generate_signal(ms("BTC-USD", 150.0))
        intents = s.order_intents(sig, ms("BTC-USD", 150.0))
        self.assertAlmostEqual(intents[0]["size_hint"], 1.0 * (1.5 ** 2))
        self.assertIn(2, s._filled["BTC-USD"])

    def test_no_rebuy_filled_line(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0)
        s.generate_signal(ms("BTC-USD", 200.0))  # buy grid 0
        s._filled["BTC-USD"] = {0}
        s._layer["BTC-USD"] = 0
        # price still at 200 but grid 0 filled -> does not rebuy; it closes (sell)
        sig = s.generate_signal(ms("BTC-USD", 200.0))
        self.assertEqual(sig.score, -0.8)
        intents = s.order_intents(sig, ms("BTC-USD", 200.0))
        self.assertEqual(intents[0]["side"], "SELL")


class TestUpCrossSell(unittest.TestCase):
    def test_sell_filled_grid(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0, multiplier=2.0)
        # realistic top-down filled state reaching grid 2
        s._filled["BTC-USD"] = {0, 1, 2}
        s._layer["BTC-USD"] = 2
        # price rises to line 2 (150) -> sell grid 2, size = 1*2**2 = 4
        sig = s.generate_signal(ms("BTC-USD", 150.0))
        self.assertIsNotNone(sig)
        self.assertEqual(sig.score, -0.8)
        intents = s.order_intents(sig, ms("BTC-USD", 150.0))
        self.assertEqual(intents[0]["side"], "SELL")
        self.assertAlmostEqual(intents[0]["price"], 150.0)
        self.assertAlmostEqual(intents[0]["size_hint"], 4.0)
        self.assertNotIn(2, s._filled["BTC-USD"])

    def test_sell_deepest_filled(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0, multiplier=1.5)
        s._filled["BTC-USD"] = {0, 1, 3}
        s._layer["BTC-USD"] = 3
        # price rises to line 0 (200): deepest filled <= p is grid 3 (largest i)
        sig = s.generate_signal(ms("BTC-USD", 200.0))
        intents = s.order_intents(sig, ms("BTC-USD", 200.0))
        self.assertEqual(intents[0]["side"], "SELL")
        self.assertAlmostEqual(intents[0]["price"], 125.0)
        self.assertNotIn(3, s._filled["BTC-USD"])

    def test_no_sell_when_nothing_filled(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0)
        # nothing filled, price inside -> no buy (all unfilled below? at 150 line2 unfilled -> buy!)
        # use crafted state to hit final return None
        s._filled["BTC-USD"] = {0, 1}
        s._layer["BTC-USD"] = 1
        # price between line1(175) and line2(150): 160 -> line>=p:0,1 filled; line<=p:2.. unfilled
        self.assertIsNone(s.generate_signal(ms("BTC-USD", 160.0)))


class TestMaxLayers(unittest.TestCase):
    def test_max_layers_blocks_deep_buy(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0, max_layers=2)
        # drop to grid 4 (>= max_layers) -> skipped, nothing filled before -> None
        self.assertIsNone(s.generate_signal(ms("BTC-USD", 100.0)))
        self.assertEqual(s._filled["BTC-USD"], set())

    def test_max_layers_allows_shallow_buy(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0, max_layers=2)
        # drop to grid 1 (< max_layers) -> buy
        sig = s.generate_signal(ms("BTC-USD", 175.0))
        self.assertIsNotNone(sig)
        self.assertIn(1, s._filled["BTC-USD"])


class TestOutOfBounds(unittest.TestCase):
    def test_below_lower(self):
        s = make_strategy(lower=100, upper=200, grids=5)
        self.assertIsNone(s.generate_signal(ms("BTC-USD", 50.0)))

    def test_above_upper(self):
        s = make_strategy(lower=100, upper=200, grids=5)
        self.assertIsNone(s.generate_signal(ms("BTC-USD", 250.0)))


class TestMultiplierBehavior(unittest.TestCase):
    def test_multiplier_growth(self):
        s = make_strategy(lower=100, upper=400, grids=4, initial_size=2.0, multiplier=3.0)
        # lines: 400, 300, 200, 100; buy grid0 size=2, grid1 size=6, grid2 size=18
        s.generate_signal(ms("BTC-USD", 400.0))
        self.assertAlmostEqual(
            s.order_intents(
                s.generate_signal(ms("BTC-USD", 400.0)), ms("BTC-USD", 400.0)
            )[0]["size_hint"],
            2.0,
        )
        s._filled["BTC-USD"] = set()
        sig = s.generate_signal(ms("BTC-USD", 300.0))
        self.assertAlmostEqual(
            s.order_intents(sig, ms("BTC-USD", 300.0))[0]["size_hint"], 6.0
        )
        s._filled["BTC-USD"] = set()
        sig = s.generate_signal(ms("BTC-USD", 200.0))
        self.assertAlmostEqual(
            s.order_intents(sig, ms("BTC-USD", 200.0))[0]["size_hint"], 18.0
        )


class TestMultipleProducts(unittest.TestCase):
    def test_independent_state(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0)
        # product A buys grid 0
        s.generate_signal(ms("BTC-USD", 200.0))
        # product B independent
        sig_b = s.generate_signal(ms("ETH-USD", 200.0))
        self.assertIsNotNone(sig_b)
        self.assertIn(0, s._filled["BTC-USD"])
        self.assertIn(0, s._filled["ETH-USD"])
        # A's state unchanged by B
        self.assertEqual(s._filled["BTC-USD"], {0})

    def test_separate_filled_sets(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1.0)
        s.generate_signal(ms("BTC-USD", 100.0))  # buy deepest
        self.assertIn(4, s._filled["BTC-USD"])
        self.assertNotIn("ETH-USD", s._filled)


class TestOrderIntents(unittest.TestCase):
    def test_no_intent_without_record(self):
        s = make_strategy(lower=100, upper=200, grids=5)
        from trading_system.strategies.base.interfaces import StrategySignal

        sig = StrategySignal(
            strategy_id="spot_martingale",
            product_id="BTC-USD",
            score=0.8,
            reason="x",
        )
        self.assertEqual(s.order_intents(sig, ms("BTC-USD", 150.0)), [])

    def test_min_size_floor(self):
        s = make_strategy(lower=100, upper=200, grids=5, initial_size=1e-9, min_size=0.001)
        sig = s.generate_signal(ms("BTC-USD", 200.0))
        intents = s.order_intents(sig, ms("BTC-USD", 200.0))
        self.assertAlmostEqual(intents[0]["size_hint"], 0.001)


if __name__ == "__main__":
    unittest.main()
