"""Coverage tests for trading_system.strategies.exchange_bots.dca.

Exercises the DcaStrategy across every branch: warmup gate, disabled config,
zero/negative price and amount, first buy, interval gating, drop-trigger
(met vs not met), max_buys enforcement, reference-price tracking, multiple
products, base_price seeding, and the timestamp-less step-counter fallback.
"""

import unittest

from trading_system.strategies.exchange_bots.dca import DcaConfig, DcaStrategy


def make_strategy(**cfg):
    params = {
        "interval_seconds": 100.0,
        "amount_usd": 100.0,
        "max_buys": 3,
    }
    params.update(cfg)
    return DcaStrategy(bot_config=DcaConfig(**params))


def ms(price=100.0, ts=0.0, product="BTC-USD", warmup=True, **extra):
    state = {
        "product_id": product,
        "price": price,
        "timestamp": ts,
        "warmup_complete": warmup,
    }
    state.update(extra)
    return state


class TestConfig(unittest.TestCase):
    def test_config_model(self):
        self.assertIs(DcaStrategy.config_model, DcaConfig)

    def test_defaults(self):
        s = DcaStrategy()
        self.assertEqual(s.bot_config.trigger_drop_pct, 0.0)
        self.assertEqual(s.bot_config.base_price, 0.0)
        self.assertEqual(s.strategy_id, "dca")


class TestGuards(unittest.TestCase):
    def test_warmup_incomplete(self):
        s = make_strategy()
        self.assertIsNone(s.generate_signal(ms(warmup=False)))

    def test_disabled(self):
        s = make_strategy(enabled=False)
        self.assertIsNone(s.generate_signal(ms()))

    def test_zero_price(self):
        s = make_strategy()
        self.assertIsNone(s.generate_signal(ms(price=0.0)))

    def test_amount_non_positive(self):
        s = make_strategy(amount_usd=0.0)
        self.assertIsNone(s.generate_signal(ms()))


class TestFirstBuy(unittest.TestCase):
    def test_first_buy_emits(self):
        s = make_strategy()
        sig = s.generate_signal(ms(price=100.0, ts=0.0))
        self.assertIsNotNone(sig)
        self.assertGreater(sig.score, 0)
        self.assertEqual(s._buys["BTC-USD"], 1)
        self.assertEqual(s._ref["BTC-USD"], 100.0)
        # Recorded order intent size = amount_usd / price.
        intents = s.order_intents(sig, ms())
        self.assertEqual(intents[0]["side"], "BUY")
        self.assertAlmostEqual(intents[0]["size_hint"], 1.0)

    def test_first_buy_uses_base_price(self):
        s = make_strategy(base_price=200.0)
        s.generate_signal(ms(price=100.0, ts=0.0))
        self.assertEqual(s._ref["BTC-USD"], 200.0)


class TestInterval(unittest.TestCase):
    def test_skip_when_interval_not_elapsed(self):
        s = make_strategy(interval_seconds=100.0)
        s.generate_signal(ms(price=100.0, ts=0.0))
        self.assertIsNone(s.generate_signal(ms(price=100.0, ts=50.0)))
        self.assertEqual(s._buys["BTC-USD"], 1)

    def test_buy_after_interval(self):
        s = make_strategy(interval_seconds=100.0)
        s.generate_signal(ms(price=100.0, ts=0.0))
        sig = s.generate_signal(ms(price=90.0, ts=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(s._buys["BTC-USD"], 2)
        self.assertEqual(s._ref["BTC-USD"], 90.0)
        self.assertEqual(s._last_buy_ts["BTC-USD"], 100.0)


class TestDropTrigger(unittest.TestCase):
    def test_drop_not_met_skips(self):
        s = make_strategy(interval_seconds=100.0, trigger_drop_pct=0.1)
        s.generate_signal(ms(price=100.0, ts=0.0))
        # Price only dropped 5% (< 10% required) -> skip, timer not reset.
        self.assertIsNone(s.generate_signal(ms(price=95.0, ts=100.0)))
        self.assertEqual(s._buys["BTC-USD"], 1)
        self.assertEqual(s._last_buy_ts["BTC-USD"], 0.0)

    def test_drop_met_buys(self):
        s = make_strategy(interval_seconds=100.0, trigger_drop_pct=0.1)
        s.generate_signal(ms(price=100.0, ts=0.0))
        sig = s.generate_signal(ms(price=90.0, ts=100.0))
        self.assertIsNotNone(sig)
        self.assertEqual(s._buys["BTC-USD"], 2)


class TestMaxBuys(unittest.TestCase):
    def test_no_more_after_max(self):
        s = make_strategy(interval_seconds=100.0, max_buys=2)
        self.assertIsNotNone(s.generate_signal(ms(price=100.0, ts=0.0)))
        self.assertIsNotNone(s.generate_signal(ms(price=100.0, ts=100.0)))
        self.assertIsNone(s.generate_signal(ms(price=100.0, ts=200.0)))
        self.assertEqual(s._buys["BTC-USD"], 2)

    def test_max_buys_zero(self):
        s = make_strategy(max_buys=0)
        self.assertIsNone(s.generate_signal(ms()))


class TestMultipleProducts(unittest.TestCase):
    def test_independent_state(self):
        s = make_strategy(interval_seconds=100.0)
        s.generate_signal(ms(price=100.0, ts=0.0, product="BTC-USD"))
        s.generate_signal(ms(price=50.0, ts=0.0, product="ETH-USD"))
        self.assertEqual(s._buys["BTC-USD"], 1)
        self.assertEqual(s._buys["ETH-USD"], 1)
        self.assertEqual(s._ref["ETH-USD"], 50.0)
        # ETH still gated by interval independently.
        self.assertIsNone(s.generate_signal(ms(price=50.0, ts=10.0, product="ETH-USD")))
        self.assertIsNotNone(s.generate_signal(ms(price=50.0, ts=100.0, product="ETH-USD")))


class TestStepFallback(unittest.TestCase):
    def test_no_timestamp_uses_steps(self):
        s = make_strategy(interval_seconds=2.0)
        state = {"product_id": "BTC-USD", "price": 100.0, "warmup_complete": True}
        # step 1 -> first buy
        self.assertIsNotNone(s.generate_signal(dict(state)))
        # step 2 -> interval (2) not elapsed (2-1=1 < 2)
        self.assertIsNone(s.generate_signal(dict(state)))
        # step 3 -> elapsed (3-1=2 >= 2) -> buy
        self.assertIsNotNone(s.generate_signal(dict(state)))
        self.assertEqual(s._buys["BTC-USD"], 2)


if __name__ == "__main__":
    unittest.main()
