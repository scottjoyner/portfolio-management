import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from _helpers import install_fakes

install_fakes({
    "core.config.settings": {"Settings": MagicMock()},
    "research.approval": {"ApprovalService": MagicMock()},
})

from trading_system.apps.worker import engine as eng_mod
from trading_system.apps.worker.engine import WorkerEngine


class FakeStrategy:
    def __init__(self, sid, products, enabled=True, signal=None, raise_exc=False):
        self.strategy_id = sid
        self._products = products
        self._enabled = enabled
        self._signal = signal
        self._raise = raise_exc

    def metadata(self):
        return {"products": self._products, "enabled": self._enabled}

    def generate_signal(self, ms):
        if self._raise:
            raise RuntimeError("boom")
        return self._signal

    def explain_trade(self, sig):
        return "explained"


class TestWorkerEngine(unittest.TestCase):
    def _make_engine(self, strategies, approval_tuple=None):
        orig = eng_mod.load_strategies
        eng_mod.load_strategies = lambda: strategies
        try:
            engine = WorkerEngine(db=MagicMock())
        finally:
            eng_mod.load_strategies = orig
        if approval_tuple is not None:
            engine._approval_svc.check_strategy_approved.return_value = approval_tuple
            engine._approval_svc.check_trade_approved.return_value = approval_tuple
        return engine

    def test_init_builds_product_map(self):
        s = FakeStrategy("a", ["BTC-USD", "ETH-USD"])
        engine = self._make_engine([s])
        self.assertIn("BTC-USD", engine._product_map)
        self.assertIn("ETH-USD", engine._product_map)

    def test_sync_disabled(self):
        s = FakeStrategy("a", ["BTC-USD"])
        engine = self._make_engine([s])
        engine.sync_disabled({"a"})
        self.assertIn("a", engine._db_disabled)
        self.assertIn("a", engine.risk_engine.disabled_strategies)

    def test_evaluate_market_state_basic(self):
        sig = {"score": 1.0, "confidence": 0.7}
        s = FakeStrategy("a", ["BTC-USD"], signal=sig)
        engine = self._make_engine([s])
        out = engine.evaluate_market_state("BTC-USD", {"price": 100})
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["strategy_id"], "a")
        self.assertEqual(out[0]["explanation"], "explained")

    def test_evaluate_market_state_skips_disabled(self):
        sig = {"score": 1.0}
        s = FakeStrategy("a", ["BTC-USD"], signal=sig)
        engine = self._make_engine([s])
        engine.sync_disabled({"a"})
        out = engine.evaluate_market_state("BTC-USD", {"price": 100})
        self.assertEqual(out, [])

    def test_evaluate_market_state_skips_not_enabled(self):
        sig = {"score": 1.0}
        s = FakeStrategy("a", ["BTC-USD"], enabled=False, signal=sig)
        engine = self._make_engine([s])
        out = engine.evaluate_market_state("BTC-USD", {"price": 100})
        self.assertEqual(out, [])

    def test_evaluate_market_state_none_signal(self):
        s = FakeStrategy("a", ["BTC-USD"], signal=None)
        engine = self._make_engine([s])
        out = engine.evaluate_market_state("BTC-USD", {"price": 100})
        self.assertEqual(out, [])

    def test_evaluate_market_state_exception(self):
        s = FakeStrategy("a", ["BTC-USD"], raise_exc=True)
        engine = self._make_engine([s])
        out = engine.evaluate_market_state("BTC-USD", {"price": 100})
        self.assertEqual(out, [])

    def test_evaluate_market_state_live_not_approved(self):
        sig = {"score": 1.0}
        s = FakeStrategy("a", ["BTC-USD"], signal=sig)
        engine = self._make_engine([s], approval_tuple=(False, "ref"))
        out = engine.evaluate_market_state("BTC-USD", {"price": 100}, mode="live")
        self.assertEqual(out, [])

    def test_evaluate_market_state_live_approved(self):
        sig = {"score": 1.0}
        s = FakeStrategy("a", ["BTC-USD"], signal=sig)
        engine = self._make_engine([s], approval_tuple=(True, "ok"))
        out = engine.evaluate_market_state("BTC-USD", {"price": 100}, mode="live")
        self.assertEqual(len(out), 1)

    def test_evaluate_order_paper(self):
        sig = {"score": 1.0}
        engine = self._make_engine([])
        ok, reason = engine.evaluate_order(
            {"strategy_id": "a", "signal": sig}, {"product_id": "BTC-USD", "price": 100}
        )
        self.assertTrue(ok)

    def test_evaluate_order_live_not_approved(self):
        sig = {"score": 1.0}
        engine = self._make_engine([], approval_tuple=(False, "ref"))
        ok, reason = engine.evaluate_order(
            {"strategy_id": "a", "signal": sig},
            {"product_id": "BTC-USD", "price": 100},
            mode="live",
        )
        self.assertFalse(ok)
        self.assertIn("not approved", reason)

    def test_evaluate_order_live_approved(self):
        sig = {"score": 1.0}
        engine = self._make_engine([], approval_tuple=(True, "ok"))
        ok, reason = engine.evaluate_order(
            {"strategy_id": "a", "signal": sig},
            {"product_id": "BTC-USD", "price": 100},
            mode="live",
        )
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
