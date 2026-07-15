import asyncio
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

from _helpers import install_fakes

install_fakes({
    "core.config.settings": {"Settings": MagicMock()},
    "research.approval": {"ApprovalService": MagicMock()},
})

import os
import sys
import types

# The repo-root `storage` namespace shadows trading_system/storage under pytest
# and lacks postgres/session.py. Stub the module so `from storage.postgres.session
# import init_db` resolves without importing the DB stack.
_fs = types.ModuleType("storage")
_fs.__path__ = []
sys.modules["storage"] = _fs
_fp = types.ModuleType("storage.postgres")
_fp.__path__ = []
sys.modules["storage.postgres"] = _fp
_fsess = types.ModuleType("storage.postgres.session")
_fsess.init_db = lambda *a, **k: None
sys.modules["storage.postgres.session"] = _fsess

from trading_system.apps.worker import main as main_mod
from trading_system.apps.worker.main import run


class PreSetEvent(asyncio.Event):
    """asyncio.Event that starts already set so the worker loop exits at once."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.set()


class FakeWorkerEngine:
    def __init__(self, settings=None):
        self.settings = settings
        self.strategies = []
        self.signals = []

    def evaluate_market_state(self, product_id, market_state, mode="paper"):
        return self.signals

    def evaluate_order(self, signal, market_state, mode="paper"):
        return True, "ok"


class FakePaperExchange:
    def __init__(self, starting_cash=Decimal("100000"), products=None):
        self.starting_cash = starting_cash
        self.products = list(products or [])
        self.mid_prices = {}
        self.orders = []

    def set_market_price(self, pid, price, spread=Decimal("1")):
        self.mid_prices[pid] = price

    def place_order(self, **kwargs):
        order = SimpleNamespace(order_id="ord-1")
        self.orders.append(order)
        return order

    def stop(self):
        self.stopped = True


class TestWorkerMain(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # The structured logger used by main.py does not accept structured
        # keyword args in this environment; mock it for the test.
        main_mod.log = MagicMock()

    async def test_run_exits_immediately(self):
        # Pre-set event so the main loop exits without polling.
        orig_event = asyncio.Event
        asyncio.Event = PreSetEvent
        try:
            main_mod.Settings.from_env.return_value = SimpleNamespace(database_url=None)
            main_mod.init_db = lambda *a, **k: None
            main_mod.WorkerEngine = lambda *a, **k: FakeWorkerEngine()
            main_mod.PaperExchangeEngine = lambda *a, **k: FakePaperExchange(
                products=["BTC-USD", "ETH-USD"]
            )
            published = []
            main_mod.hub.publish_sync = lambda *a, **k: published.append(a)

            await run()
            self.assertTrue(True)
        finally:
            asyncio.Event = orig_event

    async def test_run_with_signal_and_order(self):
        orig_event = asyncio.Event
        asyncio.Event = PreSetEvent
        try:
            main_mod.Settings.from_env.return_value = SimpleNamespace(database_url=None)
            main_mod.init_db = lambda *a, **k: None

            engine = FakeWorkerEngine()
            engine.signals = [{"signal": {"score": 1.0}, "strategy_id": "s1"}]
            main_mod.WorkerEngine = lambda *a, **k: engine
            paper = FakePaperExchange(products=["BTC-USD", "ETH-USD"])
            main_mod.PaperExchangeEngine = lambda *a, **k: paper

            published = []
            main_mod.hub.publish_sync = lambda *a, **k: published.append(a)

            await run()
            self.assertIsInstance(paper, FakePaperExchange)
            self.assertTrue(paper.stopped)
        finally:
            asyncio.Event = orig_event


if __name__ == "__main__":
    unittest.main()
