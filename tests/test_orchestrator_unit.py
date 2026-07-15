"""
Offline unit tests for ExecutionOrchestrator guard logic and paper accounting.

These catch regressions in the pre-execution safety gates and the position/PnL math
without any network access. The orchestrator is built with ``cb=None`` (PAPER) or a
MagicMock client (LIVE) so no Coinbase call is made.

Run:  python3 -m unittest tests.test_orchestrator_unit -v
"""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from coinbase.src.orchestrator import (
    ExecutionOrchestrator,
    TradeMode,
    TradeSignal,
)
from coinbase.src.protocols import Direction, InstrumentType


def make_signal(**over):
    kw = dict(
        product_id="BTC-USD",
        direction=Direction.LONG,
        entry_price=100.0,
        stop_price=90.0,
        target_price=130.0,
        size=0.01,
        confidence=1.0,
        reason="ut",
        strategy_name="ut",
        instrument_type=InstrumentType.SPOT,
        leverage=1.0,
        opportunity_score=0.9,
    )
    kw.update(over)
    return TradeSignal(**kw)


class TestPreExecutionGuards(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
        self.o.state.cash = 1000.0
        self.o.state.equity = 1000.0

    def test_paper_small_order_ok(self):
        self.assertIsNone(self.o._pre_execution_block_reason(make_signal()))

    def test_paper_max_notional_blocked(self):
        sig = make_signal(size=1.0, entry_price=100000.0)  # notional 100k > max 100
        self.assertEqual(self.o._pre_execution_block_reason(sig), "max_order_notional")

    def test_live_short_blocked(self):
        o = ExecutionOrchestrator(cb=MagicMock(), mode=TradeMode.LIVE, dry_run=False)
        o._kill_switch_active = lambda: False
        sig = make_signal(direction=Direction.SHORT)
        self.assertEqual(o._pre_execution_block_reason(sig), "shorts_disabled_for_live_spot")

    def test_live_low_confidence_blocked(self):
        o = ExecutionOrchestrator(cb=MagicMock(), mode=TradeMode.LIVE, dry_run=False)
        o._kill_switch_active = lambda: False
        sig = make_signal(confidence=0.5)  # below min_live_confidence (0.95)
        self.assertEqual(o._pre_execution_block_reason(sig), "min_live_confidence")

    def test_live_kill_switch_blocked(self):
        o = ExecutionOrchestrator(cb=MagicMock(), mode=TradeMode.LIVE, dry_run=False)
        o._kill_switch_active = lambda: True
        self.assertEqual(o._pre_execution_block_reason(make_signal()), "kill_switch")

    def test_estimated_edge_is_finite(self):
        edge = self.o._estimated_live_edge_bps(make_signal())
        self.assertIsInstance(edge, float)
        self.assertTrue(__import__("math").isfinite(edge))


class TestPaperAccounting(unittest.TestCase):
    def setUp(self):
        self.o = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
        self.o.state.cash = 1000.0
        self.o.state.equity = 1000.0

    def test_paper_execute_long_updates_state(self):
        res = self.o._paper_execute(make_signal(size=0.01, entry_price=100.0))
        self.assertTrue(res["success"])
        self.assertAlmostEqual(self.o.state.cash, 999.0, places=6)
        self.assertIn("BTC-USD", self.o.state.open_positions)
        self.assertEqual(self.o.state.open_positions["BTC-USD"]["size"], 0.01)

    def test_paper_execute_insufficient_cash(self):
        res = self.o._paper_execute(make_signal(size=100.0, entry_price=100.0))  # notional 10k
        self.assertFalse(res["success"])
        self.assertEqual(res["reason"], "insufficient cash")

    def test_merged_position_averages(self):
        self.o.state.open_positions["BTC-USD"] = {
            "direction": "long", "size": 0.01, "entry": 100.0,
            "stop": 90.0, "target": 130.0, "strategy": "ut", "bucket_id": "c",
        }
        merged = self.o._merged_position_state(make_signal(size=0.01, entry_price=200.0), "c")
        # (0.01*100 + 0.01*200) / 0.02 = 150
        self.assertAlmostEqual(merged["entry"], 150.0, places=6)
        self.assertAlmostEqual(merged["size"], 0.02, places=6)

    def test_close_long_pnl(self):
        self.o.state.open_positions["BTC-USD"] = {
            "direction": "long", "size": 0.01, "entry": 100.0,
            "stop": 90.0, "target": 130.0, "strategy": "ut", "bucket_id": "c",
        }
        cash_before = self.o.state.cash
        out = self.o.close_position("BTC-USD", 120.0, reason="test")
        self.assertAlmostEqual(out["pnl"], (120.0 - 100.0) * 0.01)
        self.assertAlmostEqual(self.o.state.cash, cash_before + 0.01 * 120.0, places=6)
        self.assertNotIn("BTC-USD", self.o.state.open_positions)

    def test_close_short_pnl(self):
        self.o.state.open_positions["BTC-USD"] = {
            "direction": "short", "size": 0.01, "entry": 100.0,
            "stop": 110.0, "target": 80.0, "strategy": "ut", "bucket_id": "c",
        }
        cash_before = self.o.state.cash
        out = self.o.close_position("BTC-USD", 90.0, reason="test")
        self.assertAlmostEqual(out["pnl"], (100.0 - 90.0) * 0.01)
        self.assertAlmostEqual(self.o.state.cash, cash_before - 0.01 * 90.0, places=6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
