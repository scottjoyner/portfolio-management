import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from coinbase.src.orchestrator import ExecutionOrchestrator, TradeMode, TradeSignal
from coinbase.src.protocols import Direction


def make_signal(product_id="BTC-USD", notional=5.0, score=10.0, bucket_id="challenge"):
    entry = 100.0
    return TradeSignal(
        product_id=product_id,
        direction=Direction.LONG,
        entry_price=entry,
        stop_price=95.0,
        target_price=110.0,
        size=notional / entry,
        confidence=0.8,
        reason="unit test",
        strategy_name="challenge_test",
        opportunity_score=score,
        bucket_id=bucket_id,
    )


class TestChallengeLiveGuards(unittest.TestCase):
    def _env(self, **overrides):
        tmp = tempfile.TemporaryDirectory()
        env = {
            "TRADER_BUCKET_STATE_PATH": os.path.join(tmp.name, "buckets.json"),
            "TRADER_CHALLENGE_CAPITAL_USDC": "100",
            "TRADER_CHALLENGE_MAX_ORDER_USD": "10",
            "TRADER_MAX_ORDERS_PER_TICK": "1",
            "TRADER_MAX_NOTIONAL_PER_TICK": "10",
            "TRADER_LIVE_CHALLENGE_ONLY": "true",
        }
        env.update(overrides)
        return tmp, patch.dict(os.environ, env, clear=False)

    def test_kill_switch_blocks_live_orders_before_execution(self):
        tmp, env_patch = self._env(TRADER_KILL_SWITCH="true")
        with tmp, env_patch:
            orch = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
            results = orch.execute_signals([make_signal(notional=5.0)])
        self.assertEqual(results[0]["status"], "blocked")
        self.assertEqual(results[0]["reason"], "kill_switch")
        self.assertFalse(results[0]["success"])

    def test_max_order_notional_blocks_above_ten_usd(self):
        tmp, env_patch = self._env()
        with tmp, env_patch:
            orch = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
            results = orch.execute_signals([make_signal(notional=15.0)])
        self.assertEqual(results[0]["status"], "blocked")
        self.assertEqual(results[0]["reason"], "max_order_notional")
        self.assertFalse(results[0]["success"])

    def test_per_tick_burst_limit_executes_only_highest_scored_signal(self):
        tmp, env_patch = self._env(TRADER_MAX_ORDERS_PER_TICK="1", TRADER_MAX_NOTIONAL_PER_TICK="20")
        with tmp, env_patch:
            orch = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
            low = make_signal(product_id="ETH-USD", notional=5.0, score=1.0)
            high = make_signal(product_id="BTC-USD", notional=5.0, score=99.0)
            results = orch.execute_signals([low, high])
        self.assertEqual(results[0]["product_id"], "BTC-USD")
        self.assertTrue(results[0]["success"])
        self.assertEqual(results[1]["status"], "deferred")
        self.assertEqual(results[1]["reason"], "max_orders_per_tick")
        self.assertEqual(results[1]["product_id"], "ETH-USD")

    def test_live_challenge_only_blocks_non_challenge_bucket(self):
        tmp, env_patch = self._env()
        with tmp, env_patch:
            orch = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
            results = orch.execute_signals([make_signal(notional=5.0, bucket_id="core")])
        self.assertEqual(results[0]["status"], "blocked")
        self.assertEqual(results[0]["reason"], "bucket_not_allowed")
        self.assertFalse(results[0]["success"])

    def test_status_exposes_challenge_guard_configuration(self):
        tmp, env_patch = self._env(TRADER_KILL_SWITCH="true")
        with tmp, env_patch:
            orch = ExecutionOrchestrator(cb=None, mode=TradeMode.PAPER, dry_run=True)
            status = orch.status()
        self.assertEqual(status["execution_guards"]["challenge_max_order_usd"], 10.0)
        self.assertEqual(status["execution_guards"]["max_orders_per_tick"], 1)
        self.assertEqual(status["execution_guards"]["max_notional_per_tick"], 10.0)
        self.assertTrue(status["execution_guards"]["live_challenge_only"])
        self.assertTrue(status["execution_guards"]["kill_switch_active"])


if __name__ == "__main__":
    unittest.main()
