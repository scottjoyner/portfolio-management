import asyncio
import unittest

from trading_system.risk.auto_approval.rules_engine import AutoApprovalRulesEngine


class TestAutoApprovalRulesEngine(unittest.TestCase):
    def setUp(self):
        self.engine = AutoApprovalRulesEngine()

    def _run(self, trade, size):
        return asyncio.run(self.engine.check_auto_approval(trade, size))

    def test_whitelist_match_btc(self):
        res = self._run({"instrument": "BTC/USD", "confidence_score": 0.9}, 5000.0)
        self.assertTrue(res["auto_approved"])
        self.assertEqual(res["tier"], "FULL_SCALE")
        self.assertEqual(res["analyst_reviews_required"], 1)

    def test_whitelist_match_eth(self):
        res = self._run({"instrument": "eth/usd", "confidence_score": 0.95}, 20000.0)
        self.assertTrue(res["auto_approved"])
        self.assertEqual(res["tier"], "FULL_SCALE")

    def test_whitelist_size_too_big(self):
        # size exceeds BTC max 10000 -> not matched -> falls to default deny
        res = self._run({"instrument": "BTC/USD", "confidence_score": 0.9}, 20000.0)
        self.assertFalse(res["auto_approved"])

    def test_whitelist_confidence_too_low(self):
        res = self._run({"instrument": "BTC/USD", "confidence_score": 0.5}, 5000.0)
        self.assertFalse(res["auto_approved"])

    def test_default_approved_canary(self):
        res = self._run({"instrument": "SOL/USD", "confidence_score": 0.85}, 10000.0)
        self.assertTrue(res["auto_approved"])
        self.assertEqual(res["tier"], "CANARY_PHASE")
        self.assertEqual(res["analyst_reviews_required"], 1)

    def test_default_size_too_big(self):
        res = self._run({"instrument": "SOL/USD", "confidence_score": 0.9}, 20000.0)
        self.assertFalse(res["auto_approved"])
        self.assertEqual(res["tier"], "FULL_SCALE")
        self.assertEqual(res["analyst_reviews_required"], 2)

    def test_default_confidence_too_low(self):
        res = self._run({"instrument": "SOL/USD", "confidence_score": 0.5}, 10000.0)
        self.assertFalse(res["auto_approved"])

    def test_missing_instrument_default_branch(self):
        res = self._run({}, 5000.0)
        self.assertFalse(res["auto_approved"])


if __name__ == "__main__":
    unittest.main()
