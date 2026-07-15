import asyncio
import unittest


class TestFundamentalAgent(unittest.TestCase):
    def test_analyze(self):
        from trading_system.research.agentic.fundamental_agent import analyze_fundamental
        res = asyncio.run(analyze_fundamental("BTC-USD"))
        self.assertEqual(res["agent_type"], "fundamental")
        self.assertEqual(res["instrument"], "BTC-USD")
        self.assertIn(res["signal"], ("buy", "sell", "hold"))


if __name__ == "__main__":
    unittest.main()
