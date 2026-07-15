import asyncio
import unittest


class TestTechnicalAgent(unittest.TestCase):
    def test_analyze(self):
        from trading_system.research.agentic.technical_agent import analyze_technical
        res = asyncio.run(analyze_technical("BTC-USD"))
        self.assertEqual(res["agent_type"], "technical")
        self.assertEqual(res["instrument"], "BTC-USD")
        self.assertIn(res["signal"], ("buy", "sell", "neutral"))


if __name__ == "__main__":
    unittest.main()
