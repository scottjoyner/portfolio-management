import asyncio
import unittest


class TestSentimentAgent(unittest.TestCase):
    def test_analyze(self):
        from trading_system.research.agentic.sentiment_agent import analyze_sentiment
        res = asyncio.run(analyze_sentiment("BTC-USD"))
        self.assertEqual(res["agent_type"], "sentiment")
        self.assertEqual(res["instrument"], "BTC-USD")
        self.assertIn(res["signal"], ("buy", "sell", "neutral"))


if __name__ == "__main__":
    unittest.main()
