import asyncio
import unittest

from trading_system.research.agentic.orchestrator import ResearchOrchestrator


async def _agent(signal, confidence=0.8):
    async def _run(instrument):
        return {"signal": signal, "confidence": confidence, "instrument": instrument}
    return _run


class TestOrchestrator(unittest.TestCase):
    def test_register_agent(self):
        o = ResearchOrchestrator()
        o.register_agent("sentiment", lambda i: None)
        self.assertIn("sentiment", o.agents)

    def test_run_workflow_no_agents(self):
        o = ResearchOrchestrator()
        res = asyncio.run(o.run_workflow("BTC", 30))
        self.assertEqual(res["instrument"], "BTC")
        self.assertEqual(res["consensus"]["signal"], "neutral")
        self.assertEqual(res["market_regime"]["regime"], "neutral")

    def test_run_workflow_buy_consensus(self):
        o = ResearchOrchestrator()
        asyncio.run(_agent("buy"))  # not registered; just ensure importable
        o.register_agent("sentiment", asyncio.run(_make("buy", 0.9)))
        o.register_agent("technical", asyncio.run(_make("buy", 0.7)))
        res = asyncio.run(o.run_workflow("BTC"))
        self.assertEqual(res["consensus"]["signal"], "buy")
        self.assertGreater(res["consensus"]["bullish_confidence"], 0)
        self.assertEqual(res["market_regime"]["regime"], "bullish")

    def test_run_workflow_sell_consensus(self):
        o = ResearchOrchestrator()
        o.register_agent("fundamental", asyncio.run(_make("sell", 0.9)))
        o.register_agent("technical", asyncio.run(_make("sell", 0.5)))
        res = asyncio.run(o.run_workflow("BTC"))
        self.assertEqual(res["consensus"]["signal"], "sell")
        self.assertEqual(res["market_regime"]["regime"], "bearish")

    def test_run_workflow_exception_in_agent(self):
        async def _boom(instrument):
            raise RuntimeError("agent failed")
        o = ResearchOrchestrator()
        o.register_agent("sentiment", _boom)
        o.register_agent("technical", asyncio.run(_make("buy", 0.5)))
        res = asyncio.run(o.run_workflow("BTC"))
        # Exception result is skipped; only technical counts as bullish
        self.assertEqual(res["consensus"]["signal"], "buy")

    def test_calculate_consensus_total_zero(self):
        o = ResearchOrchestrator()
        # Results with no signal / zero confidence
        r = o._calculate_consensus([{"signal": "neutral", "confidence": 0.0}])
        self.assertEqual(r["signal"], "neutral")
        self.assertEqual(r["confidence_score"], 0.0)
        # Empty results
        r2 = o._calculate_consensus([])
        self.assertEqual(r2["signal"], "neutral")

    def test_calculate_consensus_missing_keys(self):
        o = ResearchOrchestrator()
        r = o._calculate_consensus([{"foo": "bar"}])
        self.assertEqual(r["signal"], "neutral")

    def test_detect_regime_equal(self):
        o = ResearchOrchestrator()
        reg = o._detect_regime([
            {"signal": "buy", "confidence": 0.5},
            {"signal": "sell", "confidence": 0.5},
        ])
        self.assertEqual(reg["regime"], "neutral")


async def _make(signal, confidence):
    async def _run(instrument):
        return {"signal": signal, "confidence": confidence, "instrument": instrument}
    return _run


if __name__ == "__main__":
    unittest.main()
