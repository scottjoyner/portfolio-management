import asyncio
import unittest

from research_agent.component import (
    PMOpportunity, PMSignal, PMResearchAgent,
)


class TestResearchAgentComponent(unittest.TestCase):
    def test_dataclasses(self):
        opp = PMOpportunity("kalshi", "ev", "yes", 0.4, 0.6, 1.0, 30.0, 35.0)
        self.assertEqual(opp.marketplace, "kalshi")
        sig = PMSignal("id", "LONG_YES", 100.0, 0.7, "t", None, None)
        self.assertEqual(sig.action, "LONG_YES")

    def test_init(self):
        a = PMResearchAgent()
        self.assertEqual(a.active_positions, {})
        self.assertEqual(a.signal_history, [])

    def test_scan_kalshi(self):
        a = PMResearchAgent()
        ops = a.scan_kalshi_btc_options()
        self.assertEqual(len(ops), 1)
        self.assertEqual(ops[0]["marketplace"], "kalshi")

    def test_scan_polymarket(self):
        a = PMResearchAgent()
        self.assertEqual(a.scan_polymarket_events(), [])

    def test_generate_signal_long_yes(self):
        a = PMResearchAgent()
        data = {"event_title": "E", "yes_price": 0.4, "volume_24h_usd": 500000}
        sig = a.generate_signal(data)
        self.assertIsNotNone(sig)
        self.assertEqual(sig.action, "LONG_YES")
        self.assertEqual(sig.position_size_usd, 1000)

    def test_generate_signal_hold(self):
        a = PMResearchAgent()
        data = {"event_title": "E", "yes_price": 0.5, "volume_24h_usd": 0}
        self.assertIsNone(a.generate_signal(data))

    def test_generate_signal_no_yes_price(self):
        a = PMResearchAgent()
        self.assertIsNone(a.generate_signal({"event_title": "E"}))

    def test_calculate_fair_probability(self):
        a = PMResearchAgent()
        self.assertEqual(a._calculate_fair_probability({"yes_price": 0.5}), 0.5)
        high = a._calculate_fair_probability(
            {"yes_price": 0.5, "volume_24h_usd": 2_000_000})
        self.assertGreater(high, 0.5)

    def test_track_signal(self):
        a = PMResearchAgent()
        sig = PMSignal("id", "LONG_YES", 100.0, 0.7, "t", None, None)
        res = a.track_signal(sig, True)
        self.assertEqual(res["win"], True)
        self.assertEqual(len(a.signal_history), 1)

    def test_close_position_present(self):
        a = PMResearchAgent()
        a.active_positions["E-open"] = {"unrealized_pnl_usd": 12.0}
        res = a.close_position({"event_title": "E"})
        self.assertTrue(res["position_closed"])
        self.assertEqual(res["realized_pnl_usd"], 12.0)

    def test_close_position_absent(self):
        a = PMResearchAgent()
        self.assertIsNone(a.close_position({"event_title": "E"}))


if __name__ == "__main__":
    unittest.main()
