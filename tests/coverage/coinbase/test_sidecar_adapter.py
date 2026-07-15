"""Tests for coinbase/src/sidecar_adapter.py"""
import unittest
from unittest import mock

from coinbase.src import sidecar_adapter as sc
from coinbase.src.protocols import BracketSetup, Direction, InstrumentType


class TestSidecarResearchRecord(unittest.TestCase):
    def test_is_coinbase_product(self):
        r = sc.SidecarResearchRecord(strategy_name="s", product_id="BTC-USD", ticker="BTC")
        self.assertTrue(r.is_coinbase_product)
        r2 = sc.SidecarResearchRecord(strategy_name="s", product_id="BTCUP", ticker="BTCUP")
        self.assertFalse(r2.is_coinbase_product)

    def test_research_score_mid(self):
        r = sc.SidecarResearchRecord(strategy_name="s", product_id="BTC-USD", ticker="BTC",
                                     profit_factor=1.5, sharpe=1.0,
                                     win_rate_pct=60, max_drawdown_pct=10,
                                     num_trades=40)
        s = r.research_score
        self.assertGreater(s, 0)
        self.assertLessEqual(s, 1.0)

    def test_research_score_empty(self):
        r = sc.SidecarResearchRecord(strategy_name="s", product_id="BTC-USD", ticker="BTC")
        # dd_component contributes 1.0 with zero drawdown -> 0.15
        self.assertAlmostEqual(r.research_score, 0.15)

    def test_research_score_clamping(self):
        r = sc.SidecarResearchRecord(strategy_name="s", product_id="BTC-USD", ticker="BTC",
                                     profit_factor=99, sharpe=99,
                                     win_rate_pct=999, max_drawdown_pct=999,
                                     num_trades=9999)
        s = r.research_score
        self.assertLessEqual(s, 1.0)

    def test_research_score_negative(self):
        r = sc.SidecarResearchRecord(strategy_name="s", product_id="BTC-USD", ticker="BTC",
                                     sharpe=-5.0, max_drawdown_pct=-5.0)
        s = r.research_score
        self.assertGreaterEqual(s, 0.0)


class TestAsHelpers(unittest.TestCase):
    def test_as_float_none(self):
        self.assertEqual(sc._as_float(None), 0.0)

    def test_as_float_value(self):
        self.assertEqual(sc._as_float("3.5"), 3.5)

    def test_as_float_error(self):
        self.assertEqual(sc._as_float("abc"), 0.0)
        self.assertEqual(sc._as_float([1, 2]), 0.0)

    def test_as_int_none(self):
        self.assertEqual(sc._as_int(None), 0)

    def test_as_int_value(self):
        self.assertEqual(sc._as_int("7"), 7)

    def test_as_int_error(self):
        self.assertEqual(sc._as_int("x"), 0)


class TestManifestParsing(unittest.TestCase):
    def test_product_id_from_manifest(self):
        m = {"config": {"product_id": "btc-usd"}}
        self.assertEqual(sc.product_id_from_manifest(m), "BTC-USD")

    def test_product_id_coinbase(self):
        m = {"config": {"coinbase_product_id": "eth-usd"}}
        self.assertEqual(sc.product_id_from_manifest(m), "ETH-USD")

    def test_product_id_ticker(self):
        m = {"config": {"ticker": "sol-usd"}}
        self.assertEqual(sc.product_id_from_manifest(m), "SOL-USD")

    def test_product_id_top(self):
        m = {"product_id": "ada-usd"}
        self.assertEqual(sc.product_id_from_manifest(m), "ADA-USD")

    def test_product_id_empty(self):
        self.assertEqual(sc.product_id_from_manifest({}), "")

    def test_strategy_name_default(self):
        self.assertEqual(sc.strategy_name_from_manifest({}), "sidecar_rsi_cross")

    def test_strategy_name_config(self):
        m = {"config": {"strategy_name": "my_strat"}}
        self.assertEqual(sc.strategy_name_from_manifest(m), "my_strat")

    def test_strategy_name_top(self):
        m = {"strategy_name": "top_strat"}
        self.assertEqual(sc.strategy_name_from_manifest(m), "top_strat")

    def test_research_record_from_manifest(self):
        m = {
            "config": {"product_id": "btc-usd", "ticker": "BTC", "strategy_name": "sc"},
            "summary": {
                "total_return_pct": "10",
                "max_drawdown_pct": "5",
                "sharpe": "1.2",
                "profit_factor": "1.5",
                "win_rate_pct": "55",
                "num_trades": "30",
            },
        }
        r = sc.research_record_from_manifest(m, manifest_path="/p/manifest.json")
        self.assertEqual(r.product_id, "BTC-USD")
        self.assertEqual(r.ticker, "BTC")
        self.assertEqual(r.num_trades, 30)
        self.assertEqual(r.manifest_path, "/p/manifest.json")

    def test_research_record_defaults(self):
        r = sc.research_record_from_manifest({})
        self.assertEqual(r.num_trades, 0)
        self.assertEqual(r.strategy_name, "sidecar_rsi_cross")


def make_setup(conf=0.5):
    return BracketSetup(direction=Direction.LONG, entry_price=100.0,
                        stop_price=90.0, target_price=110.0, risk_reward=2.0,
                        confidence=conf, reason="r", strategy_name="sc")


class TestBracketToOpportunity(unittest.TestCase):
    def test_no_research(self):
        opp = sc.bracket_to_opportunity("BTC-USD", make_setup(0.5))
        self.assertEqual(opp.confidence, 0.5)
        self.assertEqual(opp.score, 0.5 * 2.0)
        self.assertNotIn("sidecar_research_score", opp.meta)

    def test_with_research(self):
        research = sc.SidecarResearchRecord(strategy_name="sc", product_id="BTC-USD", ticker="BTC",
                                            profit_factor=3.0, sharpe=3.0,
                                            win_rate_pct=100, max_drawdown_pct=0,
                                            num_trades=100)
        setup = make_setup(0.5)
        opp = sc.bracket_to_opportunity("BTC-USD", setup, research=research)
        self.assertGreater(opp.confidence, 0.5)
        self.assertLessEqual(opp.confidence, 0.99)
        self.assertIn("sidecar_research_score", opp.meta)
        self.assertEqual(opp.meta["sidecar_research_score"], research.research_score)

    def test_with_research_full_meta(self):
        research = sc.SidecarResearchRecord(strategy_name="sc", product_id="BTC-USD", ticker="BTC",
                                            total_return_pct=10, max_drawdown_pct=5,
                                            profit_factor=1.5, num_trades=30,
                                            manifest_path="/p.json")
        opp = sc.bracket_to_opportunity("BTC-USD", make_setup(0.5), research=research)
        self.assertEqual(opp.meta["sidecar_manifest"], "/p.json")
        self.assertEqual(opp.meta["sidecar_total_return_pct"], 10)
        self.assertEqual(opp.meta["sidecar_max_drawdown_pct"], 5)
        self.assertEqual(opp.meta["sidecar_profit_factor"], 1.5)
        self.assertEqual(opp.meta["sidecar_num_trades"], 30)


if __name__ == "__main__":
    unittest.main()
