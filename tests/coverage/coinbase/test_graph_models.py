import unittest

from coinbase.src.graph.models import (
    GraphAsset, TokenContract, WalletObservation, GraphAssetSignal,
)


class TestGraphAsset(unittest.TestCase):
    def test_symbol_key(self):
        a = GraphAsset(cg_id="bitcoin", symbol="btc")
        self.assertEqual(a.symbol_key, "BTC")

    def test_to_dict_includes_symbol_key(self):
        a = GraphAsset(cg_id="bitcoin", symbol="btc", name="Bitcoin",
                       product_id="BTC-USD", market_cap_rank=1,
                       available_on_coinbase=True, categories=["layer-1"],
                       networks=["ethereum"])
        d = a.to_dict()
        self.assertEqual(d["symbol"], "btc")
        self.assertEqual(d["symbol_key"], "BTC")
        self.assertEqual(d["categories"], ["layer-1"])
        self.assertTrue(d["available_on_coinbase"])

    def test_defaults(self):
        a = GraphAsset(cg_id="x", symbol="x")
        self.assertEqual(a.categories, [])
        self.assertEqual(a.networks, [])
        self.assertIsNone(a.market_cap_rank)


class TestTokenContract(unittest.TestCase):
    def test_to_dict(self):
        t = TokenContract(key="k", asset_id="a", network="eth", address="0x1",
                          symbol="TKN", name="Token", decimals=18)
        d = t.to_dict()
        self.assertEqual(d["key"], "k")
        self.assertEqual(d["decimals"], 18)


class TestWalletObservation(unittest.TestCase):
    def test_to_dict(self):
        w = WalletObservation(address="0xabc", project_slug="proj", source="manual",
                               first_seen="2024-01-01", labels=["whale"])
        d = w.to_dict()
        self.assertEqual(d["address"], "0xabc")
        self.assertEqual(d["labels"], ["whale"])


class TestGraphAssetSignal(unittest.TestCase):
    def test_to_dict(self):
        s = GraphAssetSignal(
            product_id="BTC-USD", symbol="BTC", graph_score=0.8,
            category_count=3, network_count=2, token_count=1, wallet_count=5,
            tx_count=100, market_cap_rank=1, available_on_coinbase=True,
            reasons=["top_100_market_cap"],
        )
        d = s.to_dict()
        self.assertEqual(d["graph_score"], 0.8)
        self.assertEqual(d["reasons"], ["top_100_market_cap"])


if __name__ == "__main__":
    unittest.main()
