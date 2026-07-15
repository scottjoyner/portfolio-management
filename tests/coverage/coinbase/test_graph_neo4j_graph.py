import unittest
from unittest import mock

from coinbase.src.graph import neo4j_graph
from coinbase.src.graph.neo4j_graph import (
    CryptoGraphStore, _score_graph_features, _signal_reasons,
)
from coinbase.src.graph.models import GraphAsset, GraphAssetSignal, TokenContract, WalletObservation


class _Row:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data


def _make_store(single=None, rows=None):
    driver = mock.MagicMock()
    sess = mock.MagicMock()
    sess.run.return_value.single.return_value = single
    if rows is not None:
        sess.run.return_value = [_Row(r) for r in rows]
    driver.session.return_value.__enter__.return_value = sess
    with mock.patch.object(neo4j_graph, "GraphDatabase") as GraphDatabase:
        GraphDatabase.driver.return_value = driver
        store = CryptoGraphStore()
    return store, sess


class TestScoringHelpers(unittest.TestCase):
    def test_score_full(self):
        data = {
            "category_count": 6, "network_count": 8, "token_count": 10,
            "wallet_count": 100, "tx_count": 1000, "market_cap_rank": 1,
        }
        score = _score_graph_features(data)
        self.assertGreater(score, 0.99)
        self.assertLessEqual(score, 1.0)

    def test_score_empty(self):
        score = _score_graph_features({})
        self.assertEqual(score, 0.0)

    def test_score_clamped(self):
        data = {"market_cap_rank": 1, "category_count": 100}
        self.assertLessEqual(_score_graph_features(data), 1.0)

    def test_reasons(self):
        data = {"available_on_coinbase": True, "market_cap_rank": 50,
                "network_count": 3, "category_count": 5}
        reasons = _signal_reasons(data)
        self.assertIn("available_on_coinbase", reasons)
        self.assertIn("multi_network_presence", reasons)
        self.assertIn("multi_category_asset", reasons)

    def test_reasons_top100_and_limited(self):
        self.assertIn("top_100_market_cap", _signal_reasons({"market_cap_rank": 100}))
        self.assertIn("limited_graph_evidence", _signal_reasons({}))


class TestCryptoGraphStore(unittest.TestCase):
    def test_driver_missing_raises(self):
        with mock.patch.object(neo4j_graph, "GraphDatabase", None):
            with self.assertRaises(RuntimeError):
                CryptoGraphStore()

    def test_upsert_asset(self):
        store, sess = _make_store()
        asset = GraphAsset(cg_id="bitcoin", symbol="BTC", available_on_coinbase=True,
                           categories=["layer-1"])
        store.upsert_asset(asset)
        sess.run.assert_called()

    def test_upsert_token(self):
        store, sess = _make_store()
        tok = TokenContract(key="k", asset_id="bitcoin", network="eth", address="0x1")
        store.upsert_token(tok)
        sess.run.assert_called()

    def test_upsert_wallet_observation(self):
        store, sess = _make_store()
        w = WalletObservation(address="0xabc", project_slug="proj")
        store.upsert_wallet_observation(w)
        sess.run.assert_called()

    def test_upsert_assets_and_tokens(self):
        store, sess = _make_store()
        assets = [GraphAsset(cg_id=str(i), symbol=f"S{i}") for i in range(3)]
        tokens = [TokenContract(key=str(i), asset_id=str(i), network="eth", address="0x"+str(i))
                  for i in range(2)]
        self.assertEqual(store.upsert_assets(assets), 3)
        self.assertEqual(store.upsert_tokens(tokens), 2)

    def test_apply_schema(self):
        store, sess = _make_store()
        store.apply_schema()
        self.assertGreater(sess.run.call_count, 0)

    def test_asset_signal_found(self):
        row = _Row({
            "symbol": "BTC", "product_id": "BTC-USD", "market_cap_rank": 1,
            "available_on_coinbase": True, "category_count": 6, "network_count": 8,
            "token_count": 10, "wallet_count": 100, "tx_count": 1000,
        })
        store, sess = _make_store(single=row)
        sig = store.asset_signal("BTC-USD")
        self.assertEqual(sig.product_id, "BTC-USD")
        self.assertGreater(sig.graph_score, 0.99)
        self.assertIn("top_100_market_cap", sig.reasons)

    def test_asset_signal_not_found(self):
        store, sess = _make_store(single=None)
        sig = store.asset_signal("X-USD")
        self.assertEqual(sig.graph_score, 0.0)
        self.assertIn("asset not found in graph", sig.reasons)

    def test_top_graph_assets(self):
        rows = [
            {"symbol": "BTC", "product_id": "BTC-USD", "market_cap_rank": 1,
             "available_on_coinbase": True, "category_count": 6, "network_count": 8,
             "token_count": 10, "wallet_count": 0, "tx_count": 0},
            {"symbol": "ETH", "product_id": "ETH-USD", "market_cap_rank": 2,
             "available_on_coinbase": True, "category_count": 5, "network_count": 7,
             "token_count": 9, "wallet_count": 0, "tx_count": 0},
        ]
        store, sess = _make_store(rows=rows)
        out = store.top_graph_assets(limit=10)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0].symbol, "BTC")
        self.assertGreaterEqual(out[0].graph_score, out[1].graph_score)

    def test_top_graph_assets_default_product_id(self):
        rows = [{"symbol": "DOGE", "product_id": None, "market_cap_rank": 10,
                 "available_on_coinbase": False, "category_count": 1, "network_count": 1,
                 "token_count": 1, "wallet_count": 0, "tx_count": 0}]
        store, sess = _make_store(rows=rows)
        out = store.top_graph_assets(limit=5, only_coinbase=False)
        self.assertEqual(out[0].product_id, "DOGE-USD")

    def test_close(self):
        store, sess = _make_store()
        store.close()
        store._driver.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
