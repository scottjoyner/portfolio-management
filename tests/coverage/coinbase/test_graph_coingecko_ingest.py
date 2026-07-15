import json
import os
import tempfile
import unittest
from unittest import mock

from coinbase.src.graph import coingecko_ingest
from coinbase.src.graph.coingecko_ingest import (
    assets_from_market_rows, assets_and_tokens_from_coin_meta,
    load_json_payload, ingest_markets_file, ingest_meta_file,
)


class TestAssetsFromMarketRows(unittest.TestCase):
    def test_basic(self):
        rows = [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin",
             "market_cap_rank": 1, "market_cap": 1e12},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum",
             "market_cap_rank": 2, "market_cap": 5e11},
        ]
        assets = assets_from_market_rows(rows)
        self.assertEqual(len(assets), 2)
        self.assertEqual(assets[0].product_id, "BTC-USD")
        self.assertEqual(assets[0].market_cap_rank, 1)

    def test_coinbase_flag(self):
        rows = [{"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}]
        assets = assets_from_market_rows(rows, coinbase_symbols={"BTC"})
        self.assertTrue(assets[0].available_on_coinbase)

    def test_skips_empty_symbol(self):
        rows = [{"id": "x", "symbol": "", "name": "X"}]
        self.assertEqual(assets_from_market_rows(rows), [])

    def test_wrapped_payload(self):
        rows = [{"id": "btc", "symbol": "btc", "name": "Bitcoin"}]
        assets = assets_from_market_rows({"data": rows})
        self.assertEqual(len(assets), 1)


class TestAssetsAndTokensFromCoinMeta(unittest.TestCase):
    def test_basic(self):
        meta = {
            "bitcoin": {"symbol": "btc", "name": "Bitcoin",
                        "categories": ["layer-1"], "platforms": {"ethereum": "0xbtc"}},
            "ethereum": {"symbol": "eth", "name": "Ethereum",
                         "categories": [], "platforms": {}},
        }
        assets, tokens = assets_and_tokens_from_coin_meta(meta)
        self.assertEqual(len(assets), 2)
        self.assertEqual(len(tokens), 1)
        self.assertEqual(tokens[0].address, "0xbtc")
        self.assertEqual(assets[0].categories, ["layer-1"])

    def test_skips_errors_and_missing(self):
        meta = {
            "bad": {"error": "rate limited"},
            "x": "notadict",
            "good": {"symbol": "sol", "name": "Solana", "platforms": {"solana": "mint"}},
        }
        assets, tokens = assets_and_tokens_from_coin_meta(meta)
        self.assertEqual(len(assets), 1)
        self.assertEqual(len(tokens), 1)

    def test_coinbase_flag(self):
        meta = {"sol": {"symbol": "sol", "name": "Solana", "platforms": {}}}
        assets, _ = assets_and_tokens_from_coin_meta(meta, coinbase_symbols={"SOL"})
        self.assertTrue(assets[0].available_on_coinbase)

    def test_skips_empty_symbol(self):
        meta = {
            "": {"symbol": "", "name": "X", "platforms": {}},
            "btc": {"symbol": "btc", "name": "Bitcoin", "platforms": {}},
        }
        assets, tokens = assets_and_tokens_from_coin_meta(meta)
        self.assertEqual(len(assets), 1)
        self.assertEqual(assets[0].symbol, "BTC")

    def test_skips_empty_address_platform(self):
        meta = {"btc": {"symbol": "btc", "name": "Bitcoin",
                        "platforms": {"ethereum": "", "solana": "mint"}}}
        assets, tokens = assets_and_tokens_from_coin_meta(meta)
        self.assertEqual(len(tokens), 1)
        self.assertEqual(tokens[0].address, "mint")

    def test_address_lowercased(self):
        meta = {"sol": {"symbol": "sol", "name": "Solana", "platforms": {"solana": "MINT"}}}
        _, tokens = assets_and_tokens_from_coin_meta(meta)
        self.assertEqual(tokens[0].address, "mint")


class TestLoadJsonPayload(unittest.TestCase):
    def test_load(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump({"data": [1, 2]}, f)
            path = f.name
        try:
            self.assertEqual(load_json_payload(path), {"data": [1, 2]})
        finally:
            os.unlink(path)


class TestIngest(unittest.TestCase):
    def test_ingest_markets_file(self):
        payload = {"data": [{"id": "btc", "symbol": "btc", "name": "Bitcoin"}]}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            store = mock.MagicMock()
            store.upsert_assets.return_value = 1
            n = ingest_markets_file(path, store)
            self.assertEqual(n, 1)
            store.upsert_assets.assert_called_once()
        finally:
            os.unlink(path)

    def test_ingest_meta_file(self):
        payload = {"data": {"btc": {"symbol": "btc", "name": "Bitcoin",
                                    "categories": [], "platforms": {}}}}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(payload, f)
            path = f.name
        try:
            store = mock.MagicMock()
            store.upsert_assets.return_value = 1
            store.upsert_tokens.return_value = 0
            res = ingest_meta_file(path, store)
            self.assertEqual(res["assets"], 1)
            self.assertEqual(res["tokens"], 0)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
