import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import coinbase.src.graph.sync_coingecko_universe as sync_mod
from coinbase.src.graph.sync_coingecko_universe import (
    sync_coingecko_universe, _load_coinbase_symbols, main,
)


def _write_json(payload):
    f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(payload, f)
    f.close()
    return f.name


class TestLoadCoinbaseSymbols(unittest.TestCase):
    def test_extra_only(self):
        with mock.patch("coinbase.src.cb_client.CBClient", side_effect=RuntimeError("no client")):
            syms = _load_coinbase_symbols(["btc", "ETH "])
        self.assertEqual(syms, {"BTC", "ETH"})

    def test_from_env(self):
        with mock.patch.dict(os.environ, {"COINBASE_SYMBOLS": "btc, sol ,ada"}):
            with mock.patch("coinbase.src.cb_client.CBClient", side_effect=RuntimeError("no client")):
                syms = _load_coinbase_symbols()
        self.assertEqual(syms, {"BTC", "SOL", "ADA"})

    def test_cb_client_unavailable(self):
        with mock.patch("coinbase.src.cb_client.CBClient", side_effect=RuntimeError("no client")):
            syms = _load_coinbase_symbols(["btc"])
        self.assertEqual(syms, {"BTC"})

    def test_load_coinbase_symbols_from_client(self):
        fake_client = mock.MagicMock()
        fake_client.get_products.return_value = [
            {"product_id": "BTC-USD"}, {"product_id": "ETH-USD"}, {"id": "sol"},
        ]
        with mock.patch("coinbase.src.cb_client.CBClient", return_value=fake_client):
            syms = _load_coinbase_symbols()
        self.assertIn("BTC", syms)
        self.assertIn("ETH", syms)


class TestFetchLiveMarkets(unittest.TestCase):
    def test_client_unavailable(self):
        with mock.patch.object(sync_mod, "coins_markets", None):
            with self.assertRaises(RuntimeError):
                sync_mod._fetch_live_markets()

    def test_multi_page_with_save(self):
        markets_path = _write_json({"data": []})
        try:
            page1 = [{"id": f"c{i}", "symbol": f"s{i}"} for i in range(250)]
            page2 = [{"id": "last", "symbol": "lt"}]

            def fake(page, per_page):
                return {"data": page1 if page == 1 else page2}

            with mock.patch.object(sync_mod, "coins_markets", fake):
                rows = sync_mod._fetch_live_markets(
                    pages=2, per_page=250, save_to=Path(markets_path)
                )
            self.assertEqual(len(rows["data"]), 251)
        finally:
            os.unlink(markets_path)

    def test_empty_page_breaks(self):
        def fake(page, per_page):
            return {"data": []}

        with mock.patch.object(sync_mod, "coins_markets", fake):
            rows = sync_mod._fetch_live_markets(pages=3, per_page=10)
        self.assertEqual(rows["data"], [])


class TestSyncCoingeckoUniverse(unittest.TestCase):
    def test_sync_from_files(self):
        markets = {"data": [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
        ]}
        meta = {"data": {
            "bitcoin": {"symbol": "btc", "name": "Bitcoin",
                        "categories": [], "platforms": {"ethereum": "0xbtc"}},
        }}
        markets_path = _write_json(markets)
        meta_path = _write_json(meta)
        try:
            store = mock.MagicMock()
            store.upsert_assets.return_value = 1
            store.upsert_tokens.return_value = 1
            with mock.patch("coinbase.src.cb_client.CBClient", side_effect=RuntimeError("no client")):
                summary = sync_coingecko_universe(
                    store=store, markets_path=markets_path, meta_path=meta_path,
                    coinbase_symbols=["BTC"],
                )
            self.assertEqual(summary["assets"], 1)
            self.assertEqual(summary["meta_assets"], 1)
            self.assertEqual(summary["tokens"], 1)
            self.assertEqual(summary["source_assets"], 1)
            self.assertEqual(summary["coinbase_symbols"], 1)
            store.apply_schema.assert_called_once()
        finally:
            os.unlink(markets_path)
            os.unlink(meta_path)

    def test_sync_no_meta_file(self):
        markets = {"data": [{"id": "btc", "symbol": "btc", "name": "Bitcoin"}]}
        markets_path = _write_json(markets)
        try:
            store = mock.MagicMock()
            store.upsert_assets.return_value = 1
            with mock.patch("coinbase.src.cb_client.CBClient", side_effect=RuntimeError("no client")):
                summary = sync_coingecko_universe(
                    store=store, markets_path=markets_path, meta_path="/nope.json",
                )
            self.assertEqual(summary["meta_assets"], 0)
            self.assertEqual(summary["tokens"], 0)
        finally:
            os.unlink(markets_path)

    def test_sync_fetch_live(self):
        markets_path = _write_json({"data": []})
        try:
            store = mock.MagicMock()
            store.upsert_assets.return_value = 1
            with mock.patch("coinbase.src.cb_client.CBClient", side_effect=RuntimeError("no client")), \
                 mock.patch.object(sync_mod, "coins_markets") as cm, \
                 mock.patch.object(sync_mod, "cache_json"):
                cm.return_value = {"data": [
                    {"id": "btc", "symbol": "btc", "name": "Bitcoin"},
                ]}
                summary = sync_coingecko_universe(
                    store=store, markets_path=markets_path, meta_path="/nope.json",
                    fetch_live=True, pages=1, per_page=10,
                )
                self.assertEqual(summary["assets"], 1)
                cm.assert_called_once()
        finally:
            os.unlink(markets_path)

    def test_main(self):
        import argparse as _argparse
        ns = _argparse.Namespace(markets_path="x", meta_path="y",
                                  fetch_live=False, pages=20, per_page=250)
        with mock.patch.object(_argparse, "ArgumentParser") as AP, \
             mock.patch.object(sync_mod, "sync_coingecko_universe", return_value={"assets": 0}) as synced:
            AP.return_value.parse_args.return_value = ns
            rc = main()
        self.assertEqual(rc, 0)
        synced.assert_called_once()


if __name__ == "__main__":
    unittest.main()
