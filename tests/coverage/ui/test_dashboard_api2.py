import types
import unittest
from unittest import mock

import trading_system.ui.dashboard_server as ds


def patch(attr, **kw):
    return mock.patch.object(ds, attr, **kw)


def pm_item(**kw):
    base = dict(platform="kalshi", market_id="m1", question="Q sport", category="crypto",
                volume=20000.0, yes_bid=0.4, yes_ask=0.45, spread=0.05,
                liquidity_score=0.8, mid_price=0.42, probability_extremity=0.3,
                keywords=["nba"], is_relevant=True)
    base.update(kw)
    return types.SimpleNamespace(**base)


class TestPredictionMarkets(unittest.TestCase):
    def test_no_client(self):
        with patch("_get_prediction_client", return_value=None):
            out = ds.api_prediction_markets()
        self.assertEqual(out["total_markets"], 0)

    def test_success(self):
        pm = mock.MagicMock()
        cats = {"crypto": [pm_item(), pm_item(question="plain", keywords=[], volume=100)]}
        with patch("_get_prediction_client", return_value=pm), \
                patch("_call_with_timeout", return_value=cats):
            out = ds.api_prediction_markets()
        self.assertEqual(out["total_markets"], 2)
        self.assertIn("heat_score", out["markets"][0])

    def test_scan_exception(self):
        pm = mock.MagicMock()
        with patch("_get_prediction_client", return_value=pm), \
                patch("_call_with_timeout", side_effect=RuntimeError("x")):
            out = ds.api_prediction_markets()
        self.assertEqual(out["total_markets"], 0)

    def test_timeout_with_cache(self):
        pm = mock.MagicMock()
        ds.PREDICTION_MARKETS_CACHE["data"] = {"markets": [], "total_markets": 5}
        ds.PREDICTION_MARKETS_CACHE["ts"] = __import__("time").time()
        with patch("_get_prediction_client", return_value=pm), \
                patch("_call_with_timeout", return_value=None):
            out = ds.api_prediction_markets()
        self.assertEqual(out["total_markets"], 5)
        ds.PREDICTION_MARKETS_CACHE["data"] = None

    def test_timeout_no_cache(self):
        pm = mock.MagicMock()
        ds.PREDICTION_MARKETS_CACHE["data"] = None
        with patch("_get_prediction_client", return_value=pm), \
                patch("_call_with_timeout", return_value=None):
            out = ds.api_prediction_markets()
        self.assertEqual(out["total_markets"], 0)


class TestArbitrage(unittest.TestCase):
    def test_no_scanner(self):
        with patch("_get_event_arbitrage_scanner", return_value=None):
            out = ds.api_arbitrage_opportunities()
        self.assertEqual(out["total_opportunities"], 0)

    def test_success_to_dict(self):
        scanner = mock.MagicMock()
        opp = mock.MagicMock()
        opp.to_dict.return_value = {"event_key": "e1"}
        with patch("_get_event_arbitrage_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=[opp]), \
                patch("_attach_orderbook_depth"):
            out = ds.api_arbitrage_opportunities()
        self.assertEqual(out["total_opportunities"], 1)

    def test_success_manual(self):
        scanner = mock.MagicMock()
        opp = types.SimpleNamespace(
            event_key="e", category="c", platform_buy="kalshi", platform_hedge="poly",
            leg_buy=types.SimpleNamespace(market_id="b"),
            leg_hedge=types.SimpleNamespace(market_id="h"),
            buy_yes_price=0.4, hedge_yes_price=0.5, total_cost=0.9,
            guaranteed_payout=1.0, edge=0.1, edge_pct=10.0, confidence=0.8,
            reason="r", source_markets=[])
        with patch("_get_event_arbitrage_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=[opp]), \
                patch("_attach_orderbook_depth"):
            out = ds.api_arbitrage_opportunities()
        self.assertEqual(out["opportunities"][0]["event_key"], "e")

    def test_exception(self):
        scanner = mock.MagicMock()
        with patch("_get_event_arbitrage_scanner", return_value=scanner), \
                patch("_call_with_timeout", side_effect=RuntimeError("x")), \
                patch("_attach_orderbook_depth"):
            out = ds.api_arbitrage_opportunities()
        self.assertEqual(out["total_opportunities"], 0)

    def test_timeout_cache(self):
        scanner = mock.MagicMock()
        ds.ARBITRAGE_CACHE["data"] = {"opportunities": [], "total_opportunities": 3}
        ds.ARBITRAGE_CACHE["ts"] = __import__("time").time()
        with patch("_get_event_arbitrage_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=None):
            out = ds.api_arbitrage_opportunities()
        self.assertEqual(out["total_opportunities"], 3)
        ds.ARBITRAGE_CACHE["data"] = None

    def test_timeout_no_cache(self):
        scanner = mock.MagicMock()
        ds.ARBITRAGE_CACHE["data"] = None
        with patch("_get_event_arbitrage_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=None):
            out = ds.api_arbitrage_opportunities()
        self.assertEqual(out["total_opportunities"], 0)


class TestKalshiInternal(unittest.TestCase):
    def test_scanner_import_fail(self):
        with mock.patch.dict("sys.modules", {"event_markets.kalshi_internal_arb": None}):
            self.assertIsNone(ds._get_kalshi_internal_scanner())

    def test_scanner_no_kc(self):
        fake = mock.MagicMock()
        client = mock.MagicMock()
        client._kalshi = None
        with mock.patch.dict("sys.modules", {"event_markets.kalshi_internal_arb": fake}), \
                patch("_get_prediction_client", return_value=client):
            self.assertIsNone(ds._get_kalshi_internal_scanner())

    def test_scanner_ok(self):
        fake = mock.MagicMock()
        client = mock.MagicMock()
        client._kalshi = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"event_markets.kalshi_internal_arb": fake}), \
                patch("_get_prediction_client", return_value=client):
            self.assertIsNotNone(ds._get_kalshi_internal_scanner())

    def test_api_no_scanner(self):
        with patch("_get_kalshi_internal_scanner", return_value=None):
            out = ds.api_kalshi_internal_arb()
        self.assertFalse(out["available"])

    def test_api_success(self):
        scanner = mock.MagicMock()
        o = mock.MagicMock()
        o.to_dict.return_value = {"guaranteed": True}
        o2 = mock.MagicMock()
        o2.to_dict.return_value = {"guaranteed": False}
        with patch("_get_kalshi_internal_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=[o, o2]):
            out = ds.api_kalshi_internal_arb()
        self.assertEqual(out["total"], 2)
        self.assertEqual(out["guaranteed_count"], 1)

    def test_api_exception(self):
        scanner = mock.MagicMock()
        with patch("_get_kalshi_internal_scanner", return_value=scanner), \
                patch("_call_with_timeout", side_effect=RuntimeError("x")):
            out = ds.api_kalshi_internal_arb()
        self.assertEqual(out["total"], 0)

    def test_api_timeout_cache(self):
        scanner = mock.MagicMock()
        ds.KALSHI_INTERNAL_CACHE["data"] = {"total": 9}
        ds.KALSHI_INTERNAL_CACHE["ts"] = __import__("time").time()
        with patch("_get_kalshi_internal_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=None):
            out = ds.api_kalshi_internal_arb()
        self.assertEqual(out["total"], 9)
        ds.KALSHI_INTERNAL_CACHE["data"] = None

    def test_api_timeout_no_cache(self):
        scanner = mock.MagicMock()
        ds.KALSHI_INTERNAL_CACHE["data"] = None
        with patch("_get_kalshi_internal_scanner", return_value=scanner), \
                patch("_call_with_timeout", return_value=None):
            out = ds.api_kalshi_internal_arb()
        self.assertTrue(out["stale"])


class TestKalshiBook(unittest.TestCase):
    def test_full(self):
        book = {"orderbook_fp": {"yes_dollars": [["0.4", 100], ["0.45", 50]],
                                 "no_dollars": [["0.5", 60]]}}
        out = ds._summarize_kalshi_book(book)
        self.assertEqual(out["yes_bid"], 0.45)
        self.assertIsNotNone(out["yes_ask"])

    def test_empty(self):
        self.assertEqual(ds._summarize_kalshi_book({})["yes_bid"], None)

    def test_bad(self):
        book = {"orderbook": {"yes": [["bad"]], "no": []}}
        out = ds._summarize_kalshi_book(book)
        self.assertIsNone(out["yes_bid"])


class TestAttachOrderbook(unittest.TestCase):
    def test_no_kc(self):
        scanner = mock.MagicMock()
        scanner.client = None
        scanner._client = None
        ds._attach_orderbook_depth(scanner, [{"platform_buy": "kalshi", "buy_market_id": "m"}])

    def test_with_kc(self):
        kc = mock.MagicMock()
        kc.get_order_book.return_value = {"orderbook_fp": {"yes_dollars": [["0.4", 1]],
                                                           "no_dollars": []}}
        client = mock.MagicMock()
        client._kalshi = kc
        scanner = mock.MagicMock()
        scanner.client = client
        rankings = [{"platform_buy": "kalshi", "buy_market_id": "m",
                     "platform_hedge": "poly", "hedge_market_id": ""}]
        with patch("_call_with_timeout", return_value={"orderbook_fp": {"yes_dollars": [["0.4", 1]],
                                                                        "no_dollars": []}}):
            ds._attach_orderbook_depth(scanner, rankings)
        self.assertIn("depth_buy", rankings[0])

    def test_with_kc_exception(self):
        kc = mock.MagicMock()
        client = mock.MagicMock()
        client._kalshi = kc
        scanner = mock.MagicMock()
        scanner.client = client
        rankings = [{"platform_buy": "kalshi", "buy_market_id": "m",
                     "platform_hedge": "x", "hedge_market_id": ""}]
        with patch("_call_with_timeout", side_effect=RuntimeError("x")):
            ds._attach_orderbook_depth(scanner, rankings)


class TestSignalCache(unittest.TestCase):
    def setUp(self):
        ds._cache = {}
        ds._cache_ts = 0.0

    def test_refresh_from_cache(self):
        sig = {"opportunity_score": 0.9, "action": "BUY"}
        with patch("_load_json", return_value={"signals": [sig], "status": "ok",
                                               "updated_at": "t"}):
            ds._refresh_cache()
        self.assertEqual(ds._cache["total_signals"], 1)

    def test_refresh_stale(self):
        ds._cache = {"queue": [1]}
        ds._cache_ts = 0.0
        with patch("_load_json", return_value={"signals": []}):
            ds._refresh_cache()
        self.assertEqual(ds._cache["source"], "stale")

    def test_refresh_unavailable(self):
        ds._cache = {}
        ds._cache_ts = 0.0
        with patch("_load_json", return_value={}):
            ds._refresh_cache()
        self.assertEqual(ds._cache["status"], "unavailable")

    def test_refresh_fresh_returns_early(self):
        ds._cache = {"x": 1}
        ds._cache_ts = __import__("time").time()
        ds._refresh_cache()
        self.assertEqual(ds._cache, {"x": 1})

    def test_api_opportunities(self):
        queue = [{"opportunity_score": 0.9, "action": "BUY", "symbol": "BTC-USD",
                  "signal_type": "x_new_listing_momentum"}]
        ds._cache = {"queue": queue}
        with patch("_refresh_cache"), patch("_enrich_signals_with_graph", side_effect=lambda q: q):
            out = ds.api_opportunities()
        self.assertEqual(out["total_signals"], 1)
        self.assertEqual(out["new_listing_signals"], 1)

    def test_api_signal_feed(self):
        ds._cache = {"a": 1}
        with patch("_refresh_cache"):
            self.assertEqual(ds.api_signal_feed(), {"a": 1})

    def test_api_strategies_performance(self):
        ds._cache = {"strategy_breakdown": {"S": 3}}
        with patch("_refresh_cache"), \
                patch("_load_json", return_value={"signals": [{"strategy_name": "S",
                                                               "confidence": 0.8},
                                                              {"strategy_name": "New",
                                                               "confidence": 0.5}]}):
            out = ds.api_strategies_performance()
        names = {s["name"] for s in out["strategies"]}
        self.assertIn("S", names)
        self.assertIn("New", names)

    def test_api_strategies_performance_defaults(self):
        ds._cache = {"strategy_breakdown": {}}
        with patch("_refresh_cache"), patch("_load_json", return_value={"signals": []}):
            out = ds.api_strategies_performance()
        self.assertTrue(len(out["strategies"]) > 5)

    def test_api_diversification(self):
        ds._cache = {"queue": [{"strategy_name": "kalman_mr"}]}
        with patch("_refresh_cache"):
            out = ds.api_diversification_signals()
        self.assertEqual(out["total_strategies"], 5)
        self.assertGreaterEqual(out["active_strategies"], 1)

    def test_enrich_signals(self):
        queue = [{"symbol": "BTC-USD"}, {"instrument": "no-dash"}]
        graph = {"available": True, "top_assets": [{"product_id": "BTC-USD",
                                                    "graph_score": 0.9, "overlay": 1.1}]}
        with patch("_graph_summary_for_products", return_value=graph):
            out = ds._enrich_signals_with_graph(queue)
        self.assertEqual(out[0]["graph_score"], 0.9)

    def test_enrich_signals_no_products(self):
        self.assertEqual(ds._enrich_signals_with_graph([{"symbol": "nodash"}]),
                         [{"symbol": "nodash"}])

    def test_enrich_signals_unavailable(self):
        queue = [{"symbol": "BTC-USD"}]
        with patch("_graph_summary_for_products", return_value={"available": False}):
            self.assertEqual(ds._enrich_signals_with_graph(queue), queue)


class TestCryptoDivergence(unittest.TestCase):
    def test_from_operator_state(self):
        op = {"marketIntelligence": {"crypto_divergence": {"total": 5}}}
        with patch("_load_json", return_value=op):
            out = ds._crypto_divergence_from_operator_state()
        self.assertTrue(out["available"])

    def test_fetch_spot(self):
        cb = mock.MagicMock()
        cb.get_price.side_effect = [{"price": 100}, RuntimeError("x")]
        with patch("_get_coinbase_cli", return_value=cb):
            out = ds._fetch_coinbase_spot(["BTC-USD", "ETH-USD"])
        self.assertEqual(out["BTC-USD"], 100)

    def test_fetch_spot_no_cb(self):
        with patch("_get_coinbase_cli", return_value=None):
            self.assertEqual(ds._fetch_coinbase_spot(["BTC-USD"]), {})

    def test_divergence_cache(self):
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = {"total": 7}
        ds.CRYPTO_DIVERGENCE_CACHE["ts"] = __import__("time").time()
        out = ds.api_crypto_divergence()
        self.assertEqual(out["total"], 7)
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None

    def test_divergence_success(self):
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None
        r = mock.MagicMock()
        r.to_dict.return_value = {"is_significant": True}
        with patch("_call_with_timeout", return_value=([r], {"BTC-USD": 100})):
            out = ds.api_crypto_divergence()
        self.assertEqual(out["total"], 1)
        self.assertEqual(out["significant_count"], 1)
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None

    def test_divergence_timeout_fallback(self):
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None
        with patch("_call_with_timeout", return_value=None), \
                patch("_crypto_divergence_from_operator_state", return_value={"total": 1}):
            out = ds.api_crypto_divergence()
        self.assertTrue(out["stale"])

    def test_divergence_error_fallback(self):
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None
        with patch("_call_with_timeout", side_effect=RuntimeError("x")), \
                patch("_crypto_divergence_from_operator_state", return_value={}):
            out = ds.api_crypto_divergence()
        self.assertTrue(out["stale"])

    def test_divergence_scan_body(self):
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None
        r = mock.MagicMock()
        r.to_dict.return_value = {"is_significant": False}
        detector = mock.MagicMock()
        detector.analyze_markets.return_value = [r]
        cd_mod = mock.MagicMock()
        cd_mod.CryptoPriceDivergenceDetector.return_value = detector
        client = mock.MagicMock()
        client.search_all_categories.return_value = {"crypto": [object()]}
        with mock.patch.dict("sys.modules", {"event_markets.crypto_divergence": cd_mod}), \
                patch("_get_prediction_client", return_value=client), \
                patch("_fetch_coinbase_spot", return_value={"BTC-USD": 100}), \
                patch("_call_with_timeout", side_effect=lambda fn, t: fn()):
            out = ds.api_crypto_divergence()
        self.assertEqual(out["total"], 1)
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None

    def test_divergence_scan_no_client(self):
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None
        cd_mod = mock.MagicMock()
        with mock.patch.dict("sys.modules", {"event_markets.crypto_divergence": cd_mod}), \
                patch("_get_prediction_client", return_value=None), \
                patch("_crypto_divergence_from_operator_state", return_value={}), \
                patch("_call_with_timeout", side_effect=lambda fn, t: fn()):
            out = ds.api_crypto_divergence()
        self.assertTrue(out["stale"])
        ds.CRYPTO_DIVERGENCE_CACHE["data"] = None


if __name__ == "__main__":
    unittest.main()
