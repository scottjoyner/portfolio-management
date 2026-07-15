import sys
import types
import unittest
from unittest import mock

from coinbase.src import futures_execution as fe


class _NullCtx:
    def __enter__(self):
        return None

    def __exit__(self, *a):
        return False


def build_executor(products=None, api_secret="secret", **kw):
    fake_rest = types.ModuleType("coinbase.rest")
    fake_rest.RESTClient = mock.MagicMock(name="RESTClient")
    with mock.patch.object(fe, "_sdk_import_context", lambda: _NullCtx()), \
            mock.patch.dict(sys.modules, {"coinbase.rest": fake_rest}):
        ex = fe.CoinbaseFuturesExecutor(api_key="key", api_secret=api_secret, **kw)
    client = ex.client
    if products is not None:
        client.get_products.return_value = {"products": products}
        ex._load_perp_products()
    return ex, client


class TestHelpers(unittest.TestCase):
    def test_repo_root(self):
        self.assertTrue(fe._repo_root().endswith("portfolio-management"))

    def test_sdk_import_context(self):
        fake_dist = mock.MagicMock()
        fake_dist.locate_file.return_value = "/tmp/nonexistent_dist_root"
        with mock.patch.object(fe.importlib_metadata, "distribution", return_value=fake_dist):
            with fe._sdk_import_context():
                sys.modules["coinbase._fake_for_test"] = object()  # forces del at finally

    def test_sdk_import_context_dist_already_on_path(self):
        fake_dist = mock.MagicMock()
        fake_dist.locate_file.return_value = "/tmp/onpath"
        with mock.patch.object(fe.importlib_metadata, "distribution", return_value=fake_dist):
            sys.path.insert(0, "/tmp/onpath")
            try:
                with fe._sdk_import_context():
                    pass
            finally:
                sys.path.remove("/tmp/onpath")

    def test_as_dict_none(self):
        self.assertEqual(fe._as_dict(None), {})

    def test_as_dict_model_dump(self):
        class M:
            def model_dump(self):
                return {"a": 1}
        self.assertEqual(fe._as_dict(M()), {"a": 1})

    def test_as_dict_to_dict(self):
        class M:
            def to_dict(self):
                return {"b": 2}
        self.assertEqual(fe._as_dict(M()), {"b": 2})

    def test_as_dict_dict_attr(self):
        class M:
            def dict(self):
                return {"c": 3}
        self.assertEqual(fe._as_dict(M()), {"c": 3})

    def test_as_dict_plain_dict(self):
        self.assertEqual(fe._as_dict({"x": 9}), {"x": 9})

    def test_as_dict_dict_instance(self):
        class D(dict):
            pass
        self.assertEqual(fe._as_dict(D(a=1)), {"a": 1})

    def test_as_dict_fallback_value(self):
        # objects without __dict__ fall back to {"value": obj}
        out = fe._as_dict(5)
        self.assertEqual(out, {"value": 5})

    def test_as_dict_model_dump_raises(self):
        class M:
            def model_dump(self):
                raise ValueError("nope")
        # model_dump raises -> skipped, no other converters -> empty __dict__
        self.assertEqual(fe._as_dict(M()), {})

    def test_order_id_variants(self):
        self.assertEqual(fe._order_id({"order_id": "a"}), "a")
        self.assertEqual(fe._order_id({"id": "b"}), "b")
        self.assertEqual(fe._order_id({"client_order_id": "c"}), "c")
        self.assertEqual(fe._order_id({}), "")
        self.assertEqual(fe._order_id(None), "")

    def test_futures_order_result(self):
        r = fe.FuturesOrderResult(success=True, order_id="o", client_order_id="c",
                                  raw={"k": 1}, error="")
        self.assertTrue(r.success)


class TestInit(unittest.TestCase):
    def test_requires_key_secret(self):
        fake_rest = types.ModuleType("coinbase.rest")
        fake_rest.RESTClient = mock.MagicMock()
        with mock.patch.object(fe, "_sdk_import_context", lambda: _NullCtx()), \
                mock.patch.dict(sys.modules, {"coinbase.rest": fake_rest}):
            with self.assertRaises(ValueError):
                fe.CoinbaseFuturesExecutor(api_key="", api_secret="")

    def test_init_success(self):
        ex, client = build_executor(products=[])
        self.assertEqual(ex.margin_type, "CROSS")
        self.assertEqual(ex.default_leverage, 2.0)
        self.assertIsNotNone(client)


class TestLoadPerpProducts(unittest.TestCase):
    def test_load_success(self):
        products = [
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"},
            {"product_id": "ETH-PERP", "contract_expiry_type": "PERPETUAL"},
            {"product_id": "BTC-USD", "contract_expiry_type": "DATED"},
            {"product_id": "SOL-PERP", "contract_expiry_type": "PERPETUAL", "id": "SOL-PERP"},
        ]
        ex, _ = build_executor(products=products)
        self.assertEqual(ex._perp_products.get("BTC"), "BTC-PERP")
        self.assertEqual(ex._perp_products.get("ETH"), "ETH-PERP")
        self.assertEqual(ex._perp_products.get("SOL"), "SOL-PERP")
        self.assertNotIn("BTC-USD", ex._perp_products)

    def test_load_exception(self):
        ex, client = build_executor(products=[])
        client.get_products.side_effect = RuntimeError("fail")
        ex._load_perp_products()
        self.assertEqual(ex._perp_products, {})

    def test_load_non_list(self):
        ex, _ = build_executor(products=[])
        ex.client.get_products.return_value = None
        ex._load_perp_products()
        self.assertEqual(ex._perp_products, {})

    def test_load_products_non_list_value(self):
        ex, _ = build_executor(products=[])
        ex.client.get_products.return_value = {"products": "notalist"}
        ex._load_perp_products()
        self.assertEqual(ex._perp_products, {})

    def test_load_product_missing_id(self):
        ex, _ = build_executor(products=[
            {"contract_expiry_type": "PERPETUAL"},
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"},
        ])
        self.assertEqual(ex._perp_products.get("BTC"), "BTC-PERP")

    def test_load_non_dict_item(self):
        ex, _ = build_executor(products=[42, "x", None])
        self.assertEqual(ex._perp_products, {})


class TestValidateAndDiscover(unittest.TestCase):
    def test_validate_with_portfolio(self):
        ex, client = build_executor(products=[], portfolio_uuid="pu")
        client.get_perps_portfolio_summary.return_value = {"a": 1}
        self.assertEqual(ex.validate(), {"a": 1})

    def test_validate_with_portfolio_error(self):
        ex, client = build_executor(products=[], portfolio_uuid="pu")
        client.get_perps_portfolio_summary.side_effect = RuntimeError("x")
        with self.assertRaises(RuntimeError):
            ex.validate()

    def test_validate_no_portfolio(self):
        ex, client = build_executor(products=[])
        client.get_futures_balance_summary.return_value = {"b": 2}
        self.assertEqual(ex.validate(), {"b": 2})

    def test_validate_no_portfolio_error(self):
        ex, client = build_executor(products=[])
        client.get_futures_balance_summary.side_effect = RuntimeError("x")
        with self.assertRaises(RuntimeError):
            ex.validate()

    def test_discover_found(self):
        ex, _ = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        self.assertEqual(ex.discover_product_id("BTC"), "BTC-PERP")
        self.assertEqual(ex.discover_product_id("BTC-USD"), "BTC-PERP")
        self.assertEqual(ex.discover_product_id("BTC-PERP"), "BTC-PERP")
        self.assertEqual(ex.discover_product_id("BTC-INTX"), "BTC-PERP")

    def test_discover_not_found(self):
        ex, _ = build_executor(products=[])
        self.assertIsNone(ex.discover_product_id("DOGE"))


class TestSummaryPositions(unittest.TestCase):
    def test_summary_with_portfolio(self):
        ex, client = build_executor(products=[], portfolio_uuid="pu")
        client.get_perps_portfolio_summary.return_value = {"x": 1}
        self.assertEqual(ex.summary(), {"x": 1})

    def test_summary_with_portfolio_error(self):
        ex, client = build_executor(products=[], portfolio_uuid="pu")
        client.get_perps_portfolio_summary.side_effect = RuntimeError("x")
        self.assertEqual(ex.summary(), {})

    def test_summary_no_portfolio(self):
        ex, client = build_executor(products=[])
        client.get_futures_balance_summary.return_value = {"y": 2}
        self.assertEqual(ex.summary(), {"y": 2})

    def test_summary_no_portfolio_error(self):
        ex, client = build_executor(products=[])
        client.get_futures_balance_summary.side_effect = RuntimeError("x")
        self.assertEqual(ex.summary(), {})

    def test_positions_no_portfolio(self):
        ex, _ = build_executor(products=[])
        self.assertEqual(ex.positions(), {})

    def test_positions_with_portfolio(self):
        ex, client = build_executor(products=[], portfolio_uuid="pu")
        client.list_perps_positions.return_value = {"p": 1}
        self.assertEqual(ex.positions(), {"p": 1})

    def test_positions_error(self):
        ex, client = build_executor(products=[], portfolio_uuid="pu")
        client.list_perps_positions.side_effect = RuntimeError("x")
        self.assertEqual(ex.positions(), {})


class TestOrderKwargs(unittest.TestCase):
    def test_with_portfolio(self):
        ex, _ = build_executor(products=[], portfolio_uuid="pu")
        kw = ex._order_kwargs(3.0)
        self.assertEqual(kw["leverage"], "3.0")
        self.assertEqual(kw["margin_type"], "CROSS")
        self.assertEqual(kw["retail_portfolio_id"], "pu")

    def test_without_portfolio(self):
        ex, _ = build_executor(products=[])
        kw = ex._order_kwargs(4.0)
        self.assertEqual(kw["leverage"], "4.0")
        self.assertNotIn("retail_portfolio_id", kw)

    def test_leverage_default(self):
        ex, _ = build_executor(products=[])
        kw = ex._order_kwargs(None)
        self.assertEqual(kw["leverage"], "2.0")


class TestMarketEntryStopTarget(unittest.TestCase):
    def test_market_entry_buy(self):
        ex, client = build_executor(products=[])
        client.market_order_buy.return_value = {"order_id": "o1"}
        r = ex._market_entry("BTC-PERP", "BUY", "1.0", 2.0)
        self.assertEqual(r["order_id"], "o1")
        client.market_order_buy.assert_called_once()

    def test_market_entry_sell(self):
        ex, client = build_executor(products=[])
        client.market_order_sell.return_value = {"order_id": "o2"}
        r = ex._market_entry("BTC-PERP", "sell", "1.0", 2.0)
        self.assertEqual(r["order_id"], "o2")

    def test_stop_exit_sell(self):
        ex, client = build_executor(products=[])
        client.stop_limit_order_gtc_sell.return_value = {"order_id": "s1"}
        r = ex._stop_exit("P", "SELL", "1.0", "100.0", 2.0)
        self.assertEqual(r["order_id"], "s1")
        _, kwargs = client.stop_limit_order_gtc_sell.call_args
        # limit price = 100 * 0.999
        self.assertEqual(kwargs["limit_price"], "99.90")

    def test_stop_exit_buy(self):
        ex, client = build_executor(products=[])
        client.stop_limit_order_gtc_buy.return_value = {"order_id": "s2"}
        r = ex._stop_exit("P", "BUY", "1.0", "100.0", 2.0)
        self.assertEqual(r["order_id"], "s2")
        _, kwargs = client.stop_limit_order_gtc_buy.call_args
        self.assertEqual(kwargs["limit_price"], "100.10")

    def test_target_exit_sell(self):
        ex, client = build_executor(products=[])
        client.limit_order_gtc_sell.return_value = {"order_id": "t1"}
        r = ex._target_exit("P", "SELL", "1.0", "150.0", 2.0)
        self.assertEqual(r["order_id"], "t1")

    def test_target_exit_buy(self):
        ex, client = build_executor(products=[])
        client.limit_order_gtc_buy.return_value = {"order_id": "t2"}
        r = ex._target_exit("P", "BUY", "1.0", "150.0", 2.0)
        self.assertEqual(r["order_id"], "t2")


class TestPlaceBracket(unittest.TestCase):
    def test_place_success(self):
        ex, client = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        client.market_order_buy.return_value = {"order_id": "e", "client_order_id": "ce"}
        client.stop_limit_order_gtc_sell.return_value = {"order_id": "s"}
        client.limit_order_gtc_sell.return_value = {"order_id": "t"}
        res = ex.place_bracket(symbol="BTC", side="BUY", base_size=1.5,
                               stop_price=100.0, target_price=150.0, leverage=2.0)
        self.assertTrue(res.success)
        self.assertEqual(res.order_id, "e")
        self.assertEqual(res.client_order_id, "ce")
        self.assertEqual(res.raw["product_id"], "BTC-PERP")

    def test_place_exception(self):
        ex, client = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        client.market_order_buy.side_effect = RuntimeError("boom")
        res = ex.place_bracket(symbol="BTC", side="BUY", base_size=1.0,
                               stop_price=100.0, target_price=150.0, leverage=2.0)
        self.assertFalse(res.success)
        self.assertEqual(res.error, "boom")
        self.assertEqual(res.raw["product_id"], "BTC-PERP")

    def test_base_size_format(self):
        ex, client = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        client.market_order_buy.return_value = {"order_id": "e"}
        client.stop_limit_order_gtc_sell.return_value = {"order_id": "s"}
        client.limit_order_gtc_sell.return_value = {"order_id": "t"}
        ex.place_bracket(symbol="BTC", side="BUY", base_size=1.0,
                         stop_price=100.0, target_price=150.0, leverage=2.0)
        _, kwargs = client.market_order_buy.call_args
        # base_size 1.0 -> "1"
        self.assertEqual(kwargs["base_size"], "1")


class TestClosePosition(unittest.TestCase):
    def test_close_no_size(self):
        ex, client = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        client.close_position.return_value = {"order_id": "c1"}
        res = ex.close_position("BTC")
        self.assertTrue(res.success)
        self.assertEqual(res.order_id, "c1")
        args, _ = client.close_position.call_args
        self.assertEqual(len(args), 2)  # uuid + product_id

    def test_close_with_size(self):
        ex, client = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        client.close_position.return_value = {"order_id": "c2"}
        res = ex.close_position("BTC", size=2.5)
        self.assertTrue(res.success)
        args, kwargs = client.close_position.call_args
        self.assertEqual(kwargs["size"], "2.50000000")

    def test_close_exception(self):
        ex, client = build_executor(products=[
            {"product_id": "BTC-PERP", "contract_expiry_type": "PERPETUAL"}])
        client.close_position.side_effect = RuntimeError("fail")
        res = ex.close_position("BTC")
        self.assertFalse(res.success)
        self.assertEqual(res.error, "fail")


if __name__ == "__main__":
    unittest.main()
