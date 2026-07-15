from unittest.mock import MagicMock

from _helpers import install_fakes

install_fakes({
    "trading_system.connectors.coinbase_v3": {"CoinbaseConnectorV3": MagicMock()},
})

from trading_system.apps.worker import market_fetcher as mf


def test_get_connector_real_lazy_path():
    f = mf.MarketDataFetcher(products=["BTC-USD"])
    conn = f._get_connector()
    assert conn is not None
    assert f._get_connector() is conn


def make_fetcher(connector):
    f = mf.MarketDataFetcher(products=["BTC-USD", "ETH-USD"])
    f._get_connector = lambda: connector
    return f


def test_fetch_all_with_book():
    connector = MagicMock()
    connector.get_price.return_value = {
        "price": 100, "quote_volume": 50, "price_percent_change_24h": 2.5,
    }
    connector.get_order_book.return_value = {
        "bids": [{"price": 99}], "asks": [{"price": 101}],
    }
    f = make_fetcher(connector)
    out = f.fetch_all()
    assert out["BTC-USD"]["price"] == 100.0
    assert 0 < out["BTC-USD"]["spread"] < 1


def test_fetch_all_no_book_fallback():
    connector = MagicMock()
    connector.get_price.return_value = {
        "price": 100, "quote_volume": 50, "price_percent_change_24h": 2.5,
    }
    connector.get_order_book.return_value = {"bids": [], "asks": []}
    f = make_fetcher(connector)
    out = f.fetch_all()
    assert out["BTC-USD"]["price"] == 100.0
    assert out["BTC-USD"]["spread"] == 0.0


def test_fetch_all_zero_price():
    connector = MagicMock()
    connector.get_price.return_value = {
        "price": 0, "quote_volume": 0, "price_percent_change_24h": 0,
    }
    connector.get_order_book.return_value = {"bids": [], "asks": []}
    f = make_fetcher(connector)
    out = f.fetch_all()
    assert out["BTC-USD"]["price"] == 0.0
    assert out["BTC-USD"]["spread"] == 0.0


def test_fetch_all_exception_stale_then_default():
    connector = MagicMock()
    connector.get_price.return_value = {
        "price": 100, "quote_volume": 50, "price_percent_change_24h": 1.0,
    }
    connector.get_order_book.return_value = {
        "bids": [{"price": 99}], "asks": [{"price": 101}],
    }
    # first call succeeds, caching BTC-USD
    f = make_fetcher(connector)
    f.fetch_all()

    # now connector fails
    connector.get_price.side_effect = RuntimeError("boom")
    out = f.fetch_all()
    # stale fallback used for BTC-USD
    assert out["BTC-USD"]["price"] == 100.0

    # a product never fetched that fails -> default
    connector.get_price.side_effect = RuntimeError("boom")
    f2 = make_fetcher(connector)
    out2 = f2.fetch_all()
    assert out2["BTC-USD"] == {"price": 0.0, "spread": 0.0}


def test_fetch_single():
    connector = MagicMock()
    connector.get_price.return_value = {"price": 100, "quote_volume": 1, "price_percent_change_24h": 0}
    connector.get_order_book.return_value = {"bids": [{"price": 99}], "asks": [{"price": 101}]}
    f = make_fetcher(connector)
    assert f.fetch_single("BTC-USD")["price"] == 100.0
    assert f.fetch_single("MISSING") == {"price": 0.0, "spread": 0.0}


def test_build_market_state():
    prices = {"BTC-USD": {"price": 100, "spread": 0.001, "volume_24h": 5, "change_pct": 0.02}}
    state = mf.build_market_state(prices, "BTC-USD", regime="bull", sentiment_score=0.5, global_consensus=-0.2)
    assert state["product_id"] == "BTC-USD"
    assert state["regime"] == "bull"
    assert state["market_leaders"] == ["BTC-USD"]
    # missing product falls back to zeros
    state2 = mf.build_market_state({}, "ETH-USD")
    assert state2["price"] == 0
