"""Coverage tests for event_markets client modules:
unified_client, signal_adapter, kalshi_client, polymarket_client,
polymarket_relayer, streaming."""
import json
from types import SimpleNamespace
from unittest import mock

import pytest

import event_markets.unified_client as uc
from event_markets.unified_client import (
    UnifiedPredictionMarketClient, PredictionMarket,
)
from event_markets.signal_adapter import PredictionMarketAdapter as PMA
from event_markets.kalshi_client import KalshiClient, KalshiMarket, format_market as kalshi_format_market
from event_markets.polymarket_client import (
    PolymarketClient, PolymarketMarket, PolymarketBook, format_market,
)


# ───────────── PredictionMarket dataclass ─────────────

def _pm(platform="kalshi", market_id="k1", question="Will BTC hit $100k?",
        yes=0.6, no=0.4, volume=50000.0, spread=0.02, liq=0.8,
        category="crypto", keywords=None, end_date="2030-01-01T00:00:00Z",
        is_open=True, raw=None):
    return PredictionMarket(
        platform=platform, market_id=market_id, question=question,
        outcomes=["YES", "NO"], outcome_prices={"YES": yes, "NO": no},
        volume=volume, end_date=end_date, is_open=is_open,
        yes_bid=yes - spread/2, yes_ask=yes + spread/2, spread=spread,
        liquidity_score=liq, category=category,
        keywords=keywords or [], raw_data=raw or {},
    )


def test_pm_mid_price():
    assert _pm(yes=0.7).mid_price == 0.7
    m = _pm(yes=0.0)
    m.outcome_prices = {}
    assert m.mid_price == 0.0
    m2 = _pm(yes=0.0)
    m2.outcome_prices = {"X": 0.3}
    assert m2.mid_price == 0.3


def test_pm_extremity_relevant_format():
    assert _pm(yes=0.5).probability_extremity == 0.0
    assert _pm(yes=1.0).probability_extremity == 0.0
    assert _pm(yes=0.9).probability_extremity == 0.8
    assert _pm(question="Will BTC rise?").is_relevant is True
    assert _pm(question="Who will win the world cup?").is_relevant is False
    assert "BTC" in _pm().format()


def test_detect_category():
    assert uc.UnifiedPredictionMarketClient._detect_category("Will BTC hit 100k?") == "crypto"
    assert uc.UnifiedPredictionMarketClient._detect_category("Super bowl winner?") == "sports"
    assert uc.UnifiedPredictionMarketClient._detect_category("Fed rate decision?") == "economics"
    assert uc.UnifiedPredictionMarketClient._detect_category("random nonsense here") == "general"


# ───────────── UnifiedPredictionMarketClient ─────────────

def _client():
    c = UnifiedPredictionMarketClient()
    c._kalshi = mock.MagicMock()
    c._polymarket = mock.MagicMock()
    c._polymarket_relayer = mock.MagicMock()
    return c


def test_search_kalshi_no_auth():
    c = _client()
    assert c.search_kalshi() == []


def test_search_kalshi_with_auth():
    c = _client()
    c._kalshi.email = "a@b.com"
    c._kalshi.password = "pw"
    km = KalshiMarket(ticker="K1", title="Will BTC hit 100k?", event_ticker="btc",
                      yes_bid=0.59, yes_ask=0.61, no_bid=0.39, no_ask=0.41,
                      volume=50000.0, open_interest=200, close_date="2030-01-01T00:00:00Z",
                      status="open", settled=False, category="Crypto")
    c._kalshi.get_relevant_markets.return_value = [km]
    res = c.search_kalshi(limit=10)
    assert len(res) == 1
    assert res[0].platform == "kalshi"


def test_search_kalshi_term_uses_search_markets():
    c = _client()
    c._kalshi.api_key_id = "kid"
    c._kalshi.private_key_path = "/tmp/k.pem"
    km = KalshiMarket(ticker="K1", title="BTC price", event_ticker="btc",
                      yes_bid=0.59, yes_ask=0.61, no_bid=0.39, no_ask=0.41,
                      volume=50000.0, open_interest=200, close_date="2030-01-01T00:00:00Z",
                      status="open", settled=False)
    c._kalshi.search_markets.return_value = [km]
    res = c.search_kalshi(term="btc", limit=5)
    assert len(res) == 1


def _poly_market(question="Will BTC hit 100k?", condition="c1", yes=0.6, spread=0.02):
    return PolymarketMarket(
        condition_id=condition, question=question, description="",
        outcomes=["YES", "NO"], outcome_prices={"YES": yes, "NO": 1-yes},
        volume=50000.0, end_date_iso="2030-01-01T00:00:00Z", closed=False,
        accepting_orders=True, tokens=[{"token_id": "t1"}], ticker="slug",
        event_slug="ev", yes_bid=yes-spread/2, yes_ask=yes+spread/2, spread=spread,
    )


def test_search_polymarket():
    c = _client()
    pm = _poly_market()
    c._polymarket.search_markets.return_value = [pm]
    res = c.search_polymarket(limit=10)
    assert len(res) == 1
    assert res[0].platform == "polymarket"


def test_search_polymarket_spread_fallback_to_book():
    c = _client()
    pm = _poly_market(spread=0.0)
    pm.yes_bid = 0.0
    pm.yes_ask = 0.0
    c._polymarket.search_markets.return_value = [pm]
    c._polymarket.get_order_book.return_value = PolymarketBook(
        bids=[(0.58, 10)], asks=[(0.62, 10)], spread=0.04, mid_price=0.6)
    res = c.search_polymarket(limit=10)
    assert len(res) == 1


def test_search_all_combines_and_exceptions():
    c = _client()
    c._kalshi.email = "a"
    c._kalshi.password = "b"
    km = KalshiMarket(ticker="K1", title="BTC", event_ticker="btc",
                      yes_bid=0.59, yes_ask=0.61, no_bid=0.39, no_ask=0.41,
                      volume=50000.0, open_interest=200, close_date="2030-01-01T00:00:00Z",
                      status="open", settled=False)
    c._kalshi.get_relevant_markets.return_value = [km]
    c._polymarket.search_markets.side_effect = RuntimeError("boom")
    res = c.search_all(limit=10)
    assert len(res) == 1
    # polymarket fails, kalshi still returns


def test_search_all_categories_cache_and_both():
    c = _client()
    pm = _poly_market()
    c._polymarket.fetch_markets.return_value = [pm]
    c._kalshi.api_key_id = "kid"
    c._kalshi.private_key_path = "/tmp/k.pem"
    c._kalshi.get_markets_by_categories.return_value = {
        "Crypto": [KalshiMarket(ticker="K1", title="BTC", event_ticker="btc",
                                yes_bid=0.59, yes_ask=0.61, no_bid=0.39, no_ask=0.41,
                                volume=50000.0, open_interest=200,
                                close_date="2030-01-01T00:00:00Z", status="open",
                                settled=False, category="Crypto")]
    }
    out = c.search_all_categories(limit_per_platform=5)
    assert "crypto" in out
    assert len(out["crypto"]) >= 1
    # second call hits cache
    out2 = c.search_all_categories(limit_per_platform=5)
    assert out2 == out


def test_search_all_categories_no_api_auth_fallback():
    c = _client()
    pm = _poly_market()
    c._polymarket.fetch_markets.return_value = [pm]
    c._kalshi.search_broad.return_value = [
        KalshiMarket(ticker="K1", title="BTC", event_ticker="btc",
                     yes_bid=0.59, yes_ask=0.61, no_bid=0.39, no_ask=0.41,
                     volume=50000.0, open_interest=200,
                     close_date="2030-01-01T00:00:00Z", status="open", settled=False)]
    out = c.search_all_categories(limit_per_platform=5)
    assert "crypto" in out


def test_category_getters():
    c = _client()
    c._polymarket.get_crypto_markets.return_value = [_poly_market()]
    c._kalshi.get_relevant_markets.return_value = []
    assert len(c.get_crypto_markets()) == 1
    for fn in (c.get_sports_markets, c.get_politics_markets,
               c.get_entertainment_markets, c.get_economics_markets,
               c.get_technology_markets):
        c._polymarket.get_sports_markets.return_value = []
        c._polymarket.get_politics_markets.return_value = []
        c._polymarket.get_entertainment_markets.return_value = []
        c._polymarket.get_economics_markets.return_value = []
        c._polymarket.get_technology_markets.return_value = []
    assert c.get_sports_markets() == []
    c._polymarket.get_sports_markets.side_effect = RuntimeError("x")
    assert c.get_sports_markets() == []
    c._polymarket.get_politics_markets.side_effect = RuntimeError("x")
    assert c.get_politics_markets() == []
    c._polymarket.get_entertainment_markets.side_effect = RuntimeError("x")
    assert c.get_entertainment_markets() == []
    c._polymarket.get_economics_markets.side_effect = RuntimeError("x")
    assert c.get_economics_markets() == []
    c._polymarket.get_technology_markets.side_effect = RuntimeError("x")
    assert c.get_technology_markets() == []
    c._kalshi.get_relevant_markets.side_effect = RuntimeError("x")
    c._polymarket.get_crypto_markets.side_effect = RuntimeError("x")
    assert c.get_crypto_markets() == []


def test_relayer_and_format():
    c = _client()
    c._polymarket_relayer.list_api_keys.return_value = [{"key": "1"}]
    assert c.get_polymarket_relayer_keys() == [{"key": "1"}]
    c._kalshi.get_order_book.return_value = {"bids": [], "asks": []}
    assert c.get_kalshi_order_book_depth("k") == {"bids": [], "asks": []}
    c._polymarket.get_order_book.return_value = PolymarketBook()
    assert c.get_polymarket_order_book("t") is not None
    assert "Platform" in c.format_markets([_pm()])


# ───────────── signal_adapter ─────────────

def _adapter():
    a = PMA.__new__(PMA)
    a._client = mock.MagicMock()
    a.min_volume = 2000
    a.min_extremity = 0.2
    a.min_open_interest = 100
    a.max_spread = 0.20
    a.categories = ["crypto"]
    return a


def test_adapter_crypto_signals():
    a = _adapter()
    pm = _pm(platform="polymarket", market_id="p1", question="Will BTC hit 100k?",
            yes=0.85, no=0.15, volume=50000.0, spread=0.03, liq=0.9,
            category="crypto")
    pm.raw_data = {"token_ids": ["t1"], "open_interest": 500}
    a._client.get_crypto_markets.return_value = [pm]
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.83, 100)], asks=[(0.87, 100)], spread=0.04, mid_price=0.85)
    sigs = a.get_signals()
    assert len(sigs) == 1
    assert sigs[0]["action"] == "BUY"
    assert sigs[0]["symbol"] == "BTC-USD"


def test_adapter_filters_and_dedup():
    a = _adapter()
    pm = _pm(question="Will BTC hit 100k?", yes=0.3, no=0.7, volume=500.0,
            spread=0.5, liq=0.1, category="crypto")
    pm.raw_data = {}
    a._client.get_crypto_markets.return_value = [pm]
    assert a.get_signals() == []


def test_adapter_all_categories():
    a = _adapter()
    a.categories = ["*"]
    pm = _pm(question="Will BTC hit 100k?", yes=0.8, no=0.2, volume=50000,
            spread=0.03, liq=0.9, category="crypto")
    pm.raw_data = {"token_ids": ["t1"], "open_interest": 500}
    a._client.search_all_categories.return_value = {"crypto": [pm]}
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.78, 100)], asks=[(0.82, 100)], spread=0.04, mid_price=0.8)
    sigs = a.get_signals()
    assert len(sigs) == 1


def test_adapter_symbol_mapping_sports():
    assert PMA._question_to_symbol(
        "Who wins the super bowl?", "sports") == "BTC-USD"
    assert PMA._question_to_symbol(
        "Will ETH surpass 5000?", "crypto") == "ETH-USD"
    assert PMA._question_to_symbol(
        "random", "general") == "BTC-USD"


def test_adapter_hours_to_expiry():
    a = _adapter()
    h = a._hours_to_expiry("2030-01-01T00:00:00Z")
    assert h > 0
    assert a._hours_to_expiry("not-a-date") == 168


def test_adapter_make_signal_kelly():
    a = _adapter()
    pm = _pm(platform="polymarket", question="Will BTC hit 100k?", yes=0.85,
            no=0.15, volume=50000, spread=0.03, liq=0.9, category="crypto")
    pm.raw_data = {"token_ids": ["t1"], "open_interest": 500}
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.83, 100)], asks=[(0.87, 100)], spread=0.04, mid_price=0.85)
    sig = a._make_signal("BTC-USD", "BUY", 0.5, 0.25, pm, "reason")
    assert sig["kelly_fraction"] >= 0
    sig2 = a._make_signal("BTC-USD", "SELL", 0.5, 0.25, pm, "reason")
    assert sig2["kelly_fraction"] >= 0


def test_adapter_exceptions():
    a = _adapter()
    a._client.get_crypto_markets.side_effect = RuntimeError("boom")
    assert a.get_signals() == []
    a._client.get_crypto_markets.side_effect = None
    a._client.get_crypto_markets.return_value = [_pm(yes=0.8, raw={"open_interest": 500})]
    a._client.get_polymarket_order_book.side_effect = RuntimeError("x")
    # should still return signals (depth 0)
    assert len(a.get_signals()) >= 1


# ───────────── kalshi_client via urlopen ─────────────

class _Resp:
    def __init__(self, data, status=200, headers=None):
        self._b = json.dumps(data).encode() if not isinstance(data, (bytes, bytearray)) else data
        self.status = status
        self.headers = headers or {}

    def read(self):
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _kc(email="", password="", api_key_id="", private_key_path=""):
    return KalshiClient(email=email, password=password, api_key_id=api_key_id,
                        private_key_path=private_key_path, timeout=5)


def test_kalshi_parse_market_v2_and_v1():
    m = KalshiClient()._parse_market({"ticker": "K", "title": "t",
        "yes_bid_dollars": 0.4, "yes_ask_dollars": 0.6, "no_bid_dollars": 0.4,
        "no_ask_dollars": 0.6, "volume_24h_fp": 100, "open_interest_fp": 10,
        "close_time": "2030-01-01T00:00:00Z", "status": "open", "settled": False,
        "category": "Crypto"})
    assert m.yes_bid == 0.4
    m1 = KalshiClient()._parse_market({"ticker": "K", "title": "t",
        "yes_bid": 40, "yes_ask": 60, "no_bid": 40, "no_ask": 60, "volume": 100,
        "open_interest": 10, "close_date": "2030-01-01T00:00:00Z", "status": "open"})
    assert m1.yes_bid == 0.4


def test_kalshi_login_paths(monkeypatch):
    kc = _kc(email="a@b.com", password="pw")

    def fake(req, timeout=None):
        assert "/login" in req.full_url
        return _Resp({"token": "T", "member_id": "M"})

    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake)
    kc._login()
    assert kc._token == "T"
    # no creds
    kc2 = _kc()
    kc2._login()
    assert kc2._token == ""
    # login failure
    def fake_fail(req, timeout=None):
        raise OSError("net")
    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake_fail)
    kc3 = _kc(email="a", password="b")
    kc3._login()
    assert kc3._token == ""


def _make_pem(tmp_path):
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    p = tmp_path / "k.pem"
    p.write_bytes(key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()))
    return str(p)


def test_kalshi_get_and_auth_header(tmp_path):
    pem = _make_pem(tmp_path)
    kc = _kc(api_key_id="kid", private_key_path=pem)
    assert kc._use_api_key_auth() is True
    h = kc._auth_header("GET", "/x")
    assert "KALSHI-ACCESS-KEY" in h
    assert kc._sign_request("1", "GET", "/x")
    # email path
    kc2 = _kc(email="a", password="b")
    kc2._token = ""
    h2 = kc2._auth_header("GET", "/x")
    assert "Bearer" in h2["Authorization"]
    assert kc2._use_api_key_auth() is False


def test_kalshi_get_routes(monkeypatch):
    routes = {
        "/markets": {"markets": [{"ticker": "K", "title": "BTC price",
            "yes_bid_dollars": 0.4, "yes_ask_dollars": 0.6, "no_bid_dollars": 0.4,
            "no_ask_dollars": 0.6, "volume_24h_fp": 100000, "open_interest_fp": 500,
            "close_time": "2030-01-01T00:00:00Z", "status": "open", "settled": False,
            "category": "Crypto"}]},
        "/events": {"events": [{"category": "Crypto", "event_ticker": "btc",
            "markets": [{"ticker": "K", "title": "BTC", "yes_bid_dollars": 0.4,
                         "yes_ask_dollars": 0.6, "no_bid_dollars": 0.4,
                         "no_ask_dollars": 0.6, "volume_24h_fp": 100000,
                         "open_interest_fp": 500, "close_time": "2030-01-01T00:00:00Z",
                         "status": "open", "settled": False}]}]},
        "/markets/K/orderbook": {"bids": [{"price": 0.4, "size": 5}],
                                 "asks": [{"price": 0.6, "size": 5}]},
        "/portfolio/balance": {"balance": 1000},
    }

    def fake(req, timeout=None):
        url = req.full_url
        if "/markets/" in url:
            return _Resp({"market": {"ticker": "K", "title": "BTC",
                "yes_bid_dollars": 0.4, "yes_ask_dollars": 0.6, "no_bid_dollars": 0.4,
                "no_ask_dollars": 0.6, "volume_24h_fp": 100000, "open_interest_fp": 500,
                "close_time": "2030-01-01T00:00:00Z", "status": "open", "settled": False}})
        for sub, data in routes.items():
            if sub in url:
                return _Resp(data)
        return _Resp({})

    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake)
    kc = _kc(email="a", password="b")
    kc._token = "T"
    ms = kc.search_markets(term="btc", limit=5)
    assert len(ms) == 1
    # search_markets exception
    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen",
                        lambda req, timeout=None: (_ for _ in ()).throw(OSError("x")))
    assert kc.search_markets(term="btc") == []
    # get_market / get_settlement
    kc._token = "T"
    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake)
    mk = kc.get_market("K")
    assert mk["ticker"] == "K"
    assert kc.get_settlement("K") is None  # not settled


def test_kalshi_get_market_empty():
    assert KalshiClient().get_market("") is None


def test_kalshi_relevant_and_broad(monkeypatch):
    def fake(req, timeout=None):
        url = req.full_url
        if "/events" in url:
            return _Resp({"events": [{"category": "Crypto", "event_ticker": "btc",
                "markets": [{"ticker": "K", "title": "BTC", "yes_bid_dollars": 0.43,
                             "yes_ask_dollars": 0.57, "no_bid_dollars": 0.43,
                             "no_ask_dollars": 0.57, "volume_24h_fp": 100000,
                             "open_interest_fp": 500, "close_time": "2030-01-01T00:00:00Z",
                             "status": "open", "settled": False}]}]})
        if "/markets" in url:
            return _Resp({"markets": [{"ticker": "K", "title": "BTC", "yes_bid_dollars": 0.43,
                           "yes_ask_dollars": 0.57, "no_bid_dollars": 0.43,
                           "no_ask_dollars": 0.57, "volume_24h_fp": 100000,
                           "open_interest_fp": 500, "close_time": "2030-01-01T00:00:00Z",
                           "status": "open", "settled": False}]})
        return _Resp({})

    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake)
    kc = _kc(email="a", password="b")
    kc._token = "T"
    rel = kc.get_relevant_markets(limit=5)
    assert len(rel) == 1
    broad = kc.search_broad(limit=5)
    assert len(broad) == 1
    # get_event_markets
    em = kc.get_event_markets("btc")
    assert len(em) == 1
    # fetch_events_with_markets with categories filter
    fe = kc.fetch_events_with_markets(limit=10, categories=["Crypto"])
    assert len(fe) == 1
    # fetch_all_events
    assert len(kc.fetch_all_events()) == 1


def test_kalshi_filter_keywords():
    kc = KalshiClient()
    evs = [{"title": "Bitcoin cup", "event_ticker": "btc"},
           {"title": "soccer match", "event_ticker": "soc"}]
    assert kc._filter_events_by_keywords(evs, ["bitcoin"]) == ["btc"]
    assert kc._filter_events_by_keywords(evs, ["btc"]) == ["btc"]


def test_kalshi_order_book_and_balance_fail(monkeypatch):
    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen",
                        lambda req, timeout=None: (_ for _ in ()).throw(OSError("x")))
    kc = _kc(email="a", password="b")
    kc._token = "T"
    assert kc.get_order_book("K") == {}
    assert kc.get_balance() == {}


def test_kalshi_write_create_cancel_orders(monkeypatch, tmp_path):
    pem = _make_pem(tmp_path)
    # No api-key auth -> _write raises RuntimeError
    kc_noauth = _kc(email="a", password="b")
    with pytest.raises(RuntimeError):
        kc_noauth.create_order("K", "yes", "buy", 1, price=0.5)
    with pytest.raises(RuntimeError):
        kc_noauth.cancel_order("o1")

    def fake_write(req, timeout=None):
        return _Resp({"order_id": "O1"})

    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake_write)
    kc = _kc(api_key_id="kid", private_key_path=pem)
    res = kc.create_order("K", "yes", "buy", 1, price=0.5)
    assert res["order_id"] == "O1"
    res2 = kc.cancel_order("O1")
    assert res2["order_id"] == "O1"


def test_kalshi_get_orders_positions_fills(monkeypatch):
    def fake(req, timeout=None):
        url = req.full_url
        if "/orders" in url:
            return _Resp({"orders": [{"id": "1"}]})
        if "/positions" in url:
            return _Resp({"market_positions": [{"id": "p"}]})
        if "/fills" in url:
            return _Resp({"fills": [{"id": "f"}]})
        return _Resp({})

    monkeypatch.setattr("event_markets.kalshi_client.urllib.request.urlopen", fake)
    kc = _kc(email="a", password="b")
    kc._token = "T"
    assert kc.get_orders("K") == [{"id": "1"}]
    assert kc.get_positions() == [{"id": "p"}]
    assert kc.get_fills("K") == [{"id": "f"}]


def test_kalshi_format_market():
    m = KalshiMarket(ticker="K", title="BTC", event_ticker="btc", yes_bid=0.4,
                      yes_ask=0.6, no_bid=0.4, no_ask=0.6, volume=100,
                      open_interest=0, close_date="2030", status="open", settled=False)
    assert "BTC" in kalshi_format_market(m)


# ───────────── polymarket_client via urlopen ─────────────

def test_polymarket_parse_gamma_market():
    raw = {"conditionId": "c1", "question": "BTC?", "outcomes": '["YES","NO"]',
           "outcomePrices": '["0.6","0.4"]', "volume": "1000", "events": [{"slug": "ev"}],
           "spread": 0.02, "bestBid": 0.59, "bestAsk": 0.61, "clobTokenIds": '["t1"]',
           "acceptingOrders": True, "closed": False, "slug": "sl"}
    pm = PolymarketClient()._parse_gamma_market(raw)
    assert pm.outcome_prices["YES"] == 0.6
    # string outcomes fallback
    raw2 = {"conditionId": "c2", "question": "ETH?", "outcomes": ["YES", "NO"],
            "outcomePrices": ["0.5", "0.5"], "volume": 100, "spread": 0,
            "bestBid": 0, "bestAsk": 1, "clobTokenIds": [], "acceptingOrders": True,
            "closed": False}
    pm2 = PolymarketClient()._parse_gamma_market(raw2)
    assert pm2.outcome_prices["YES"] == 0.5
    # bad json outcomes
    raw3 = dict(raw2, outcomes='notjson', outcomePrices='bad')
    pm3 = PolymarketClient()._parse_gamma_market(raw3)
    assert pm3.outcome_prices.get("YES", pm3.outcome_prices.get("Yes")) == 0.5


def test_polymarket_gamma_get(monkeypatch):
    def fake(req, timeout=None):
        if "/markets" in req.full_url:
            return _Resp([{"conditionId": "c1", "question": "BTC price?",
                "outcomes": ["YES", "NO"], "outcomePrices": ["0.6", "0.4"],
                "volume": 1000, "spread": 0.02, "bestBid": 0.59, "bestAsk": 0.61,
                "clobTokenIds": ["t1"], "acceptingOrders": True, "closed": False}])
        if "/events" in req.full_url:
            return _Resp([{"slug": "crypto-ev"}])
        return _Resp({})

    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen", fake)
    pc = PolymarketClient(timeout=5)
    ms = pc.fetch_markets(limit=10)
    assert len(ms) == 1
    ms2 = pc.search_markets(term="btc", limit=10)
    assert len(ms2) == 1
    assert pc.fetch_crypto_events(limit=10) == ["crypto-ev"]


def test_polymarket_gamma_get_fail(monkeypatch):
    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen",
                        lambda req, timeout=None: (_ for _ in ()).throw(OSError("x")))
    pc = PolymarketClient()
    assert pc.fetch_markets() == []
    assert pc.search_markets() == []
    assert pc.fetch_crypto_events() == []
    assert pc.get_markets_by_tag("crypto") == []
    assert pc.fetch_market_detail("c1") is None


def test_polymarket_crypto_markets_and_categories(monkeypatch):
    def fake(req, timeout=None):
        url = req.full_url
        if "/markets" in url:
            return _Resp([{"conditionId": "c1", "question": "Will BTC hit 100k?",
                "outcomes": ["YES", "NO"], "outcomePrices": ["0.6", "0.4"],
                "volume": 1000, "spread": 0.02, "bestBid": 0.59, "bestAsk": 0.61,
                "clobTokenIds": ["t1"], "acceptingOrders": True, "closed": False}])
        return _Resp({})

    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen", fake)
    pc = PolymarketClient()
    cms = pc.get_crypto_markets(limit=10)
    assert len(cms) == 1
    # fallback path when tag returns empty
    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen",
                        lambda req, timeout=None: _Resp([]))
    cms2 = pc.get_crypto_markets(limit=10)
    assert cms2 == []
    # category getters
    assert pc.get_sports_markets() == []
    assert pc.get_politics_markets() == []
    assert pc.get_entertainment_markets() == []
    assert pc.get_economics_markets() == []
    assert pc.get_technology_markets() == []
    # all category markets
    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen", fake)
    allc = pc.get_all_category_markets(limit_per_category=5)
    assert "crypto" in allc


def test_polymarket_order_book(monkeypatch):
    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen",
                        lambda req, timeout=None: _Resp(
                            {"asks": [{"price": "0.62", "size": "10"}],
                             "bids": [{"price": "0.58", "size": "10"}]}))
    pc = PolymarketClient()
    book = pc.get_order_book("t1")
    assert book.spread == pytest.approx(0.04)
    # failure
    monkeypatch.setattr("event_markets.polymarket_client.urllib.request.urlopen",
                        lambda req, timeout=None: (_ for _ in ()).throw(OSError("x")))
    assert pc.get_order_book("t1").bids == []


def test_polymarket_filter_keywords():
    pc = PolymarketClient()
    ms = [_poly_market(question="Will BTC rise?"), _poly_market(question="soccer match")]
    out = pc._filter_by_keywords(ms, ["bitcoin", "btc"], 10)
    assert len(out) == 1
    out2 = pc._markets_by_event_slugs(["ev"], ms)
    assert len(out2) == 2


def test_polymarket_get_market_detail_invalid():
    pc = PolymarketClient()
    assert pc.fetch_market_detail("") is None


def test_polymarket_format():
    assert "BTC" in format_market(_poly_market())


# ───────────── polymarket_relayer ─────────────

def test_relayer_credentials_from_file(tmp_path):
    from event_markets.polymarket_relayer import (
        PolymarketRelayerCredentials, PolymarketBuilderCredentials,
        PolymarketRelayerClient, load_relayer_credentials, load_builder_credentials,
    )
    f = tmp_path / "relayer.txt"
    f.write_text("RELAYER_API_KEY: abc\nRELAYER_API_KEY_ADDRESS: addr\n")
    c = PolymarketRelayerCredentials.from_file(str(f))
    assert c.api_key == "abc"
    f2 = tmp_path / "builder.txt"
    f2.write_text("APIKEY: a\nSECRET: s\nPASSPHRASE: p\n")
    b = PolymarketBuilderCredentials.from_file(str(f2))
    assert b.api_key == "a"
    assert load_relayer_credentials(str(f)).api_key == "abc"
    assert load_builder_credentials(str(f2)).secret == "s"


def test_relayer_client(monkeypatch, tmp_path):
    from event_markets.polymarket_relayer import PolymarketRelayerClient
    monkeypatch.setattr("event_markets.polymarket_relayer.urlopen",
                        lambda req, timeout=None: _Resp({"data": [{"key": "1"}]}))
    rc = PolymarketRelayerClient(api_key="k", api_key_address="a")
    assert rc.list_api_keys() == [{"key": "1"}]
    # dict form
    monkeypatch.setattr("event_markets.polymarket_relayer.urlopen",
                        lambda req, timeout=None: _Resp([{"key": "2"}]))
    assert rc.list_api_keys() == [{"key": "2"}]
    assert rc.list_api_keys() == [{"key": "2"}]
    assert rc.ping()["ok"] is True
    # no creds -> _headers raises
    rc2 = PolymarketRelayerClient()
    with pytest.raises(RuntimeError):
        rc2._headers()
    # builder_auth_headers raises
    with pytest.raises(NotImplementedError):
        rc.builder_auth_headers(None)
    # request failure
    monkeypatch.setattr("event_markets.polymarket_relayer.urlopen",
                        lambda req, timeout=None: (_ for _ in ()).throw(
                            __import__("urllib.error").error.HTTPError(
                                "u", 500, "e", {}, None)))
    with pytest.raises(RuntimeError):
        rc.list_api_keys()


# ───────────── streaming ─────────────

def test_streaming_price_update_and_manager():
    from event_markets.streaming import (
        PredictionMarketStreamManager, PolymarketStream, KalshiStream, PriceUpdate,
    )
    upd = PriceUpdate("polymarket", "t1", 0.6, 0.58, 0.62)
    assert upd.yes_price == 0.6
    mgr = PredictionMarketStreamManager(polymarket_asset_ids=["t1"])
    assert mgr._key("polymarket", "t1") == "polymarket:t1"
    mgr._on_update(upd)
    got = mgr.latest("polymarket", "t1")
    assert got.yes_price == 0.6
    # stale
    got.ts = 0
    assert mgr.latest("polymarket", "t1", max_age_s=1) is None
    # no stream
    assert mgr.latest("x", "y") is None
    assert mgr.any_connected() is False
    assert "streams" in mgr.status()


def test_streaming_polymarket_handle():
    from event_markets.streaming import PolymarketStream, PriceUpdate
    captured = []
    s = PolymarketStream(["t1"], lambda u: captured.append(u))
    # book event
    s._handle({"event_type": "book", "asset_id": "t1",
               "bids": [{"price": 0.58}], "asks": [{"price": 0.62}]})
    assert captured[-1].yes_price == 0.6
    # price_change
    s._handle({"event_type": "price_change", "asset_id": "t1", "price": "0.7"})
    assert captured[-1].yes_price == 0.7
    # list batch
    s._on_message("ws", '[{"event_type":"book","asset_id":"t1","bids":[{"price":0.5}],"asks":[{"price":0.6}]}]')
    # bad json
    s._on_message("ws", "not json")
    # non-dict
    s._on_message("ws", "[1,2]")
    assert s._subscribe_messages() == [{"type": "market", "assets_ids": ["t1"]}]
    assert s.available() is True


def test_streaming_kalshi_handle():
    from event_markets.streaming import KalshiStream, PriceUpdate
    captured = []
    s = KalshiStream(["K1"], lambda u: captured.append(u), api_key_id="kid",
                     private_key_path="/tmp/x.pem")
    s._handle({"type": "ticker", "msg": {"market_ticker": "K1", "yes_bid": 60,
                                          "yes_ask": 70, "price": 65}})
    assert captured[-1].platform == "kalshi"
    s._handle({"type": "other"})
    assert s._subscribe_messages() == [{"id": 1, "cmd": "subscribe",
        "params": {"channels": ["ticker"], "market_tickers": ["K1"]}}]
    assert s.available() is True
