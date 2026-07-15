import json
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from event_markets import kalshi_client as kc
from event_markets.kalshi_client import KalshiClient, KalshiMarket, KalshiSeries, format_market


@pytest.fixture
def key_path(tmp_path):
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    p = tmp_path / "key.pem"
    p.write_bytes(priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ))
    return str(p)


class ErrResp:
    def __init__(self, payload):
        self._data = json.dumps(payload).encode()
    def read(self): return self._data
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _http_error(code, headers=None):
    fp = MagicMock()
    return urllib.error.HTTPError("url", code, "msg", headers or {}, fp)


def test_default_base_url_env(monkeypatch):
    monkeypatch.setenv("KALSHI_ENV", "prod")
    c = KalshiClient()
    assert c.base_url == kc.PROD_BASE_URL
    monkeypatch.setenv("KALSHI_ENV", "demo")
    c2 = KalshiClient()
    assert c2.base_url == kc.DEMO_BASE_URL


def test_use_api_key_auth(key_path):
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)
    assert c._use_api_key_auth() is True
    c2 = KalshiClient(email="e", password="p")
    assert c2._use_api_key_auth() is False


def test_load_private_key(key_path):
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)
    assert c._load_private_key() is not None
    c2 = KalshiClient()
    assert c2._load_private_key() is None


def test_sign_request_and_auth_header(key_path):
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)
    sig = c._sign_request("123", "GET", "/trade-api/v2/markets")
    assert isinstance(sig, str) and sig
    hdr = c._auth_header("GET", "/markets")
    assert hdr["KALSHI-ACCESS-KEY"] == "kid"
    assert "KALSHI-ACCESS-SIGNATURE" in hdr


def test_auth_header_token_path_no_creds():
    c = KalshiClient()
    c._login = MagicMock()
    c._auth_header("GET", "/markets")
    c._login.assert_called_once()


def test_login_success(monkeypatch):
    payload = json.dumps({"token": "T", "member_id": "M"}).encode()
    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=0: ErrResp({"token": "T", "member_id": "M"}))
    c = KalshiClient(email="e", password="p")
    c._login()
    assert c._token == "T"
    assert c._member_id == "M"


def test_login_no_creds():
    c = KalshiClient()
    c._login()
    assert c._token == ""


def test_login_failure(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=0: _raise_conn())
    c = KalshiClient(email="e", password="p")
    c._login()
    assert c._token == ""


def _raise_conn():
    raise OSError("boom")


def test_get_success(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=0: ErrResp({"markets": []}))
    c = KalshiClient()
    assert c._get("/markets") == {"markets": []}


def test_get_401_retries_after_relogin(monkeypatch):
    state = {"calls": 0, "token": "old"}
    def fake(req, timeout=0):
        state["calls"] += 1
        if state["calls"] == 1:
            raise _http_error(401)
        return ErrResp({"ok": True})
    monkeypatch.setattr("urllib.request.urlopen", fake)
    c = KalshiClient()
    c._login = MagicMock()
    c._token = "old"
    out = c._get("/markets")
    assert out == {"ok": True}
    c._login.assert_called_once()


def test_get_429_backoff(monkeypatch):
    state = {"calls": 0}
    def fake(req, timeout=0):
        state["calls"] += 1
        if state["calls"] == 1:
            raise _http_error(429, {"Retry-After": "0"})
        return ErrResp({"ok": 1})
    monkeypatch.setattr("urllib.request.urlopen", fake)
    with patch("event_markets.kalshi_client.time.sleep"):
        c = KalshiClient()
        assert c._get("/markets") == {"ok": 1}


def test_get_other_error_raises(monkeypatch):
    monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=0: _boom())
    c = KalshiClient()
    c._token = ""
    with pytest.raises(urllib.error.HTTPError):
        c._get("/markets")


def _boom():
    raise _http_error(500)


def test_write_success(key_path, monkeypatch):
    captured = {}
    def fake(req, timeout=0):
        captured["body"] = req.data
        return ErrResp({"order_id": "o1", "fill_count": 5})
    monkeypatch.setattr("urllib.request.urlopen", fake)
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)
    out = c._write("POST", "/portfolio/events/orders", {"x": 1})
    assert out["order_id"] == "o1"
    assert captured["body"]


def test_write_requires_api_key():
    c = KalshiClient()
    with pytest.raises(RuntimeError):
        c._write("POST", "/x", {"a": 1})


def test_parse_market_v1():
    raw = {"ticker": "T", "title": "q", "event_ticker": "E",
           "yes_bid": 40, "yes_ask": 45, "no_bid": 55, "no_ask": 60,
           "volume": 100, "open_interest": 10, "close_date": "2026-01-01",
           "status": "open", "settled": False, "category": "Crypto"}
    m = KalshiClient()._parse_market(raw)
    assert m.yes_bid == 0.40 and m.yes_ask == 0.45 and m.volume == 100


def test_parse_market_v2():
    raw = {"ticker": "T", "title": "q", "event_ticker": "E",
           "yes_bid_dollars": 0.4, "yes_ask_dollars": 0.45,
           "no_bid_dollars": 0.55, "no_ask_dollars": 0.6,
           "volume_24h_fp": 999, "open_interest_fp": 5,
           "close_time": "2026-01-01T00:00:00Z", "status": "open",
           "settled": True, "category": "Crypto"}
    m = KalshiClient()._parse_market(raw)
    assert m.yes_bid == 0.4 and m.volume == 999 and m.settled is True


def test_search_markets_filters():
    c = KalshiClient()
    c._get = MagicMock(return_value={"markets": [
        {"ticker": "ok", "title": "BTC", "event_ticker": "E", "yes_bid": 0.4, "yes_ask": 0.45,
         "no_bid": 0.55, "no_ask": 0.6, "volume": 5000, "open_interest": 100,
         "close_date": "2026-01-01", "status": "open", "settled": False, "category": "Crypto"},
        {"ticker": "lowvol", "title": "X", "event_ticker": "E", "yes_bid": 0.4, "yes_ask": 0.45,
         "no_bid": 0.55, "no_ask": 0.6, "volume": 10, "open_interest": 100,
         "close_date": "2026-01-01", "status": "open", "settled": False, "category": "Crypto"},
    ]})
    out = c.search_markets(min_volume=1000, max_spread=0.15)
    assert [m.ticker for m in out] == ["ok"]


def test_search_markets_exception_returns_empty():
    c = KalshiClient()
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.search_markets() == []


def test_get_market():
    c = KalshiClient()
    c._get = MagicMock(return_value={"market": {"ticker": "T"}})
    assert c.get_market("T") == {"ticker": "T"}
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.get_market("T") is None
    assert c.get_market("") is None


def test_get_settlement():
    c = KalshiClient()
    c._get = MagicMock(return_value={"market": {"settled": True, "result": "yes"}})
    assert c.get_settlement("T") == 1
    c._get = MagicMock(return_value={"market": {"settled": True, "result": "no"}})
    assert c.get_settlement("T") == 0
    c._get = MagicMock(return_value={"market": {"status": "open", "result": ""}})
    assert c.get_settlement("T") is None
    c._get = MagicMock(return_value={"market": {"settled": True, "result": "maybe"}})
    assert c.get_settlement("T") is None


def test_get_relevant_markets():
    c = KalshiClient()
    ev = [{"event_ticker": "E1", "title": "BTC"}]
    c.fetch_all_events = MagicMock(return_value=ev)
    c._filter_events_by_keywords = MagicMock(return_value=["E1"])
    c.get_event_markets = MagicMock(return_value=[
        KalshiMarket(ticker="ok", title="q", event_ticker="E1", yes_bid=0.4, yes_ask=0.45,
                     no_bid=0.55, no_ask=0.6, volume=5000, open_interest=100,
                     close_date="2026-01-01", status="open", settled=False, category="Crypto")])
    out = c.get_relevant_markets(limit=5)
    assert out[0].ticker == "ok"
    # fallback branch when no results
    c.get_event_markets = MagicMock(return_value=[])
    c.search_markets = MagicMock(return_value=[KalshiMarket(ticker="fb", title="q", event_ticker="E",
        yes_bid=0.4, yes_ask=0.45, no_bid=0.55, no_ask=0.6, volume=5000, open_interest=100,
        close_date="2026-01-01", status="open", settled=False, category="Crypto")])
    out2 = c.get_relevant_markets(limit=5)
    assert out2[0].ticker == "fb"


def test_search_broad():
    c = KalshiClient()
    c.fetch_all_events = MagicMock(return_value=[{"event_ticker": "E1", "title": "BTC"}])
    c._filter_events_by_keywords = MagicMock(return_value=["E1"])
    m = KalshiMarket(ticker="ok", title="q", event_ticker="E1", yes_bid=0.4, yes_ask=0.45,
                     no_bid=0.55, no_ask=0.6, volume=5000, open_interest=100,
                     close_date="2026-01-01", status="open", settled=False, category="Crypto")
    c.get_event_markets = MagicMock(return_value=[m])
    out = c.search_broad(limit=5)
    assert out[0].ticker == "ok"
    # exception branch inside loop
    c.get_event_markets = MagicMock(side_effect=RuntimeError("x"))
    c.search_markets = MagicMock(return_value=[])
    assert c.search_broad(limit=5) == []


def test_get_event_markets():
    c = KalshiClient()
    c._get = MagicMock(return_value={"markets": [{"ticker": "T", "title": "q", "event_ticker": "E",
        "yes_bid": 0.4, "yes_ask": 0.45, "no_bid": 0.55, "no_ask": 0.6, "volume": 1,
        "open_interest": 1, "close_date": "2026-01-01", "status": "open", "settled": False, "category": "Crypto"}]})
    assert c.get_event_markets("E")[0].ticker == "T"
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.get_event_markets("E") == []
    assert c.get_event_markets("") == []


def test_fetch_events_with_markets():
    c = KalshiClient()
    c._get = MagicMock(return_value={"events": [
        {"category": "Crypto", "title": "t", "event_ticker": "E",
         "markets": [{"ticker": "T", "title": "q", "event_ticker": "E", "yes_bid": 0.4,
                      "yes_ask": 0.45, "no_bid": 0.55, "no_ask": 0.6, "volume": 1,
                      "open_interest": 1, "close_date": "2026-01-01", "status": "open",
                      "settled": False, "category": "Crypto"}]}], "cursor": ""})
    out = c.fetch_events_with_markets(limit=10, categories=["Crypto"])
    assert out[0]["parsed_markets"]
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.fetch_events_with_markets() == []


def test_get_markets_by_categories():
    c = KalshiClient()
    ev = [{"category": "Crypto", "title": "t", "event_ticker": "E",
           "parsed_markets": [KalshiMarket(ticker="T", title="q", event_ticker="E",
                        yes_bid=0.4, yes_ask=0.45, no_bid=0.55, no_ask=0.6, volume=1,
                        open_interest=1, close_date="2026-01-01", status="open",
                        settled=False, category="Crypto")]}]
    c.fetch_events_with_markets = MagicMock(return_value=ev)
    out = c.get_markets_by_categories(total_event_limit=5)
    assert "Crypto" in out and out["Crypto"]


def test_fetch_all_events():
    c = KalshiClient()
    c._get = MagicMock(return_value={"events": [{"event_ticker": "E"}]})
    assert c.fetch_all_events()[0]["event_ticker"] == "E"
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.fetch_all_events() == []


def test_filter_events_by_keywords():
    events = [{"title": "Bitcoin cup", "event_ticker": "BTC"}, {"title": "soccer", "event_ticker": "S"}]
    out = KalshiClient()._filter_events_by_keywords(events, kc.WATCHED_EVENTS)
    assert "BTC" in out
    out2 = KalshiClient()._filter_events_by_keywords(events, kc.BROAD_SEARCH_TERMS)
    assert "S" in out2


def test_get_order_book_and_balance():
    c = KalshiClient()
    c._get = MagicMock(return_value={"bids": [], "asks": []})
    assert c.get_order_book("T") == {"bids": [], "asks": []}
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.get_order_book("T") == {}

    c._get = MagicMock(return_value={"balance_dollars": 100})
    assert c.get_balance()["balance_dollars"] == 100
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.get_balance() == {}


def test_create_order_and_others(key_path):
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)

    def fake_write(method, path, body=None):
        return {"order_id": "o", "price": body.get("price") if body else None}

    c._write = fake_write
    r = c.create_order("T", "yes", "buy", 5, price=0.4)
    assert r["order_id"] == "o"
    # side 'no' buy -> ask side, price 1-p
    r2 = c.create_order("T", "no", "buy", 5, price=0.4)
    assert r2["price"] == "0.6000"
    with pytest.raises(ValueError):
        c.create_order("T", "yes", "buy", 5)  # no price
    with pytest.raises(ValueError):
        c.create_order("T", "bad", "buy", 5, price=0.4)
    # cancel
    c.cancel_order("o1")
    # get_orders / positions / fills
    c._get = MagicMock(return_value={"orders": [1]})
    assert c.get_orders() == [1]
    c._get = MagicMock(return_value={"market_positions": [2]})
    assert c.get_positions() == [2]
    c._get = MagicMock(return_value={"fills": [3]})
    assert c.get_fills() == [3]


def test_create_order_post_only(key_path):
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)
    captured = {}

    def fake_write(method, path, body=None):
        captured["body"] = body
        return {"order_id": "o"}

    c._write = fake_write
    c.create_order("T", "yes", "buy", 5, price=0.4, post_only=True)
    assert captured["body"]["post_only"] is True


def test_format_market():
    m = KalshiMarket(ticker="T", title="q", event_ticker="E", yes_bid=0.4, yes_ask=0.45,
                     no_bid=0.55, no_ask=0.6, volume=1000, open_interest=10,
                     close_date="2026-01-01", status="open", settled=False, category="Crypto")
    assert "T" in format_market(m)


def test_get_with_none_param(monkeypatch):
    # exercise the `if v is not None` False branch in _get param building
    captured = {}
    def fake(req, timeout=0):
        captured["url"] = req.full_url if hasattr(req, "full_url") else str(req)
        return ErrResp({"ok": 1})
    monkeypatch.setattr("urllib.request.urlopen", fake)
    c = KalshiClient()
    out = c._get("/markets", {"a": 1, "b": None, "c": "x"})
    assert out == {"ok": 1}
    assert "b=" not in captured["url"]


def test_write_exception_branch(key_path):
    c = KalshiClient(api_key_id="kid", private_key_path=key_path)
    fp = MagicMock()
    fp.read.return_value = b"detail"
    from urllib.error import HTTPError
    def fake(req, timeout=0):
        raise HTTPError("u", 500, "err", {}, fp)
    import urllib.request as ur
    orig = ur.urlopen
    ur.urlopen = fake
    try:
        with pytest.raises(HTTPError):
            c._write("POST", "/portfolio/events/orders", {"x": 1})
    finally:
        ur.urlopen = orig


def test_search_markets_o_i_and_spread_filters():
    c = KalshiClient()
    c._get = MagicMock(return_value={"markets": [
        {"ticker": "oi", "title": "BTC", "event_ticker": "E",
         "yes_bid_dollars": 0.4, "yes_ask_dollars": 0.45,
         "no_bid_dollars": 0.55, "no_ask_dollars": 0.6,
         "volume_24h_fp": 5000, "open_interest_fp": 1,
         "close_time": "2026-01-01", "status": "open", "settled": False, "category": "Crypto"},
        {"ticker": "wide", "title": "BTC", "event_ticker": "E",
         "yes_bid_dollars": 0.1, "yes_ask_dollars": 0.9,
         "no_bid_dollars": 0.1, "no_ask_dollars": 0.9,
         "volume_24h_fp": 5000, "open_interest_fp": 100,
         "close_time": "2026-01-01", "status": "open", "settled": False, "category": "Crypto"},
    ]})
    out = c.search_markets(min_volume=1000, max_spread=0.15, min_open_interest=100)
    # 'oi' filtered by open_interest, 'wide' filtered by spread
    assert out == []


def test_get_settlement_no_market():
    c = KalshiClient()
    c.get_market = MagicMock(return_value=None)
    assert c.get_settlement("X") is None


def test_get_relevant_markets_break_and_spread():
    c = KalshiClient()
    ev = [{"event_ticker": "E1", "title": "BTC"}]
    c.fetch_all_events = MagicMock(return_value=ev)
    c._filter_events_by_keywords = MagicMock(return_value=["E1"])
    good = KalshiMarket(ticker="ok", title="q", event_ticker="E1", yes_bid=0.4, yes_ask=0.45,
                        no_bid=0.55, no_ask=0.6, volume=5000, open_interest=100,
                        close_date="2026-01-01", status="open", settled=False, category="Crypto")
    wide = KalshiMarket(ticker="wide", title="q", event_ticker="E1", yes_bid=0.1, yes_ask=0.9,
                        no_bid=0.1, no_ask=0.9, volume=5000, open_interest=100,
                        close_date="2026-01-01", status="open", settled=False, category="Crypto")
    c.get_event_markets = MagicMock(return_value=[good, wide])
    # limit=1 -> break after first appended
    out = c.get_relevant_markets(limit=1)
    assert [m.ticker for m in out] == ["ok"]


def test_search_broad_limit_and_fallback_exception():
    c = KalshiClient()
    ev = [{"event_ticker": "E1", "title": "BTC"}]
    c.fetch_all_events = MagicMock(return_value=ev)
    c._filter_events_by_keywords = MagicMock(return_value=["E1"])
    m = KalshiMarket(ticker="ok", title="q", event_ticker="E1", yes_bid=0.4, yes_ask=0.45,
                     no_bid=0.55, no_ask=0.6, volume=5000, open_interest=100,
                     close_date="2026-01-01", status="open", settled=False, category="Crypto")
    c.get_event_markets = MagicMock(return_value=[m])
    # first call appends, then fallback search_markets raises
    c.search_markets = MagicMock(side_effect=RuntimeError("x"))
    out = c.search_broad(limit=5)
    assert len(out) == 1


def test_fetch_events_with_markets_categories_and_cursor():
    c = KalshiClient()
    page1 = {"events": [
        {"category": "Crypto", "title": "t", "event_ticker": "E",
         "markets": [{"ticker": "T", "title": "q", "event_ticker": "E", "yes_bid": 0.4,
                      "yes_ask": 0.45, "no_bid": 0.55, "no_ask": 0.6, "volume": 1,
                      "open_interest": 1, "close_date": "2026-01-01", "status": "open",
                      "settled": False, "category": "Crypto"}]},
        {"category": "Sports", "title": "s", "event_ticker": "S", "markets": []},
    ], "cursor": "next"}
    page2 = {"events": [], "cursor": ""}
    c._get = MagicMock(side_effect=[page1, page2])
    out = c.fetch_events_with_markets(limit=10, categories=["Crypto"])
    assert len(out) == 1  # Sports filtered out by categories
    assert out[0]["parsed_markets"]


def test_fetch_events_with_markets_empty_batch():
    c = KalshiClient()
    c._get = MagicMock(return_value={"events": []})
    assert c.fetch_events_with_markets() == []


def test_get_markets_by_categories_inner_break():
    c = KalshiClient()
    mk = lambda t: KalshiMarket(ticker=t, title="q", event_ticker="E", yes_bid=0.4, yes_ask=0.45,
                                no_bid=0.55, no_ask=0.6, volume=1, open_interest=1,
                                close_date="2026-01-01", status="open", settled=False, category="Crypto")
    ev = [
        {"category": "Crypto", "title": "t", "event_ticker": "E1", "parsed_markets": [mk("T1")]},
        {"category": "Crypto", "title": "t", "event_ticker": "E2", "parsed_markets": [mk("T2")]},
    ]
    c.fetch_events_with_markets = MagicMock(return_value=ev)
    out = c.get_markets_by_categories(total_event_limit=1)
    assert len(out["Crypto"]) == 1  # inner break after first event reaches limit


def test_filter_events_by_keywords_short():
    events = [{"title": "BTC cup", "event_ticker": "BTC2"},
              {"title": "NBA finals", "event_ticker": "NBA"}]
    out = KalshiClient()._filter_events_by_keywords(events, kc.BROAD_SEARCH_TERMS)
    assert "BTC2" in out and "NBA" in out


def test_get_orders_positions_fills_exceptions():
    c = KalshiClient()
    c._get = MagicMock(side_effect=RuntimeError("x"))
    assert c.get_orders() == []
    assert c.get_positions() == []
    assert c.get_fills() == []


def test_get_positions_non_dict():
    c = KalshiClient()
    c._get = MagicMock(return_value=[1, 2])  # not a dict
    assert c.get_positions() == []


def test_get_fills_non_dict():
    c = KalshiClient()
    c._get = MagicMock(return_value=[3])
    assert c.get_fills() == []
