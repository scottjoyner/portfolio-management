"""Coverage tests for event_markets.polymarket_client (network mocked)."""

from unittest.mock import patch

import pytest

from event_markets import polymarket_client as pc
from event_markets.polymarket_client import (
    PolymarketClient, PolymarketMarket, PolymarketBook, format_market,
)
from em_helpers import FakeResp, UrlRouter


def mkpm(**kw):
    kw.setdefault("description", "")
    return PolymarketMarket(**kw)


def _market_dict(**kw):
    base = {
        "conditionId": "cond-1",
        "question": "Will ETH reach $5000?",
        "outcomes": '["YES","NO"]',
        "outcomePrices": '["0.45","0.55"]',
        "volume": "40000",
        "endDateIso": "2026-12-31T00:00:00Z",
        "closed": False,
        "acceptingOrders": True,
        "bestBid": 0.42,
        "bestAsk": 0.46,
        "spread": 0.04,
        "clobTokenIds": '["tok1","tok2"]',
        "slug": "eth-5000",
        "events": [{"slug": "eth"}],
    }
    base.update(kw)
    return base


def test_search_markets():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": [_market_dict()]})):
        out = c.search_markets(term="ethereum", limit=5)
    assert len(out) == 1
    assert out[0].condition_id == "cond-1"
    assert out[0].outcome_prices["YES"] == 0.45


def test_search_markets_exception():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", side_effect=OSError("boom")):
        assert c.search_markets() == []


def test_search_markets_dict_data():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": {"data": [_market_dict()]}})):
        out = c.search_markets()
    assert len(out) == 1


def test_fetch_markets():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": [_market_dict()]})):
        out = c.fetch_markets(limit=5)
    assert len(out) == 1


def test_fetch_markets_exception():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", side_effect=OSError("x")):
        assert c.fetch_markets() == []


def test_fetch_crypto_events():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/events": [{"slug": "crypto-btc"}]})):
        assert c.fetch_crypto_events() == ["crypto-btc"]


def test_fetch_crypto_events_exception():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", side_effect=OSError("x")):
        assert c.fetch_crypto_events() == []


def test_filter_by_keywords_short_and_long():
    m1 = mkpm(condition_id="a", question="Will BTC go up?", outcomes=["YES"],
                          outcome_prices={"YES": 0.5}, volume=10, end_date_iso="", closed=False,
                          accepting_orders=True)
    m2 = mkpm(condition_id="b", question="Will Ethereum go up?", outcomes=["YES"],
                          outcome_prices={"YES": 0.5}, volume=5, end_date_iso="", closed=False,
                          accepting_orders=True)
    m3 = mkpm(condition_id="c", question="Will the Lakers win?", outcomes=["YES"],
                          outcome_prices={"YES": 0.5}, volume=1, end_date_iso="", closed=False,
                          accepting_orders=True)
    out = pc.PolymarketClient()._filter_by_keywords([m1, m2, m3], ["btc", "ethereum"], limit=10)
    assert {m.condition_id for m in out} == {"a", "b"}
    # short keyword word-boundary: "sol" should not match "solana"
    msol = mkpm(condition_id="s", question="Will sol reach 100?", outcomes=["YES"],
                            outcome_prices={"YES": 0.5}, volume=1, end_date_iso="", closed=False,
                            accepting_orders=True)
    mnotsol = mkpm(condition_id="ns", question="solana price today", outcomes=["YES"],
                               outcome_prices={"YES": 0.5}, volume=1, end_date_iso="", closed=False,
                               accepting_orders=True)
    out2 = pc.PolymarketClient()._filter_by_keywords([msol, mnotsol], ["sol"], limit=10)
    assert [m.condition_id for m in out2] == ["s"]


def test_markets_by_event_slugs():
    m = mkpm(condition_id="a", question="q", outcomes=["YES"],
                         outcome_prices={"YES": 0.5}, volume=1, end_date_iso="", closed=False,
                         accepting_orders=True, event_slug="ev1")
    out = pc.PolymarketClient()._markets_by_event_slugs(["ev1"], [m])
    assert out == [m]
    assert pc.PolymarketClient()._markets_by_event_slugs(["nope"], [m]) == []


def test_get_markets_by_tag():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": [_market_dict()]})):
        out = c.get_markets_by_tag("crypto")
    assert len(out) == 1
    with patch("urllib.request.urlopen", side_effect=OSError("x")):
        assert c.get_markets_by_tag("crypto") == []


def test_fetch_market_detail():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets/cond-1": _market_dict()})):
        out = c.fetch_market_detail("cond-1")
    assert out is not None and out.condition_id == "cond-1"
    with patch("urllib.request.urlopen", side_effect=OSError("x")):
        assert c.fetch_market_detail("cond-1") is None


def test_get_crypto_markets_and_fallback():
    m = _market_dict(question="Will BTC go up?")
    c = PolymarketClient()
    # primary tag returns matches
    with patch("urllib.request.urlopen", UrlRouter({"/markets": [m]})):
        out = c.get_crypto_markets()
    assert out and out[0].condition_id == "cond-1"
    # empty match -> fallback path
    with patch.object(c, "get_markets_by_tag", return_value=[]):
        with patch.object(c, "fetch_markets", return_value=[mkpm(
            condition_id="fb", question="Will BTC go up?", outcomes=["YES"],
            outcome_prices={"YES": 0.5}, volume=1, end_date_iso="", closed=False,
            accepting_orders=True)]):
            out2 = c.get_crypto_markets()
    assert out2[0].condition_id == "fb"
    # both empty -> logged info, returns []
    with patch.object(c, "get_markets_by_tag", return_value=[]):
        with patch.object(c, "fetch_markets", return_value=[]):
            assert c.get_crypto_markets() == []


@pytest.mark.parametrize("fn", [
    "get_sports_markets", "get_politics_markets", "get_entertainment_markets",
    "get_economics_markets", "get_technology_markets",
])
def test_category_market_getters(fn):
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": [_market_dict()]})):
        out = getattr(c, fn)()
    assert len(out) == 1
    # empty results path
    with patch.object(c, "get_markets_by_tag", return_value=[]):
        assert getattr(c, fn)() == []


def test_get_all_category_markets_tag_and_fallback():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": [_market_dict()]})):
        out = c.get_all_category_markets(limit_per_category=5)
    assert "crypto" in out and out["crypto"]
    # fallback path
    with patch.object(c, "get_markets_by_tag", return_value=[]):
        with patch.object(c, "fetch_markets", return_value=[mkpm(
            condition_id="x", question="Will BTC go up?", outcomes=["YES"],
            outcome_prices={"YES": 0.5}, volume=1, end_date_iso="", closed=False,
            accepting_orders=True)]):
            out2 = c.get_all_category_markets(limit_per_category=5)
    assert out2["crypto"]


def test_get_order_book():
    book = {
        "asks": [{"price": "0.60", "size": "100"}, {"price": "0.61", "size": "50"}],
        "bids": [{"price": "0.40", "size": "100"}, {"price": "0.39", "size": "50"}],
    }
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/book": book})):
        b = c.get_order_book("tok1")
    assert b.spread == pytest.approx(0.2)
    assert b.mid_price == pytest.approx(0.5)
    assert len(b.asks) == 2 and len(b.bids) == 2


def test_get_order_book_exception():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", side_effect=OSError("x")):
        b = c.get_order_book("tok1")
    assert isinstance(b, PolymarketBook)
    assert b.asks == [] and b.bids == []


def test_parse_gamma_market_branches():
    # outcomes/prices as lists, vol as float, token_ids as list
    d = {
        "conditionId": "c1", "question": "q", "outcomes": ["YES", "NO"],
        "outcomePrices": [0.45, 0.55], "volume": 40000.0, "endDateIso": "2026-12-31",
        "closed": False, "acceptingOrders": True, "bestBid": 0.42, "bestAsk": 0.46,
        "spread": 0.04, "clobTokenIds": ["tok1", "tok2"], "slug": "s", "events": [{"slug": "e"}],
    }
    m = PolymarketClient()._parse_gamma_market(d)
    assert m.outcome_prices["YES"] == 0.45
    assert m.tokens[0]["token_id"] == "tok1"

    # outcomes/prices as malformed strings -> defaults
    d2 = {"conditionId": "c2", "question": "q2", "outcomes": "not-json",
          "outcomePrices": "bad", "volume": "1,000", "clobTokenIds": "bad-json",
          "events": [{"slug": "e"}]}
    m2 = PolymarketClient()._parse_gamma_market(d2)
    assert m2.outcome_prices["Yes"] == 0.5
    assert m2.tokens == []

    # volume as non-string, no events
    d3 = {"conditionId": "c3", "question": "q3", "outcomes": ["YES"], "outcomePrices": [0.5],
          "volume": 5.0, "clobTokenIds": ["t"]}
    m3 = PolymarketClient()._parse_gamma_market(d3)
    assert m3.event_slug == ""

    # outcomes list shorter than prices index
    d4 = {"conditionId": "c4", "question": "q4", "outcomes": ["YES"], "outcomePrices": [0.5, 0.6],
          "volume": 5}
    m4 = PolymarketClient()._parse_gamma_market(d4)
    assert m4.outcome_prices["YES"] == 0.5


def test_is_valid():
    assert PolymarketClient()._is_valid({"conditionId": "c", "question": "q"}) is True
    assert PolymarketClient()._is_valid({"question": "q"}) is False
    assert PolymarketClient()._is_valid({"conditionId": "c"}) is False


def test_gamma_get_skips_none_params():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/x": {"ok": 1}})) as router:
        # v is None -> the param is skipped (145->144 branch)
        out = c._gamma_get("/x", {"a": "1", "b": None})
    assert out == {"ok": 1}


def test_clob_get_no_params_and_none_params():
    c = PolymarketClient()
    # No params: `if params` False -> 154->160 branch
    with patch("urllib.request.urlopen", UrlRouter({"/plain": {"ok": 1}})):
        assert c._clob_get("/plain") == {"ok": 1}
    # None value inside params -> 157->156 branch
    with patch("urllib.request.urlopen", UrlRouter({"/plain": {"ok": 2}})):
        assert c._clob_get("/plain", {"a": None, "b": "1"}) == {"ok": 2}


def test_fetch_markets_dict_data():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": {"data": [_market_dict()]}})):
        out = c.fetch_markets()
    assert len(out) == 1


def test_fetch_crypto_events_dict_data():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/events": {"data": [{"slug": "s1"}]}})):
        assert c.fetch_crypto_events() == ["s1"]


def test_get_markets_by_tag_dict_data():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/markets": {"data": [_market_dict()]}})):
        out = c.get_markets_by_tag("crypto")
    assert len(out) == 1


def test_parse_gamma_market_bad_list_entries():
    # outcomePrices as a LIST with a non-numeric entry -> ValueError branch (351-352)
    d = {"conditionId": "cp", "question": "q", "outcomes": ["YES", "NO"],
         "outcomePrices": ["oops", "0.5"], "volume": 1.0, "clobTokenIds": []}
    m = PolymarketClient()._parse_gamma_market(d)
    assert m.outcome_prices["YES"] == 0.5
    # volume as an unparsable string -> ValueError branch (357-358)
    d2 = {"conditionId": "cv", "question": "q", "outcomes": ["YES"],
          "outcomePrices": [0.5], "volume": "not-a-number", "clobTokenIds": []}
    m2 = PolymarketClient()._parse_gamma_market(d2)
    assert m2.volume == 0.0


def test_format_market():
    m = mkpm(condition_id="cond-1", question="Will BTC go up?",
                         outcomes=["YES", "NO"], outcome_prices={"YES": 0.6, "NO": 0.4},
                         volume=40000, end_date_iso="2026-12-31", closed=False,
                         accepting_orders=True, yes_bid=0.55, yes_ask=0.65, spread=0.1)
    out = format_market(m)
    assert "BTC" in out and "cond-1" in out
