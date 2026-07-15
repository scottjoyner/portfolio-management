"""Tests for tests/coverage/event_markets/em_helpers.py."""
import importlib
import os
import sys

import pytest

HELPERS = "tests.coverage.event_markets.em_helpers"
ROOT = "/home/scott/git/portfolio-management"


@pytest.fixture
def mod():
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    return importlib.import_module(HELPERS)


def test_fake_resp_bytes(mod):
    r = mod.FakeResp(b"\x00\x01")
    assert r.read() == b"\x00\x01"
    assert r.__enter__() is r
    assert r.__exit__() is False


def test_fake_resp_json(mod):
    r = mod.FakeResp({"a": 1})
    assert r.read() == b'{"a": 1}'


def test_fake_resp_raise(mod):
    r = mod.FakeResp("x", raise_on_read=True)
    with pytest.raises(OSError):
        r.read()


def test_make_kalshi_defaults(mod):
    m = mod.make_kalshi_market()
    assert m.ticker == "KALS-1"
    assert m.volume == 50000.0
    assert m.yes_bid == 0.4


def test_make_kalshi_overrides(mod):
    m = mod.make_kalshi_market(ticker="X", yes_bid=0.1, volume=2.0)
    assert m.ticker == "X"
    assert m.yes_bid == 0.1
    assert m.volume == 2.0


def test_make_polymarket_defaults(mod):
    m = mod.make_polymarket_market()
    assert m.question == "Will ETH reach $5000?"
    assert m.outcomes == ["YES", "NO"]
    assert m.outcome_prices == {"YES": 0.45, "NO": 0.55}
    assert len(m.tokens) == 2
    assert m.yes_bid == 0.42


def test_make_polymarket_overrides(mod):
    m = mod.make_polymarket_market(
        outcomes=["YES", "NO", "MAYBE"],
        outcome_prices={"YES": 0.9, "NO": 0.05, "MAYBE": 0.05},
        token_ids=["a", "b", "c"],
    )
    assert m.outcomes == ["YES", "NO", "MAYBE"]
    assert m.tokens == [{"token_id": "a"}, {"token_id": "b"}, {"token_id": "c"}]


def test_make_book(mod):
    b = mod.make_book(bids=((0.3, 50),), asks=((0.7, 60),), spread=0.4, mid=0.5)
    assert b.bids[0] == (0.3, 50.0)
    assert b.asks[0] == (0.7, 60.0)
    assert b.spread == 0.4
    assert b.mid_price == 0.5


def test_url_router_match(mod):
    router = mod.UrlRouter({"kalshi": {"x": 1}, "poly": {"y": 2}}, default={"d": 9})
    assert router("http://x/kalshi/foo").read() == b'{"x": 1}'
    assert router("http://x/poly/foo").read() == b'{"y": 2}'
    assert router("http://x/unknown").read() == b'{"d": 9}'


def test_url_router_default_none_raises(mod):
    router = mod.UrlRouter({"kalshi": {"x": 1}})
    with pytest.raises(AssertionError):
        router("http://x/unknown")


def test_url_router_req_object(mod):
    class Req:
        full_url = "http://x/kalshi"
    router = mod.UrlRouter({"kalshi": {"x": 1}})
    assert router(Req()).read() == b'{"x": 1}'


def test_url_router_str_req(mod):
    router = mod.UrlRouter({"kalshi": {"x": 1}})
    assert router("http://x/kalshi").read() == b'{"x": 1}'
