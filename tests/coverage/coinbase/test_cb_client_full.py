"""Coverage tests for coinbase/src/cb_client.py"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from coinbase.src import cb_client


def make_client(monkeypatch, handler):
    calls = []

    def fake_run(cmd, **kw):
        calls.append(tuple(cmd))
        args = tuple(cmd[3:])  # drop cli, -e, env
        out = handler(args)
        if isinstance(out, dict):
            return SimpleNamespace(returncode=0, stdout=json.dumps(out), stderr="")
        if isinstance(out, Exception):
            return SimpleNamespace(returncode=1, stdout="", stderr=str(out))
        return SimpleNamespace(returncode=0, stdout=out, stderr="")

    monkeypatch.setattr(cb_client.subprocess, "run", fake_run)
    monkeypatch.setattr(cb_client.shutil, "which", lambda c: None)
    client = cb_client.CBClient(api_key="k", api_secret="s")
    client._calls = calls
    return client


def balance_with(*currencies):
    accts = []
    for cur, val in currencies:
        accts.append({
            "currency": cur,
            "available_balance": {"value": str(val)},
        })
    return {"accounts": accts}


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------
def test_rate_limiter(monkeypatch):
    clock = {"t": 0.0}

    class FakeTime:
        def time(self):
            return clock["t"]

        def sleep(self, s):
            clock["t"] += s

    monkeypatch.setattr(cb_client.time, "time", FakeTime().time)
    monkeypatch.setattr(cb_client.time, "sleep", FakeTime().sleep)
    rl = cb_client.RateLimiter(max_calls=2, period=1.0)
    rl.acquire()
    rl.acquire()
    # third acquire must wait until window opens (clock advances via sleep)
    clock["t"] = 0.5
    rl.acquire()
    assert clock["t"] >= 1.0
    assert len(rl.calls) == 1


# ---------------------------------------------------------------------------
# Settlement currency detection
# ---------------------------------------------------------------------------
def test_detect_settlement_usd(monkeypatch):
    c = make_client(monkeypatch, lambda a: balance_with(("USD", 500)))
    assert c.settlement_currency == "USD"


def test_detect_settlement_usdc(monkeypatch):
    c = make_client(monkeypatch, lambda a: balance_with(("USDC", 500), ("USD", 0)))
    assert c.settlement_currency == "USDC"


def test_detect_settlement_env(monkeypatch):
    monkeypatch.setenv("COINBASE_SETTLEMENT_CURRENCY", "EUR")
    c = make_client(monkeypatch, lambda a: balance_with(("USD", 1)))
    assert c.settlement_currency == "EUR"


def test_detect_settlement_exception(monkeypatch):
    def handler(a):
        raise RuntimeError("boom")
    c = make_client(monkeypatch, handler)
    assert c.settlement_currency == "USD"


# ---------------------------------------------------------------------------
# CLI json / parse
# ---------------------------------------------------------------------------
def test_parse_cli_output():
    assert cb_client.CBClient._parse_cli_output("", False) == {}
    assert cb_client.CBClient._parse_cli_output('{"a":1}', False) == {"a": 1}
    # invalid json (non-dry-run) raises
    with pytest.raises(RuntimeError):
        cb_client.CBClient._parse_cli_output("not json", False)
    # dry-run parse
    out = 'would execute orders\n{"preview_id":"x"}'
    assert cb_client.CBClient._parse_cli_output(out, True) == {"preview_id": "x"}
    # dry-run invalid json (no '{') returns empty dict gracefully
    assert cb_client.CBClient._parse_cli_output("would execute xxx", True) == {}


def test_cli_json_failure(monkeypatch):
    def handler(a):
        return RuntimeError("fail")
    c = make_client(monkeypatch, handler)
    with pytest.raises(RuntimeError):
        c._cli_json("balance")


def test_norm_stop_direction():
    assert cb_client.CBClient._norm_stop_direction("up") == "up"
    assert cb_client.CBClient._norm_stop_direction("down") == "down"
    assert cb_client.CBClient._norm_stop_direction("stop_direction_stop_up") == "up"
    assert cb_client.CBClient._norm_stop_direction("stop_direction_stop_down") == "down"
    assert cb_client.CBClient._norm_stop_direction("") == "up"
    assert cb_client.CBClient._norm_stop_direction("weird") == "weird"


def test_remap(monkeypatch):
    c = make_client(monkeypatch, lambda a: balance_with(("USD", 1)))
    c.settlement_currency = "USD"
    assert c._remap("BTC-USD") == "BTC-USD"
    c.settlement_currency = "USDC"
    assert c._remap("BTC-USD") == "BTC-USDC"
    assert c._remap("") == ""
    assert c._remap("ETH-USDC") == "ETH-USDC"


# ---------------------------------------------------------------------------
# Accounts / positions
# ---------------------------------------------------------------------------
def test_list_accounts(monkeypatch):
    c = make_client(monkeypatch, lambda a: balance_with(("USD", 1)))
    assert c.list_accounts() == balance_with(("USD", 1))


def test_get_positions_full(monkeypatch):
    portfolios = {"portfolios": [
        {"type": "DEFAULT", "uuid": "u1"},
        {"type": "OTHER", "uuid": "u2"},
    ]}

    def handler(a):
        if a[0:2] == ("portfolios", "list"):
            return portfolios
        if a[:3] == ("portfolios", "get", "u1"):
            return {"spot_positions": [
                {"asset": "BTC", "total_balance_crypto": "2.0",
                 "total_balance_fiat": "400.0", "is_cash": False,
                 "available_to_trade_fiat": "400.0"},
                {"asset": "USD", "total_balance_crypto": "0",
                 "total_balance_fiat": "0", "is_cash": True},
                {"asset": "ETH", "total_balance_crypto": "0",
                 "total_balance_fiat": "0"},
            ]}
        return {}

    c = make_client(monkeypatch, handler)
    positions = c.get_positions()
    assert len(positions) == 1  # USD (size 0) skipped
    assert positions[0]["product_id"] == "BTC-USD"
    assert positions[0]["entry_price"] == 200.0


def test_get_positions_no_portfolios(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"portfolios": []})
    assert c.get_positions() == []


def test_get_positions_no_default(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"portfolios": [{"type": "X", "uuid": "u9"}]})
    assert c.get_positions() == []


def test_get_positions_no_uuid(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"portfolios": [{"type": "DEFAULT"}]})
    assert c.get_positions() == []


def test_get_positions_exception(monkeypatch):
    def handler(a):
        raise RuntimeError("x")
    c = make_client(monkeypatch, handler)
    assert c.get_positions() == []


# ---------------------------------------------------------------------------
# best_bid_ask / synthetic books
# ---------------------------------------------------------------------------
def test_best_bid_ask_single_and_list(monkeypatch):
    def handler(a):
        if a[:3] == ("products", "book", "BTC-USD"):
            return {"pricebook": {"product_id": "BTC-USD", "bids": [], "asks": []}}
        return {}

    c = make_client(monkeypatch, handler)
    out = c.best_bid_ask("BTC-USD")
    assert out["pricebooks"][0]["product_id"] == "BTC-USD"
    # list input + dedup
    out2 = c.best_bid_ask(["BTC-USD", "BTC-USD"])
    assert len(out2["pricebooks"]) == 1


def test_best_bid_ask_error_synthetic(monkeypatch):
    def handler(a):
        if a[0:2] == ("products", "book", "BTC-USD"):
            return RuntimeError("fail")
        if a[0] == "products" and a[1] == "candles":
            return {"candles": [{"start": 1, "open": 100, "high": 110,
                                 "low": 90, "close": 105, "volume": 1}]}
        return {}

    c = make_client(monkeypatch, handler)
    out = c.best_bid_ask("BTC-USD")
    assert out["pricebooks"]
    # tuple-form candle fallback
    def handler2(a):
        if a[0:2] == ("products", "book", "ETH-USD"):
            return RuntimeError("fail")
        if a[0] == "products" and a[1] == "candles":
            return {"candles": [(1, 90, 110, 100, 105, 1)]}
        return {}

    c2 = make_client(monkeypatch, handler2)
    out2 = c2.best_bid_ask("ETH-USD")
    assert out2["pricebooks"]


def test_synthetic_books_empty(monkeypatch):
    def handler(a):
        if a[0] == "products" and a[1] == "candles":
            return {"candles": []}
        return {}
    c = make_client(monkeypatch, handler)
    assert c._synthetic_books(["ZZZ-USD"]) == []


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------
def test_preview_order_buy(monkeypatch):
    seen = {}

    def handler(a):
        seen[tuple(a)] = True
        return {"preview_id": "p1", "product_id": "BTC-USD"}
    c = make_client(monkeypatch, handler)
    r = c.preview_order("BUY", "BTC-USD", quote_size="100")
    assert r["preview_id"] == "p1"
    r2 = c.preview_order("BUY", "BTC-USD", base_size="0.01")
    assert r2["preview_id"] == "p1"
    # neither -> error dict
    r3 = c.preview_order("BUY", "BTC-USD")
    assert r3["status"] == "preview_error"
    # invalid side
    with pytest.raises(ValueError):
        c.preview_order("HOLD", "BTC-USD", base_size="1")


def test_preview_order_sell(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"preview_id": "p2"})
    r = c.preview_order("SELL", "BTC-USD", base_size="0.5")
    assert r["preview_id"] == "p2"
    r2 = c.preview_order("SELL", "BTC-USD")
    assert r2["status"] == "preview_error"


def test_create_market_order(monkeypatch):
    calls = {}

    def handler(a):
        calls[tuple(a)] = True
        return {"order_id": "o1"}
    c = make_client(monkeypatch, handler)
    r = c.create_market_order("BUY", "BTC-USD", quote_size="100",
                              client_order_id="c1", preview_id="p1")
    assert r["order_id"] == "o1"
    # sell no base -> ValueError
    with pytest.raises(ValueError):
        c.create_market_order("SELL", "BTC-USD")
    # buy no size -> ValueError
    with pytest.raises(ValueError):
        c.create_market_order("BUY", "BTC-USD")
    assert c.market_order("BUY", "BTC-USD", quote_size="1")["order_id"] == "o1"


def test_create_limit_stop_orders(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"order_id": "o"})
    assert c.create_limit_order("BUY", "BTC-USD", base_size="1", price="100",
                                post_only=True, client_order_id="c")["order_id"] == "o"
    assert c.create_stop_limit_order("SELL", "BTC-USD", base_size="1",
                                     limit_price="90", stop_price="89")["order_id"] == "o"
    assert c.create_stop_market_order("BUY", "BTC-USD", base_size="1",
                                      stop_price="110", stop_direction="up")["order_id"] == "o"


def test_close_position(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"order_id": "o"})
    r = c.close_position("BTC-USD", size="1", client_order_id="c")
    assert r["order_id"] == "o"
    # no client id -> generated uuid


def test_cancel_list_get_fees_products(monkeypatch):
    def handler(a):
        if a[0] == "orders" and a[1] == "list":
            return {"orders": [{"id": 1}]}
        if a[0] == "orders" and a[1] == "get":
            return {"order_id": a[2]}
        if a[0] == "orders" and a[1] == "cancel":
            return {"order_id": a[2].split("=", 1)[1]}
        if a[0] == "fees":
            return {"tier": "1"}
        if a[0] == "products" and a[1] == "list":
            return {"products": [{"id": "BTC-USD"}]}
        return {}
    c = make_client(monkeypatch, handler)
    assert c.cancel_order("x") == {"order_id": "x"}
    assert c.list_orders(product_id="BTC-USD") == [{"id": 1}]
    assert c.get_order("abc") == {"order_id": "abc"}
    assert c.get_fees() == {"tier": "1"}
    assert c.get_products(product_type="SPOT") == [{"id": "BTC-USD"}]


def test_list_orders_exception(monkeypatch):
    def handler(a):
        raise RuntimeError("x")
    c = make_client(monkeypatch, handler)
    assert c.list_orders() == []


def test_get_order_empty_and_exc(monkeypatch):
    c = make_client(monkeypatch, lambda a: {})
    assert c.get_order("") == {}
    def handler(a):
        raise RuntimeError("x")
    c2 = make_client(monkeypatch, handler)
    assert c2.get_order("x") == {}


def test_get_fees_exception(monkeypatch):
    def handler(a):
        raise RuntimeError("x")
    c = make_client(monkeypatch, handler)
    assert c.get_fees() == {}


def test_public_candles(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"candles": []})
    out = c.public_candles("BTC-USD", 0, 3600, granularity="ONE_HOUR", limit=10)
    assert out == {"candles": []}
    # granularity mapping passthrough
    c.public_candles("BTC-USD", 0, 60, granularity="FIVE_MINUTE")


# ---------------------------------------------------------------------------
# RateLimiter pruning + blocking
# ---------------------------------------------------------------------------
def test_rate_limiter_prune(monkeypatch):
    clock = {"t": 0.0}

    class FakeTime:
        def time(self):
            return clock["t"]

        def sleep(self, s):
            clock["t"] += s

    monkeypatch.setattr(cb_client.time, "time", FakeTime().time)
    monkeypatch.setattr(cb_client.time, "sleep", FakeTime().sleep)
    rl = cb_client.RateLimiter(max_calls=2, period=1.0)
    rl.acquire()
    rl.acquire()
    # advance well past the window so old calls prune
    clock["t"] = 5.0
    rl.acquire()
    assert len(rl.calls) == 1


def test_rate_limiter_blocks(monkeypatch):
    clock = {"t": 0.0}

    class FakeTime:
        def time(self):
            return clock["t"]

        def sleep(self, s):
            clock["t"] += s

    monkeypatch.setattr(cb_client.time, "time", FakeTime().time)
    monkeypatch.setattr(cb_client.time, "sleep", FakeTime().sleep)
    rl = cb_client.RateLimiter(max_calls=1, period=1.0)
    rl.acquire()  # t=0
    clock["t"] = 0.5
    rl.acquire()  # must wait until t=1.0
    assert clock["t"] >= 1.0


# ---------------------------------------------------------------------------
# CLI dry-run path
# ---------------------------------------------------------------------------
def test_cli_json_dry_run(monkeypatch):
    seen = {}

    def handler(a):
        seen[tuple(a)] = True
        return {"ok": True}

    c = make_client(monkeypatch, handler)
    out = c._cli_json("orders", "create", "product_id=BTC-USD", dry_run=True)
    assert out == {"ok": True}
    assert any("--dry-run" in k for k in seen)


def test_parse_cli_output_dry_run_invalid(monkeypatch):
    with pytest.raises(RuntimeError):
        cb_client.CBClient._parse_cli_output("would execute {bad json", True)


# ---------------------------------------------------------------------------
# Settlement detection edge cases
# ---------------------------------------------------------------------------
def test_detect_settlement_non_usd(monkeypatch):
    c = make_client(monkeypatch, lambda a: balance_with(("USDT", 500), ("USD", 0)))
    assert c.settlement_currency == "USDT"


def test_detect_settlement_bad_value(monkeypatch):
    accts = [{"currency": "USDC", "available_balance": {"value": "not-a-number"}}]
    c = make_client(monkeypatch, lambda a: {"accounts": accts})
    # unparseable value -> skipped -> falls back to USD
    assert c.settlement_currency == "USD"


def test_remap_non_usd_pair(monkeypatch):
    c = make_client(monkeypatch, lambda a: balance_with(("USD", 1)))
    c.settlement_currency = "USDC"
    # already a USDC pair -> unchanged
    assert c._remap("ETH-USDC") == "ETH-USDC"
    assert c._remap("BTC-USD") == "BTC-USDC"


# ---------------------------------------------------------------------------
# best_bid_ask list-form book
# ---------------------------------------------------------------------------
def test_best_bid_ask_list_book(monkeypatch):
    def handler(a):
        if a[:3] == ("products", "book", "BTC-USD"):
            return {"pricebooks": [{"product_id": "BTC-USD", "bids": [], "asks": []}]}
        return {}

    c = make_client(monkeypatch, handler)
    out = c.best_bid_ask("BTC-USD")
    assert out["pricebooks"][0]["product_id"] == "BTC-USD"
    # empty product id skipped
    assert c.best_bid_ask(["", "BTC-USD"])["pricebooks"]


# ---------------------------------------------------------------------------
# synthetic books dict + tuple
# ---------------------------------------------------------------------------
def test_synthetic_books_dict_and_tuple(monkeypatch):
    def handler_dict(a):
        if a[0] == "products" and a[1] == "candles":
            return {"candles": [{"start": 1, "open": 100, "high": 110,
                                 "low": 90, "close": 105, "volume": 1}]}
        return {}

    c = make_client(monkeypatch, handler_dict)
    books = c._synthetic_books(["BTC-USD"])
    assert books and "BTC-USD" in books[0]["product_id"]

    def handler_tuple(a):
        if a[0] == "products" and a[1] == "candles":
            return {"candles": [(1, 90, 110, 100, 105, 1)]}
        return {}

    c2 = make_client(monkeypatch, handler_tuple)
    books2 = c2._synthetic_books(["ETH-USD"])
    assert books2 and "ETH-USD" in books2[0]["product_id"]


# ---------------------------------------------------------------------------
# Order placement edge cases
# ---------------------------------------------------------------------------
def test_create_market_order_sell_no_base(monkeypatch):
    c = make_client(monkeypatch, lambda a: {"order_id": "o"})
    with pytest.raises(ValueError):
        c.create_market_order("SELL", "BTC-USD")


def test_limit_order_extras(monkeypatch):
    seen = {}

    def handler(a):
        seen[tuple(a)] = True
        return {"order_id": "o"}

    c = make_client(monkeypatch, handler)
    c.create_limit_order("BUY", "BTC-USD", base_size="1", price="100",
                         post_only=True, client_order_id="cid")
    assert any("post_only=true" in k for k in seen)
    assert any("client_order_id=cid" in k for k in seen)
    c.create_limit_order("BUY", "BTC-USD", base_size="1", price="100",
                         time_in_force="IOC")
    assert any("time_in_force=IOC" in k for k in seen)


def test_stop_orders_with_client_id(monkeypatch):
    seen = {}

    def handler(a):
        seen[tuple(a)] = True
        return {"order_id": "o"}

    c = make_client(monkeypatch, handler)
    c.create_stop_limit_order("SELL", "BTC-USD", base_size="1",
                              limit_price="90", stop_price="89",
                              client_order_id="c1")
    assert any("client_order_id=c1" in k for k in seen)
    c.create_stop_market_order("BUY", "BTC-USD", base_size="1",
                               stop_price="110", stop_direction="up",
                               client_order_id="c2")
    assert any("client_order_id=c2" in k for k in seen)


def test_close_position_size(monkeypatch):
    seen = {}

    def handler(a):
        seen[tuple(a)] = True
        return {"order_id": "o"}

    c = make_client(monkeypatch, handler)
    c.close_position("BTC-USD", size="2.0", client_order_id="c1")
    assert any("size=2.0" in k for k in seen)


def test_list_orders_with_status(monkeypatch):
    def handler(a):
        if a[0] == "orders" and a[1] == "list":
            return {"orders": [{"id": 1}]}
        return {}

    c = make_client(monkeypatch, handler)
    assert c.list_orders(product_id="BTC-USD", status="OPEN") == [{"id": 1}]


def test_get_products_no_type(monkeypatch):
    def handler(a):
        if a[0] == "products" and a[1] == "list":
            return {"products": [{"id": "BTC-USD"}]}
        return {}

    c = make_client(monkeypatch, handler)
    assert c.get_products() == [{"id": "BTC-USD"}]

