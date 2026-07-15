"""Extended coverage tests for coinbase/src/cb_client.py"""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

import coinbase.src.cb_client as cbmod
from coinbase.src.cb_client import CBClient, RateLimiter


def _completed(stdout, rc=0):
    m = MagicMock()
    m.returncode = rc
    m.stdout = stdout
    m.stderr = ""
    return m


@pytest.fixture
def cli(monkeypatch):
    state = {"responses": {}, "calls": [], "raise": set()}

    def fake_run(cmd, *a, **k):
        state["calls"].append(cmd)
        s = " ".join(cmd)
        for key in state["raise"]:
            if key in s:
                raise RuntimeError("forced failure: " + key)
        for key, val in state["responses"].items():
            if key in s:
                if isinstance(val, Exception):
                    raise val
                return _completed(val)
        return _completed(json.dumps({"ok": True}))

    monkeypatch.setattr(cbmod.subprocess, "run", fake_run)
    monkeypatch.setattr(cbmod.shutil, "which", lambda x: "/usr/bin/coinbase")
    return state


# ── RateLimiter ─────────────────────────────────────────────────────
def test_rate_limiter_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(cbmod.time, "sleep", lambda s: slept.append(s))
    rl = RateLimiter(max_calls=1, period=1.0)
    rl.acquire()
    rl.acquire()  # triggers wait branch
    assert slept


def test_rate_limiter_no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(cbmod.time, "sleep", lambda s: slept.append(s))
    rl = RateLimiter(max_calls=5, period=1.0)
    rl.acquire()
    assert not slept


# ── _cli_json dry-run ───────────────────────────────────────────────
def test_cli_json_dry_run(cli):
    c = CBClient(dry_run_cli=True)
    out = c._cli_json("orders", "preview", dry_run=True)
    assert isinstance(out, dict)


def test_rate_limiter_old_call_popped(monkeypatch):
    slept = []
    monkeypatch.setattr(cbmod.time, "sleep", lambda s: slept.append(s))
    rl = RateLimiter(max_calls=2, period=1.0)
    rl.calls.append(0.0)  # very old timestamp
    rl.acquire()  # pops the old call, no sleep
    assert len(rl.calls) >= 1


def test_best_bid_ask_string_product(cli):
    cli["responses"]["book"] = json.dumps({
        "pricebook": {"product_id": "BTC-USD", "bids": [{"price": "99"}], "asks": [{"price": "101"}]}
    })
    c = CBClient()
    merged = c.best_bid_ask("BTC-USD")
    assert merged["pricebooks"]


def test_best_bid_ask_list_book_with_nondict(cli):
    cli["responses"]["book"] = json.dumps({
        "pricebooks": [{"product_id": "BTC-USD"}, "notadict"]
    })
    c = CBClient()
    merged = c.best_bid_ask(["BTC-USD"])
    assert merged["pricebooks"]


def test_preview_order_sell_success(cli):
    out = CBClient().preview_order("SELL", "BTC-USD", base_size="0.1")
    assert out["ok"] is True


def test_create_market_order_sell_success(cli):
    out = CBClient().create_market_order("SELL", "BTC-USD", base_size="0.1")
    assert out["ok"] is True


def test_synthetic_books_raises(cli):
    cli["raise"].add("candles")
    c = CBClient()
    assert c._synthetic_books(["BTC-USD"]) == []


def test_detect_settlement_invalid_balances(cli):
    cli["responses"]["balance"] = json.dumps({
        "accounts": [
            {"currency": "USD", "available_balance": {"value": "bad"}},
            {"currency": "USDC", "available_balance": {"value": "0"}},
            {"currency": "EUR", "available_balance": {"value": "50"}},
        ]
    })
    c = CBClient()
    # EUR wins (non-USD, positive)
    assert c.settlement_currency == "EUR"


def test_cli_json_nonzero_rc(cli):
    cli["raise"]  # noop
    # force nonzero by making run return rc=1
    def fake_run(cmd, *a, **k):
        return _completed("boom", rc=1)
    with patch.object(cbmod.subprocess, "run", fake_run):
        with pytest.raises(RuntimeError):
            CBClient()._cli_json("orders", "preview")


def test_parse_cli_output_dry_run_error():
    with pytest.raises(RuntimeError):
        CBClient._parse_cli_output("would execute orders\n{not valid json", dry_run=True)


def test_parse_cli_output_dry_run_ok():
    txt = 'would execute orders create\n{"product_id": "BTC-USD"}'
    assert CBClient._parse_cli_output(txt, dry_run=True)["product_id"] == "BTC-USD"


# ── remap ───────────────────────────────────────────────────────────
def test_remap_usd():
    c = CBClient()
    c.settlement_currency = "USD"
    assert c._remap("BTC-USD") == "BTC-USD"


def test_remap_usdc():
    c = CBClient()
    c.settlement_currency = "USDC"
    assert c._remap("BTC-USD") == "BTC-USDC"


def test_remap_no_product():
    c = CBClient()
    c.settlement_currency = "USDC"
    assert c._remap("") == ""
    c.settlement_currency = ""
    assert c._remap("BTC-USD") == "BTC-USD"


# ── settlement detection ────────────────────────────────────────────
def test_detect_settlement_env(cli, monkeypatch):
    monkeypatch.setenv("COINBASE_SETTLEMENT_CURRENCY", "USDC")
    c = CBClient()
    assert c.settlement_currency == "USDC"


def test_detect_settlement_from_balance(cli):
    cli["responses"]["balance"] = json.dumps({
        "accounts": [
            {"currency": "USDC", "available_balance": {"value": "500"}},
            {"currency": "USD", "available_balance": {"value": "100"}},
        ]
    })
    c = CBClient()
    assert c.settlement_currency == "USDC"


def test_detect_settlement_usd_only(cli):
    cli["responses"]["balance"] = json.dumps({
        "accounts": [{"currency": "USD", "available_balance": {"value": "100"}}]
    })
    c = CBClient()
    assert c.settlement_currency == "USD"


def test_detect_settlement_exception(cli):
    cli["raise"].add("balance")
    c = CBClient()
    assert c.settlement_currency == "USD"


# ── get_positions variants ──────────────────────────────────────────
def test_get_positions_no_uuid(cli):
    cli["responses"]["portfolios list"] = json.dumps({
        "portfolios": [{"type": "NOTDEFAULT"}]
    })
    c = CBClient()
    assert c.get_positions() == []


def test_get_positions_default(cli):
    cli["responses"]["portfolios list"] = json.dumps({
        "portfolios": [{"type": "DEFAULT", "uuid": "u1"}]
    })
    cli["responses"]["portfolios get"] = json.dumps({
        "spot_positions": [
            {"asset": "BTC", "total_balance_crypto": "0.5",
             "total_balance_fiat": "25000", "available_to_trade_fiat": "1000"},
        ]
    })
    c = CBClient()
    pos = c.get_positions()
    assert pos and pos[0]["asset"] == "BTC"


def test_get_positions_skip_zero_and_cash(cli):
    cli["responses"]["portfolios list"] = json.dumps({
        "portfolios": [{"type": "DEFAULT", "uuid": "u1"}]
    })
    cli["responses"]["portfolios get"] = json.dumps({
        "spot_positions": [
            {"asset": "BTC", "total_balance_crypto": "0", "is_cash": False},
            {"asset": "USD", "total_balance_crypto": "0", "is_cash": True},
            {"asset": "ETH", "total_balance_crypto": "1.0", "is_cash": False},
        ]
    })
    c = CBClient()
    pos = c.get_positions()
    assert len(pos) == 1 and pos[0]["asset"] == "ETH"


# ── best_bid_ask ────────────────────────────────────────────────────
def test_best_bid_ask_empty(cli):
    c = CBClient()
    assert c.best_bid_ask([]) == {"pricebooks": []}
    assert c.best_bid_ask(["", None]) == {"pricebooks": []}


def test_best_bid_ask_dedup(cli):
    cli["responses"]["book"] = json.dumps({
        "pricebook": {"product_id": "BTC-USD", "bids": [{"price": "99"}], "asks": [{"price": "101"}]}
    })
    c = CBClient()
    merged = c.best_bid_ask(["BTC-USD", "BTC-USD"])
    assert len(merged["pricebooks"]) == 1


def test_best_bid_ask_list_book(cli):
    cli["responses"]["book"] = json.dumps({
        "pricebooks": [{"product_id": "BTC-USD"}]
    })
    c = CBClient()
    merged = c.best_bid_ask(["BTC-USD"])
    assert merged["pricebooks"]


def test_best_bid_ask_synthetic_dict(cli):
    cli["raise"].add("book")
    cli["responses"]["candles"] = json.dumps({"candles": [
        {"start": 1, "open": 100, "high": 110, "low": 90, "close": 105, "volume": 10}
    ]})
    c = CBClient()
    merged = c.best_bid_ask(["BTC-USD"])
    assert merged["pricebooks"]


def test_best_bid_ask_synthetic_tuple(cli):
    cli["raise"].add("book")
    cli["responses"]["candles"] = json.dumps({
        "candles": [[1, 90, 110, 100, 105, 10]]
    })
    c = CBClient()
    merged = c.best_bid_ask(["BTC-USD"])
    assert merged["pricebooks"]


def test_best_bid_ask_synthetic_empty(cli):
    cli["raise"].add("book")
    cli["responses"]["candles"] = json.dumps({"candles": []})
    c = CBClient()
    merged = c.best_bid_ask(["BTC-USD"])
    assert merged["pricebooks"] == []


# ── synthetic books direct ───────────────────────────────────────────
def test_synthetic_books_empty(cli):
    cli["responses"]["candles"] = json.dumps({"candles": []})
    c = CBClient()
    assert c._synthetic_books(["BTC-USD"]) == []


def test_synthetic_books_dict(cli):
    cli["responses"]["candles"] = json.dumps({"candles": [
        {"start": 1, "open": 100, "high": 110, "low": 90, "close": 105, "volume": 10}
    ]})
    c = CBClient()
    assert c._synthetic_books(["BTC-USD"])


def test_synthetic_books_tuple(cli):
    cli["responses"]["candles"] = json.dumps({
        "candles": [[1, 90, 110, 100, 105, 10]]
    })
    c = CBClient()
    assert c._synthetic_books(["BTC-USD"])


# ── preview_order branches ──────────────────────────────────────────
def test_preview_order_buy_base(cli):
    c = CBClient()
    out = c.preview_order("BUY", "BTC-USD", base_size="0.1")
    assert out["ok"] is True


def test_preview_order_sell_missing(cli):
    # sell without base_size raises -> synthetic error returned
    out = CBClient().preview_order("SELL", "BTC-USD")
    assert out["status"] == "preview_error"


def test_preview_order_bad_side(cli):
    with pytest.raises(ValueError):
        CBClient().preview_order("HODL", "BTC-USD", quote_size="1")


# ── create_market_order branches ────────────────────────────────────
def test_create_market_order_buy_quote(cli):
    c = CBClient()
    assert c.create_market_order("BUY", "BTC-USD", quote_size="100")["ok"]


def test_create_market_order_buy_base(cli):
    c = CBClient()
    assert c.create_market_order("BUY", "BTC-USD", base_size="0.1")["ok"]


def test_create_market_order_sell_missing(cli):
    with pytest.raises(ValueError):
        CBClient().create_market_order("SELL", "BTC-USD")


def test_create_market_order_with_ids(cli):
    c = CBClient()
    out = c.create_market_order("BUY", "BTC-USD", quote_size="100",
                                client_order_id="cid", preview_id="pid")
    assert out["ok"]


# ── limit / stop orders with client id ─────────────────────────────
def test_limit_order_post_only(cli):
    c = CBClient()
    out = c.create_limit_order("BUY", "BTC-USD", base_size="0.1", price="100",
                               post_only=True, client_order_id="c1")
    assert out["ok"]


def test_stop_limit_client(cli):
    c = CBClient()
    out = c.create_stop_limit_order("SELL", "BTC-USD", base_size="0.1",
                                    limit_price="90", stop_price="95",
                                    client_order_id="c2")
    assert out["ok"]


def test_stop_market_client(cli):
    c = CBClient()
    out = c.create_stop_market_order("SELL", "BTC-USD", base_size="0.1",
                                     stop_price="95", client_order_id="c3")
    assert out["ok"]


# ── close_position ──────────────────────────────────────────────────
def test_close_position_with_id_and_size(cli):
    c = CBClient()
    out = c.close_position("BTC-USD", size="0.1", client_order_id="c4")
    assert out["ok"]


def test_close_position_generated_id(cli):
    c = CBClient()
    out = c.close_position("BTC-USD")
    assert out["ok"]


# ── list_orders / get_order / get_fees / get_products ───────────────
def test_list_orders_both_none(cli):
    c = CBClient()
    assert c.list_orders() == []


def test_list_orders_product_and_status(cli):
    c = CBClient()
    assert c.list_orders("BTC-USD", "OPEN") == []


def test_list_orders_exception(cli):
    cli["raise"].add("orders list")
    assert CBClient().list_orders("BTC-USD") == []


def test_get_order_empty(cli):
    assert CBClient().get_order("") == {}


def test_get_order_exception(cli):
    cli["raise"].add("orders get")
    assert CBClient().get_order("oid") == {}


def test_get_fees_exception(cli):
    cli["raise"].add("fees")
    assert CBClient().get_fees() == {}


def test_get_products_with_type(cli):
    c = CBClient()
    assert c.get_products("SPOT") == []


def test_get_products_no_type(cli):
    c = CBClient()
    assert c.get_products() == []


def test_list_accounts(cli):
    assert CBClient().list_accounts()["ok"]


def test_cancel_order(cli):
    assert CBClient().cancel_order("oid")["ok"]
