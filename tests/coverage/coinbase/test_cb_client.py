import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

import coinbase.src.cb_client as cbmod
from coinbase.src.cb_client import CBClient, RateLimiter


def _completed(stdout, returncode=0):
    cp = MagicMock()
    cp.returncode = returncode
    cp.stdout = stdout
    cp.stderr = ""
    return cp


@pytest.fixture
def run_mock(monkeypatch):
    runs = []

    def fake_run(cmd, *a, **k):
        runs.append(cmd)
        return _completed(json.dumps({"ok": True, "accounts": []}))

    monkeypatch.setattr(cbmod.subprocess, "run", fake_run)
    monkeypatch.setattr(cbmod.shutil, "which", lambda x: "/usr/bin/coinbase")
    return runs


def test_rate_limiter_acquire():
    rl = RateLimiter(max_calls=2, period=0.05)
    rl.acquire()
    rl.acquire()
    assert rl.calls


def test_client_init(run_mock):
    c = CBClient(api_key="k", api_secret="s")
    assert c.api_key == "k"
    assert c.settlement_currency == "USD"


def test_parse_cli_output_json():
    out = cbmod.CBClient._parse_cli_output('{"a": 1}', dry_run=False)
    assert out["a"] == 1


def test_parse_cli_output_empty():
    assert cbmod.CBClient._parse_cli_output("", dry_run=False) == {}
    with pytest.raises(RuntimeError):
        cbmod.CBClient._parse_cli_output("no json here", dry_run=False)


def test_parse_cli_output_dry_run():
    txt = 'would execute orders create\n{"product_id": "BTC-USD"}'
    out = cbmod.CBClient._parse_cli_output(txt, dry_run=True)
    assert out["product_id"] == "BTC-USD"


def test_parse_cli_output_dry_run_bad():
    with pytest.raises(RuntimeError):
        cbmod.CBClient._parse_cli_output("would execute orders\n{not valid json", dry_run=True)


def test_norm_stop_direction():
    assert cbmod.CBClient._norm_stop_direction("up") == "up"
    assert cbmod.CBClient._norm_stop_direction("stop_direction_stop_down") == "down"
    assert cbmod.CBClient._norm_stop_direction("weird") == "weird"


def test_remap(run_mock):
    c = CBClient()
    c.settlement_currency = "USDC"
    assert c._remap("BTC-USD") == "BTC-USDC"
    c.settlement_currency = "USD"
    assert c._remap("BTC-USD") == "BTC-USD"


def test_list_accounts(run_mock):
    assert CBClient().list_accounts() == {"ok": True, "accounts": []}


def test_get_positions(run_mock):
    runs = []
    portfolio = {"portfolios": [{"type": "DEFAULT", "uuid": "u1"}]}
    pf_get = {"spot_positions": [
        {"asset": "BTC", "total_balance_crypto": "0.5", "is_cash": False,
         "total_balance_fiat": "25000", "available_to_trade_fiat": "1000"},
        {"asset": "USD", "total_balance_crypto": "0", "is_cash": True,
         "total_balance_fiat": "100", "available_to_trade_fiat": "0"},
    ]}

    def fake_run(cmd, *a, **k):
        runs.append(cmd)
        if "portfolios" in cmd and "get" not in cmd:
            return _completed(json.dumps(portfolio))
        if "get" in cmd:
            return _completed(json.dumps(pf_get))
        return _completed(json.dumps({"ok": True}))

    with patch.object(cbmod.subprocess, "run", fake_run):
        with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
            positions = CBClient().get_positions()
    assert positions and positions[0]["asset"] == "BTC"
    assert positions[0]["product_id"] == "BTC-USD"


def test_get_positions_empty(run_mock):
    with patch.object(cbmod.subprocess, "run",
                      lambda *a, **k: _completed(json.dumps({"portfolios": []}))):
        with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
            assert CBClient().get_positions() == []


def test_get_positions_error(run_mock):
    def fake_run(cmd, *a, **k):
        raise RuntimeError("boom")
    with patch.object(cbmod.subprocess, "run", fake_run):
        with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
            assert CBClient().get_positions() == []


def test_best_bid_ask(run_mock):
    def fake_run(cmd, *a, **k):
        return _completed(json.dumps({"pricebook": {"product_id": "BTC-USD",
                                                    "bids": [{"price": "99"}], "asks": [{"price": "101"}]}}))
    with patch.object(cbmod.subprocess, "run", fake_run):
        with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
            merged = CBClient().best_bid_ask(["BTC-USD", "ETH-USD"])
    assert merged["pricebooks"]


def test_best_bid_ask_synthetic(run_mock):
    def fake_run(cmd, *a, **k):
        raise RuntimeError("no book")
    with patch.object(cbmod.subprocess, "run", fake_run):
        with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
            c = CBClient()
            c.public_candles = MagicMock(return_value={"candles": [
                {"start": 1, "open": 100, "high": 110, "low": 90, "close": 105, "volume": 10}]})
            merged = c.best_bid_ask(["BTC-USD"])
    assert merged["pricebooks"]


def test_preview_order_buy(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        out = CBClient().preview_order("BUY", "BTC-USD", quote_size="100")
    assert out["ok"] is True


def test_preview_order_bad_side(run_mock):
    with pytest.raises(ValueError):
        CBClient().preview_order("HODL", "BTC-USD", quote_size="100")


def test_preview_order_buy_missing(run_mock):
    out = CBClient().preview_order("BUY", "BTC-USD")
    assert out["status"] == "preview_error"


def test_preview_order_sell_missing(run_mock):
    out = CBClient().preview_order("SELL", "BTC-USD")
    assert out["status"] == "preview_error"


def test_preview_order_synthetic_on_error(run_mock):
    def fake_run(cmd, *a, **k):
        return _completed("error", returncode=1)
    with patch.object(cbmod.subprocess, "run", fake_run):
        with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
            out = CBClient().preview_order("BUY", "BTC-USD", quote_size="100")
    assert out["status"] == "preview_error"


def test_create_market_order(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        out = CBClient().create_market_order("BUY", "BTC-USD", quote_size="100")
        assert out["ok"] is True
        out2 = CBClient().market_order("SELL", "BTC-USD", base_size="0.01")
        assert out2["ok"] is True


def test_create_limit_order(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        out = CBClient().create_limit_order("BUY", "BTC-USD", base_size="0.01", price="100")
    assert out["ok"] is True


def test_stop_limit_and_market(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        c = CBClient()
        sl = c.create_stop_limit_order("SELL", "BTC-USD", base_size="0.01",
                                       limit_price="90", stop_price="95")
        sm = c.create_stop_market_order("SELL", "BTC-USD", base_size="0.01", stop_price="95")
    assert sl["ok"] and sm["ok"]


def test_close_and_cancel(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        c = CBClient()
        cl = c.close_position("BTC-USD", size="0.01")
        ca = c.cancel_order("order-id")
    assert cl["ok"] and ca["ok"]


def test_list_and_get_orders(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        c = CBClient()
        assert c.list_orders("BTC-USD", "OPEN") == []
        assert c.get_order("") == {}
        assert isinstance(c.get_fees(), dict)


def test_get_products(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        out = CBClient().get_products("SPOT")
    assert out == []


def test_public_candles(run_mock):
    with patch.object(cbmod.shutil, "which", lambda x: "coinbase"):
        out = CBClient().public_candles("BTC-USD", 1, 2, granularity="ONE_HOUR", limit=10)
    assert out["ok"] is True
