"""Coverage tests for event_markets.polymarket_executor (network/lib mocked)."""
import json
import urllib.error
import urllib.request
from unittest.mock import MagicMock, patch

import pytest

import event_markets.polymarket_executor as pe
from event_markets.polymarket_executor import PolymarketExecutionClient


class FakeClobClient:
    def __init__(self, *a, **k):
        self.signer = MagicMock()
        self.creds = MagicMock()
        self.host = "https://clob.polymarket.com"
        self.create_or_derive_api_creds = MagicMock(return_value=MagicMock())
        self.set_api_creds = MagicMock()
        self.create_and_post_order = MagicMock(return_value={"orderID": "o1", "status": "matched"})
        self.create_order = MagicMock(return_value={"signed": True})
        self.post_order = MagicMock(return_value={"orderID": "o2", "status": "matched"})
        self.cancel = MagicMock(return_value={"ok": True})
        self.get_orders = MagicMock(return_value=[{"orderID": "o1"}])
        self.get_address = MagicMock(return_value="0xADDR")
        self.get_balance_allowance = MagicMock(return_value={"balance": 50000})


def _client(**kw):
    return PolymarketExecutionClient(
        private_key=kw.get("private_key", "0x" + "11" * 32),
        funder=kw.get("funder", ""),
        signature_type=kw.get("signature_type", 0),
        host=kw.get("host", ""),
        chain_id=kw.get("chain_id", None),
        data_client=kw.get("data_client", None),
    )


def _patch_clob(monkeypatch):
    import py_clob_client.client as cc
    import py_clob_client.clob_types as ct
    import py_clob_client.headers.headers as hh
    import py_clob_client.http_helpers.helpers as hp
    import py_clob_client.endpoints as ep
    import py_clob_client.order_builder.constants as obc
    monkeypatch.setattr(cc, "ClobClient", FakeClobClient)
    monkeypatch.setattr(ct, "ApiCreds", MagicMock())
    monkeypatch.setattr(ct, "OrderArgs", MagicMock())
    monkeypatch.setattr(ct, "OrderType", type("OT", (), {"GTC": "GTC", "FOK": "FOK"})())
    monkeypatch.setattr(ct, "BalanceAllowanceParams", MagicMock())
    monkeypatch.setattr(ct, "AssetType", type("AT", (), {"COLLATERAL": "COLLATERAL"})())
    monkeypatch.setattr(ct, "RequestArgs", MagicMock())
    monkeypatch.setattr(obc, "BUY", "BUY")
    monkeypatch.setattr(obc, "SELL", "SELL")
    monkeypatch.setattr(hh, "create_level_2_headers", lambda *a, **k: {})
    monkeypatch.setattr(hp, "add_balance_allowance_params_to_url", lambda url, *a, **k: url)
    monkeypatch.setattr(ep, "UPDATE_BALANCE_ALLOWANCE", "/ba/update")
    monkeypatch.setattr(ep, "GET_BALANCE_ALLOWANCE", "/ba/get")


# ── config / capability ─────────────────────────────────────────────
def test_is_configured_no_key():
    c = _client(private_key="")
    ok, why = c.is_configured()
    assert ok is False and "POLYMARKET_PRIVATE_KEY" in why


def test_is_configured_lib_missing(monkeypatch):
    c = _client()
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_lib_available", staticmethod(lambda: False))
    ok, why = c.is_configured()
    assert ok is False and "py_clob_client" in why


def test_is_configured_ok(monkeypatch):
    c = _client()
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_lib_available", staticmethod(lambda: True))
    ok, why = c.is_configured()
    assert ok and why == ""


def test_constructor_defaults(monkeypatch):
    for e in ("POLYMARKET_PRIVATE_KEY", "POLYMARKET_FUNDER", "POLYMARKET_SIGNATURE_TYPE",
              "POLYMARKET_CLOB_HOST", "POLYMARKET_CHAIN_ID"):
        monkeypatch.delenv(e, raising=False)
    c = PolymarketExecutionClient()
    assert c.host == pe.DEFAULT_HOST
    assert c.chain_id == pe.DEFAULT_CHAIN_ID
    assert c.signature_type == 0


def test_constructor_bad_signature_type(monkeypatch):
    monkeypatch.setenv("POLYMARKET_SIGNATURE_TYPE", "notanint")
    c = PolymarketExecutionClient()
    assert c.signature_type == 0


def test_constructor_env_chain_id(monkeypatch):
    monkeypatch.setenv("POLYMARKET_CHAIN_ID", "80001")
    c = PolymarketExecutionClient()
    assert c.chain_id == 80001


# ── client bootstrap ────────────────────────────────────────────────
def test_lib_available_false(monkeypatch):
    import builtins
    real = builtins.__import__
    def fake(name, *a, **k):
        if name == "py_clob_client":
            raise ImportError("missing")
        return real(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", fake)
    assert pe.PolymarketExecutionClient._lib_available() is False


def test_resolve_token_clob_empty_tokenid(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: [("FOO", ""), ("BAR", "")]))
    import event_markets.polymarket_client as pcm
    monkeypatch.setattr(pcm, "PolymarketClient", MagicMock(side_effect=RuntimeError("x")))
    c = _client(data_client=None)
    with pytest.raises(RuntimeError):
        c.resolve_token_id("CID", "yes")


def test_resolve_token_gamma_empty_tokenid(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: []))
    gamma = MagicMock()
    gamma.fetch_market_detail.return_value = MagicMock(
        outcomes=["YES", "NO"], tokens=[{"token_id": ""}, {"token_id": ""}])
    c = _client(data_client=gamma)
    with pytest.raises(RuntimeError):
        c.resolve_token_id("G", "yes")


def test_balance_allowance_request_explicit_asset_type(monkeypatch):
    _patch_clob(monkeypatch)
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda *a, **k: _urlopen_resp(json.dumps({"balance": 1}).encode()))
    c = _client()
    assert c._balance_allowance_request("/ba/get", asset_type="COLLATERAL") == {"balance": 1}


def test_get_client_requires_config():
    c = _client(private_key="")
    with pytest.raises(RuntimeError):
        c._get_client()


def test_get_client_success(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    client = c._get_client()
    assert isinstance(client, FakeClobClient)
    assert c._get_client() is client  # cached


def test_data_client_init_failure(monkeypatch):
    import event_markets.polymarket_client as pcm
    monkeypatch.setattr(pcm, "PolymarketClient", MagicMock(side_effect=RuntimeError("x")))
    c = _client(data_client=None)
    assert c._data_client() is None


def test_data_client_cached(monkeypatch):
    gamma = MagicMock()
    c = _client(data_client=gamma)
    assert c._data_client() is gamma
    assert c._data_client() is gamma


# ── token resolution ────────────────────────────────────────────────
def test_resolve_token_numeric():
    c = _client()
    assert c.resolve_token_id("123456789", "yes") == "123456789"


def test_resolve_token_clob_tokens(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: [("Yes", "T_YES"), ("No", "T_NO")]))
    c = _client()
    assert c.resolve_token_id("CID", "yes") == "T_YES"
    assert c.resolve_token_id("CID", "no") == "T_NO"


def test_resolve_token_positional_fallback(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: [("FOO", "T0"), ("BAR", "T1")]))
    c = _client()
    assert c.resolve_token_id("CID", "yes") == "T0"
    assert c.resolve_token_id("CID", "no") == "T1"


def test_resolve_token_gamma_yes_no(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: []))
    gamma = MagicMock()
    gamma.fetch_market_detail.return_value = MagicMock(
        outcomes=["YES", "NO"], tokens=[{"token_id": "G_YES"}, {"token_id": "G_NO"}])
    c = _client(data_client=gamma)
    assert c.resolve_token_id("GAMMAID", "yes") == "G_YES"
    assert c.resolve_token_id("GAMMAID", "no") == "G_NO"


def test_resolve_token_gamma_true_false(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: []))
    gamma = MagicMock()
    gamma.fetch_market_detail.return_value = MagicMock(
        outcomes=["true", "false"], tokens=[{"token_id": "G_T"}, {"token_id": "G_F"}])
    c = _client(data_client=gamma)
    assert c.resolve_token_id("G", "yes") == "G_T"
    assert c.resolve_token_id("G", "no") == "G_F"


def test_resolve_token_gamma_positional(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: []))
    gamma = MagicMock()
    gamma.fetch_market_detail.return_value = MagicMock(
        outcomes=["A", "B"], tokens=[{"token_id": "GA"}, {"token_id": "GB"}])
    c = _client(data_client=gamma)
    assert c.resolve_token_id("G", "yes") == "GA"
    assert c.resolve_token_id("G", "no") == "GB"


def test_resolve_token_raises(monkeypatch):
    monkeypatch.setattr(pe.PolymarketExecutionClient, "_clob_market_tokens",
                        staticmethod(lambda cid: []))
    c = _client(data_client=None)
    with pytest.raises(RuntimeError):
        c.resolve_token_id("CID", "yes")


def test_clob_market_tokens_success(monkeypatch):
    payload = {"tokens": [{"outcome": "Yes", "token_id": "T1"}]}
    resp = MagicMock()
    resp.read.return_value = json.dumps(payload).encode()
    resp.__enter__ = lambda s: s
    resp.__exit__ = lambda *a: False
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: resp)
    assert pe.PolymarketExecutionClient._clob_market_tokens("CID") == [("Yes", "T1")]


def test_clob_market_tokens_failure(monkeypatch):
    monkeypatch.setattr(urllib.request, "urlopen",
                        MagicMock(side_effect=RuntimeError("net")))
    assert pe.PolymarketExecutionClient._clob_market_tokens("CID") == []


# ── orders ──────────────────────────────────────────────────────────
def test_place_order_gtc(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    assert c.place_order("CID", "buy", 0.4, 10, outcome="yes", token_id="TK")["orderID"] == "o1"


def test_place_order_fok(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    assert c.place_order("CID", "sell", 0.4, 10, outcome="no", order_type="FOK", token_id="TK")["orderID"] == "o2"


def test_place_order_explicit_token(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    assert c.place_order("CID", "buy", 0.4, 10, token_id="TK")["orderID"] == "o1"


def test_place_order_non_dict_resp(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    c._get_client().create_and_post_order.return_value = "raw"
    assert c.place_order("CID", "buy", 0.4, 10, token_id="TK")["raw"] == "raw"


def test_place_order_unknown_type(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    # order_type not GTC -> uses create_order/post_order path
    r = c.place_order("CID", "buy", 0.4, 10, token_id="TK", order_type="FOK")
    assert r["orderID"] == "o2"


def test_cancel(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    assert c.cancel("o1") == {"ok": True}


def test_get_orders(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    assert c.get_orders() == [{"orderID": "o1"}]


def test_get_orders_exception(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    c._get_client().get_orders.side_effect = RuntimeError("x")
    assert c.get_orders() == []


# ── balances / allowances ───────────────────────────────────────────
def _urlopen_resp(body: bytes):
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = lambda *a: False
    return resp


def test_balance_allowance_request_success(monkeypatch):
    _patch_clob(monkeypatch)
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda *a, **k: _urlopen_resp(json.dumps({"balance": 50000}).encode()))
    c = _client()
    assert c._balance_allowance_request("/ba/get") == {"balance": 50000}


def test_balance_allowance_request_empty_body(monkeypatch):
    _patch_clob(monkeypatch)
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda *a, **k: _urlopen_resp(b"   "))
    c = _client()
    assert c._balance_allowance_request("/ba/get") == {}


def test_balance_allowance_request_http_error(monkeypatch):
    _patch_clob(monkeypatch)
    import urllib.error
    resp = MagicMock()
    resp.read.return_value = b"{}"
    monkeypatch.setattr(urllib.request, "urlopen",
                        MagicMock(side_effect=urllib.error.HTTPError("u", 403, "f", {}, resp)))
    c = _client()
    assert c._balance_allowance_request("/ba/get") is None


def test_balance_allowance_request_other_error(monkeypatch):
    _patch_clob(monkeypatch)
    monkeypatch.setattr(urllib.request, "urlopen",
                        MagicMock(side_effect=RuntimeError("net")))
    c = _client()
    assert c._balance_allowance_request("/ba/get") is None


def test_refresh_balance_allowance(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    with patch.object(c, "_balance_allowance_request", return_value={"ok": 1}):
        assert c.refresh_balance_allowance() is True
    with patch.object(c, "_balance_allowance_request", return_value=None):
        assert c.refresh_balance_allowance() is False


def test_get_balance_allowance(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    with patch.object(c, "_balance_allowance_request", return_value={"balance": 1}):
        assert c.get_balance_allowance() == {"balance": 1}


def test_get_usdc_balance(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    with patch.object(c, "get_balance_allowance", return_value={"balance": 6000000}):
        assert c.get_usdc_balance() == 6.0


def test_get_usdc_balance_none_fallback(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    with patch.object(c, "_balance_allowance_request", return_value=None):
        c._get_client().get_balance_allowance.return_value = None
        assert c.get_usdc_balance() is None


def test_get_usdc_balance_exception(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    with patch.object(c, "get_balance_allowance", side_effect=RuntimeError("x")):
        assert c.get_usdc_balance() is None


def test_address(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    assert c.address() == "0xADDR"


def test_address_exception(monkeypatch):
    _patch_clob(monkeypatch)
    c = _client()
    c._get_client().get_address.side_effect = RuntimeError("x")
    assert c.address() == ""
