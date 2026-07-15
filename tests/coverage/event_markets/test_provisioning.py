"""Coverage tests for event_markets.provisioning (web3/network mocked)."""
import argparse
import json
from unittest.mock import MagicMock, patch

import pytest

import event_markets.provisioning as pv
from event_markets.provisioning import (
    generate_wallet, get_web3, wallet_status, swap_usdc_to_usdce,
    set_polymarket_allowances, refresh_clob_cache, _resolve_key,
    _eip1559_fees, _send, _main, CHAIN_ID, MAX_UINT256,
    USDC_NATIVE, USDCE, CTF, UNISWAP_V3_ROUTER, POLYMARKET_SPENDERS,
)


class FakeSigned:
    def __init__(self):
        self.raw_transaction = b"rawtx"


class FakeReceipt:
    def __init__(self, status=1):
        self.status = status
        self.transactionHash = b"0xhash"
        self.gasUsed = 120000


class FakeFn:
    def __init__(self, name, store):
        self.name = name
        self.store = store

    def call(self, *a, **k):
        if self.name == "balanceOf":
            return self.store.get("balance", 0)
        if self.name == "allowance":
            return 0
        if self.name == "isApprovedForAll":
            return False
        return 0

    def estimate_gas(self, *a, **k):
        return 100000

    def build_transaction(self, *a, **k):
        return {"nonce": 0, "from": "0xme"}


class FakeFunctions:
    def __init__(self, store):
        self._store = store

    def __getattr__(self, name):
        def make(*args, **kwargs):
            fn = FakeFn(name, self._store)
            # allowance / isApprovedForAll keyed by spender (last arg)
            if name in ("allowance", "isApprovedForAll") and args:
                sp = args[-1]
                if name == "allowance":
                    fn.call = lambda *a, **k: self._store.get("allowances", {}).get(sp, 0)
                else:
                    fn.call = lambda *a, **k: self._store.get("approved", {}).get(sp, False)
            if name == "balanceOf":
                fn.call = lambda *a, **k: self._store.get("balance", 0)
            return fn
        return make


class FakeContract:
    def __init__(self, store=None):
        self._store = store or {}
        self.functions = FakeFunctions(self._store)


class FakeAcct:
    def __init__(self, address="0xMOCKADDRESS"):
        self.address = address

    def sign_transaction(self, tx):
        return FakeSigned()


class FakeW3:
    def __init__(self, connected=True, block_number=100, base_fee=None):
        self._connected = connected
        self.eth = MagicMock()
        self.to_wei = staticmethod(lambda v, u: int(v * 10 ** (9 if u == "gwei" else 18)))
        self.middleware_onion = MagicMock()
        self.eth.block_number = block_number
        self.eth.get_block = lambda *a, **k: ({"baseFeePerGas": base_fee}
                                              if base_fee is not None else {})
        self.eth.account = MagicMock()
        self.eth.account.from_key = lambda key: FakeAcct()
        self.eth.get_balance = lambda me: 2 * 10 ** 18
        self.eth.get_transaction_count = lambda *a, **k: 0
        self.eth.send_raw_transaction = lambda raw: b"0xhash"
        self.eth.wait_for_transaction_receipt = lambda h, timeout=240: FakeReceipt()
        self.eth.contract = self._contract
        self._contracts = {}

    def _contract(self, address, abi):
        if address not in self._contracts:
            self._contracts[address] = FakeContract()
        return self._contracts[address]

    def is_connected(self):
        return self._connected


def _c(w3, addr):
    from web3 import Web3 as _W
    key = _W.to_checksum_address(addr)
    if key not in w3._contracts:
        w3.eth.contract(key, [])
    return w3._contracts[key]


def _set_allowance(w3, spender, value, usdce=True):
    addr = USDCE if usdce else USDC_NATIVE
    c = _c(w3, addr)
    c._store.setdefault("allowances", {})[spender] = value


def _set_approved(w3, spender, value):
    c = _c(w3, CTF)
    c._store.setdefault("approved", {})[spender] = value


# ── generate_wallet ────────────────────────────────────────────────
def test_generate_wallet(tmp_path):
    kf = str(tmp_path / "wallet.txt")
    res = generate_wallet(kf)
    assert res["keyfile"] == kf
    assert res["address"].startswith("0x")
    assert res["private_key"].startswith("0x")
    import os
    assert os.path.exists(kf)
    txt = open(kf).read()
    assert "address:" in txt and "private_key:" in txt


# ── _resolve_key ───────────────────────────────────────────────────
def test_resolve_key_valid(monkeypatch):
    monkeypatch.delenv("POLYMARKET_PRIVATE_KEY", raising=False)
    assert _resolve_key("0x" + "ab" * 32).startswith("0x")


def test_resolve_key_from_env(monkeypatch):
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "0x" + "cd" * 32)
    assert _resolve_key().startswith("0x")


def test_resolve_key_invalid(monkeypatch):
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "bad")
    with pytest.raises(ValueError):
        _resolve_key()


# ── get_web3 ───────────────────────────────────────────────────────
def test_get_web3_success(monkeypatch):
    import web3
    fw3 = FakeW3(connected=True, block_number=10)
    wc = MagicMock()
    wc.HTTPProvider = MagicMock(return_value="prov")
    wc.return_value = fw3
    monkeypatch.setattr(web3, "Web3", wc)
    monkeypatch.setattr(web3.middleware, "ExtraDataToPOAMiddleware", object)
    assert get_web3() is fw3


def test_get_web3_fallback_then_success(monkeypatch):
    import web3
    good = FakeW3(connected=True, block_number=10)
    wc = MagicMock()
    wc.HTTPProvider = MagicMock(return_value="prov")
    wc.side_effect = [RuntimeError("conn"), RuntimeError("conn"), good]
    monkeypatch.setattr(web3, "Web3", wc)
    monkeypatch.setattr(web3.middleware, "ExtraDataToPOAMiddleware", object)
    assert get_web3() is good


def test_get_web3_all_fail(monkeypatch):
    import web3
    wc = MagicMock()
    wc.HTTPProvider = MagicMock(return_value="prov")
    wc.side_effect = RuntimeError("x")
    monkeypatch.setattr(web3, "Web3", wc)
    monkeypatch.setattr(web3.middleware, "ExtraDataToPOAMiddleware", object)
    with pytest.raises(RuntimeError):
        get_web3()


# ── _eip1559_fees ──────────────────────────────────────────────────
def test_eip1559_fees_with_base():
    w3 = FakeW3(base_fee=100)
    fees = _eip1559_fees(w3)
    assert fees["maxFeePerGas"] > 0 and fees["maxPriorityFeePerGas"] > 0


def test_eip1559_fees_default_base():
    w3 = FakeW3(base_fee=None)
    fees = _eip1559_fees(w3)
    assert fees["maxFeePerGas"] > 0


# ── _send ──────────────────────────────────────────────────────────
def test_send_with_estimate():
    w3 = FakeW3()
    acct = FakeAcct()
    fn = FakeFn("approve", {})
    out = _send(w3, acct, fn)
    assert out["gas_used"] == 120000


def test_send_with_gas():
    w3 = FakeW3()
    acct = FakeAcct()
    fn = FakeFn("approve", {})
    out = _send(w3, acct, fn, gas=50000)
    assert out["gas_used"] == 120000


def test_send_reverted():
    w3 = FakeW3()
    w3.eth.wait_for_transaction_receipt = lambda h, timeout=240: FakeReceipt(status=0)
    acct = FakeAcct()
    fn = FakeFn("approve", {})
    with pytest.raises(RuntimeError):
        _send(w3, acct, fn)


# ── wallet_status ──────────────────────────────────────────────────
def test_wallet_status(monkeypatch):
    w3 = FakeW3()
    c = _c(w3, USDC_NATIVE)
    c._store["balance"] = 50_000_000
    c2 = _c(w3, USDCE)
    c2._store["balance"] = 30_000_000
    for sp in POLYMARKET_SPENDERS:
        _set_allowance(w3, sp, MAX_UINT256 // 2 + 1, usdce=True)
        _set_approved(w3, sp, True)
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    st = wallet_status("0x" + "ab" * 32)
    assert st["address"]
    assert st["all_allowances_set"] is True
    assert st["usdc_native"] == 50.0
    assert st["usdce"] == 30.0


def test_wallet_status_partial(monkeypatch):
    w3 = FakeW3()
    for sp in POLYMARKET_SPENDERS:
        _set_allowance(w3, sp, 0)
        _set_approved(w3, sp, False)
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    st = wallet_status("0x" + "ab" * 32)
    assert st["all_allowances_set"] is False


# ── swap_usdc_to_usdce ─────────────────────────────────────────────
def test_swap_all_balance(monkeypatch):
    w3 = FakeW3()
    c = _c(w3, USDC_NATIVE)
    c._store["balance"] = 1_000_000_000  # 1000 USDC
    _set_allowance(w3, UNISWAP_V3_ROUTER, 0)  # triggers approve
    c2 = _c(w3, USDCE)
    c2._store["balance"] = 0
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    out = swap_usdc_to_usdce("0x" + "ab" * 32)
    assert out["amount_in"] == 1000.0
    assert "approve" in out
    assert "swap" in out


def test_swap_specific_amount(monkeypatch):
    w3 = FakeW3()
    c = _c(w3, USDC_NATIVE)
    c._store["balance"] = 5_000_000_000
    _set_allowance(w3, UNISWAP_V3_ROUTER, 10 ** 12, usdce=False)  # already approved -> skip approve
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    out = swap_usdc_to_usdce("0x" + "ab" * 32, amount_usdc=500.0)
    assert out["amount_in"] == 500.0
    assert "approve" not in out


def test_get_web3_connected_no_block(monkeypatch):
    import web3
    fw3 = FakeW3(connected=True, block_number=0)
    wc = MagicMock()
    wc.HTTPProvider = MagicMock(return_value="prov")
    wc.return_value = fw3
    monkeypatch.setattr(web3, "Web3", wc)
    monkeypatch.setattr(web3.middleware, "ExtraDataToPOAMiddleware", object)
    with pytest.raises(RuntimeError):
        get_web3()


def test_main_dotenv_missing(tmp_path, monkeypatch, capsys):
    import builtins
    real = builtins.__import__
    def fake(name, *a, **k):
        if name == "dotenv":
            raise ImportError("no dotenv")
        return real(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", fake)
    _run_main(monkeypatch, ["status", "--key", "0x" + "ab" * 32],
              wallet_status=lambda k: {"address": "0x", "all_allowances_set": True})


def test_swap_invalid_amount_zero(monkeypatch):
    w3 = FakeW3()
    _c(w3, USDC_NATIVE)._store["balance"] = 0
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    with pytest.raises(ValueError):
        swap_usdc_to_usdce("0x" + "ab" * 32)


def test_swap_invalid_amount_too_big(monkeypatch):
    w3 = FakeW3()
    _c(w3, USDC_NATIVE)._store["balance"] = 1_000_000  # 1 USDC
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    with pytest.raises(ValueError):
        swap_usdc_to_usdce("0x" + "ab" * 32, amount_usdc=500.0)


# ── set_polymarket_allowances ──────────────────────────────────────
def test_set_allowances_all(monkeypatch):
    w3 = FakeW3()
    # usdce allowance small -> approve; ctf not approved -> setApprovalForAll
    for sp in POLYMARKET_SPENDERS:
        _set_allowance(w3, sp, 0)
        _set_approved(w3, sp, False)
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    res = set_polymarket_allowances("0x" + "ab" * 32)
    assert len(res) == len(POLYMARKET_SPENDERS)
    for sp, r in res.items():
        assert "usdce_approve" in r
        assert "ctf_approve" in r


def test_set_allowances_already_approved(monkeypatch):
    w3 = FakeW3()
    for sp in POLYMARKET_SPENDERS:
        _set_allowance(w3, sp, MAX_UINT256)  # already approved -> skip
        _set_approved(w3, sp, True)
    monkeypatch.setattr(pv, "get_web3", lambda *a, **k: w3)
    res = set_polymarket_allowances("0x" + "ab" * 32)
    for sp, r in res.items():
        assert r["usdce_approve"] == "already approved"
        assert r["ctf_approve"] == "already approved"


# ── refresh_clob_cache ─────────────────────────────────────────────
def test_refresh_clob_cache_true(monkeypatch):
    import event_markets.polymarket_executor as pme
    fake = MagicMock()
    fake.refresh_balance_allowance.return_value = True
    monkeypatch.setattr(pme, "PolymarketExecutionClient", lambda *a, **k: fake)
    assert refresh_clob_cache("0x" + "ab" * 32) is True


def test_refresh_clob_cache_false(monkeypatch):
    import event_markets.polymarket_executor as pme
    fake = MagicMock()
    fake.refresh_balance_allowance.return_value = False
    monkeypatch.setattr(pme, "PolymarketExecutionClient", lambda *a, **k: fake)
    assert refresh_clob_cache("0x" + "ab" * 32) is False


# ── _main (CLI) ────────────────────────────────────────────────────
def _run_main(monkeypatch, argv, **patches):
    monkeypatch.setattr("sys.argv", ["provisioning"] + argv)
    for name, val in patches.items():
        monkeypatch.setattr(pv, name, val)
    _main()


def test_main_generate(tmp_path, monkeypatch, capsys):
    kf = str(tmp_path / "w.txt")
    _run_main(monkeypatch, ["generate", "--keyfile", kf],
              generate_wallet=generate_wallet)
    assert "address" in capsys.readouterr().out


def test_main_status(tmp_path, monkeypatch, capsys):
    _run_main(monkeypatch, ["status", "--key", "0x" + "ab" * 32],
              wallet_status=lambda k: {"address": "0x", "all_allowances_set": True})


def test_main_swap_without_confirm(tmp_path, monkeypatch, capsys):
    _run_main(monkeypatch, ["swap", "--key", "0x" + "ab" * 32])
    assert "REAL" in capsys.readouterr().out


def test_main_swap_confirm(tmp_path, monkeypatch, capsys):
    _run_main(monkeypatch, ["swap", "--key", "0x" + "ab" * 32, "--yes"],
              swap_usdc_to_usdce=lambda k, a: {"amount_in": 1.0})


def test_main_allowances_without_confirm(tmp_path, monkeypatch, capsys):
    _run_main(monkeypatch, ["allowances", "--key", "0x" + "ab" * 32])
    assert "REAL" in capsys.readouterr().out


def test_main_allowances_confirm(tmp_path, monkeypatch, capsys):
    _run_main(monkeypatch, ["allowances", "--key", "0x" + "ab" * 32, "--yes"],
              set_polymarket_allowances=lambda k: {"sp": {}})


def test_main_refresh(tmp_path, monkeypatch, capsys):
    _run_main(monkeypatch, ["refresh", "--key", "0x" + "ab" * 32],
              refresh_clob_cache=lambda k: True)
