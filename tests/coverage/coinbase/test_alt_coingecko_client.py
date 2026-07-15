"""Coverage tests for coinbase/src/alt/coingecko_client.py"""
from __future__ import annotations

import pathlib

import pytest

import coinbase.src.alt.coingecko_client as cg


class FakeResp:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"ok": True}
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


def make_client(resp):
    class C:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, path, params=None):
            return resp
    return C


def make_raising_client(exc):
    class C:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, path, params=None):
            raise exc
    return C


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setattr(cg, "CG_MAX_RETRIES", 1)
    monkeypatch.setattr(cg, "CG_BACKOFF_BASE", 0.0)
    monkeypatch.setattr(cg, "CG_BACKOFF_CAP", 0.0)
    monkeypatch.setattr(cg.time, "sleep", lambda *a, **k: None)


def test_success(fast, monkeypatch):
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(FakeResp(200, {"a": 1}))})())
    assert cg._get_json("/coins/markets", {}, prefer_pro=True) == {"a": 1}


def test_prefer_pro_false(fast, monkeypatch):
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(FakeResp(200, {"b": 2}))})())
    assert cg._get_json("/coins/markets", {}, prefer_pro=False) == {"b": 2}


def test_429_with_retry_after(fast, monkeypatch):
    slept = {"n": 0}
    monkeypatch.setattr(cg.time, "sleep", lambda *a, **k: slept.__setitem__("n", slept["n"] + 1))
    resp = FakeResp(429, headers={"Retry-After": "1"})
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(resp)})())
    with pytest.raises(RuntimeError):
        cg._get_json("/x", {}, prefer_pro=False)
    assert slept["n"] >= 1


def test_429_without_retry_after(fast, monkeypatch):
    resp = FakeResp(429)
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(resp)})())
    with pytest.raises(RuntimeError):
        cg._get_json("/x", {}, prefer_pro=False)


def test_5xx(fast, monkeypatch):
    resp = FakeResp(503)
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(resp)})())
    with pytest.raises(RuntimeError):
        cg._get_json("/x", {}, prefer_pro=False)


def test_other_client_error(fast, monkeypatch):
    resp = FakeResp(400)
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(resp)})())
    with pytest.raises(RuntimeError):
        cg._get_json("/x", {}, prefer_pro=False)


def test_network_error(fast, monkeypatch):
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_raising_client(RuntimeError("net"))})())
    with pytest.raises(RuntimeError):
        cg._get_json("/x", {}, prefer_pro=False)


def test_pro_then_public_fallback(fast, monkeypatch):
    # PRO fails, PUBLIC (second base) succeeds
    calls = {"n": 0}

    class C:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def get(self, path, params=None):
            calls["n"] += 1
            if calls["n"] == 1:
                return FakeResp(503)
            return FakeResp(200, {"fallback": True})
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": C})())
    assert cg._get_json("/x", {}, prefer_pro=True) == {"fallback": True}


def test_cache_json(tmp_path, monkeypatch):
    monkeypatch.setattr(cg, "CACHE_DIR", tmp_path)
    p = cg.cache_json("mymarkets", {"data": [1]})
    assert p.exists()
    assert p.read_text().find("data") >= 0


def test_coins_markets(fast, monkeypatch):
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(FakeResp(200, [{"id": "bitcoin"}]))})())
    out = cg.coins_markets(vs="usd")
    assert out["data"] == [{"id": "bitcoin"}]


def test_coin_meta(fast, monkeypatch):
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(FakeResp(200, {"id": "bitcoin"}))})())
    assert cg.coin_meta("bitcoin") == {"id": "bitcoin"}


def test_pro_api_key_header(monkeypatch):
    monkeypatch.setattr(cg, "CG_API_KEY", "secret")
    monkeypatch.setattr(cg, "BASE", "https://pro-api.coingecko.com/api/v3")
    c = cg._client(cg.BASE)
    assert c.headers["x-cg-pro-api-key"] == "secret"


def test_base_equals_public_only(fast, monkeypatch):
    monkeypatch.setattr(cg, "BASE", cg.PUBLIC_BASE)
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(FakeResp(200, {"z": 9}))})())
    # when BASE == PUBLIC_BASE, PUBLIC is not appended a second time
    assert cg._get_json("/x", {}, prefer_pro=True) == {"z": 9}


def test_429_invalid_retry_after(fast, monkeypatch):
    resp = FakeResp(429, headers={"Retry-After": "not-a-number"})
    monkeypatch.setattr(cg, "httpx", type("X", (), {"Client": make_client(resp)})())
    with pytest.raises(RuntimeError):
        cg._get_json("/x", {}, prefer_pro=False)
