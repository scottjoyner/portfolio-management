"""Coverage tests for coinbase/src/strategies/onchain_flows.py"""
from __future__ import annotations

import json

import pytest

from coinbase.src.strategies import onchain_flows
from coinbase.src.strategies.onchain_flows import OnChainFlowStrategy, PRODUCT_TO_COINGECKO


class FakeResp:
    def __init__(self, data: bytes):
        self._data = data

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._data


def fake_urlopen_factory(body):
    def _fake(req, timeout=10):
        return FakeResp(json.dumps(body).encode())
    return _fake


def make_body(prices, volumes):
    return {
        "prices": [[i, p] for i, p in enumerate(prices)],
        "total_volumes": [[i, v] for i, v in enumerate(volumes)],
    }


@pytest.fixture
def strat():
    return OnChainFlowStrategy()


def test_unknown_product_skipped(strat, monkeypatch):
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body([1] * 48, [1] * 48)))
    # XYZ-USD not in PRODUCT_TO_COINGECKO -> skipped
    assert strat.get_signals(["XYZ-USD"]) == []


def test_buy_signal(strat, monkeypatch):
    prices = [100.0] * 47 + [102.0]
    volumes = [100.0] * 47 + [500.0]
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body(prices, volumes)))
    sigs = strat.get_signals(["BTC-USD"])
    assert len(sigs) == 1
    assert sigs[0]["action"] == "BUY"
    assert sigs[0]["confidence"] >= 0.30


def test_sell_signal(strat, monkeypatch):
    prices = [100.0] * 47 + [90.0]
    volumes = [100.0] * 47 + [500.0]
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body(prices, volumes)))
    sigs = strat.get_signals(["ETH-USD"])
    assert sigs[0]["action"] == "SELL"


def test_hold_with_spike(strat, monkeypatch):
    prices = [100.0] * 48
    volumes = [100.0] * 47 + [500.0]
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body(prices, volumes)))
    # spike but price flat -> HOLD (confidence 0, so get_signals filters it out)
    res = strat._fetch_flow_proxy("solana")
    assert res["action"] == "HOLD"
    assert strat.get_signals(["SOL-USD"]) == []


def test_low_volume_buy(strat, monkeypatch):
    prices = [100.0] * 47 + [90.0]
    volumes = [100.0] * 47 + [20.0]
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body(prices, volumes)))
    sigs = strat.get_signals(["ADA-USD"])
    assert sigs[0]["action"] == "BUY"
    assert sigs[0]["confidence"] == 0.30


def test_low_confidence_filtered(strat, monkeypatch):
    prices = [100.0] * 47 + [100.5]
    volumes = [100.0] * 47 + [120.0]
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body(prices, volumes)))
    # spike below threshold, price change small, confidence 0 -> filtered out
    assert strat.get_signals(["BTC-USD"]) == []


def test_cache_hit(strat, monkeypatch):
    prices = [100.0] * 47 + [102.0]
    volumes = [100.0] * 47 + [500.0]
    calls = {"n": 0}

    def fake(req, timeout=10):
        calls["n"] += 1
        return FakeResp(json.dumps(make_body(prices, volumes)).encode())
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen", fake)
    sigs1 = strat.get_signals(["BTC-USD"])
    sigs2 = strat.get_signals(["BTC-USD"])
    assert calls["n"] == 1  # second call served from cache
    assert sigs1 and sigs2


def test_fetch_failure(strat, monkeypatch):
    def boom(req, timeout=10):
        raise OSError("network down")
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen", boom)
    # fetch fails -> returns None -> no signals
    assert strat.get_signals(["BTC-USD"]) == []


def test_breaker_open_uses_cache(strat, monkeypatch):
    # open the breaker, no cache -> None returned gracefully
    strat._breaker.failure_threshold = 1
    strat._breaker.on_failure(RuntimeError("x"))
    assert strat._fetch_flow_proxy("bitcoin") is None


def test_insufficient_data(strat, monkeypatch):
    # fewer than 12 prices -> returns None
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body([1.0] * 5, [1.0] * 5)))
    assert strat._fetch_flow_proxy("bitcoin") is None


def test_empty_prices_or_volumes(strat, monkeypatch):
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory({"prices": [], "total_volumes": []}))
    assert strat._fetch_flow_proxy("bitcoin") is None


def test_invalidate_cache(strat, monkeypatch):
    prices = [100.0] * 47 + [102.0]
    volumes = [100.0] * 47 + [500.0]
    monkeypatch.setattr(onchain_flows.urllib.request, "urlopen",
                        fake_urlopen_factory(make_body(prices, volumes)))
    strat.get_signals(["BTC-USD"])
    assert "bitcoin" in strat._cache
    strat.invalidate_cache()
    assert strat._cache == {}


def test_mapping_complete():
    # at least the core pairs are mapped
    for pid in ("BTC-USD", "ETH-USD", "SOL-USD"):
        assert pid in PRODUCT_TO_COINGECKO
