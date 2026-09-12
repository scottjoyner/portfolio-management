from __future__ import annotations

import copy

import scripts.certified_strategy_signal as certified
from scripts.strategy_signal_scanner import CandleSeries


CERT = {
    "certification_hash": "c" * 64,
    "symbol": "BTC-USD",
    "granularity": "3600",
    "strategy_name": "rsi_revert",
    "candidate_config": {"period": 14, "oversold": 30.0, "overbought": 70.0},
    "terminal_metrics": {
        "win_rate": 0.66,
        "trade_count": 20,
        "total_return_pct": 8.0,
        "sharpe": 1.3,
        "profit_factor": 1.5,
        "max_drawdown_pct": 5.0,
    },
}


class Registry:
    def __init__(self, valid: bool = True):
        self.valid = valid

    def verify_active_runtime_config(self, *, reverify_source: bool = True):
        if self.valid:
            return True, [], copy.deepcopy(CERT)
        return False, ["active_runtime_certification_missing"], None


def _candles(source: str = "live_cli") -> CandleSeries:
    rows = []
    for index in range(60):
        price = 100.0 + index
        rows.append({
            "open": price - 0.5,
            "high": price + 1.0,
            "low": price - 1.0,
            "close": price,
            "volume": 10.0,
        })
    return CandleSeries(product_id="BTC-USD", source=source, candles=rows)


def test_invalid_active_certification_fails_before_market_fetch(monkeypatch):
    monkeypatch.setattr(
        certified,
        "_fetch_live_candles",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )
    result = certified.scan_active_certified_strategy(registry=Registry(valid=False))
    assert result["certified"] is False
    assert result["signal"] is None
    assert "active_runtime_certification_missing" in result["reasons"]


def test_certified_lane_runs_only_exact_scope_without_discovery_consensus(monkeypatch):
    observed = {}

    def bind(certification, *, product_id, granularity):
        observed["bind"] = (product_id, granularity)
        return {
            **copy.deepcopy(certification),
            "runtime_binding": {"method": "configured_rust_signal_v1"},
        }

    def fetch(product_id, granularity, days_back, **kwargs):
        observed["fetch"] = (product_id, granularity, days_back)
        return _candles()

    def run(binding, *, closes, volumes, highs, lows):
        observed["run"] = binding["candidate_config"]
        return {
            "action": "BUY",
            "confidence": 0.73,
            "reason": "configured RSI",
            "strategy": "rsi_revert",
            "research_certification": binding,
        }

    monkeypatch.setattr(certified, "bind_certification_to_runtime", bind)
    monkeypatch.setattr(certified, "_fetch_live_candles", fetch)
    monkeypatch.setattr(certified, "run_certified_current_signal", run)

    result = certified.scan_active_certified_strategy(registry=Registry())
    assert result["certified"] is True
    assert result["signal"]["strategy"] == "rsi_revert"
    assert result["signal"]["validation_scope"] == "tournament_terminal_promoted_exact_runtime"
    assert result["signal"]["weighted_confidence"] == 0.73
    assert result["signal"]["win_rate"] == 0.66
    assert observed["bind"] == ("BTC-USD", "ONE_HOUR")
    assert observed["fetch"][0:2] == ("BTC-USD", "ONE_HOUR")
    assert observed["run"] == CERT["candidate_config"]


def test_certified_lane_rejects_non_coinbase_fallback_source(monkeypatch):
    monkeypatch.setattr(
        certified,
        "bind_certification_to_runtime",
        lambda certification, **kwargs: copy.deepcopy(certification),
    )
    monkeypatch.setattr(
        certified,
        "_fetch_live_candles",
        lambda *args, **kwargs: _candles(source="coingecko"),
    )
    monkeypatch.setattr(
        certified,
        "run_certified_current_signal",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    result = certified.scan_active_certified_strategy(registry=Registry())
    assert result["certified"] is False
    assert "certified_runtime_market_source_unavailable" in result["reasons"]
