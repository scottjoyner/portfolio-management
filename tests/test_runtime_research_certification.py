from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from scripts.alpha_validation import stable_hash
from scripts.runtime_research_certification import (
    bind_certification_to_runtime,
    build_runtime_research_certification,
    load_active_research_certification,
    run_certified_current_signal,
    verify_static_certification,
)


def _fixture():
    config = {"period": 14, "oversold": 30.0, "overbought": 70.0}
    challenger = {"id": "challenger-rsi", "parameters": config}
    alpha = {
        "candidate_config_hash": stable_hash(config),
        "evidence_hash": "a" * 64,
    }
    metrics = {
        "trade_count": 12,
        "total_return_pct": 8.5,
        "annualized_return_pct": 14.0,
        "mean_return_pct": 0.7,
        "sharpe": 1.4,
        "sortino": 1.9,
        "profit_factor": 1.6,
        "max_drawdown_pct": 4.0,
        "calmar": 3.5,
        "worst_trade_pct": -1.2,
        "expected_shortfall_pct": -1.0,
        "win_rate": 0.6666666667,
    }
    terminal = {
        "candidate_id": challenger["id"],
        "candidate_source_sha": "b" * 40,
        "candidate_config_hash": stable_hash(config),
        "candidate_config": config,
        "alpha_validation_evidence_hash": alpha["evidence_hash"],
        "evidence_hash": "c" * 64,
        "experiment_hash": "d" * 64,
        "selection_hash": "e" * 64,
        "terminal_result_hash": "f" * 64,
        "strategy_name": "rsi_revert",
        "strategy_config": config,
        "terminal_dataset": {
            "kind": "coinbase_candles",
            "symbol": "BTC-USD",
            "granularity": 3600,
        },
        "metrics": metrics,
    }
    return challenger, alpha, terminal


def test_certification_binds_exact_candidate_config_and_terminal_scope(tmp_path):
    challenger, alpha, terminal = _fixture()
    certification = build_runtime_research_certification(challenger, alpha, terminal)
    assert certification is not None
    valid, reasons = verify_static_certification(certification)
    assert valid is True, reasons
    assert certification["candidate_id"] == challenger["id"]
    assert certification["strategy_name"] == "rsi_revert"
    assert certification["symbol"] == "BTC-USD"
    assert certification["granularity"] == "3600"
    assert certification["candidate_config"] == challenger["parameters"]
    assert certification["candidate_config_hash"] == stable_hash(challenger["parameters"])
    assert certification["terminal_metrics_hash"] == stable_hash(terminal["metrics"])

    active_path = tmp_path / "agent_runtime_config.json"
    active_path.write_text(json.dumps({
        "active_challenger_id": challenger["id"],
        "parameters": challenger["parameters"],
        "alpha_validation_evidence_hash": alpha["evidence_hash"],
        "terminal_holdout_evidence_hash": terminal["evidence_hash"],
        "research_certification": certification,
    }))
    loaded = load_active_research_certification(active_path)
    assert loaded == certification

    bound = bind_certification_to_runtime(
        loaded, product_id="BTC-USD", granularity="ONE_HOUR"
    )
    assert bound is not None
    assert bound["runtime_binding"]["strategy_config_hash"] == certification["candidate_config_hash"]
    assert len(bound["runtime_binding"]["runtime_binding_hash"]) == 64

    assert bind_certification_to_runtime(
        loaded, product_id="ETH-USD", granularity="ONE_HOUR"
    ) is None
    assert bind_certification_to_runtime(
        loaded, product_id="BTC-USD", granularity="FIFTEEN_MINUTE"
    ) is None


def test_tampered_static_certification_is_rejected():
    challenger, alpha, terminal = _fixture()
    certification = build_runtime_research_certification(challenger, alpha, terminal)
    assert certification is not None
    certification["symbol"] = "ETH-USD"
    valid, reasons = verify_static_certification(certification)
    assert valid is False
    assert "research_certification_hash_mismatch" in reasons


def test_certified_signal_uses_exact_configured_rust_binding(monkeypatch):
    challenger, alpha, terminal = _fixture()
    certification = build_runtime_research_certification(challenger, alpha, terminal)
    bound = bind_certification_to_runtime(
        certification, product_id="BTC-USD", granularity=3600
    )
    assert bound is not None

    observed = {}

    def configured_runner(closes, opens, volumes, highs, lows, *, period, oversold, overbought):
        observed.update({
            "closes": closes,
            "opens": opens,
            "period": period,
            "oversold": oversold,
            "overbought": overbought,
        })
        return ("BUY", 0.73, "configured RSI")

    monkeypatch.setitem(
        sys.modules,
        "rust_core",
        SimpleNamespace(run_rsi_revert_opens_configured_py=configured_runner),
    )
    signal = run_certified_current_signal(
        bound,
        closes=[100.0 + index for index in range(40)],
        volumes=[10.0] * 40,
        highs=[101.0 + index for index in range(40)],
        lows=[99.0 + index for index in range(40)],
    )
    assert signal is not None
    assert signal["action"] == "BUY"
    assert signal["strategy"] == "rsi_revert"
    assert signal["research_certification"]["certification_hash"] == certification["certification_hash"]
    assert observed["opens"] == observed["closes"]
    assert observed["period"] == 14
    assert observed["oversold"] == 30.0
    assert observed["overbought"] == 70.0
