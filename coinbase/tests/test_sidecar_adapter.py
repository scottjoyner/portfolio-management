from __future__ import annotations

from coinbase.src.sidecar_adapter import research_record_from_manifest


def test_research_record_from_manifest():
    manifest = {
        "config": {"ticker": "BTC-USD", "strategy_name": "sidecar_rsi_cross"},
        "summary": {
            "ticker": "BTC-USD",
            "total_return_pct": 12.5,
            "max_drawdown_pct": -5.0,
            "sharpe": 1.2,
            "profit_factor": 1.8,
            "win_rate_pct": 60.0,
            "num_trades": 30,
        },
    }
    record = research_record_from_manifest(manifest, manifest_path="manifest.json")
    assert record.product_id == "BTC-USD"
    assert record.strategy_name == "sidecar_rsi_cross"
    assert record.is_coinbase_product
    assert record.research_score > 0
