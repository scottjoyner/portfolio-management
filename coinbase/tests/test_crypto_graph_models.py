from __future__ import annotations

from coinbase.src.graph.coingecko_ingest import assets_and_tokens_from_coin_meta, assets_from_market_rows
from coinbase.src.graph.models import GraphAssetSignal
from coinbase.src.graph.portfolio_overlay import apply_graph_overlay, graph_weight_overlays


def test_assets_from_market_rows_marks_coinbase_availability():
    rows = [{"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1, "market_cap": 1000}]
    assets = assets_from_market_rows(rows, coinbase_symbols={"BTC"})
    assert assets[0].cg_id == "bitcoin"
    assert assets[0].product_id == "BTC-USD"
    assert assets[0].available_on_coinbase is True


def test_assets_and_tokens_from_coin_meta_builds_contracts():
    meta = {
        "chainlink": {
            "id": "chainlink",
            "symbol": "link",
            "name": "Chainlink",
            "categories": ["Oracle"],
            "platforms": {"ethereum": "0xabc"},
        }
    }
    assets, tokens = assets_and_tokens_from_coin_meta(meta, coinbase_symbols={"LINK"})
    assert assets[0].symbol == "LINK"
    assert assets[0].categories == ["Oracle"]
    assert tokens[0].key == "chainlink:ethereum:0xabc"


def test_graph_weight_overlay_normalizes_weights():
    signals = [
        GraphAssetSignal(product_id="BTC-USD", symbol="BTC", graph_score=0.9),
        GraphAssetSignal(product_id="ETH-USD", symbol="ETH", graph_score=0.3),
    ]
    overlays = graph_weight_overlays(signals, max_boost=0.2)
    weights = apply_graph_overlay({"BTC-USD": 0.5, "ETH-USD": 0.5}, overlays)
    assert round(sum(weights.values()), 10) == 1.0
    assert weights["BTC-USD"] > weights["ETH-USD"]
