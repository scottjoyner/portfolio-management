from __future__ import annotations

from typing import Iterable

from .models import GraphAssetSignal
from .neo4j_graph import CryptoGraphStore


def graph_weight_overlays(signals: Iterable[GraphAssetSignal], *, max_boost: float = 0.35) -> dict[str, float]:
    """Return multiplicative weight overlays from graph scores.

    A score of 0.5 is neutral. Higher scores get up to `1 + max_boost`; lower
    scores are clipped down toward `1 - max_boost`.
    """
    overlays: dict[str, float] = {}
    for signal in signals:
        delta = (signal.graph_score - 0.5) * 2.0 * max_boost
        overlays[signal.product_id] = max(0.0, 1.0 + delta)
    return overlays


def fetch_graph_weight_overlays(products: list[str], *, max_boost: float = 0.35) -> dict[str, float]:
    store = CryptoGraphStore()
    try:
        signals = [store.asset_signal(product_id) for product_id in products]
    finally:
        store.close()
    return graph_weight_overlays(signals, max_boost=max_boost)


def apply_graph_overlay(weights: dict[str, float], overlays: dict[str, float]) -> dict[str, float]:
    adjusted = {product: max(0.0, weight * overlays.get(product, 1.0)) for product, weight in weights.items()}
    total = sum(adjusted.values()) or 1.0
    return {product: weight / total for product, weight in adjusted.items()}
