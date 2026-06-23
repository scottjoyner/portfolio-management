from __future__ import annotations

from typing import Any

from ..strategies.graph_signal import GraphSignalStrategy


def register_graph_strategy(scanner: Any, *, min_graph_score: float = 0.45) -> GraphSignalStrategy:
    """Register the graph strategy on an OpportunityScanner-like object."""
    strategy = GraphSignalStrategy(min_graph_score=min_graph_score)
    scanner.register(strategy)
    return strategy


def build_graph_strategy(*, min_graph_score: float = 0.45) -> GraphSignalStrategy:
    return GraphSignalStrategy(min_graph_score=min_graph_score)
