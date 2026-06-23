from __future__ import annotations

from coinbase.src.protocols import Bar
from coinbase.src.strategies.sidecar_rsi import SidecarRSICrossStrategy


def test_sidecar_rsi_strategy_emits_setup():
    closes = [100, 98, 96, 94, 92, 90, 89, 88, 90, 92, 94, 96, 98, 100, 102, 104]
    bars = [Bar(timestamp=i, open=c, high=c * 1.01, low=c * 0.99, close=c, volume=1000) for i, c in enumerate(closes)]
    strategy = SidecarRSICrossStrategy(rsi_period=3, buy_rsi_cross=30, min_bars=5)
    setup = None
    for idx in range(5, len(bars)):
        setup = strategy.on_bar(bars[idx], bars[:idx])
        if setup:
            break
    assert setup is not None
    assert setup.strategy_name == "sidecar_rsi_cross"
    assert setup.target_price > setup.entry_price
    assert setup.stop_price < setup.entry_price
