"""Regression tests for full-registry live-tick evaluation.

The Rust fast path accelerates an explicit subset of strategies. The live tick
must also evaluate Python-only strategies so the remainder of the registry is
not silently dead.
"""

from strategy_engine import _RUST_STRATEGIES, run_strategies
from trading_system.strategies.registry.registry import load_strategies


EXPECTED_REGISTRY_COUNT = 200


def _make_ohlcv(n: int = 50, seed: int = 7):
    import numpy as np

    rng = np.random.RandomState(seed)
    closes = list(np.linspace(100.0, 130.0, n) + rng.randn(n))
    volumes = [1000.0] * n
    highs = [close + 1.0 for close in closes]
    lows = [close - 1.0 for close in closes]
    return closes, volumes, highs, lows


def test_registry_has_full_200_strategies():
    strategies = load_strategies()
    assert len(strategies) == EXPECTED_REGISTRY_COUNT, (
        f"registry changed: expected {EXPECTED_REGISTRY_COUNT} strategies, "
        f"got {len(strategies)}"
    )


def test_rust_fast_path_is_nonempty_proper_subset():
    """Avoid stale hard-coded counts while preserving the architecture guard."""
    assert _RUST_STRATEGIES, "Rust fast path unexpectedly contains no strategies"
    assert len(_RUST_STRATEGIES) < EXPECTED_REGISTRY_COUNT, (
        "Rust fast path unexpectedly covers the full registry; the Python-only "
        "path regression test would no longer exercise a distinct path"
    )
    assert all(
        isinstance(strategy_id, str) and strategy_id.strip()
        for strategy_id in _RUST_STRATEGIES
    )


def test_live_tick_evaluates_python_only_strategies():
    closes, volumes, highs, lows = _make_ohlcv()
    price = closes[-1]

    evaluated = set()
    rust_evaluated = set()
    python_only_evaluated = set()

    for asset_class in ("safe", "growth", "speculative"):
        signals = run_strategies(
            currency="BTC",
            asset_class=asset_class,
            closes=closes,
            volumes=volumes,
            current_price=price,
            highs=highs,
            lows=lows,
        )
        for signal in signals:
            evaluated.add(signal.strategy)
            if signal.strategy in _RUST_STRATEGIES:
                rust_evaluated.add(signal.strategy)
            else:
                python_only_evaluated.add(signal.strategy)

    assert rust_evaluated, "no Rust fast-path strategy ids were evaluated"
    assert python_only_evaluated, (
        "No Python-only strategy id was evaluated; the live signal path may "
        "have regressed to Rust-only evaluation"
    )
    assert not (rust_evaluated & python_only_evaluated)
    assert rust_evaluated | python_only_evaluated == evaluated
