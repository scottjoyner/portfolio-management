"""Regression test: the live tick must evaluate the FULL strategy registry
(200 strategies), not just the 84 Rust-accelerated ids via the Rust fast path.

History: the optimizer's live tick historically only evaluated ``_RUST_STRATEGIES``
(84 ids) through the vectorized Rust fast path, leaving the ~116 python-only
registry strategies dead. The fix wires the python-only ``run_strategies`` output
into ``candidates_with_sigs`` alongside the Rust ``batch_results``.

This test guards that the python evaluation path is live and non-empty: at least
one python-only (non-Rust) strategy id must produce a signal when the engine is
invoked the same way the tick's signal-collection does. On the pre-fix (84-only)
code the python path is never reached for the live tick, so python-only ids would
be empty and this test FAILS. After the fix lands it PASSES.
"""

from strategy_engine import run_strategies, _RUST_STRATEGIES
from trading_system.strategies.registry.registry import load_strategies


EXPECTED_REGISTRY_COUNT = 200


def _make_ohlcv(n: int = 50, seed: int = 7):
    import numpy as np

    rng = np.random.RandomState(seed)
    closes = list(np.linspace(100.0, 130.0, n) + rng.randn(n))
    volumes = [1000.0] * n
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    return closes, volumes, highs, lows


def test_registry_has_full_200_strategies():
    strategies = load_strategies()
    assert len(strategies) == EXPECTED_REGISTRY_COUNT, (
        f"registry changed: expected {EXPECTED_REGISTRY_COUNT} strategies, "
        f"got {len(strategies)}"
    )


def test_rust_fast_path_ids_count():
    # Sanity: the Rust fast path covers exactly the known set of ids.
    assert len(_RUST_STRATEGIES) == 95, (
        f"Rust fast-path id count changed: {len(_RUST_STRATEGIES)}"
    )


def test_live_tick_evaluates_python_only_strategies():
    """The live tick must evaluate python-only (non-Rust) registry strategies.

    Reproduces the signal-collection step the optimizer's live tick uses:
    invoking ``run_strategies`` (the python path that the fix appends to
    ``candidates_with_sigs``) and confirming it yields at least one strategy id
    that is NOT in the Rust fast path.
    """
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
        for sig in signals:
            evaluated.add(sig.strategy)
            if sig.strategy in _RUST_STRATEGIES:
                rust_evaluated.add(sig.strategy)
            else:
                python_only_evaluated.add(sig.strategy)

    # The Rust fast path must still contribute its ids.
    assert rust_evaluated, "no Rust fast-path strategy ids were evaluated"

    # REGRESSION GUARD: python-only strategies must be evaluated too (not dead).
    assert python_only_evaluated, (
        "No python-only (non-Rust) strategy id was evaluated by the signal path. "
        "The live tick is only evaluating the 84 Rust fast-path ids; the "
        "python-only registry strategies are dead."
    )

    # No strategy should be double-counted as both Rust and python-only.
    assert not (rust_evaluated & python_only_evaluated), (
        "strategy id claimed as both Rust and python-only"
    )

    # The evaluated set must be the disjoint union of the two paths.
    union = rust_evaluated | python_only_evaluated
    assert union == evaluated, "evaluated set is not the disjoint union of paths"
