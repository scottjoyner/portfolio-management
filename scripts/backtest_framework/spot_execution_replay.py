"""Canonical spot-long-only replay for executable lifecycle certification.

This module is intentionally separate from ``canonical_replay.py``. Existing
research evidence keeps its historical long/short lifecycle and source hash;
this file defines a new execution-facing lifecycle that matches a cash/spot
venue where uncovered shorts are forbidden.

Lifecycle v1
------------
* BUY while flat opens one long position.
* BUY while long is a no-op (no pyramiding).
* SELL while flat is a no-op (no synthetic short).
* SELL while long closes the position.
* ``max_hold_bars`` closes on the actual expiry bar even if the strategy emits
  HOLD/no signal on that bar.
* A max-hold close never re-enters on the same bar.
* Historical evidence closes an open position at the observation-window end.

Signal evaluation still comes from the exact compiled Rust strategy evaluator.
This module has no broker or order-routing authority.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any, Sequence

from scripts.alpha_validation import generate_walk_forward_splits, stable_hash
from scripts.backtest_framework.canonical_replay import (
    DATASET_KIND,
    _require_rust_core,
    load_canonical_snapshot,
    normalize_candle_rows,
    normalize_strategy_config,
    snapshot_from_rows,
)

SPOT_EXECUTION_SCHEMA_VERSION = 1
SPOT_EXECUTION_ATTESTATION_TYPE = "canonical_spot_long_only_rust_replay_v1"
SPOT_EXECUTION_METHOD = "spot_long_only_opposite_signal_or_max_hold_v1"
SPOT_POSITION_MODE = "spot_long_only"
RUNNER_ID = "scripts.backtest_framework.spot_execution_replay"


def _runner_source_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _finite(value: Any, *, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _signal_for_bar(
    rust_core: Any,
    strategy_name: str,
    strategy_config: dict[str, Any],
    closes: list[float],
    opens: list[float],
    volumes: list[float],
    highs: list[float],
    lows: list[float],
    index: int,
):
    if strategy_config:
        return rust_core.run_rsi_revert_opens_configured_py(
            closes[: index + 1],
            opens[: index + 1],
            volumes[: index + 1],
            highs[: index + 1],
            lows[: index + 1],
            period=strategy_config["period"],
            oversold=strategy_config["oversold"],
            overbought=strategy_config["overbought"],
        )
    return rust_core.run_strategy_opens_py(
        strategy_name,
        closes[: index + 1],
        opens[: index + 1],
        volumes[: index + 1],
        highs[: index + 1],
        lows[: index + 1],
    )


def replay_spot_long_only_rust(
    strategy_name: str,
    rows: Sequence[Sequence[Any]],
    *,
    warmup: int = 30,
    fee_bps: float = 0.0,
    max_hold_bars: int = 0,
    strategy_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay a strategy under executable cash/spot long-only semantics.

    Returns both decimal trade returns and a deterministic event trace. The
    trace makes lifecycle parity testable rather than inferring it from summary
    metrics.
    """

    if not strategy_name:
        raise ValueError("strategy_name is required")
    config = normalize_strategy_config(strategy_name, strategy_config)
    normalized = normalize_candle_rows(rows)
    warmup = int(warmup)
    max_hold_bars = int(max_hold_bars)
    fee_bps = _finite(fee_bps, name="fee_bps")
    if warmup < 0 or max_hold_bars < 0 or fee_bps < 0.0:
        raise ValueError("warmup, max_hold_bars and fee_bps must be non-negative")
    if len(normalized) < warmup + 10:
        return {"returns": [], "events": []}

    rust_core = _require_rust_core()
    closes = [row[4] for row in normalized]
    opens = list(closes)
    volumes = [row[5] for row in normalized]
    highs = [row[2] for row in normalized]
    lows = [row[3] for row in normalized]

    position: dict[str, Any] | None = None
    returns: list[float] = []
    events: list[dict[str, Any]] = []

    def close_long(index: int, reason: str) -> None:
        nonlocal position
        if position is None:
            return
        entry_price = float(position["entry_price"])
        exit_price = closes[index]
        gross = (exit_price - entry_price) / entry_price
        net = gross - (fee_bps / 10000.0) * 2.0
        if not math.isfinite(net):
            raise ValueError("spot replay produced a non-finite trade return")
        returns.append(net)
        events.append({
            "event": "CLOSE_LONG",
            "bar": index,
            "price": exit_price,
            "reason": reason,
            "entry_bar": int(position["entry_bar"]),
            "return": net,
        })
        position = None

    for index in range(warmup, len(normalized)):
        # Time-based risk must not depend on the strategy emitting another
        # signal. Close first and skip this bar to prohibit same-bar reversal.
        if position is not None and max_hold_bars > 0:
            held = index - int(position["entry_bar"])
            if held >= max_hold_bars:
                close_long(index, "max_hold")
                continue

        signal = _signal_for_bar(
            rust_core,
            strategy_name,
            config,
            closes,
            opens,
            volumes,
            highs,
            lows,
            index,
        )
        if signal is None:
            continue
        action = signal[0]
        if action == "HOLD":
            continue
        if action not in {"BUY", "SELL"}:
            raise ValueError(f"spot replay received unsupported action {action!r}")

        if position is None:
            if action == "BUY":
                position = {"entry_bar": index, "entry_price": closes[index]}
                events.append({
                    "event": "OPEN_LONG",
                    "bar": index,
                    "price": closes[index],
                    "reason": "buy_signal",
                })
            else:
                events.append({
                    "event": "NOOP",
                    "bar": index,
                    "price": closes[index],
                    "reason": "sell_while_flat",
                })
            continue

        if action == "SELL":
            close_long(index, "sell_signal")
        else:
            events.append({
                "event": "NOOP",
                "bar": index,
                "price": closes[index],
                "reason": "buy_while_long",
            })

    if position is not None:
        close_long(len(normalized) - 1, "observation_end")

    return {"returns": returns, "events": events}


def replay_spot_long_only_returns_rust(*args: Any, **kwargs: Any) -> list[float]:
    return replay_spot_long_only_rust(*args, **kwargs)["returns"]


def _walk_forward_boundaries(
    n_rows: int,
    *,
    n_folds: int,
    purge_size: int,
    embargo_size: int,
):
    if n_folds < 2:
        raise ValueError("n_folds must be at least 2")
    if purge_size < 0 or embargo_size < 0:
        raise ValueError("purge_size and embargo_size must be non-negative")
    available = n_rows - purge_size - embargo_size
    fold_size = available // (n_folds + 1) if available > 0 else 0
    if fold_size < 40:
        raise ValueError("snapshot is too small for spot walk-forward replay")
    boundaries = generate_walk_forward_splits(
        n_rows,
        train_size=fold_size,
        test_size=fold_size,
        step_size=fold_size,
        purge_size=purge_size,
        embargo_size=embargo_size,
        expanding=True,
    )[:n_folds]
    if len(boundaries) != n_folds:
        raise ValueError("snapshot cannot produce the requested spot walk-forward folds")
    return boundaries


def attest_spot_snapshot_replay(
    snapshot: dict[str, Any],
    *,
    strategy_name: str,
    n_folds: int = 4,
    purge_size: int = 0,
    embargo_size: int = 0,
    warmup: int = 30,
    fee_bps: float = 0.0,
    max_hold_bars: int = 0,
    strategy_config: dict[str, Any] | None = None,
) -> tuple[list[list[float]], dict[str, Any]]:
    """Build deterministic OOS evidence for the spot-long-only lifecycle."""

    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("manifest"), dict):
        raise TypeError("snapshot must contain a canonical manifest")
    config = normalize_strategy_config(strategy_name, strategy_config)
    rows = normalize_candle_rows(snapshot.get("rows", []))
    rebuilt = snapshot_from_rows(
        rows,
        kind=snapshot["manifest"]["kind"],
        symbol=snapshot["manifest"]["symbol"],
        granularity=int(snapshot["manifest"]["granularity"]),
    )
    if rebuilt["manifest"] != snapshot["manifest"]:
        raise ValueError("snapshot manifest does not match snapshot rows")

    boundaries = _walk_forward_boundaries(
        len(rows),
        n_folds=int(n_folds),
        purge_size=int(purge_size),
        embargo_size=int(embargo_size),
    )
    fold_returns: list[list[float]] = []
    folds: list[dict[str, Any]] = []
    for boundary in boundaries:
        test_rows = rows[boundary.test_start:boundary.test_end]
        replay = replay_spot_long_only_rust(
            strategy_name,
            test_rows,
            warmup=int(warmup),
            fee_bps=float(fee_bps),
            max_hold_bars=int(max_hold_bars),
            strategy_config=config,
        )
        fold_returns.append(replay["returns"])
        folds.append({
            "fold": boundary.fold,
            "test_start": boundary.test_start,
            "test_end": boundary.test_end,
            "test_rows_hash": stable_hash(test_rows),
            "return_count": len(replay["returns"]),
            "returns_hash": stable_hash(replay["returns"]),
            "events_hash": stable_hash(replay["events"]),
        })

    lifecycle = {
        "method": SPOT_EXECUTION_METHOD,
        "position_mode": SPOT_POSITION_MODE,
        "dataset_kind": snapshot["manifest"]["kind"],
        "dataset_symbol": snapshot["manifest"]["symbol"],
        "granularity_seconds": int(snapshot["manifest"]["granularity"]),
        "warmup_bars": int(warmup),
        "fee_bps": float(fee_bps),
        "max_hold_bars": int(max_hold_bars),
        "buy_while_flat": "open_long",
        "buy_while_long": "noop",
        "sell_while_flat": "noop",
        "sell_while_long": "close_long",
        "max_hold_on_quiet_bar": True,
        "same_bar_reentry_after_forced_exit": False,
        "close_open_trade_at_observation_end": True,
    }
    core = {
        "schema_version": SPOT_EXECUTION_SCHEMA_VERSION,
        "attestation_type": SPOT_EXECUTION_ATTESTATION_TYPE,
        "runner": RUNNER_ID,
        "runner_source_sha256": _runner_source_sha256(),
        "dataset": snapshot["manifest"],
        "strategy_name": strategy_name,
        "strategy_config": config,
        "strategy_config_hash": stable_hash(config),
        "lifecycle": lifecycle,
        "lifecycle_hash": stable_hash(lifecycle),
        "n_folds": int(n_folds),
        "purge_size": int(purge_size),
        "embargo_size": int(embargo_size),
        "folds": folds,
        "fold_returns_hash": stable_hash(fold_returns),
    }
    return fold_returns, {**core, "attestation_hash": stable_hash(core)}


def verify_spot_replay_attestation(
    attestation: Any,
    fold_returns: Any,
    *,
    reverify_source: bool = True,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not isinstance(attestation, dict):
        return False, ["spot_replay_attestation_missing"]
    if not isinstance(fold_returns, list):
        return False, ["spot_replay_fold_returns_invalid"]
    required = {
        "schema_version", "attestation_type", "runner", "runner_source_sha256",
        "dataset", "strategy_name", "strategy_config", "strategy_config_hash",
        "lifecycle", "lifecycle_hash", "n_folds", "purge_size", "embargo_size",
        "folds", "fold_returns_hash", "attestation_hash",
    }
    for name in sorted(required - set(attestation)):
        reasons.append(f"spot_replay_attestation_missing_field:{name}")
    if reasons:
        return False, reasons
    if attestation["schema_version"] != SPOT_EXECUTION_SCHEMA_VERSION:
        reasons.append("spot_replay_schema_mismatch")
    if attestation["attestation_type"] != SPOT_EXECUTION_ATTESTATION_TYPE:
        reasons.append("spot_replay_type_mismatch")
    if attestation["runner"] != RUNNER_ID:
        reasons.append("spot_replay_runner_mismatch")
    if attestation["runner_source_sha256"] != _runner_source_sha256():
        reasons.append("spot_replay_runner_source_mismatch")
    try:
        config = normalize_strategy_config(attestation["strategy_name"], attestation["strategy_config"])
        if stable_hash(config) != attestation["strategy_config_hash"]:
            reasons.append("spot_replay_strategy_config_hash_mismatch")
        lifecycle = attestation["lifecycle"]
        if not isinstance(lifecycle, dict) or lifecycle.get("method") != SPOT_EXECUTION_METHOD:
            reasons.append("spot_replay_lifecycle_method_mismatch")
        if lifecycle.get("position_mode") != SPOT_POSITION_MODE:
            reasons.append("spot_replay_position_mode_mismatch")
        if stable_hash(lifecycle) != attestation["lifecycle_hash"]:
            reasons.append("spot_replay_lifecycle_hash_mismatch")
        if stable_hash(fold_returns) != attestation["fold_returns_hash"]:
            reasons.append("spot_replay_returns_hash_mismatch")
        core = dict(attestation)
        supplied_hash = core.pop("attestation_hash")
        if stable_hash(core) != supplied_hash:
            reasons.append("spot_replay_attestation_hash_mismatch")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("spot_replay_attestation_invalid")

    if reverify_source and not reasons:
        try:
            dataset = attestation["dataset"]
            snapshot = load_canonical_snapshot(
                kind=dataset["kind"],
                symbol=dataset["symbol"],
                granularity=int(dataset["granularity"]),
                start_ts=float(dataset["start_ts"]),
                end_ts=float(dataset["end_ts"]),
            )
            if snapshot["manifest"] != dataset:
                reasons.append("spot_replay_dataset_source_mismatch")
            else:
                regenerated_returns, regenerated = attest_spot_snapshot_replay(
                    snapshot,
                    strategy_name=attestation["strategy_name"],
                    n_folds=int(attestation["n_folds"]),
                    purge_size=int(attestation["purge_size"]),
                    embargo_size=int(attestation["embargo_size"]),
                    warmup=int(attestation["lifecycle"]["warmup_bars"]),
                    fee_bps=float(attestation["lifecycle"]["fee_bps"]),
                    max_hold_bars=int(attestation["lifecycle"]["max_hold_bars"]),
                    strategy_config=attestation["strategy_config"],
                )
                if regenerated_returns != fold_returns:
                    reasons.append("spot_replay_regenerated_returns_mismatch")
                if regenerated != attestation:
                    reasons.append("spot_replay_regenerated_attestation_mismatch")
        except (ImportError, RuntimeError, TypeError, ValueError, KeyError, OverflowError):
            reasons.append("spot_replay_source_reverification_failed")

    return not reasons, list(dict.fromkeys(reasons))
