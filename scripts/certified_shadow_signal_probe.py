#!/usr/bin/env python3
"""Observe certified strategy signals for forward shadow measurement only.

This probe intentionally bypasses two filters that belong to execution/discovery
rather than measurement:

1. the legacy same-window 30-day backtest / confidence screen; and
2. the execution canary routing fraction.

It also evaluates only *completed* candles at the certified replay granularity.
The observation timestamp is the candle close timestamp, so repeated worker
polls or process restarts are idempotent for the same canonical bar instead of
inflating the forward sample.

A shadow observation has no order authority. Automated execution remains
controlled elsewhere by the canary/overseer gates and
``execution_lifecycle_certified`` remains false.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]

from scripts.certified_strategy_runtime import (  # noqa: E402
    CertifiedRuntimeError,
    load_certified_runtime_context,
    run_certified_strategy,
)

MEASUREMENT_SCOPE = "certified_raw_signal_v1"
COINBASE_CANDLES_HOST = os.environ.get(
    "CERTIFIED_SHADOW_CANDLES_HOST", "https://api.exchange.coinbase.com"
).rstrip("/")
_SUPPORTED_GRANULARITIES = {60, 300, 900, 3600, 21600, 86400}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _granularity_seconds(value: Any) -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CertifiedRuntimeError("shadow_probe_granularity_invalid") from exc
    if seconds not in _SUPPORTED_GRANULARITIES:
        raise CertifiedRuntimeError(f"shadow_probe_granularity_unsupported:{seconds}")
    return seconds


def _iso_timestamp(epoch_seconds: int | float) -> str:
    return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).isoformat()


def _normalize_completed_rows(
    rows: list[Any], *, granularity_seconds: int, now_epoch: float
) -> list[dict[str, float]]:
    """Normalize Coinbase [time,low,high,open,close,volume] rows oldest-first."""

    normalized: list[dict[str, float]] = []
    for raw in rows:
        if not isinstance(raw, (list, tuple)) or len(raw) < 6:
            continue
        try:
            start = float(raw[0])
            low = float(raw[1])
            high = float(raw[2])
            open_px = float(raw[3])
            close = float(raw[4])
            volume = float(raw[5])
        except (TypeError, ValueError, OverflowError):
            continue
        if start + granularity_seconds > now_epoch:
            # The bucket is still forming. Canonical replay evaluates closed
            # rows, so forward shadow measurement must do the same.
            continue
        if min(open_px, high, low, close) <= 0 or volume < 0:
            continue
        if high < max(open_px, close, low) or low > min(open_px, close, high):
            continue
        normalized.append({
            "start": start,
            "open": open_px,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        })
    normalized.sort(key=lambda row: row["start"])
    deduped: dict[float, dict[str, float]] = {row["start"]: row for row in normalized}
    return [deduped[key] for key in sorted(deduped)]


def _fetch_completed_coinbase_candles(
    symbol: str,
    *,
    granularity_seconds: int,
    now_epoch: float | None = None,
    max_bars: int = 300,
) -> list[dict[str, float]]:
    now_value = float(now_epoch if now_epoch is not None else time.time())
    # Coinbase Exchange caps one candles request at 300 buckets. Current signal
    # evaluation needs only the certified strategy lookback, not 30-day
    # screening history, so one bounded request is sufficient and unbiased.
    count = max(20, min(300, int(max_bars)))
    closed_boundary = int(now_value // granularity_seconds) * granularity_seconds
    start = closed_boundary - count * granularity_seconds
    query = urlencode({
        "granularity": int(granularity_seconds),
        "start": _iso_timestamp(start),
        "end": _iso_timestamp(closed_boundary),
    })
    url = f"{COINBASE_CANDLES_HOST}/products/{quote(symbol, safe='-')}/candles?{query}"
    request = Request(
        url,
        headers={
            "User-Agent": "PortfolioCertifiedShadow/1.0",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise CertifiedRuntimeError("shadow_probe_coinbase_candles_unavailable") from exc
    if not isinstance(payload, list):
        raise CertifiedRuntimeError("shadow_probe_coinbase_candles_invalid")
    return _normalize_completed_rows(
        payload,
        granularity_seconds=granularity_seconds,
        now_epoch=now_value,
    )


def _certification(signal: Any) -> dict[str, Any]:
    raw = getattr(signal, "runtime_certification", None)
    if not isinstance(raw, dict) or raw.get("certified") is not True:
        raise CertifiedRuntimeError("shadow_probe_certification_missing")
    return {
        **raw,
        "certification_scope": "configured_signal_only_v1",
        "execution_lifecycle_certified": False,
        "execution_lifecycle_blocker": "trade_lifecycle_not_canonical_replay_bound",
        "shadow_measurement_scope": MEASUREMENT_SCOPE,
    }


def probe_certified_shadow_signals(
    *,
    runtime_context: dict[str, Any] | None = None,
    candle_rows: list[dict[str, float]] | None = None,
    source: str = "coinbase_exchange_public",
    observed_at: str | None = None,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    """Return every current certified signal on the evidence-bound market.

    No same-window profitability screen and no execution canary fraction are
    consulted. The strategy itself, exact configuration, market domain,
    deployed source and native evaluator are still certification-bound.
    """

    context = runtime_context or load_certified_runtime_context()
    identity = context.get("identity") if isinstance(context, dict) else None
    if not isinstance(identity, dict):
        raise CertifiedRuntimeError("shadow_probe_runtime_identity_required")
    lifecycle = identity.get("replay_execution_config")
    if not isinstance(lifecycle, dict):
        raise CertifiedRuntimeError("shadow_probe_replay_lifecycle_required")

    symbol = str(lifecycle.get("dataset_symbol") or "")
    if not symbol:
        raise CertifiedRuntimeError("shadow_probe_symbol_required")
    granularity = _granularity_seconds(lifecycle.get("granularity_seconds"))
    minimum = max(
        int(identity.get("strategy_config", {}).get("period", 0)) + 2,
        int(lifecycle.get("warmup_bars", 0)) + 2,
        3,
    )
    max_bars = max(minimum + 20, 100)
    rows = candle_rows
    if rows is None:
        rows = _fetch_completed_coinbase_candles(
            symbol,
            granularity_seconds=granularity,
            now_epoch=now_epoch,
            max_bars=max_bars,
        )
    else:
        # Tests/injected callers must still provide already-completed canonical
        # rows with explicit bucket starts; silently accepting untimestamped
        # input would break idempotence.
        if any("start" not in row for row in rows):
            raise CertifiedRuntimeError("shadow_probe_candle_timestamp_required")
        rows = sorted(rows, key=lambda row: float(row["start"]))

    valid_rows = [row for row in rows if float(row.get("close", 0) or 0) > 0]
    if len(valid_rows) < minimum:
        return {
            "ok": True,
            "measurement_scope": MEASUREMENT_SCOPE,
            "signals": [],
            "errors": [f"insufficient_certified_shadow_candles:{len(valid_rows)}<{minimum}"],
            "runtime_identity_hash": context.get("runtime_identity_hash"),
            "candidate_id": identity.get("candidate_id"),
            "symbol": symbol,
            "granularity_seconds": granularity,
            "canary_fraction_ignored_for_shadow": True,
            "same_window_screen_applied": False,
            "source": source,
        }

    closes = [float(row["close"]) for row in valid_rows]
    volumes = [float(row.get("volume", 0) or 0) for row in valid_rows]
    highs = [float(row.get("high", row["close"]) or row["close"]) for row in valid_rows]
    lows = [float(row.get("low", row["close"]) or row["close"]) for row in valid_rows]
    signals = run_certified_strategy(
        identity,
        closes=closes,
        volumes=volumes,
        highs=highs,
        lows=lows,
    )
    last_start = float(valid_rows[-1]["start"])
    canonical_observed_at = _iso_timestamp(last_start + granularity)
    at = observed_at or canonical_observed_at
    rows_out: list[dict[str, Any]] = []
    for signal in signals:
        action = str(getattr(signal, "action", "")).upper()
        if action not in {"BUY", "SELL"}:
            continue
        price = float(getattr(signal, "price", closes[-1]))
        certification = _certification(signal)
        rows_out.append({
            "product_id": symbol,
            "symbol": symbol,
            "strategy": str(getattr(signal, "strategy", identity.get("strategy_name", ""))),
            "action": action,
            "price": price,
            "entry_price": price,
            "observed_at": at,
            "generated_at": _utc_now(),
            "canonical_bar_start": _iso_timestamp(last_start),
            "canonical_bar_closed_at": canonical_observed_at,
            "canonical_granularity_seconds": granularity,
            "source": f"certified_shadow_probe:{source}",
            "reason": str(getattr(signal, "reason", "certified runtime signal")),
            "raw_confidence": float(getattr(signal, "confidence", 0.0)),
            "weighted_confidence": float(getattr(signal, "confidence", 0.0)),
            "measurement_scope": MEASUREMENT_SCOPE,
            "shadow_only": True,
            "same_window_screen_applied": False,
            "execution_canary_applied": False,
            "trade_intent": "shadow_measurement",
            "execution_purpose": "shadow_only",
            "trade_plan": {
                "plan_type": "shadow_measurement",
                "execution_purpose": "shadow_only",
                "entry_price": price,
                "position_side": "long" if action == "BUY" else "short",
                "runtime_certification": certification,
            },
        })

    return {
        "ok": True,
        "measurement_scope": MEASUREMENT_SCOPE,
        "signals": rows_out,
        "errors": [],
        "runtime_identity_hash": context.get("runtime_identity_hash"),
        "candidate_id": identity.get("candidate_id"),
        "candidate_source_sha": identity.get("candidate_source_sha"),
        "strategy_name": identity.get("strategy_name"),
        "strategy_config_hash": identity.get("strategy_config_hash"),
        "replay_execution_config_hash": identity.get("replay_execution_config_hash"),
        "deployment_bundle_hash": identity.get("deployment_bundle_hash"),
        "symbol": symbol,
        "granularity_seconds": granularity,
        "candles": len(valid_rows),
        "latest_completed_bar_start": _iso_timestamp(last_start),
        "latest_completed_bar_closed_at": canonical_observed_at,
        "canary_fraction_ignored_for_shadow": True,
        "same_window_screen_applied": False,
        "source": source,
        "observed_at": at,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    del args
    try:
        result = probe_certified_shadow_signals()
    except (CertifiedRuntimeError, OSError, ValueError, TypeError) as exc:
        result = {
            "ok": False,
            "measurement_scope": MEASUREMENT_SCOPE,
            "signals": [],
            "errors": [str(exc)],
            "canary_fraction_ignored_for_shadow": True,
            "same_window_screen_applied": False,
        }
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0 if result.get("ok") is True else 2


if __name__ == "__main__":
    raise SystemExit(main())
