#!/usr/bin/env python3
"""Observe certified strategy signals for forward shadow measurement only.

This probe intentionally bypasses two filters that belong to execution/discovery
rather than measurement:

1. the legacy same-window 30-day backtest / confidence screen; and
2. the execution canary routing fraction.

A shadow observation has no order authority, so measuring every signal emitted
by the *already-certified* strategy on the exact replayed market is both safer
and statistically cleaner than conditioning the forward sample on a second
in-sample screen.  Automated execution remains governed elsewhere by the
canary/overseer gates and ``execution_lifecycle_certified`` remains false.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import strategy_signal_scanner as scanner  # noqa: E402
from scripts.certified_strategy_runtime import (  # noqa: E402
    CertifiedRuntimeError,
    load_certified_runtime_context,
    run_certified_strategy,
)

MEASUREMENT_SCOPE = "certified_raw_signal_v1"
_GRANULARITY_NAMES = {
    60: "ONE_MINUTE",
    300: "FIVE_MINUTE",
    900: "FIFTEEN_MINUTE",
    1800: "THIRTY_MINUTE",
    3600: "ONE_HOUR",
    7200: "TWO_HOUR",
    14400: "FOUR_HOUR",
    21600: "SIX_HOUR",
    86400: "ONE_DAY",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _granularity_name(seconds: int) -> str:
    try:
        value = int(seconds)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CertifiedRuntimeError("shadow_probe_granularity_invalid") from exc
    name = _GRANULARITY_NAMES.get(value)
    if not name:
        raise CertifiedRuntimeError(f"shadow_probe_granularity_unsupported:{value}")
    return name


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
    days_back: int = 30,
    cache_ttl_seconds: int = 900,
    refresh: bool = False,
    observed_at: str | None = None,
    runtime_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return every current certified signal on the evidence-bound market.

    No same-window profitability screen and no execution canary fraction are
    consulted.  The strategy itself, exact configuration, market domain,
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
    granularity = _granularity_name(int(lifecycle.get("granularity_seconds", 0)))
    days = max(1, int(days_back))
    cache_ttl = max(0, int(cache_ttl_seconds))

    series = scanner._fetch_live_candles(
        symbol,
        granularity,
        days,
        cache_ttl_seconds=cache_ttl,
        refresh=bool(refresh),
    )
    closes = [float(row["close"]) for row in series.candles if float(row.get("close", 0) or 0) > 0]
    volumes = [float(row.get("volume", 0) or 0) for row in series.candles]
    highs = [float(row.get("high", 0) or 0) for row in series.candles]
    lows = [float(row.get("low", 0) or 0) for row in series.candles]
    minimum = max(int(identity.get("strategy_config", {}).get("period", 0)) + 2, 3)
    if len(closes) < minimum:
        return {
            "ok": True,
            "measurement_scope": MEASUREMENT_SCOPE,
            "signals": [],
            "errors": [f"insufficient_certified_shadow_candles:{len(closes)}<{minimum}"],
            "runtime_identity_hash": context.get("runtime_identity_hash"),
            "candidate_id": identity.get("candidate_id"),
            "symbol": symbol,
            "granularity": granularity,
            "canary_fraction_ignored_for_shadow": True,
            "same_window_screen_applied": False,
            "source": getattr(series, "source", "unknown"),
        }

    signals = run_certified_strategy(
        identity,
        closes=closes,
        volumes=volumes,
        highs=highs,
        lows=lows,
    )
    at = observed_at or _utc_now()
    rows: list[dict[str, Any]] = []
    for signal in signals:
        action = str(getattr(signal, "action", "")).upper()
        if action not in {"BUY", "SELL"}:
            continue
        price = float(getattr(signal, "price", closes[-1]))
        certification = _certification(signal)
        rows.append({
            "product_id": symbol,
            "symbol": symbol,
            "strategy": str(getattr(signal, "strategy", identity.get("strategy_name", ""))),
            "action": action,
            "price": price,
            "entry_price": price,
            "observed_at": at,
            "generated_at": at,
            "source": f"certified_shadow_probe:{getattr(series, 'source', 'unknown')}",
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
        "signals": rows,
        "errors": [],
        "runtime_identity_hash": context.get("runtime_identity_hash"),
        "candidate_id": identity.get("candidate_id"),
        "candidate_source_sha": identity.get("candidate_source_sha"),
        "strategy_name": identity.get("strategy_name"),
        "strategy_config_hash": identity.get("strategy_config_hash"),
        "replay_execution_config_hash": identity.get("replay_execution_config_hash"),
        "deployment_bundle_hash": identity.get("deployment_bundle_hash"),
        "symbol": symbol,
        "granularity": granularity,
        "candles": len(closes),
        "canary_fraction_ignored_for_shadow": True,
        "same_window_screen_applied": False,
        "source": getattr(series, "source", "unknown"),
        "observed_at": at,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-back", type=int, default=30)
    parser.add_argument("--cache-ttl", type=int, default=900)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    try:
        result = probe_certified_shadow_signals(
            days_back=args.days_back,
            cache_ttl_seconds=args.cache_ttl,
            refresh=args.refresh,
        )
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
