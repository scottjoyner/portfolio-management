#!/usr/bin/env python3
"""Certified runtime dispatch wrapper for the strategy signal scanner.

The historical scanner implementation is preserved verbatim in
``strategy_signal_scanner_legacy.py``. Uncertified discovery keeps using that
screen. A verified canary, however, is evaluated only by the exact configured
Rust evaluator and is never re-qualified by a default-parameter same-window
backtest.

The executable spot lifecycle is still a separate certification boundary. The
current promoted evidence uses the historical long/short replay lifecycle, so
signals remain shadow-only until fresh spot-long-only evidence is produced.

No broker or live-order authority exists here.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import strategy_signal_scanner_legacy as _legacy  # noqa: E402
from scripts.certified_strategy_runtime import (  # noqa: E402
    CertifiedRuntimeError,
    canary_selected,
    load_certified_runtime_context,
    run_certified_strategy,
)

logger = logging.getLogger(__name__)

for _name in dir(_legacy):
    if not _name.startswith("__"):
        globals().setdefault(_name, getattr(_legacy, _name))

_LEGACY_SCAN_PRODUCT = _legacy.scan_product
_RUNTIME_UNSET = object()
_runtime_context: object | dict[str, Any] | None = _RUNTIME_UNSET
_runtime_error: str | None = None

_GRANULARITY_SECONDS = {
    "ONE_MINUTE": 60,
    "FIVE_MINUTE": 300,
    "FIFTEEN_MINUTE": 900,
    "THIRTY_MINUTE": 1800,
    "ONE_HOUR": 3600,
    "TWO_HOUR": 7200,
    "SIX_HOUR": 21600,
    "ONE_DAY": 86400,
}


def reset_certified_runtime_cache() -> None:
    global _runtime_context, _runtime_error
    _runtime_context = _RUNTIME_UNSET
    _runtime_error = None


def _get_runtime_context() -> dict[str, Any] | None:
    global _runtime_context, _runtime_error
    if _runtime_context is not _RUNTIME_UNSET:
        return _runtime_context if isinstance(_runtime_context, dict) else None
    try:
        _runtime_context = load_certified_runtime_context()
        _runtime_error = None
    except (CertifiedRuntimeError, OSError, ValueError, TypeError) as exc:
        _runtime_context = None
        _runtime_error = str(exc)
        logger.warning("certified runtime unavailable; using uncertified screen: %s", exc)
    return _runtime_context if isinstance(_runtime_context, dict) else None


def certified_runtime_status() -> dict[str, Any]:
    context = _get_runtime_context()
    if context is None:
        return {"available": False, "error": _runtime_error}
    identity = context["identity"]
    return {
        "available": True,
        "candidate_id": identity["candidate_id"],
        "runtime_identity_hash": context["runtime_identity_hash"],
        "deployment": context["deployment"],
        "canary_fraction": context["canary_fraction"],
        "dataset_symbol": identity.get("replay_execution_config", {}).get("dataset_symbol"),
        "granularity_seconds": identity.get("replay_execution_config", {}).get("granularity_seconds"),
        "execution_lifecycle_certified": False,
    }


def _canary_context_for(product_id: str, granularity: str) -> dict[str, Any] | None:
    context = _get_runtime_context()
    if context is None:
        return None
    identity = context["identity"]
    lifecycle = identity.get("replay_execution_config")
    if not isinstance(lifecycle, dict):
        return None
    expected_seconds = int(lifecycle.get("granularity_seconds") or 0)
    actual_seconds = _GRANULARITY_SECONDS.get(str(granularity).upper())
    if actual_seconds != expected_seconds:
        return None
    if not canary_selected(identity, product_id, fraction=float(context["canary_fraction"])):
        return None
    return context


def run_strategies(
    currency: str,
    asset_class: str,
    closes: list[float],
    volumes: list[float],
    current_price: float,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
):
    """Compatibility dispatch used by direct callers.

    ``scan_product`` below owns the stronger certified path because the legacy
    scanner would otherwise apply a second default-config backtest to this
    result. Direct callers still get exact configured evaluation when selected.
    """

    context = _get_runtime_context()
    if context is None:
        return _legacy.run_strategies(
            currency, asset_class, closes, volumes, current_price, highs=highs, lows=lows
        )
    identity = context["identity"]
    if not canary_selected(identity, currency, fraction=float(context["canary_fraction"])):
        return _legacy.run_strategies(
            currency, asset_class, closes, volumes, current_price, highs=highs, lows=lows
        )
    try:
        return run_certified_strategy(
            identity, closes=closes, volumes=volumes, highs=highs, lows=lows
        )
    except (CertifiedRuntimeError, OSError, ValueError, TypeError) as exc:
        logger.error("certified canary failed closed for %s: %s", currency, exc)
        return []


def _certified_trade_plan(signal: Any, identity: dict[str, Any], current_price: float) -> dict[str, Any]:
    action = str(getattr(signal, "action", "HOLD")).upper()
    lifecycle = identity["replay_execution_config"]
    certification = getattr(signal, "runtime_certification", None)
    if not isinstance(certification, dict) or certification.get("certified") is not True:
        raise CertifiedRuntimeError("certified_signal_identity_missing")
    is_buy = action == "BUY"
    return {
        "family": "mean_reversion",
        "plan_type": "entry" if is_buy else "exit",
        "position_side": "long",
        "execution_purpose": "open_long" if is_buy else "close_long",
        "entry_price": float(current_price),
        # TP/SL are deliberately absent from execution authority. Canonical
        # spot lifecycle exits on SELL or max-hold, not a heuristic bracket.
        "take_profit_price": None,
        "stop_loss_price": None,
        "risk_reward_ratio": 0.0,
        "holding_bias": "canonical_lifecycle",
        "trail_pct": 0.0,
        "runtime_certification": {
            **certification,
            "certification_scope": "configured_signal_plus_replay_lifecycle_v1",
            "execution_lifecycle_certified": False,
            "execution_lifecycle_blocker": "fresh_spot_long_only_attestation_required",
        },
        "research_replay_lifecycle": lifecycle,
        "candidate_execution_lifecycle": {
            "method": "spot_long_only_opposite_signal_or_max_hold_v1",
            "position_mode": "spot_long_only",
            "buy_while_flat": "open_long",
            "buy_while_long": "noop",
            "sell_while_flat": "noop",
            "sell_while_long": "close_long",
            "max_hold_on_quiet_bar": True,
            "same_bar_reentry_after_forced_exit": False,
        },
    }


def _scan_certified_product(
    product_id: str,
    granularity: str,
    days_back: int,
    cache_ttl_seconds: int,
    refresh: bool,
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    series = _legacy._fetch_live_candles(
        product_id,
        granularity,
        days_back,
        cache_ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    closes = [row["close"] for row in series.candles if row.get("close", 0) > 0]
    volumes = [row.get("volume", 0.0) for row in series.candles]
    highs = [row.get("high", 0.0) for row in series.candles]
    lows = [row.get("low", 0.0) for row in series.candles]
    if len(closes) < 40:
        return []

    identity = context["identity"]
    try:
        signals = run_certified_strategy(
            identity, closes=closes, volumes=volumes, highs=highs, lows=lows
        )
    except (CertifiedRuntimeError, OSError, ValueError, TypeError) as exc:
        logger.error("certified canary failed closed for %s: %s", product_id, exc)
        return []

    sentiment = _legacy._market_sentiment(closes)
    regime = _legacy._regime(closes)
    current_price = closes[-1]
    results: list[dict[str, Any]] = []
    for signal in signals:
        action = str(getattr(signal, "action", "HOLD")).upper()
        if action not in {"BUY", "SELL"}:
            continue
        confidence = max(0.0, min(1.0, float(getattr(signal, "confidence", 0.0))))
        plan = _certified_trade_plan(signal, identity, current_price)
        results.append({
            "product_id": product_id,
            "symbol": product_id,
            "base_symbol": _legacy._base_symbol(product_id),
            "strategy": str(identity["strategy_name"]),
            "action": action,
            "trade_intent": "entry" if action == "BUY" else "exit",
            "trade_plan": plan,
            "take_profit_price": None,
            "stop_loss_price": None,
            "entry_price": current_price,
            "execution_purpose": plan["execution_purpose"],
            "price": current_price,
            # Do not manufacture a forward probability from an in-sample
            # re-backtest. Economic admission uses the separate forecast/shadow
            # calibration chain; this field is neutral compatibility metadata.
            "win_rate": 0.5,
            "total_trades": 0,
            "sentiment_score": round(sentiment, 4),
            "consensus": 1.0,
            "regime": regime,
            "raw_confidence": round(confidence, 4),
            "weighted_confidence": round(confidence, 4),
            "score": round(confidence, 4),
            "profit_score": 0.0,
            "source": series.source,
            "reason": str(getattr(signal, "reason", "certified configured signal")),
            "backtest_reason": "certified_runtime_bypasses_same_window_default_backtest",
            "backtest_total_return_pct": 0.0,
            "backtest_sharpe": 0.0,
            "backtest_profit_factor": 0.0,
            "backtest_max_drawdown_pct": 0.0,
            "candles": len(closes),
            "market_direction": "bullish" if sentiment >= 0 else "bearish",
            "runtime_certification": plan["runtime_certification"],
            "validation_scope": "promoted_exact_config_runtime_signal",
            "same_window_backtest_applied": False,
        })
    return results


def scan_product(
    product_id: str,
    granularity: str,
    days_back: int,
    min_win_rate: float,
    min_weighted_confidence: float,
    cache_ttl_seconds: int = 900,
    refresh: bool = False,
) -> list[dict[str, Any]]:
    context = _canary_context_for(product_id, granularity)
    if context is None:
        return _LEGACY_SCAN_PRODUCT(
            product_id,
            granularity,
            days_back,
            min_win_rate,
            min_weighted_confidence,
            cache_ttl_seconds,
            refresh,
        )
    return _scan_certified_product(
        product_id,
        granularity,
        days_back,
        cache_ttl_seconds,
        refresh,
        context,
    )


# The legacy CLI resolves scan_product from its own module globals. Patch that
# seam so command-line/API scans use the certified bypass when applicable.
_legacy.scan_product = scan_product
main = _legacy.main


if __name__ == "__main__":
    main()
