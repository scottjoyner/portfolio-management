#!/usr/bin/env python3
"""Certified runtime dispatch wrapper for the strategy signal scanner.

The historical scanner implementation is preserved verbatim in
``strategy_signal_scanner_legacy.py``.  This entry point changes only runtime
dispatch:

* a successfully re-verified promoted canary runs the exact configured Rust
  evaluator that produced canonical research evidence;
* an invalid/missing promotion remains an ordinary *uncertified* screen;
* once a market is selected into a verified canary, any certified-runtime
  failure returns no signal instead of silently falling back to defaults;
* certification carried downstream is explicitly signal-only until the live
  trade lifecycle is proven equivalent to canonical replay.

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

# Preserve the legacy module surface for existing imports/tests.  Dunder names
# are intentionally excluded, but private helpers remain available because some
# repository tests exercise them directly.
for _name in dir(_legacy):
    if not _name.startswith("__"):
        globals().setdefault(_name, getattr(_legacy, _name))

_LEGACY_RUN_STRATEGIES = _legacy.run_strategies
_LEGACY_BUILD_TRADE_PLAN = _legacy._build_trade_plan
_RUNTIME_UNSET = object()
_runtime_context: object | dict[str, Any] | None = _RUNTIME_UNSET
_runtime_error: str | None = None


def reset_certified_runtime_cache() -> None:
    """Reset process-local certification state (primarily for tests/operators)."""

    global _runtime_context, _runtime_error
    _runtime_context = _RUNTIME_UNSET
    _runtime_error = None


def certified_runtime_status() -> dict[str, Any]:
    """Return non-secret process-local certification status."""

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
    }


def _get_runtime_context() -> dict[str, Any] | None:
    global _runtime_context, _runtime_error
    if _runtime_context is not _RUNTIME_UNSET:
        return _runtime_context if isinstance(_runtime_context, dict) else None
    try:
        _runtime_context = load_certified_runtime_context()
        _runtime_error = None
    except (CertifiedRuntimeError, OSError, ValueError, TypeError) as exc:
        # Missing/invalid certification does not make the research scanner
        # unavailable.  It simply cannot claim certified runtime authority.
        _runtime_context = None
        _runtime_error = str(exc)
        logger.warning("certified runtime unavailable; using uncertified screen: %s", exc)
    return _runtime_context if isinstance(_runtime_context, dict) else None


def run_strategies(
    currency: str,
    asset_class: str,
    closes: list[float],
    volumes: list[float],
    current_price: float,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
):
    """Dispatch a deterministic canary to certified behavior, else screen-only."""

    context = _get_runtime_context()
    if context is None:
        return _LEGACY_RUN_STRATEGIES(
            currency, asset_class, closes, volumes, current_price, highs=highs, lows=lows
        )

    identity = context["identity"]
    if not canary_selected(identity, currency, fraction=float(context["canary_fraction"])):
        return _LEGACY_RUN_STRATEGIES(
            currency, asset_class, closes, volumes, current_price, highs=highs, lows=lows
        )

    try:
        return run_certified_strategy(
            identity,
            closes=closes,
            volumes=volumes,
            highs=highs,
            lows=lows,
        )
    except (CertifiedRuntimeError, OSError, ValueError, TypeError) as exc:
        # The market was selected into the certified canary.  Falling through
        # to default parameters here would violate the certified identity.
        logger.error("certified canary failed closed for %s: %s", currency, exc)
        return []


def _build_trade_plan(signal, verdict, current_price, closes, highs, lows, sentiment_score):
    plan = _LEGACY_BUILD_TRADE_PLAN(
        signal, verdict, current_price, closes, highs, lows, sentiment_score
    )
    certification = getattr(signal, "runtime_certification", None)
    if isinstance(certification, dict) and certification.get("certified") is True:
        plan["runtime_certification"] = {
            **certification,
            "certification_scope": "configured_signal_only_v1",
            "execution_lifecycle_certified": False,
            "execution_lifecycle_blocker": "trade_lifecycle_not_canonical_replay_bound",
        }
    return plan


# scan_product() resolves these names from the legacy module globals, so patch
# the two seams rather than duplicating the scanner implementation.
_legacy.run_strategies = run_strategies
_legacy._build_trade_plan = _build_trade_plan

# Keep the wrapper's imported API coherent as well.
scan_product = _legacy.scan_product
main = _legacy.main


if __name__ == "__main__":
    main()
