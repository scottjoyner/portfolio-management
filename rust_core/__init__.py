"""Optional Python bindings for the Rust acceleration layer.

The compiled extension is an optimization, not a package-import prerequisite.
When it is unavailable, small deterministic Python compatibility functions keep
callers on their existing safe fallback paths instead of failing at import time.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

RUST_CORE_AVAILABLE = False
RUST_CORE_IMPORT_ERROR: Exception | None = None

try:
    from .rust_core import *  # type: ignore  # noqa: F401,F403

    RUST_CORE_AVAILABLE = True
except (ImportError, ModuleNotFoundError) as exc:
    RUST_CORE_IMPORT_ERROR = exc

    def _missing_extension(*args, **kwargs):
        raise RuntimeError(
            "rust_core extension is unavailable; build/install the optional "
            "Rust acceleration module or use the Python fallback path"
        ) from RUST_CORE_IMPORT_ERROR

    evaluate_all_opens_py = _missing_extension

    _CONFIDENCE_GROUPS = {
        "ema_cross": "trend",
        "macd": "trend",
        "hma": "trend",
        "aroon": "trend",
        "adx": "trend",
        "psar": "trend",
        "rsi_revert": "momentum",
        "zscore_revert": "momentum",
        "cmo": "momentum",
        "williams_r": "momentum",
        "boll_break": "volatility",
        "vwap_revert": "volatility",
        "keltner": "volatility",
        "donchian": "volatility",
        "vol_mom": "volume",
        "obv_div": "volume",
        "chaikin_mf": "volume",
    }
    _DEFAULT_CONFIDENCE_WEIGHTS = {
        "ema_cross": 0.6,
        "macd": 0.6,
        "hma": 0.6,
        "aroon": 0.6,
        "rsi_revert": 0.4,
        "zscore_revert": 0.4,
        "adx": 0.6,
        "psar": 0.4,
    }

    def confidence_default_weight_py(strategy: str) -> float:
        return float(_DEFAULT_CONFIDENCE_WEIGHTS.get(str(strategy), 0.5))

    def confidence_weight_from_bt_py(win_rate: float, sharpe: float) -> float:
        return min(
            1.0,
            max(0.0, 0.3 + float(win_rate) * 0.4 + float(sharpe) * 0.3),
        )

    def confidence_aggregate_py(
        signals: list[tuple[str, str, float, str]],
        asset_class: str,
        currency: str,
        bt_weights: dict[str, float],
    ) -> list[tuple[Any, ...]]:
        """Compatibility implementation matching the Rust result shape."""
        by_direction: dict[str, list[tuple[str, float, str]]] = defaultdict(list)
        for strategy, action, confidence, reason in signals:
            direction = str(action).upper()
            if direction in {"BUY", "SELL"}:
                by_direction[direction].append(
                    (str(strategy), float(confidence), str(reason))
                )

        results: list[tuple[Any, ...]] = []
        for direction, rows in by_direction.items():
            weighted_total = 0.0
            total_weight = 0.0
            best_reason = ""
            best_confidence = -1.0
            strategies: list[str] = []
            groups: set[str] = set()

            for strategy, confidence, reason in rows:
                weight = max(0.0, float(bt_weights.get(strategy, 0.5)))
                weighted_total += confidence * weight
                total_weight += weight
                if strategy not in strategies:
                    strategies.append(strategy)
                group = _CONFIDENCE_GROUPS.get(strategy)
                if group:
                    groups.add(group)
                if confidence > best_confidence:
                    best_confidence = confidence
                    best_reason = reason

            raw_confidence = (
                weighted_total / total_weight if total_weight > 0 else 0.0
            )
            agreeing_groups = len(groups)
            confidence = raw_confidence
            if agreeing_groups >= 2:
                confidence *= min(1.0 + (agreeing_groups - 1) * 0.10, 1.5)
            elif agreeing_groups == 0:
                confidence *= 0.5
            if len(strategies) >= 5:
                confidence *= 1.10
            elif len(strategies) >= 3:
                confidence *= 1.05
            confidence = min(max(confidence, 0.0), 1.0)

            results.append(
                (
                    currency,
                    direction,
                    round(confidence, 4),
                    round(raw_confidence, 4),
                    agreeing_groups,
                    12,
                    len(strategies),
                    strategies,
                    best_reason,
                    asset_class,
                )
            )

        results.sort(key=lambda row: row[2], reverse=True)
        return results


if RUST_CORE_AVAILABLE:
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = [
        "RUST_CORE_AVAILABLE",
        "RUST_CORE_IMPORT_ERROR",
        "evaluate_all_opens_py",
        "confidence_default_weight_py",
        "confidence_weight_from_bt_py",
        "confidence_aggregate_py",
    ]
