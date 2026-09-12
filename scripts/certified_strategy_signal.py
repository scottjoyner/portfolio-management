#!/usr/bin/env python3
"""Run exactly one executable research-certified strategy on current market data.

This is deliberately separate from ``strategy_signal_scanner.py``.  The broad
scanner is a discovery/search surface; this module is a runtime binding surface.
It never searches strategy families, never re-ranks candidates, and never falls
back to default strategy parameters.

No broker/order authority exists here.  The output is a certified signal/evidence
payload for downstream paper/shadow opportunity evaluation.
"""
from __future__ import annotations

import argparse
import json
import math
from types import SimpleNamespace
from typing import Any

from scripts.challenger_manager import ChallengerRegistry
from scripts.runtime_research_certification import (
    bind_certification_to_runtime,
    run_certified_current_signal,
)
from scripts.strategy_signal_scanner import (
    _base_symbol,
    _build_trade_plan,
    _fetch_live_candles,
    _market_sentiment,
    _regime,
)

_GRANULARITY_LABEL = {
    "60": "ONE_MINUTE",
    "300": "FIVE_MINUTE",
    "900": "FIFTEEN_MINUTE",
    "1800": "THIRTY_MINUTE",
    "3600": "ONE_HOUR",
    "7200": "TWO_HOUR",
    "21600": "SIX_HOUR",
    "86400": "ONE_DAY",
}


def _days_back(certification: dict[str, Any]) -> int:
    try:
        seconds = int(certification["granularity"])
        period = int(certification["candidate_config"]["period"])
    except (KeyError, TypeError, ValueError):
        return 30
    bars = max(40, period + 20)
    return max(30, int(math.ceil((bars * seconds) / 86400.0)) + 2)


def _empty(*reasons: str, certification_hash: str | None = None) -> dict[str, Any]:
    return {
        "ok": True,
        "certified": False,
        "certification_hash": certification_hash,
        "signal": None,
        "reasons": list(dict.fromkeys(reason for reason in reasons if reason)),
    }


def scan_active_certified_strategy(
    *,
    registry: ChallengerRegistry | None = None,
    cache_ttl_seconds: int = 900,
    refresh: bool = False,
) -> dict[str, Any]:
    """Return the current exact-config signal for the verified active canary.

    The active config and persisted research evidence are revalidated before any
    market-data fetch.  Source replay is not repeated on every scan; immutable
    hashes, lineage, terminal evidence, executable scope, and active-config
    identity are still checked by ``verify_active_runtime_config``.
    """

    registry = registry or ChallengerRegistry()
    valid, verify_reasons, certification = registry.verify_active_runtime_config(
        reverify_source=False
    )
    if not valid or not isinstance(certification, dict):
        return _empty(*verify_reasons)

    certification_hash = certification.get("certification_hash")
    symbol = str(certification.get("symbol") or "")
    granularity_seconds = str(certification.get("granularity") or "")
    granularity = _GRANULARITY_LABEL.get(granularity_seconds)
    if not symbol or not granularity:
        return _empty(
            "certified_runtime_scope_invalid",
            certification_hash=certification_hash,
        )

    bound = bind_certification_to_runtime(
        certification,
        product_id=symbol,
        granularity=granularity,
    )
    if bound is None:
        return _empty(
            "certified_runtime_binding_failed",
            certification_hash=certification_hash,
        )

    series = _fetch_live_candles(
        symbol,
        granularity,
        _days_back(certification),
        cache_ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
    )
    # A fallback venue/data source is useful for discovery but is not the exact
    # runtime scope certified from Coinbase candles.  Fail closed here.
    if series.source != "live_cli":
        return _empty(
            "certified_runtime_market_source_unavailable",
            certification_hash=certification_hash,
        )

    closes = [row["close"] for row in series.candles if row.get("close", 0) > 0]
    volumes = [row.get("volume", 0.0) for row in series.candles]
    highs = [row.get("high", 0.0) for row in series.candles]
    lows = [row.get("low", 0.0) for row in series.candles]
    try:
        minimum_bars = int(certification["candidate_config"]["period"]) + 1
    except (KeyError, TypeError, ValueError):
        minimum_bars = 40
    if len(closes) < max(3, minimum_bars):
        return _empty(
            "certified_runtime_insufficient_market_history",
            certification_hash=certification_hash,
        )

    signal = run_certified_current_signal(
        bound,
        closes=closes,
        volumes=volumes,
        highs=highs,
        lows=lows,
    )
    if signal is None:
        return _empty(
            "certified_runtime_signal_unavailable",
            certification_hash=certification_hash,
        )
    if signal.get("action") == "HOLD":
        return {
            "ok": True,
            "certified": True,
            "certification_hash": certification_hash,
            "signal": None,
            "reasons": ["certified_strategy_hold"],
        }

    metrics = certification.get("terminal_metrics") or {}
    try:
        terminal_win_rate = float(metrics["win_rate"])
        trade_count = int(metrics["trade_count"])
        total_return_pct = float(metrics["total_return_pct"])
        sharpe = float(metrics["sharpe"])
        profit_factor = float(metrics["profit_factor"])
        max_drawdown_pct = float(metrics["max_drawdown_pct"])
    except (KeyError, TypeError, ValueError):
        return _empty(
            "certified_terminal_metrics_invalid",
            certification_hash=certification_hash,
        )
    if not all(math.isfinite(value) for value in (
        terminal_win_rate, total_return_pct, sharpe, profit_factor, max_drawdown_pct
    )) or trade_count <= 0:
        return _empty(
            "certified_terminal_metrics_invalid",
            certification_hash=certification_hash,
        )

    current_price = closes[-1]
    sentiment_score = _market_sentiment(closes)
    verdict = SimpleNamespace(
        total_trades=trade_count,
        win_rate=terminal_win_rate,
        total_return_pct=total_return_pct,
        sharpe_ratio=sharpe,
        profit_factor=profit_factor,
        max_drawdown_pct=max_drawdown_pct,
        reason="Certified one-shot terminal holdout",
    )
    signal_obj = SimpleNamespace(
        action=signal["action"],
        confidence=float(signal["confidence"]),
        reason=str(signal["reason"]),
        strategy=signal["strategy"],
    )
    trade_plan = _build_trade_plan(
        signal_obj,
        verdict,
        current_price,
        closes,
        highs,
        lows,
        sentiment_score,
    )
    trade_intent = "exit" if signal_obj.action == "SELL" else "entry"
    raw_confidence = max(0.0, min(1.0, signal_obj.confidence))
    payload = {
        "product_id": symbol,
        "symbol": symbol,
        "base_symbol": _base_symbol(symbol),
        "strategy": signal_obj.strategy,
        "action": signal_obj.action,
        "trade_intent": trade_intent,
        "trade_plan": trade_plan,
        "take_profit_price": trade_plan["take_profit_price"],
        "stop_loss_price": trade_plan["stop_loss_price"],
        "entry_price": trade_plan["entry_price"],
        "execution_purpose": trade_plan["execution_purpose"],
        "price": current_price,
        "win_rate": round(terminal_win_rate, 6),
        "total_trades": trade_count,
        "sentiment_score": round(sentiment_score, 6),
        "consensus": 0.0,
        "regime": _regime(closes),
        "raw_confidence": round(raw_confidence, 6),
        "weighted_confidence": round(raw_confidence, 6),
        "score": round(raw_confidence * terminal_win_rate, 6),
        "profit_score": round(max(0.0, total_return_pct) * raw_confidence * max(0.01, terminal_win_rate) / 100.0, 8),
        "source": series.source,
        "reason": signal_obj.reason,
        "backtest_reason": verdict.reason,
        "backtest_total_return_pct": round(total_return_pct, 6),
        "backtest_sharpe": round(sharpe, 6),
        "backtest_profit_factor": round(profit_factor, 6),
        "backtest_max_drawdown_pct": round(max_drawdown_pct, 6),
        "candles": len(closes),
        "market_direction": "bullish" if sentiment_score >= 0 else "bearish",
        "tournament_certified": True,
        "validation_scope": "tournament_terminal_promoted_exact_runtime",
        "research_certification": bound,
    }
    return {
        "ok": True,
        "certified": True,
        "certification_hash": certification_hash,
        "signal": payload,
        "reasons": [],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the active exact research-certified strategy")
    parser.add_argument("--cache-ttl", type=int, default=900)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    print(json.dumps(scan_active_certified_strategy(
        cache_ttl_seconds=args.cache_ttl,
        refresh=args.refresh,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
