#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from coinbase.src.cb_client import CBClient
from coinbase.src.orchestrator import ExecutionOrchestrator, TradeMode, TradeSignal
from coinbase.src.protocols import Direction, InstrumentType
def _mid_from_books(book: Dict[str, Any]) -> Optional[float]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    if not bids or not asks:
        return None
    try:
        bid = float(bids[0].get("price", 0))
        ask = float(asks[0].get("price", 0))
    except Exception:
        return None
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2.0


def _fetch_mid(cb: CBClient, product_id: str) -> float:
    books = cb.best_bid_ask([product_id])
    for book in books.get("pricebooks", []) or []:
        if book.get("product_id") == product_id:
            mid = _mid_from_books(book)
            if mid:
                return mid
    raise RuntimeError(f"Unable to determine mid price for {product_id}")


def _latest_price(cb: CBClient, product_id: str) -> float:
    return _fetch_mid(cb, product_id)
def main() -> int:
    parser = argparse.ArgumentParser(description="Strict live Coinbase canary")
    parser.add_argument("--product", default="BTC-USD")
    parser.add_argument("--notional-usd", type=float, default=100.0)
    parser.add_argument("--single-trade-cap-pct", type=float, default=0.5)
    parser.add_argument("--stop-pct", type=float, default=0.005)
    parser.add_argument("--target-pct", type=float, default=0.010)
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument("--poll-seconds", type=int, default=3)
    parser.add_argument("--stale-after-seconds", type=int, default=15)
    parser.add_argument("--force-flatten-after-seconds", type=int, default=60)
    parser.add_argument("--strategy", default="live_canary")
    parser.add_argument("--reason", default="strict live canary")
    parser.add_argument("--confidence", type=float, default=0.99)
    parser.add_argument("--opportunity-score", type=float, default=0.99)
    parser.add_argument("--min-confidence", type=float, default=0.95)
    parser.add_argument("--min-edge-bps", type=float, default=25.0)
    parser.add_argument("--min-cash-reserve-usd", type=float, default=100.0)
    parser.add_argument("--max-order-usd", type=float, default=50.0)
    parser.add_argument("--max-total-notional-usd", type=float, default=50.0)
    parser.add_argument("--max-open-positions", type=int, default=1)
    args = parser.parse_args()

    if args.confidence < args.min_confidence:
        raise SystemExit(f"confidence {args.confidence} below minimum {args.min_confidence}")

    os.environ["TRADER_LIVE_MIN_CASH_RESERVE_USD"] = str(args.min_cash_reserve_usd)
    os.environ["TRADER_LIVE_MAX_ORDER_USD"] = str(args.max_order_usd)
    os.environ["TRADER_LIVE_MAX_TOTAL_NOTIONAL_USD"] = str(args.max_total_notional_usd)
    os.environ["TRADER_LIVE_MAX_OPEN_POSITIONS"] = str(args.max_open_positions)
    os.environ["TRADER_CHALLENGE_MAX_ORDER_USD"] = str(args.max_order_usd)
    os.environ["TRADER_LIVE_CHALLENGE_ONLY"] = "true"
    os.environ["TRADER_LIVE_ALLOW_SHORT"] = "false"
    os.environ["TRADER_MIN_LIVE_CONFIDENCE"] = str(args.min_confidence)
    os.environ["TRADER_MIN_LIVE_EDGE_BPS"] = str(args.min_edge_bps)

    cb = CBClient()
    entry = _latest_price(cb, args.product)
    standard_notional = max(0.0, args.notional_usd)
    capped_notional = min(
        standard_notional,
        standard_notional * max(0.0, min(1.0, args.single_trade_cap_pct)),
        args.max_order_usd,
        args.max_total_notional_usd,
    )
    live_cash = None
    try:
        accounts = cb.list_accounts()
        items = accounts.get("accounts") or accounts.get("data") or accounts
        if isinstance(items, list):
            for acct in items:
                if isinstance(acct, dict) and str(acct.get("currency") or acct.get("asset") or "").upper() == "USD":
                    avail = acct.get("available_balance") or acct.get("available") or acct.get("balance") or 0
                    if isinstance(avail, dict):
                        avail = avail.get("value", 0)
                    live_cash = float(avail or 0.0)
                    break
    except Exception:
        pass
    if live_cash is not None:
        capped_notional = min(capped_notional, max(0.0, live_cash - args.min_cash_reserve_usd))
    if capped_notional <= 0:
        raise SystemExit("effective notional is zero after caps/reserve checks")

    size = capped_notional / max(entry, 1e-9)
    stop = entry * (1.0 - args.stop_pct)
    target = entry * (1.0 + args.target_pct)

    orch = ExecutionOrchestrator(cb=cb, mode=TradeMode.LIVE, dry_run=False, pending_file="pending_approvals.json")
    orch.live_min_cash_reserve_usd = args.min_cash_reserve_usd
    orch.live_max_order_usd = args.max_order_usd
    orch.live_max_total_notional_usd = args.max_total_notional_usd
    orch.live_max_open_positions = args.max_open_positions
    orch.live_allow_short = False

    sig = TradeSignal(
        product_id=args.product,
        direction=Direction.LONG,
        entry_price=entry,
        stop_price=stop,
        target_price=target,
        size=size,
        confidence=args.confidence,
        reason=args.reason,
        strategy_name=args.strategy,
        instrument_type=InstrumentType.SPOT,
        leverage=1.0,
        opportunity_score=args.opportunity_score,
    )

    result = orch.execute_signals([sig])[0]
    payload: Dict[str, Any] = {
        "entry": entry,
        "standard_notional_usd": standard_notional,
        "effective_notional_usd": capped_notional,
        "size": size,
        "stop": stop,
        "target": target,
        "result": result,
        "strict_caps": {
            "single_trade_cap_pct": args.single_trade_cap_pct,
            "min_cash_reserve_usd": args.min_cash_reserve_usd,
            "max_order_usd": args.max_order_usd,
            "max_total_notional_usd": args.max_total_notional_usd,
            "max_open_positions": args.max_open_positions,
            "min_confidence": args.min_confidence,
            "min_edge_bps": args.min_edge_bps,
        },
    }

    bracket_id = result.get("bracket_id") or result.get("token") or ""
    started = time.time()
    final_state: Dict[str, Any] = {}
    while bracket_id and (time.time() - started) < args.timeout_seconds:
        orch.bracket_mgr.reconcile_open_brackets(
            stale_after_s=args.stale_after_seconds,
            force_flatten_after_s=args.force_flatten_after_seconds,
        )
        bracket = orch.bracket_mgr._brackets.get(bracket_id)
        if bracket:
            final_state = {
                "status": bracket.get("status"),
                "exit_reason": bracket.get("exit_reason"),
                "exit_price": bracket.get("exit_price"),
                "stop_order_id": bracket.get("stop_order_id"),
                "target_order_id": bracket.get("target_order_id"),
            }
            if bracket.get("status") == "CLOSED":
                break
        time.sleep(max(1, args.poll_seconds))

    if bracket_id:
        bracket = orch.bracket_mgr._brackets.get(bracket_id)
        if bracket and bracket.get("status") == "OPEN":
            final_state = orch.bracket_mgr.force_flatten_bracket(bracket_id, reason="canary_timeout")

    payload["final_state"] = final_state
    payload["round_trip_complete"] = final_state.get("status") == "CLOSED"
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if result.get("success") and payload["round_trip_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
