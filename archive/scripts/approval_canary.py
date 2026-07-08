#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from urllib.request import urlopen

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from coinbase.src.orchestrator import ExecutionOrchestrator, TradeMode, TradeSignal
from coinbase.src.protocols import Direction, InstrumentType


def _fetch_json(url: str) -> Dict[str, Any]:
    with urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def build_signal(args: argparse.Namespace) -> TradeSignal:
    return TradeSignal(
        product_id=args.product,
        direction=Direction.LONG if args.side.lower() == "buy" else Direction.SHORT,
        entry_price=args.entry,
        stop_price=args.stop,
        target_price=args.target,
        size=args.size,
        confidence=args.confidence,
        reason=args.reason,
        strategy_name=args.strategy,
        instrument_type=InstrumentType.SPOT,
        leverage=args.leverage,
        opportunity_score=args.score,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Approval-mode canary for Coinbase trading")
    parser.add_argument("--product", default="BTC-USD")
    parser.add_argument("--side", choices=["buy", "sell"], default="buy")
    parser.add_argument("--entry", type=float, default=100.0)
    parser.add_argument("--stop", type=float, default=95.0)
    parser.add_argument("--target", type=float, default=110.0)
    parser.add_argument("--size", type=float, default=0.001)
    parser.add_argument("--confidence", type=float, default=0.50)
    parser.add_argument("--score", type=float, default=0.50)
    parser.add_argument("--strategy", default="canary")
    parser.add_argument("--reason", default="approval canary")
    parser.add_argument("--leverage", type=float, default=1.0)
    parser.add_argument("--health-url", default="", help="Optional health endpoint to include in the report")
    parser.add_argument("--pending-file", default="", help="Where to write pending approvals (default: temp file)")
    args = parser.parse_args()

    pending_file = args.pending_file
    if not pending_file:
        pending_file = str(Path(tempfile.mkdtemp(prefix="approval-canary-")) / "pending_approvals.json")

    orch = ExecutionOrchestrator(mode=TradeMode.LIVE_APPROVAL, dry_run=True, pending_file=pending_file)
    sig = build_signal(args)
    results = orch.execute_signals([sig])

    payload: Dict[str, Any] = {
        "pending_file": pending_file,
        "result": results[0] if results else None,
        "pending_count": len(orch.state.pending_approvals),
    }
    if args.health_url:
        try:
            payload["health"] = _fetch_json(args.health_url)
        except Exception as exc:
            payload["health_error"] = str(exc)

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["result"] and payload["result"].get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
