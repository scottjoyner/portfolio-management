#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from trading_system.core.exchange.coinbase_service import CoinbaseService
from trading_system.core.runtime.events import EventRecorder
from trading_system.core.runtime.models import RuntimeStatus, TradingEvent


def parse_products(raw: str) -> List[str]:
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


def build_status(products: List[str]) -> Dict[str, Any]:
    service = CoinbaseService()
    recorder = EventRecorder()
    coinbase_status = service.get_connection_status()

    prices: Dict[str, Any] = {}
    for product in products:
        try:
            prices[product] = service.get_price(product)
        except Exception as exc:  # keep status usable even when one product fails
            prices[product] = {"product_id": product, "error": str(exc)}

    top_balances: List[Dict[str, Any]] = []
    balance_error = None
    try:
        snapshot = service.get_balances_snapshot()
        accounts = snapshot.accounts
        top_balances = sorted(
            accounts,
            key=lambda item: float(item.get("available") or 0),
            reverse=True,
        )[:10]
    except Exception as exc:
        balance_error = str(exc)

    events = recorder.tail(limit=1)
    runtime = RuntimeStatus(
        mode=os.getenv("TRADING_MODE", "paper").lower(),
        live_trading_enabled=os.getenv("LIVE_TRADING_ENABLED", "false").lower() in {"1", "true", "yes", "on"},
        coinbase_connected=bool(coinbase_status.get("connected")),
        worker_status="unknown",
        event_log_status="available" if recorder.path.exists() else "empty",
    )

    payload = {
        "runtime": runtime.to_dict(),
        "coinbase": coinbase_status,
        "balances": {
            "top": top_balances,
            "error": balance_error,
        },
        "prices": prices,
        "last_event": events[-1] if events else None,
    }
    recorder.record(TradingEvent(source="script.status", event_type="status_checked", payload={"products": products}))
    return payload


def print_table(payload: Dict[str, Any]) -> None:
    runtime = payload["runtime"]
    coinbase = payload["coinbase"]
    print("Trading Runtime Status")
    print("======================")
    print(f"Mode:                 {runtime['mode']}")
    print(f"Live enabled:         {runtime['live_trading_enabled']}")
    print(f"Coinbase connected:   {coinbase['connected']}")
    print(f"Coinbase accounts:    {coinbase.get('account_count', 0)}")
    if coinbase.get("error"):
        print(f"Coinbase error:       {coinbase['error']}")
    print()
    print("Top balances")
    print("------------")
    for account in payload["balances"]["top"]:
        print(f"{account.get('currency', ''):<12} {account.get('available', ''):>20} hold={account.get('hold', '0')}")
    if payload["balances"].get("error"):
        print(f"Balance error: {payload['balances']['error']}")
    print()
    print("Prices")
    print("------")
    for product, price in payload["prices"].items():
        print(f"{product:<12} {price.get('price', price.get('error', 'n/a'))}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Show trading runtime/Coinbase read-only status.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument("--products", default="BTC-USD,ETH-USD,SOL-USD", help="Comma-separated products to price")
    args = parser.parse_args()

    payload = build_status(parse_products(args.products))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_table(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
