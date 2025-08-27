#!/usr/bin/env python3
import argparse, json
from app.exec.order_engine import fetch_today_signals, create_orders
from app.exec.broker_adapters import FidelityViaSnapTrade

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", choices=["fidelity","merrill"], required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cash", type=float, default=100000.0)
    args = ap.parse_args()

    cands = fetch_today_signals(limit=20)
    orders = create_orders(cands, cash=args.cash)
    if not orders:
        print("No orders to place.")
        return

    print("Proposed orders:")
    print(json.dumps(orders, indent=2))

    if args.dry_run:
        print("Dry run: not placing orders.")
        return

    if args.broker == "fidelity":
        broker = FidelityViaSnapTrade()
        for o in orders:
            payload = {"symbol": o["symbol"], "orderType": "MARKET", "action":"BUY", "quantity": o["qty"]}
            prev = broker.preview(payload)
            print("Preview:", json.dumps(prev, indent=2))
            res = broker.place(payload)
            print("Placed:", json.dumps(res, indent=2))
    else:
        print("Merrill has no public trading API. Export orders for manual placement.")

if __name__ == "__main__":
    main()
