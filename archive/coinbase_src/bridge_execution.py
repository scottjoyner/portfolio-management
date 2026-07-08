"""Bridge script for the JS execution engine to call Coinbase V3 API via Python.

Usage: python3 bridge_execution.py '<json-command>'

This script is called as a subprocess by the CoinbaseBrokerAdapter.
It handles all Coinbase API interactions and returns JSON responses.
"""

from __future__ import annotations
import json
import sys
import time
import os
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from coinbase.src.cb_client import CBClient


def _fmt_base(v: Any) -> str:
    return f"{float(v):.8f}".rstrip("0").rstrip(".") or "0"


def _fmt_quote(v: Any) -> str:
    return f"{float(v):.2f}".rstrip("0").rstrip(".") or "0"


def list_accounts(cb: CBClient) -> list[dict[str, Any]]:
    resp = cb.list_accounts()
    accounts = resp.get("accounts", resp)
    if isinstance(accounts, list):
        return [
            {
                "currency": a.get("currency", ""),
                "balance": float(a.get("available_balance", {}).get("value", a.get("balance", 0)) or 0),
                "available": float(a.get("available_balance", {}).get("value", a.get("available", 0)) or 0),
                "hold": float(a.get("hold", {}).get("value", a.get("hold", 0)) or 0),
            }
            for a in accounts
        ]
    return []


def best_bid_ask(cb: CBClient, payload: dict[str, Any]) -> dict[str, Any]:
    product_ids = payload.get("product_ids", [])
    return cb.best_bid_ask(product_ids)


def preview_order(cb: CBClient, payload: dict[str, Any]) -> dict[str, Any]:
    side = payload.get("side", "buy")
    product_id = payload.get("product_id", "")
    base_size = payload.get("base_size")
    quote_size = payload.get("quote_size")

    kwargs = {}
    if base_size:
        kwargs["base_size"] = _fmt_base(base_size)
    if quote_size:
        kwargs["quote_size"] = _fmt_quote(quote_size)

    if side.lower() == "buy":
        result = cb.preview_order("buy", product_id, **kwargs)
    else:
        result = cb.preview_order("sell", product_id, **kwargs)
    return result if isinstance(result, dict) else {"preview": str(result)}


def submit_order(cb: CBClient, payload: dict[str, Any]) -> dict[str, Any]:
    side = payload.get("side", "buy")
    product_id = payload.get("product_id", "")
    base_size = payload.get("base_size")
    quote_size = payload.get("quote_size")

    kwargs = {"client_order_id": payload.get("client_order_id", "")}
    if base_size:
        kwargs["base_size"] = _fmt_base(base_size)
    if quote_size:
        kwargs["quote_size"] = _fmt_quote(quote_size)

    result = cb.market_order(side, product_id, **kwargs)
    return result if isinstance(result, dict) else {"order": str(result)}


def get_candles(cb: CBClient, payload: dict[str, Any]) -> list[dict[str, Any]]:
    product_id = payload.get("product_id", "")
    start = int(payload.get("start_unix", int(time.time()) - 86400))
    end = int(payload.get("end_unix", int(time.time())))
    granularity = payload.get("granularity", "ONE_HOUR")
    limit = int(payload.get("limit", 300))

    result = cb.public_candles(product_id, start, end, granularity, limit)
    candles = result.get("candles", []) if isinstance(result, dict) else []
    return candles


def list_orders(cb: CBClient, payload: dict[str, Any]) -> list[dict[str, Any]]:
    product_id = payload.get("product_id")
    order_status = payload.get("order_status", "OPEN")
    limit = int(payload.get("limit", 50))

    kwargs = {"order_status": order_status, "limit": limit}
    if product_id:
        kwargs["product_id"] = product_id

    raw = cb.client.list_orders(**kwargs)
    data = raw.to_dict() if hasattr(raw, "to_dict") else raw
    orders = data.get("orders", []) if isinstance(data, dict) else data
    return [
        {
            "order_id": o.get("order_id", ""),
            "product_id": o.get("product_id", ""),
            "side": o.get("side", ""),
            "status": o.get("status", ""),
            "size": o.get("size", o.get("filled_size", "0")),
            "filled_size": o.get("filled_size", "0"),
            "price": o.get("price", "0"),
            "average_filled_price": o.get("average_filled_price", "0"),
            "filled_value": o.get("filled_value", "0"),
            "client_order_id": o.get("client_order_id", ""),
            "leaves_quantity": o.get("leaves_quantity", "0"),
            "created_time": o.get("created_time", o.get("created_at", "")),
        }
        for o in (orders if isinstance(orders, list) else [])
    ]


def get_order(cb: CBClient, payload: dict[str, Any]) -> dict[str, Any] | None:
    order_id = payload.get("order_id", "")
    if not order_id:
        return None
    raw = cb.client.get_order(order_id)
    return raw.to_dict() if hasattr(raw, "to_dict") else raw


def list_fills(cb: CBClient, payload: dict[str, Any]) -> list[dict[str, Any]]:
    order_id = payload.get("order_id")
    product_id = payload.get("product_id")
    limit = int(payload.get("limit", 100))

    kwargs: dict[str, Any] = {"limit": limit}
    if order_id:
        kwargs["order_id"] = order_id
    if product_id:
        kwargs["product_id"] = product_id

    raw = cb.client.get_fills(**kwargs)
    data = raw.to_dict() if hasattr(raw, "to_dict") else raw
    fills = data.get("fills", []) if isinstance(data, dict) else data
    return [
        {
            "fill_id": f.get("fill_id", f.get("entry_id", "")),
            "order_id": f.get("order_id", ""),
            "product_id": f.get("product_id", ""),
            "side": f.get("side", ""),
            "liquidity": f.get("liquidity_indicator", ""),
            "size": f.get("filled_size", f.get("size", "0")),
            "price": f.get("price", "0"),
            "fee": f.get("commission", f.get("fee", "0")),
            "value": f.get("filled_value", "0"),
            "created_at": f.get("created_at", f.get("created_at_time", "")),
        }
        for f in (fills if isinstance(fills, list) else [])
    ]


def get_product(cb: CBClient, payload: dict[str, Any]) -> dict[str, Any] | None:
    product_id = payload.get("product_id", "")
    if not product_id:
        return None
    raw = cb.client.get_product(product_id)
    return raw.to_dict() if hasattr(raw, "to_dict") else raw


def get_products(cb: CBClient) -> list[dict[str, Any]]:
    raw = cb.client.get_products()
    data = raw.to_dict() if hasattr(raw, "to_dict") else raw
    products = data.get("products", []) if isinstance(data, dict) else data
    return [
        {
            "product_id": p.get("product_id", ""),
            "price": p.get("price", "0"),
            "price_percentage_change_24h": p.get("price_percentage_change_24h", "0"),
            "volume_24h": p.get("volume_24h", "0"),
            "status": p.get("status", ""),
        }
        for p in (products if isinstance(products, list) else [])
    ]


def health(cb: CBClient | None = None) -> dict[str, Any]:
    try:
        if cb is None:
            cb = CBClient()
        cb.list_accounts()
        return {"ok": True, "data": {"status": "healthy"}}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main() -> None:
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "no_command_provided"}))
        sys.exit(1)

    try:
        command = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"ok": False, "error": f"invalid_json: {e}"}))
        sys.exit(1)

    action = command.get("action", "")
    payload = command.get("payload", {})

    try:
        cb = CBClient()
        cb.list_accounts()
    except Exception as e:
        if action == "health":
            print(json.dumps(health(None)))
            return
        print(json.dumps({"ok": False, "error": f"coinbase_auth_failed: {e}"}))
        return

    try:
        if action == "list_accounts":
            result = list_accounts(cb)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "best_bid_ask":
            result = best_bid_ask(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "preview_order":
            result = preview_order(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "submit_order":
            result = submit_order(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "get_candles":
            result = get_candles(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "list_orders":
            result = list_orders(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "get_order":
            result = get_order(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "list_fills":
            result = list_fills(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "get_product":
            result = get_product(cb, payload)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "get_products":
            result = get_products(cb)
            print(json.dumps({"ok": True, "data": result}))
        elif action == "health":
            print(json.dumps(health(cb)))
        else:
            print(json.dumps({"ok": False, "error": f"unknown_action: {action}"}))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))


if __name__ == "__main__":
    main()
