from __future__ import annotations

from decimal import Decimal
from typing import Any


def normalize_product(product: dict[str, Any]) -> dict[str, Any]:
    return {
        "product_id": product.get("product_id", ""),
        "price": product.get("price", ""),
        "base_currency": product.get("base_currency", ""),
        "quote_currency": product.get("quote_currency", ""),
        "base_increment": product.get("base_increment", ""),
        "quote_increment": product.get("quote_increment", ""),
        "base_min_size": product.get("base_min_size", ""),
        "base_max_size": product.get("base_max_size", ""),
        "min_market_funds": product.get("min_market_funds", ""),
        "max_market_funds": product.get("max_market_funds", ""),
        "status": product.get("status", ""),
        "trading_disabled": product.get("trading_disabled", False),
        "fx_stablecoin": product.get("fx_stablecoin", False),
    }


def normalize_account(account: dict[str, Any]) -> dict[str, Any]:
    bal = account.get("available_balance", {})
    hold = account.get("hold", {})
    return {
        "uuid": account.get("uuid", ""),
        "name": account.get("name", ""),
        "currency": account.get("currency", ""),
        "available_balance": Decimal(str(bal.get("value", "0"))),
        "hold": Decimal(str(hold.get("value", "0"))),
        "ledger_balance": Decimal(str(account.get("ledger_balance", {}).get("value", "0"))),
    }


def normalize_order(order: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_id": order.get("order_id", ""),
        "product_id": order.get("product_id", ""),
        "side": order.get("side", ""),
        "status": order.get("status", ""),
        "size": order.get("size", ""),
        "price": order.get("price", ""),
        "filled_size": order.get("filled_size", "0"),
        "filled_value": order.get("filled_value", "0"),
        "created_at": order.get("created_at", ""),
        "commission": order.get("commission", "0"),
    }


def normalize_fill(fill: dict[str, Any]) -> dict[str, Any]:
    return {
        "fill_id": fill.get("fill_id", ""),
        "order_id": fill.get("order_id", ""),
        "product_id": fill.get("product_id", ""),
        "side": fill.get("side", ""),
        "price": fill.get("price", ""),
        "size": fill.get("size", ""),
        "fee": fill.get("commission", "0"),
        "liquidity": fill.get("liquidity", ""),
        "created_at": fill.get("created_at", ""),
    }
