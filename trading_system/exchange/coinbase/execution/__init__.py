from __future__ import annotations

from typing import Any


def build_market_order(product_id: str, side: str, size: str) -> dict[str, Any]:
    return {
        "client_order_id": "",
        "product_id": product_id,
        "side": side.upper(),
        "order_configuration": {
            "market_market_ioc": {
                "quote_size": size,
            }
        },
    }


def build_limit_order(product_id: str, side: str, size: str, price: str, post_only: bool = True) -> dict[str, Any]:
    return {
        "client_order_id": "",
        "product_id": product_id,
        "side": side.upper(),
        "order_configuration": {
            "limit_limit_gtc": {
                "base_size": size,
                "limit_price": price,
                "post_only": post_only,
            }
        },
    }


def build_stop_order(product_id: str, side: str, size: str, stop_price: str, limit_price: str) -> dict[str, Any]:
    return {
        "client_order_id": "",
        "product_id": product_id,
        "side": side.upper(),
        "order_configuration": {
            "stop_limit_stop_limit_gtc": {
                "base_size": size,
                "limit_price": limit_price,
                "stop_price": stop_price,
                "stop_direction": "STOP_DIRECTION_STOP_DOWN" if side.upper() == "SELL" else "STOP_DIRECTION_STOP_UP",
            }
        },
    }
