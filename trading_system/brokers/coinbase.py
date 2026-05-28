from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from brokers.base import (
    BrokerAccount, BrokerAdapter, BrokerFill, BrokerOrder, BrokerPosition, OrderStatus, TimeInForce,
)
from exchange.coinbase.rest.client import CoinbaseRestClient


class CoinbaseBrokerAdapter(BrokerAdapter):
    def __init__(self, client: CoinbaseRestClient | None = None, credentials: dict[str, str] | None = None) -> None:
        self._client = client or CoinbaseRestClient(
            api_key=credentials.get("api_key", "") if credentials else "",
            api_secret=credentials.get("api_secret", "") if credentials else "",
        )
        self._capability_cache: dict[str, Any] = {}

    def broker_name(self) -> str:
        return "coinbase"

    async def get_accounts(self) -> list[BrokerAccount]:
        raw = await self._client.get_accounts()
        return [
            BrokerAccount(
                account_id=a.get("uuid", a.get("id", "")),
                name=a.get("name", ""),
                currency=a.get("currency", "USD"),
                available_balance=Decimal(str(a.get("available_balance", {}).get("value", "0"))),
                hold_balance=Decimal(str(a.get("hold", {}).get("value", "0"))),
                total_balance=Decimal(str(a.get("available_balance", {}).get("value", "0")))
                + Decimal(str(a.get("hold", {}).get("value", "0"))),
            )
            for a in (raw if isinstance(raw, list) else raw.get("accounts", []))
        ]

    async def get_account(self, account_id: str) -> BrokerAccount:
        accounts = await self.get_accounts()
        for a in accounts:
            if a.account_id == account_id:
                return a
        raise ValueError(f"account {account_id} not found")

    async def preview_order(self, order: BrokerOrder) -> tuple[bool, str]:
        product = await self._get_product_info(order.product_id)
        base_increment = Decimal(str(product.get("base_increment", "0.00000001")))
        quote_increment = Decimal(str(product.get("quote_increment", "0.01")))

        if order.size % base_increment != 0:
            return False, f"size must be multiple of {base_increment}"
        if order.price and order.price % quote_increment != 0:
            return False, f"price must be multiple of {quote_increment}"
        return True, "preview passed"

    async def submit_order(self, order: BrokerOrder) -> BrokerOrder:
        payload = self._build_order_payload(order)
        result = await self._client.create_order(**payload)
        broker_id = result.get("order_id", result.get("id", ""))
        order.broker_order_id = broker_id
        order.status = OrderStatus.OPEN
        return order

    async def cancel_order(self, broker_order_id: str) -> bool:
        return await self._client.cancel_order(broker_order_id)

    async def get_order(self, broker_order_id: str) -> BrokerOrder | None:
        raw = await self._client.get_order(broker_order_id)
        if not raw:
            return None
        return self._raw_to_order(raw)

    async def list_orders(
        self, product_id: str | None = None, status: OrderStatus | None = None,
    ) -> list[BrokerOrder]:
        params: dict[str, Any] = {}
        if product_id:
            params["product_id"] = product_id
        if status:
            params["order_status"] = status.value.upper()
        raw_list = await self._client.list_orders(**params)
        return [self._raw_to_order(o) for o in (raw_list if isinstance(raw_list, list) else raw_list.get("orders", []))]

    async def get_fills(self, broker_order_id: str) -> list[BrokerFill]:
        raw = await self._client.get_fills(order_id=broker_order_id)
        return [
            BrokerFill(
                fill_id=f.get("fill_id", f.get("trade_id", "")),
                broker_order_id=broker_order_id,
                product_id=f.get("product_id", ""),
                side=f.get("side", ""),
                size=Decimal(str(f.get("size", "0"))),
                price=Decimal(str(f.get("price", "0"))),
                notional=Decimal(str(f.get("size", "0"))) * Decimal(str(f.get("price", "0"))),
                fee=Decimal(str(f.get("fee", "0"))),
                liquidity=f.get("liquidity", "TAKER"),
                filled_at=datetime.fromisoformat(f["timestamp"].replace("Z", "+00:00")) if "timestamp" in f else datetime.utcnow(),
            )
            for f in (raw if isinstance(raw, list) else raw.get("fills", []))
        ]

    async def get_positions(self, product_id: str | None = None) -> list[BrokerPosition]:
        raw = await self._client.get_positions()
        positions = raw if isinstance(raw, list) else raw.get("positions", [])
        result = []
        for p in positions:
            pid = p.get("product_id", "")
            if product_id and pid != product_id:
                continue
            result.append(BrokerPosition(
                product_id=pid,
                side="long" if float(p.get("position_size", "0")) > 0 else "short",
                size=Decimal(str(p.get("position_size", "0"))),
                entry_price=Decimal(str(p.get("entry_price", "0"))),
                current_price=Decimal(str(p.get("current_price", "0"))),
                unrealized_pnl=Decimal(str(p.get("unrealized_pnl", "0"))),
                realized_pnl=Decimal(str(p.get("realized_pnl", "0"))),
            ))
        return result

    async def list_products(self) -> list[dict[str, Any]]:
        raw = await self._client.list_products()
        products = raw if isinstance(raw, list) else raw.get("products", [])
        self._capability_cache = {p["product_id"]: p for p in products}
        return products

    async def get_product(self, product_id: str) -> dict[str, Any] | None:
        return await self._client.get_product(product_id)

    async def get_market_price(self, product_id: str) -> Decimal | None:
        product = await self._client.get_product(product_id)
        if product and "price" in product:
            return Decimal(str(product["price"]))
        ticker = await self._client.get_product_ticker(product_id)
        if ticker and "price" in ticker:
            return Decimal(str(ticker["price"]))
        return None

    async def health_check(self) -> dict[str, Any]:
        try:
            prod = await self._client.list_products()
            return {
                "status": "healthy",
                "broker": "coinbase",
                "products_count": len(prod) if isinstance(prod, list) else len(prod.get("products", [])),
            }
        except Exception as e:
            return {"status": "unhealthy", "broker": "coinbase", "error": str(e)}

    def get_capability(self, product_id: str) -> dict[str, Any]:
        return self._capability_cache.get(product_id, {})

    async def get_exchange_capability_matrix(self) -> list[dict[str, Any]]:
        products = await self.list_products()
        return [
            {
                "product_id": p["product_id"],
                "base_currency": p.get("base_currency", ""),
                "quote_currency": p.get("quote_currency", ""),
                "base_increment": p.get("base_increment"),
                "quote_increment": p.get("quote_increment"),
                "min_order_size": p.get("base_min_size"),
                "max_order_size": p.get("base_max_size"),
                "price_decimals": p.get("quote_decimals"),
                "order_types": p.get("supported_order_types", []),
                "fees": {"maker_rate": p.get("maker_fee_rate", "0"), "taker_rate": p.get("taker_fee_rate", "0")},
                "trading_disabled": p.get("trading_disabled", False),
                "status": "disabled" if p.get("trading_disabled", False) else "active",
            }
            for p in products
        ]

    def _build_order_payload(self, order: BrokerOrder) -> dict[str, Any]:
        return {
            "client_order_id": order.client_order_id,
            "product_id": order.product_id,
            "side": order.side.upper(),
            "order_configuration": {
                "limit_limit_gtc": {
                    "base_size": str(order.size),
                    "limit_price": str(order.price) if order.price else "0",
                }
            },
        }

    def _raw_to_order(self, raw: dict[str, Any]) -> BrokerOrder:
        return BrokerOrder(
            broker_order_id=raw.get("order_id", raw.get("id", "")),
            client_order_id=raw.get("client_order_id", ""),
            account_id=raw.get("account_id", raw.get("portfolio", "")),
            product_id=raw.get("product_id", ""),
            side=raw.get("side", "").lower(),
            order_type=raw.get("order_type", raw.get("type", "limit")),
            size=Decimal(str(raw.get("size", raw.get("base_size", "0")))),
            price=Decimal(str(raw.get("price", raw.get("limit_price", "0")))) if raw.get("price") or raw.get("limit_price") else None,
            status=OrderStatus(raw.get("status", "").lower()) if raw.get("status") else OrderStatus.OPEN,
            filled_size=Decimal(str(raw.get("filled_size", raw.get("filled_value", "0")))),
            fee=Decimal(str(raw.get("fees", "0"))),
            created_at=datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00")) if "created_at" in raw else datetime.utcnow(),
        )
