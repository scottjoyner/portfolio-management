from __future__ import annotations

import json
from typing import Any

import httpx

from exchange.coinbase.auth.jwt import build_jwt_token


class CoinbaseRestClient:
    BASE_URL = "https://api.coinbase.com"

    def __init__(self, api_key: str, api_secret: str, passphrase: str | None = None, base_url: str = BASE_URL) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.base_url = base_url
        self._client = httpx.AsyncClient(base_url=base_url, timeout=15.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def _request(self, method: str, path: str, body_str: str = "") -> dict[str, Any]:
        token = build_jwt_token(self.api_key, self.api_secret, method, path, body_str)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        content = body_str.encode() if body_str else None
        response = await self._client.request(method, path, headers=headers, content=content)
        response.raise_for_status()
        return response.json()

    async def _get(self, path: str) -> dict[str, Any]:
        return await self._request("GET", path)

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        serialized = json.dumps(body, separators=(",", ":"))
        return await self._request("POST", path, body_str=serialized)

    async def list_products(self) -> list[dict[str, Any]]:
        data = await self._get("/api/v3/brokerage/products")
        return data.get("products", [])

    async def get_product(self, product_id: str) -> dict[str, Any] | None:
        data = await self._get(f"/api/v3/brokerage/products/{product_id}")
        return data.get("product")

    async def get_product_book(self, product_id: str, limit: int = 25) -> dict[str, Any]:
        return await self._get(f"/api/v3/brokerage/products/{product_id}/book?limit={limit}")

    async def list_accounts(self) -> list[dict[str, Any]]:
        data = await self._get("/api/v3/brokerage/accounts")
        return data.get("accounts", [])

    async def get_account(self, account_id: str) -> dict[str, Any] | None:
        data = await self._get(f"/api/v3/brokerage/accounts/{account_id}")
        return data.get("account")

    async def create_order(self, order: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/api/v3/brokerage/orders", order)

    async def cancel_orders(self, order_ids: list[str]) -> dict[str, Any]:
        return await self._post("/api/v3/brokerage/orders/batch_cancel", {"order_ids": order_ids})

    async def list_orders(self, **params: Any) -> list[dict[str, Any]]:
        qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        path = "/api/v3/brokerage/orders/history" + (f"?{qs}" if qs else "")
        data = await self._get(path)
        return data.get("orders", [])

    async def get_order(self, order_id: str) -> dict[str, Any] | None:
        data = await self._get(f"/api/v3/brokerage/orders/{order_id}")
        return data.get("order")

    async def list_fills(self, **params: Any) -> list[dict[str, Any]]:
        qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        path = "/api/v3/brokerage/fills" + (f"?{qs}" if qs else "")
        data = await self._get(path)
        return data.get("fills", [])

    async def list_portfolios(self) -> list[dict[str, Any]]:
        data = await self._get("/api/v3/brokerage/portfolios")
        return data.get("portfolios", [])

    async def get_portfolio(self, portfolio_id: str) -> dict[str, Any] | None:
        data = await self._get(f"/api/v3/brokerage/portfolios/{portfolio_id}")
        return data.get("portfolio")

    async def get_market_trades(self, product_id: str, limit: int = 100) -> list[dict[str, Any]]:
        data = await self._get(f"/api/v3/brokerage/products/{product_id}/trades?limit={limit}")
        return data.get("trades", [])

    async def get_candles(self, product_id: str, granularity: str = "ONE_HOUR", start: str | None = None, end: str | None = None) -> list[dict[str, Any]]:
        params = f"granularity={granularity}"
        if start:
            params += f"&start={start}"
        if end:
            params += f"&end={end}"
        data = await self._get(f"/api/v3/brokerage/products/{product_id}/candles?{params}")
        return data.get("candles", [])
