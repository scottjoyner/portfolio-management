from __future__ import annotations
import time, typing as t
from coinbase.rest import RESTClient

class CBClient:
    """
    Thin wrapper around coinbase-advanced-py to centralize REST calls.
    """
    def __init__(self, api_key: str | None = None, api_secret: str | None = None, timeout: int = 10):
        self.client = RESTClient(api_key=api_key, api_secret=api_secret, timeout=timeout)

    # ---------- Accounts / balances ----------
    def list_accounts(self) -> dict:
        resp = self.client.get_accounts()
        return resp.to_dict() if hasattr(resp, "to_dict") else resp

    # ---------- Prices / market data ----------
    def best_bid_ask(self, product_ids: list[str]) -> dict:
        params = {"product_ids": ",".join(product_ids)}
        resp = self.client.get("/api/v3/brokerage/best_bid_ask", params=params)
        return resp if isinstance(resp, dict) else getattr(resp, "to_dict", lambda: resp)()

    def public_candles(self, product_id: str, start_unix: int, end_unix: int, granularity: str = "ONE_HOUR", limit: int = 300) -> dict:
        params = {"start": str(start_unix), "end": str(end_unix), "granularity": granularity, "limit": limit}
        path = f"/api/v3/brokerage/market/products/{product_id}/candles"
        return self.client.get(path, params=params)

    # ---------- Orders ----------
    def preview_order(self, side: str, product_id: str, base_size: str | None = None, quote_size: str | None = None) -> dict:
        data = {
            "order_configuration": {
                "market_market_ioc": {"quote_size": quote_size} if side.lower()=="buy" else {"base_size": base_size}
            },
            "side": side.upper(),
            "product_id": product_id,
            "client_order_id": ""
        }
        return self.client.post("/api/v3/brokerage/orders/preview", data=data)

    def market_order(self, side: str, product_id: str, base_size: str | None = None, quote_size: str | None = None, client_order_id: str = "") -> dict:
        if side.lower() == "buy":
            return self.client.market_order_buy(client_order_id=client_order_id, product_id=product_id, quote_size=str(quote_size))
        else:
            return self.client.market_order_sell(client_order_id=client_order_id, product_id=product_id, base_size=str(base_size))
