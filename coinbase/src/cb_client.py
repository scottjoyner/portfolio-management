from __future__ import annotations
import os
from dotenv import load_dotenv
from coinbase.rest import RESTClient

load_dotenv(override=False)

class CBClient:
    def __init__(self, api_key: str | None = None, api_secret: str | None = None, timeout: int | None = None):
        api_key = api_key or os.getenv("COINBASE_API_KEY")
        api_secret = api_secret or os.getenv("COINBASE_API_SECRET")
        # allow override via env; default to 30s
        timeout = timeout or int(float(os.getenv("CB_TIMEOUT_S", "30")))
        self.client = RESTClient(api_key=api_key, api_secret=api_secret, timeout=timeout)
        
    def list_accounts(self) -> dict:
        resp = self.client.get_accounts()
        return resp.to_dict() if hasattr(resp, "to_dict") else resp

    def best_bid_ask(self, product_ids: list[str]) -> dict:
        """
        Fetch best bid/ask for many products.
        - API expects product_ids as an array (string[]) -> encoded as repeated keys.
        - We also chunk (50 per call) and merge results.
        """
        # sanitize & dedupe
        pids = []
        seen = set()
        for p in product_ids or []:
            if not p:
                continue
            pid = p.strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            pids.append(pid)

        merged = {"pricebooks": []}

        # If no product_ids given, API returns all pricebooks (we'll just return that)
        if not pids:
            data = self.client.get("/api/v3/brokerage/best_bid_ask")
            data = data if isinstance(data, dict) else getattr(data, "to_dict", lambda: data)()
            books = data.get("pricebooks") if isinstance(data, dict) else data
            if books:
                merged["pricebooks"].extend(books)
            return merged

        # Chunk and request
        for i in range(0, len(pids), 50):
            batch = pids[i:i+50]
            params = {"product_ids": batch}  # << key change: dict with list value
            data = self.client.get("/api/v3/brokerage/best_bid_ask", params=params)
            data = data if isinstance(data, dict) else getattr(data, "to_dict", lambda: data)()
            books = None
            if isinstance(data, dict):
                books = data.get("pricebooks") or data.get("best_bid_ask")
            elif isinstance(data, list):
                books = data
            if books:
                merged["pricebooks"].extend(books)

        return merged



    # ---------- FIXED PREVIEW ----------
    def preview_order(self, side: str, product_id: str, *, base_size: str | None = None, quote_size: str | None = None) -> dict:
        side_u = side.upper()
        if side_u not in ("BUY", "SELL"):
            raise ValueError("side must be 'buy' or 'sell'")
        cfg = {}
        if side_u == "BUY":
            if quote_size:
                cfg = {"market_market_ioc": {"quote_size": str(quote_size)}}
            elif base_size:
                cfg = {"market_market_ioc": {"base_size": str(base_size)}}
            else:
                raise ValueError("buy preview needs quote_size or base_size")
        else:  # SELL
            if not base_size:
                raise ValueError("sell preview needs base_size")
            cfg = {"market_market_ioc": {"base_size": str(base_size)}}

        body = {"product_id": product_id, "side": side_u, "order_configuration": cfg}
        return self.client.post("/api/v3/brokerage/orders/preview", data=body)

    # ---------- CREATE ORDER (optional preview_id) ----------
    def create_market_order(
        self, side: str, product_id: str, *,
        base_size: str | None = None, quote_size: str | None = None,
        client_order_id: str = "", preview_id: str | None = None
    ) -> dict:
        side_u = side.upper()
        cfg = {}
        if side_u == "BUY":
            if quote_size:
                cfg = {"market_market_ioc": {"quote_size": str(quote_size)}}
            elif base_size:
                cfg = {"market_market_ioc": {"base_size": str(base_size)}}
            else:
                raise ValueError("buy order needs quote_size or base_size")
        else:
            if not base_size:
                raise ValueError("sell order needs base_size")
            cfg = {"market_market_ioc": {"base_size": str(base_size)}}

        body = {
            "client_order_id": client_order_id,  # allowed here
            "product_id": product_id,
            "side": side_u,
            "order_configuration": cfg,
        }
        if preview_id:
            body["preview_id"] = preview_id  # associate with prior preview
        return self.client.post("/api/v3/brokerage/orders", data=body)

    # Convenience wrappers (SDK has these too, but this keeps it consistent)
    def market_order(self, side: str, product_id: str, base_size: str | None = None, quote_size: str | None = None, client_order_id: str = "", preview_id: str | None = None) -> dict:
        return self.create_market_order(side, product_id, base_size=base_size, quote_size=quote_size, client_order_id=client_order_id, preview_id=preview_id)
    # inside class CBClient:

    def public_candles(
        self,
        product_id: str,
        start_unix: int,
        end_unix: int,
        granularity: str = "ONE_HOUR",
        limit: int = 300,
    ) -> dict:
        """
        Public market candles (no auth needed).
        start/end are UNIX seconds; granularity is a Coinbase enum string
        (e.g., ONE_MINUTE, ONE_HOUR, ONE_DAY). Returns a dict with 'candles'.
        """
        path = f"/api/v3/brokerage/market/products/{product_id}/candles"
        params = {
            "start": str(int(start_unix)),
            "end": str(int(end_unix)),
            "granularity": granularity,
            "limit": int(limit),
        }
        resp = self.client.get(path, params=params)
        return resp if isinstance(resp, dict) else getattr(resp, "to_dict", lambda: resp)()
