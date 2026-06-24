from __future__ import annotations
import os
import time
import json
import logging
import subprocess
import shutil
from datetime import datetime, timezone
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in tests
    def load_dotenv(*args, **kwargs):
        return False

load_dotenv(override=False)

log = logging.getLogger(__name__)

class CBClient:
    def __init__(self, api_key: str | None = None, api_secret: str | None = None, timeout: int | None = None):
        api_key = api_key or os.getenv("COINBASE_API_KEY")
        api_secret = api_secret or os.getenv("COINBASE_API_SECRET")
        # allow override via env; default to 30s
        timeout = timeout or int(float(os.getenv("CB_TIMEOUT_S", "30")))
        self.timeout = timeout
        self.api_key = api_key
        self.api_secret = api_secret
        self.cli_env = os.getenv("COINBASE_CLI_ENV", "live")
        cli = os.getenv("COINBASE_CLI_PATH", "coinbase")
        self.cli = cli if shutil.which(cli) else (shutil.which("coinbase") or cli)

    def _cli_json(self, *args: str) -> dict:
        cmd = [self.cli, "-e", self.cli_env, *args, "--jq", "."]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        if out.returncode != 0:
            raise RuntimeError((out.stderr or out.stdout or "coinbase cli failed").strip())
        try:
            return json.loads(out.stdout)
        except Exception as exc:
            raise RuntimeError(f"coinbase cli returned non-JSON output: {exc}")
        
    def list_accounts(self) -> dict:
        return self._cli_json("balance")

    def best_bid_ask(self, product_ids: list[str]) -> dict:
        """
        Fetch best bid/ask for many products.
        - API expects product_ids as an array (string[]) -> encoded as repeated keys.
        - We also chunk (50 per call) and merge results.
        """
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

        for i in range(0, len(pids), 50):
            batch = pids[i:i+50]
            try:
                for pid in batch:
                    data = self._cli_json("products", "book", pid)
                    book = data.get("pricebook") or data.get("pricebooks") or data
                    if isinstance(book, dict):
                        merged["pricebooks"].append(book)
            except Exception:
                log.warning("best_bid_ask failed for %d products — synthesizing from candles", len(batch))
                merged["pricebooks"].extend(self._synthetic_books(batch))

        return merged

    def _synthetic_books(self, product_ids: list[str]) -> list[dict]:
        books = []
        now = int(time.time())
        for pid in product_ids:
            try:
                candles = self.public_candles(pid, now - 2 * 86400, now, granularity="ONE_HOUR", limit=48)
                rows = candles.get("candles", candles if isinstance(candles, list) else [])
                if not rows:
                    continue
                last = rows[-1]
                if isinstance(last, dict):
                    mid = float(last.get("close", 0) or 0)
                    spread = max(0.001, (float(last.get("high", mid)) - float(last.get("low", mid))) / max(mid, 0.01) / 2)
                else:
                    # tuple/list response fallback: [ts, low, high, open, close, volume]
                    _, lo, hi, op, cl, _vol = last
                    mid = float(cl)
                    spread = max(0.001, (float(hi) - float(lo)) / max(mid, 0.01) / 2)
                bid = max(0.0, mid * (1 - spread))
                ask = max(bid + 1e-9, mid * (1 + spread))
                books.append({"product_id": pid, "bids": [{"price": str(bid)}], "asks": [{"price": str(ask)}]})
            except Exception:
                continue
        return books



    # ---------- FIXED PREVIEW ----------
    def preview_order(self, side: str, product_id: str, *, base_size: str | None = None, quote_size: str | None = None) -> dict:
        side_u = side.upper()
        if side_u not in ("BUY", "SELL"):
            raise ValueError("side must be 'buy' or 'sell'")
        try:
            if side_u == "BUY":
                if quote_size:
                    return self._cli_json("orders", "preview", f"product_id={product_id}", "side=BUY", "type=market", f"quote_size={quote_size}")
                elif base_size:
                    return self._cli_json("orders", "preview", f"product_id={product_id}", "side=BUY", "type=market", f"base_size={base_size}")
                else:
                    raise ValueError("buy preview needs quote_size or base_size")
            else:  # SELL
                if not base_size:
                    raise ValueError("sell preview needs base_size")
                return self._cli_json("orders", "preview", f"product_id={product_id}", "side=SELL", "type=market", f"base_size={base_size}")
        except Exception as exc:
            return {
                "preview_id": f"synthetic-{product_id}-{side.lower()}",
                "product_id": product_id,
                "side": side_u,
                "status": "preview_error",
                "error": str(exc),
                "base_size": base_size,
                "quote_size": quote_size,
            }

    # ---------- CREATE ORDER (optional preview_id) ----------
    def create_market_order(
        self, side: str, product_id: str, *,
        base_size: str | None = None, quote_size: str | None = None,
        client_order_id: str = "", preview_id: str | None = None
    ) -> dict:
        side_u = side.upper()
        if side_u == "BUY":
            if quote_size:
                args = ["orders", "create", f"product_id={product_id}", "side=BUY", "type=market", f"quote_size={quote_size}"]
            elif base_size:
                args = ["orders", "create", f"product_id={product_id}", "side=BUY", "type=market", f"base_size={base_size}"]
            else:
                raise ValueError("buy order needs quote_size or base_size")
        else:
            if not base_size:
                raise ValueError("sell order needs base_size")
            args = ["orders", "create", f"product_id={product_id}", "side=SELL", "type=market", f"base_size={base_size}"]
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        if preview_id:
            args.append(f"preview_id={preview_id}")
        return self._cli_json(*args)

    # Convenience wrappers
    def market_order(self, side: str, product_id: str, base_size: str | None = None, quote_size: str | None = None, client_order_id: str = "", preview_id: str | None = None) -> dict:
        return self.create_market_order(side, product_id, base_size=base_size, quote_size=quote_size, client_order_id=client_order_id, preview_id=preview_id)

    # ---------- LIMIT ORDER ----------
    def create_limit_order(
        self, side: str, product_id: str, *,
        base_size: str, price: str,
        client_order_id: str = "", time_in_force: str = "GTC",
        post_only: bool = False,
    ) -> dict:
        side_u = side.upper()
        args = ["orders", "create", f"product_id={product_id}", f"side={side_u}",
                "type=limit", f"base_size={base_size}", f"limit_price={price}",
                f"time_in_force={time_in_force}"]
        if post_only:
            args.append("post_only=true")
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        return self._cli_json(*args)

    # ---------- STOP-LIMIT ORDER ----------
    def create_stop_limit_order(
        self, side: str, product_id: str, *,
        base_size: str, limit_price: str, stop_price: str,
        client_order_id: str = "", time_in_force: str = "GTC",
        stop_direction: str = "stop_direction_stop_up",
    ) -> dict:
        side_u = side.upper()
        args = ["orders", "create", f"product_id={product_id}", f"side={side_u}",
                "type=limit", f"base_size={base_size}",
                f"limit_price={limit_price}", f"stop_price={stop_price}",
                f"stop_direction={stop_direction}",
                f"time_in_force={time_in_force}"]
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        return self._cli_json(*args)

    # ---------- STOP MARKET ORDER ----------
    def create_stop_market_order(
        self, side: str, product_id: str, *,
        base_size: str, stop_price: str,
        client_order_id: str = "", stop_direction: str = "stop_direction_stop_up",
    ) -> dict:
        side_u = side.upper()
        args = ["orders", "create", f"product_id={product_id}", f"side={side_u}",
                "type=stop", f"base_size={base_size}", f"stop_price={stop_price}",
                f"stop_direction={stop_direction}"]
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        return self._cli_json(*args)

    # ---------- CANCEL ORDER ----------
    def cancel_order(self, order_id: str) -> dict:
        return self._cli_json("orders", "cancel", f"order_id={order_id}")

    # ---------- LIST ORDERS ----------
    def list_orders(self, product_id: str | None = None, status: str | None = None) -> list[dict]:
        args = ["orders", "list"]
        if product_id:
            args.append(f"product_id={product_id}")
        if status:
            args.append(f"order_status={status}")
        return self._cli_json(*args).get("orders", [])

    # ---------- GET PRODUCTS ----------
    def get_products(self, product_type: str | None = None) -> list[dict]:
        args = ["products", "list"]
        if product_type:
            args.append(f"product_type={product_type}")
        return self._cli_json(*args).get("products", [])

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
        start = datetime.fromtimestamp(int(start_unix), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        end = datetime.fromtimestamp(int(end_unix), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        gran = {
            "ONE_MINUTE": "1m",
            "FIVE_MINUTE": "5m",
            "FIFTEEN_MINUTE": "15m",
            "THIRTY_MINUTE": "30m",
            "ONE_HOUR": "1h",
            "TWO_HOUR": "2h",
            "FOUR_HOUR": "4h",
            "SIX_HOUR": "6h",
            "ONE_DAY": "1d",
        }.get(granularity, granularity)
        return self._cli_json("products", "candles", product_id, f"start={start}", f"end={end}", f"granularity={gran}", f"limit={int(limit)}")
