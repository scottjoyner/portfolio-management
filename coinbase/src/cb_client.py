from __future__ import annotations
import os
import time
import json
import logging
import subprocess
import shutil
import threading
from datetime import datetime, timezone
from collections import deque
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in tests
    def load_dotenv(*args, **kwargs):
        return False

load_dotenv(override=False)

log = logging.getLogger(__name__)


class RateLimiter:
    """Token bucket rate limiter for API calls."""
    
    def __init__(self, max_calls: int = 10, period: float = 1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()
        self._lock = threading.Lock()
    
    def acquire(self):
        with self._lock:
            now = time.time()
            # Remove old calls outside the window
            while self.calls and self.calls[0] <= now - self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                # Wait until oldest call expires
                sleep_time = self.calls[0] + self.period - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # Re-check after sleep
                now = time.time()
                while self.calls and self.calls[0] <= now - self.period:
                    self.calls.popleft()
            self.calls.append(time.time())


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
        
        # Rate limiter: 10 calls/second default (adjustable via env)
        max_calls = int(os.getenv("CB_RATE_LIMIT_CALLS", "10"))
        period = float(os.getenv("CB_RATE_LIMIT_PERIOD", "1.0"))
        self._rate_limiter = RateLimiter(max_calls=max_calls, period=period)

    def _cli_json(self, *args: str) -> dict:
        self._rate_limiter.acquire()
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
    
    def get_positions(self) -> list[dict]:
        """Get current open positions from Coinbase.
        
        Returns list of positions with product_id, side, size, entry_price, unrealized_pnl.
        Uses the 'portfolio' endpoint if available, otherwise synthesizes from orders.
        """
        try:
            # Try portfolio endpoint first (Advanced Trade)
            data = self._cli_json("portfolio", "list")
            positions = data.get("positions", [])
            result = []
            for pos in positions:
                if float(pos.get("size", 0)) > 0:
                    result.append({
                        "product_id": pos.get("product_id"),
                        "side": "LONG" if float(pos.get("size", 0)) > 0 else "SHORT",
                        "size": float(pos.get("size", 0)),
                        "entry_price": float(pos.get("average_entry_price", 0)),
                        "unrealized_pnl": float(pos.get("unrealized_pnl", 0)),
                        "leverage": float(pos.get("leverage", 1.0)),
                    })
            if result:
                return result
        except Exception as e:
            log.debug(f"portfolio list failed: {e}, falling back to orders")
        
        # Fallback: synthesize from recent filled orders
        return self._synthesize_positions_from_orders()
    
    def _synthesize_positions_from_orders(self) -> list[dict]:
        """Synthesize positions from recent filled orders."""
        try:
            orders = self.list_orders(status="FILLED")
            positions: dict[str, dict] = {}
            for order in orders:
                if order.get("status") != "FILLED":
                    continue
                pid = order.get("product_id")
                side = order.get("side", "").upper()
                size = float(order.get("filled_size", 0))
                price = float(order.get("average_filled_price", 0))
                if not pid or size <= 0:
                    continue
                if pid not in positions:
                    positions[pid] = {"product_id": pid, "side": "", "size": 0.0, "entry_price": 0.0, "unrealized_pnl": 0.0, "leverage": 1.0}
                pos = positions[pid]
                if not pos["side"]:
                    pos["side"] = "LONG" if side == "BUY" else "SHORT"
                if pos["side"] == "LONG" and side == "BUY":
                    # Adding to long
                    total_size = pos["size"] + size
                    pos["entry_price"] = (pos["entry_price"] * pos["size"] + price * size) / total_size
                    pos["size"] = total_size
                elif pos["side"] == "SHORT" and side == "SELL":
                    # Adding to short
                    total_size = pos["size"] + size
                    pos["entry_price"] = (pos["entry_price"] * pos["size"] + price * size) / total_size
                    pos["size"] = total_size
                elif pos["side"] == "LONG" and side == "SELL":
                    # Reducing long
                    pos["size"] -= size
                elif pos["side"] == "SHORT" and side == "BUY":
                    # Reducing short
                    pos["size"] -= size
            
            # Filter out closed positions
            return [p for p in positions.values() if p["size"] > 1e-9]
        except Exception as e:
            log.warning(f"Failed to synthesize positions: {e}")
            return []

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
            collected = 0
            try:
                for pid in batch:
                    data = self._cli_json("products", "book", pid)
                    book = data.get("pricebook") or data.get("pricebooks") or data
                    if isinstance(book, dict):
                        merged["pricebooks"].append(book)
                        collected += 1
                    elif isinstance(book, list):
                        for item in book:
                            if isinstance(item, dict):
                                merged["pricebooks"].append(item)
                                collected += 1
                            elif isinstance(item, list):
                                nested = [b for b in item if isinstance(b, dict)]
                                merged["pricebooks"].extend(nested)
                                collected += len(nested)
            except Exception:
                log.warning("best_bid_ask failed for %d products — synthesizing from candles", len(batch))
                merged["pricebooks"].extend(self._synthetic_books(batch))
                continue

            if collected == 0:
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
