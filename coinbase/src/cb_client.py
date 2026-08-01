from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Dict

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
            while self.calls and self.calls[0] <= now - self.period:
                self.calls.popleft()
            if len(self.calls) >= self.max_calls:
                sleep_time = self.calls[0] + self.period - now
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.time()
                while self.calls and self.calls[0] <= now - self.period:
                    self.calls.popleft()
            self.calls.append(time.time())


class CBClient:
    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        timeout: int | None = None,
        environment: str | None = None,
        dry_run_cli: bool = False,
    ):
        api_key = api_key or os.getenv("COINBASE_API_KEY")
        api_secret = api_secret or os.getenv("COINBASE_API_SECRET")
        timeout = timeout or int(float(os.getenv("CB_TIMEOUT_S", "30")))
        self.timeout = timeout
        self.api_key = api_key
        self.api_secret = api_secret
        self.cli_env = environment or os.getenv("COINBASE_CLI_ENV", "live")
        cli = os.getenv("COINBASE_CLI_PATH", "coinbase")
        self.cli = cli if shutil.which(cli) else (shutil.which("coinbase") or cli)
        # Dry-run mode is a fully local request assembler. It never spawns the
        # Coinbase CLI and therefore cannot submit an order accidentally.
        self.dry_run_cli = dry_run_cli

        max_calls = int(os.getenv("CB_RATE_LIMIT_CALLS", "10"))
        period = float(os.getenv("CB_RATE_LIMIT_PERIOD", "1.0"))
        self._rate_limiter = RateLimiter(max_calls=max_calls, period=period)

        self.settlement_currency = self._detect_settlement_currency()

    @staticmethod
    def _request_fields(args: tuple[str, ...]) -> dict:
        payload: dict = {}
        for token in args[2:]:
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            payload[key] = value
        return payload

    def _simulate_order_request(self, *args: str) -> dict:
        """Return a deterministic, non-submitting Coinbase order envelope."""
        action = args[1] if len(args) > 1 else "unknown"
        payload = self._request_fields(tuple(args))
        request_key = "|".join((self.cli_env, *args))
        request_id = hashlib.sha256(request_key.encode("utf-8")).hexdigest()[:20]

        payload.update(
            {
                "action": f"orders.{action}",
                "dry_run": True,
                "request_id": request_id,
            }
        )

        if action == "preview":
            payload.setdefault("preview_id", f"dry-preview-{request_id}")
            payload.setdefault("status", "PREVIEW")
        elif action == "cancel":
            payload.setdefault("status", "CANCELLED")
            payload.setdefault("order_id", payload.get("order_id", f"dry-order-{request_id}"))
        else:
            payload.setdefault("status", "OPEN")
            payload.setdefault("order_id", f"dry-order-{request_id}")

        return payload

    def _cli_json(self, *args: str, dry_run: bool = False) -> dict:
        order_action = len(args) >= 2 and args[0] == "orders"
        if order_action and (dry_run or self.dry_run_cli):
            return self._simulate_order_request(*args)

        self._rate_limiter.acquire()
        cmd = [self.cli, "-e", self.cli_env, *args]
        if dry_run:
            cmd.append("--dry-run")
        else:
            cmd.extend(["--jq", "."])
        out = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=self.timeout,
        )
        if out.returncode != 0:
            raise RuntimeError(
                (out.stderr or out.stdout or "coinbase cli failed").strip()
            )
        return self._parse_cli_output(out.stdout, dry_run)

    @staticmethod
    def _parse_cli_output(stdout: str, dry_run: bool) -> dict:
        text = (stdout or "").strip()
        if not text:
            return {}
        if not dry_run:
            try:
                return json.loads(text)
            except Exception as exc:
                raise RuntimeError(
                    f"coinbase cli returned non-JSON output: {exc}"
                ) from exc
        start = text.find("{")
        if start < 0:
            return {}
        try:
            return json.loads(text[start:])
        except Exception as exc:
            raise RuntimeError(
                f"coinbase cli dry-run returned non-JSON output: {exc}"
            ) from exc

    @staticmethod
    def _norm_stop_direction(direction: str) -> str:
        direction = (direction or "").lower()
        if direction in ("up", "down"):
            return direction
        if direction == "stop_direction_stop_up":
            return "up"
        if direction == "stop_direction_stop_down":
            return "down"
        return direction or "up"

    def _remap(self, product_id: str) -> str:
        """Translate an engine ``*-USD`` symbol to the settlement pair."""
        if not product_id or not self.settlement_currency:
            return product_id
        currency = self.settlement_currency.upper()
        if currency != "USD" and product_id.upper().endswith("-USD"):
            return product_id[:-4] + "-" + currency
        return product_id

    def _detect_settlement_currency(self) -> str:
        env = os.getenv("COINBASE_SETTLEMENT_CURRENCY", "").strip().upper()
        if env:
            return env
        try:
            accounts = self._cli_json("balance").get("accounts", [])
            fiats: Dict[str, float] = {}
            for account in accounts:
                currency = str(account.get("currency", "")).upper()
                if currency not in (
                    "USD",
                    "USDC",
                    "USDT",
                    "EUR",
                    "GBP",
                    "CGC",
                    "PYUSD",
                ):
                    continue
                try:
                    value = float(
                        (account.get("available_balance") or {}).get("value", 0)
                        or 0
                    )
                except Exception:
                    value = 0.0
                if value > 0:
                    fiats[currency] = fiats.get(currency, 0.0) + value
            if fiats:
                non_usd = {
                    currency: value
                    for currency, value in fiats.items()
                    if currency != "USD"
                }
                if non_usd:
                    return max(non_usd, key=non_usd.get)
                return "USD"
        except Exception:
            pass
        return "USD"

    def list_accounts(self) -> dict:
        return self._cli_json("balance")

    def get_positions(self) -> list[dict]:
        """Get current open spot positions from Coinbase."""
        try:
            portfolio_data = self._cli_json("portfolios", "list")
            portfolios = portfolio_data.get("portfolios", []) or []
            if not portfolios:
                return []
            default = next(
                (p for p in portfolios if p.get("type") == "DEFAULT"),
                portfolios[0],
            )
            portfolio_uuid = default.get("uuid")
            if not portfolio_uuid:
                return []
            data = self._cli_json("portfolios", "get", portfolio_uuid)
            positions = data.get("spot_positions", []) or []
            result = []
            for position in positions:
                asset = (position.get("asset") or "").upper()
                size = float(position.get("total_balance_crypto", 0) or 0)
                if size <= 0:
                    continue
                is_cash = bool(position.get("is_cash", False))
                fiat = float(position.get("total_balance_fiat", 0) or 0)
                entry = fiat / size if size > 0 else 0.0
                result.append(
                    {
                        "product_id": f"{asset}-{self.settlement_currency}",
                        "asset": asset,
                        "side": "LONG",
                        "size": size,
                        "entry_price": entry,
                        "unrealized_pnl": 0.0,
                        "available_to_trade_fiat": float(
                            position.get("available_to_trade_fiat", 0) or 0
                        ),
                        "is_cash": is_cash,
                        "portfolio_uuid": portfolio_uuid,
                    }
                )
            return result
        except Exception as exc:
            log.warning("get_positions failed: %s", exc)
            return []

    def best_bid_ask(self, product_ids: list[str] | str) -> dict:
        if isinstance(product_ids, str):
            product_ids = [product_ids]
        pids: list[str] = []
        seen: set[str] = set()
        for product_id in product_ids or []:
            if not product_id:
                continue
            pid = self._remap(product_id.strip())
            if pid and pid not in seen:
                seen.add(pid)
                pids.append(pid)

        merged = {"pricebooks": []}
        for pid in pids:
            try:
                data = self._cli_json("products", "book", pid)
                book = data.get("pricebook") or data.get("pricebooks") or data
                if isinstance(book, dict):
                    merged["pricebooks"].append(book)
                elif isinstance(book, list):
                    merged["pricebooks"].extend(
                        item for item in book if isinstance(item, dict)
                    )
            except Exception:
                log.warning(
                    "best_bid_ask failed for %s — synthesizing from candles",
                    pid,
                )
                merged["pricebooks"].extend(self._synthetic_books([pid]))
        return merged

    def _synthetic_books(self, product_ids: list[str]) -> list[dict]:
        books = []
        now = int(time.time())
        for pid in product_ids:
            try:
                candles = self.public_candles(
                    pid,
                    now - 2 * 86400,
                    now,
                    granularity="ONE_HOUR",
                    limit=48,
                )
                rows = candles.get(
                    "candles",
                    candles if isinstance(candles, list) else [],
                )
                if not rows:
                    continue
                last = rows[-1]
                if isinstance(last, dict):
                    mid = float(last.get("close", 0) or 0)
                    spread = max(
                        0.001,
                        (
                            float(last.get("high", mid))
                            - float(last.get("low", mid))
                        )
                        / max(mid, 0.01)
                        / 2,
                    )
                else:
                    _, low, high, _open, close, _volume = last
                    mid = float(close)
                    spread = max(
                        0.001,
                        (float(high) - float(low)) / max(mid, 0.01) / 2,
                    )
                bid = max(0.0, mid * (1 - spread))
                ask = max(bid + 1e-9, mid * (1 + spread))
                books.append(
                    {
                        "product_id": pid,
                        "bids": [{"price": str(bid)}],
                        "asks": [{"price": str(ask)}],
                    }
                )
            except Exception:
                continue
        return books

    def preview_order(
        self,
        side: str,
        product_id: str,
        *,
        base_size: str | None = None,
        quote_size: str | None = None,
    ) -> dict:
        side_u = side.upper()
        pid = self._remap(product_id)
        if side_u not in ("BUY", "SELL"):
            raise ValueError("side must be 'buy' or 'sell'")
        try:
            if side_u == "BUY":
                if quote_size:
                    return self._cli_json(
                        "orders",
                        "preview",
                        f"product_id={pid}",
                        "side=BUY",
                        "type=market",
                        f"quote_size={quote_size}",
                    )
                if base_size:
                    return self._cli_json(
                        "orders",
                        "preview",
                        f"product_id={pid}",
                        "side=BUY",
                        "type=market",
                        f"base_size={base_size}",
                    )
                raise ValueError("buy preview needs quote_size or base_size")

            if not base_size:
                raise ValueError("sell preview needs base_size")
            return self._cli_json(
                "orders",
                "preview",
                f"product_id={pid}",
                "side=SELL",
                "type=market",
                f"base_size={base_size}",
            )
        except Exception as exc:
            return {
                "preview_id": f"synthetic-{pid}-{side.lower()}",
                "product_id": pid,
                "side": side_u,
                "status": "preview_error",
                "error": str(exc),
                "base_size": base_size,
                "quote_size": quote_size,
            }

    def create_market_order(
        self,
        side: str,
        product_id: str,
        *,
        base_size: str | None = None,
        quote_size: str | None = None,
        client_order_id: str = "",
        preview_id: str | None = None,
    ) -> dict:
        side_u = side.upper()
        pid = self._remap(product_id)
        if side_u == "BUY":
            if quote_size:
                args = [
                    "orders",
                    "create",
                    f"product_id={pid}",
                    "side=BUY",
                    "type=market",
                    f"quote_size={quote_size}",
                ]
            elif base_size:
                args = [
                    "orders",
                    "create",
                    f"product_id={pid}",
                    "side=BUY",
                    "type=market",
                    f"base_size={base_size}",
                ]
            else:
                raise ValueError("buy order needs quote_size or base_size")
        else:
            if not base_size:
                raise ValueError("sell order needs base_size")
            args = [
                "orders",
                "create",
                f"product_id={pid}",
                "side=SELL",
                "type=market",
                f"base_size={base_size}",
            ]
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        if preview_id:
            args.append(f"preview_id={preview_id}")
        return self._cli_json(*args, dry_run=self.dry_run_cli)

    def market_order(
        self,
        side: str,
        product_id: str,
        base_size: str | None = None,
        quote_size: str | None = None,
        client_order_id: str = "",
        preview_id: str | None = None,
    ) -> dict:
        return self.create_market_order(
            side,
            product_id,
            base_size=base_size,
            quote_size=quote_size,
            client_order_id=client_order_id,
            preview_id=preview_id,
        )

    def create_limit_order(
        self,
        side: str,
        product_id: str,
        *,
        base_size: str,
        price: str,
        client_order_id: str = "",
        time_in_force: str = "GTC",
        post_only: bool = False,
    ) -> dict:
        side_u = side.upper()
        pid = self._remap(product_id)
        args = [
            "orders",
            "create",
            f"product_id={pid}",
            f"side={side_u}",
            "type=limit",
            f"base_size={base_size}",
            f"limit_price={price}",
            f"time_in_force={time_in_force}",
        ]
        if post_only:
            args.append("post_only=true")
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        return self._cli_json(*args, dry_run=self.dry_run_cli)

    def create_stop_limit_order(
        self,
        side: str,
        product_id: str,
        *,
        base_size: str,
        limit_price: str,
        stop_price: str,
        client_order_id: str = "",
        time_in_force: str = "GTC",
        stop_direction: str = "stop_direction_stop_up",
    ) -> dict:
        side_u = side.upper()
        pid = self._remap(product_id)
        args = [
            "orders",
            "create",
            f"product_id={pid}",
            f"side={side_u}",
            "type=stop_limit",
            f"base_size={base_size}",
            f"limit_price={limit_price}",
            f"stop_price={stop_price}",
            f"stop_direction={self._norm_stop_direction(stop_direction)}",
            f"time_in_force={time_in_force}",
        ]
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        return self._cli_json(*args, dry_run=self.dry_run_cli)

    def create_stop_market_order(
        self,
        side: str,
        product_id: str,
        *,
        base_size: str,
        stop_price: str,
        client_order_id: str = "",
        stop_direction: str = "stop_direction_stop_up",
    ) -> dict:
        side_u = side.upper()
        pid = self._remap(product_id)
        args = [
            "orders",
            "create",
            f"product_id={pid}",
            f"side={side_u}",
            "type=stop_limit",
            f"base_size={base_size}",
            f"limit_price={stop_price}",
            f"stop_price={stop_price}",
            f"stop_direction={self._norm_stop_direction(stop_direction)}",
        ]
        if client_order_id:
            args.append(f"client_order_id={client_order_id}")
        return self._cli_json(*args, dry_run=self.dry_run_cli)

    def close_position(
        self,
        product_id: str,
        size: str | None = None,
        client_order_id: str = "",
    ) -> dict:
        if not client_order_id:
            import uuid as _uuid

            client_order_id = str(_uuid.uuid4())
        pid = self._remap(product_id)
        args = [
            "orders",
            "close-position",
            f"client_order_id={client_order_id}",
            f"product_id={pid}",
        ]
        if size:
            args.append(f"size={size}")
        return self._cli_json(*args, dry_run=self.dry_run_cli)

    def cancel_order(self, order_id: str) -> dict:
        return self._cli_json(
            "orders",
            "cancel",
            f"order_id={order_id}",
            dry_run=self.dry_run_cli,
        )

    def list_orders(
        self,
        product_id: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        args = ["orders", "list"]
        if product_id:
            args.append(f"product_ids={product_id}")
        if status:
            args.append(f"status=={status}")
        args.append("limit=100")
        try:
            return self._cli_json(*args).get("orders", [])
        except Exception as exc:
            log.debug("list_orders failed: %s", exc)
            return []

    def get_order(self, order_id: str) -> dict:
        if not order_id:
            return {}
        try:
            return self._cli_json("orders", "get", order_id)
        except Exception as exc:
            log.debug("get_order failed for %s: %s", order_id, exc)
            return {}

    def get_fees(self) -> dict:
        try:
            return self._cli_json("fees")
        except Exception as exc:
            log.debug("get_fees failed: %s", exc)
            return {}

    def get_products(self, product_type: str | None = None) -> list[dict]:
        args = ["products", "list"]
        if product_type:
            args.append(f"product_type={product_type}")
        return self._cli_json(*args).get("products", [])

    def public_candles(
        self,
        product_id: str,
        start_unix: int,
        end_unix: int,
        granularity: str = "ONE_HOUR",
        limit: int = 300,
    ) -> dict:
        start = (
            datetime.fromtimestamp(int(start_unix), tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        end = (
            datetime.fromtimestamp(int(end_unix), tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
        granularity_value = {
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
        pid = self._remap(product_id)
        return self._cli_json(
            "products",
            "candles",
            pid,
            f"start={start}",
            f"end={end}",
            f"granularity={granularity_value}",
            f"limit={int(limit)}",
        )
