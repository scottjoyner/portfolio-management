"""
Kalshi Trade API client.

Supports both the legacy email/password login flow and the current
API-key flow backed by a PEM private key.
"""

import json
import logging
import base64
import re
import time
import os
import urllib.request
import urllib.parse
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:  # pragma: no cover - optional dependency in some envs
    hashes = serialization = padding = None

logger = logging.getLogger("kalshi")

REQUEST_TIMEOUT = 15
DEMO_BASE_URL = "https://external-api.demo.kalshi.co/trade-api/v2"
PROD_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Kalshi event tickers relevant to crypto / macro
WATCHED_EVENTS = [
    "bitcoin", "btc", "ethereum", "eth", "crypto",
    "inflation", "cpi", "fed", "interest", "rate",
    "recession", "gdp", "unemployment", "sp500",
]

# Broader search terms across all categories — used to find events on Kalshi
BROAD_SEARCH_TERMS = [
    # Crypto / macro
    "bitcoin", "btc", "ethereum", "eth", "crypto", "inflation", "cpi",
    "fed", "interest", "recession", "gdp", "unemployment", "sp500",
    # Sports — team names, leagues, events
    "super bowl", "nfl", "nba", "mlb", "nhl", "world cup", "sports",
    "soccer", "football", "basketball", "baseball", "hockey", "tennis",
    "olympic", "gold medal", "champion", "playoff", "final", "title",
    "match", "game", "team", "player", "race", "fight", "boxing",
    "ufc", "pga", "golf", "grand slam", "wimbledon",
    # Politics
    "election", "president", "presidential", "congress", "senate",
    "vote", "democrat", "republican", "governor", "nominee", "primary",
    "trump", "biden", "harris", "supreme court", "impeach", "indict",
    # Entertainment
    "oscar", "grammy", "emmy", "movie", "film", "actor", "actress",
    "music", "album", "billboard", "netflix", "disney",
    "box office", "opening weekend", "super bowl halftime",
    # Economics
    "treasury", "yield", "housing", "jobs", "tariff", "trade",
    "federal reserve", "rate hike", "rate cut", "debt", "deficit",
    # Technology
    "ai", "artificial intelligence", "gpt", "openai", "space",
    "spacex", "starship", "mars", "nasa", "launch", "rocket",
    "nvidia", "apple", "google", "microsoft", "meta",
    # General topics common on Kalshi
    "pope", "supervolcano", "erupt", "colonize",
]

KALSHI_CATEGORIES = [
    "sports", "politics", "economics", "entertainment",
    "technology", "business", "weather", "science",
    "disaster", "health", "world",
]


@dataclass
class KalshiSeries:
    series_ticker: str
    title: str
    category: str
    status: str


@dataclass
class KalshiMarket:
    ticker: str
    title: str
    event_ticker: str
    yes_bid: float
    yes_ask: float
    no_bid: float
    no_ask: float
    volume: float
    open_interest: float
    close_date: str
    status: str
    settled: bool
    category: str = ""


class KalshiClient:
    """Read-only client for Kalshi market data.

    Requires email/password authentication for API access.
    """

    def __init__(
        self,
        email: str = "",
        password: str = "",
        api_key_id: str = "",
        private_key_path: str = "",
        base_url: str = "",
        timeout: int = REQUEST_TIMEOUT,
    ):
        self.email = email
        self.password = password
        self.api_key_id = api_key_id
        self.private_key_path = private_key_path
        self.timeout = timeout
        self._token: Optional[str] = None
        self._member_id: Optional[str] = None
        self.base_url = (base_url or os.environ.get("KALSHI_API_BASE_URL", "")).rstrip("/") or self._default_base_url()
        self._private_key = None

    def _default_base_url(self) -> str:
        env = os.environ.get("KALSHI_ENV", os.environ.get("KALSHI_API_ENV", "demo")).lower()
        return DEMO_BASE_URL if env == "demo" else PROD_BASE_URL

    def _load_private_key(self):
        if self._private_key is not None:
            return self._private_key
        if not self.private_key_path:
            return None
        if serialization is None:
            raise RuntimeError("cryptography is required for Kalshi API-key auth")
        raw = Path(self.private_key_path).read_bytes()
        self._private_key = serialization.load_pem_private_key(raw, password=None)
        return self._private_key

    def _use_api_key_auth(self) -> bool:
        return bool(self.api_key_id and self.private_key_path)

    def _sign_request(self, timestamp_ms: str, method: str, path: str) -> str:
        private_key = self._load_private_key()
        if private_key is None:
            raise RuntimeError("Kalshi private key not configured")
        path_no_query = path.split("?")[0]
        # Kalshi signs the FULL request path, which includes the base_url prefix
        # (e.g. "/trade-api/v2"). base_url is like
        # "https://api.elections.kalshi.com/trade-api/v2"; its path must be
        # prepended or the signature is rejected with HTTP 401.
        base_path = urllib.parse.urlsplit(self.base_url).path.rstrip("/")
        if base_path and not path_no_query.startswith(base_path):
            path_no_query = f"{base_path}{path_no_query}"
        message = f"{timestamp_ms}{method.upper()}{path_no_query}".encode("utf-8")
        signature = private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_header(self, method: str = "GET", path: str = "") -> Dict[str, str]:
        if self._use_api_key_auth():
            ts = str(int(time.time() * 1000))
            return {
                "User-Agent": "PortfolioOptimizer/1.0",
                "Accept": "application/json",
                "KALSHI-ACCESS-KEY": self.api_key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": self._sign_request(ts, method, path),
            }
        if not self._token:
            self._login()
        return {
            "User-Agent": "PortfolioOptimizer/1.0",
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
        }

    def _login(self):
        if not self.email or not self.password:
            logger.warning("Kalshi: no credentials configured")
            self._token = ""
            return
        # NOTE (P0-1): Kalshi's legacy email/password login transmits the
        # plaintext password over HTTPS via a JSON POST to /login. There is NO
        # signed-auth path for this legacy flow — the documented "SHA256 signed"
        # form below is FICTIONAL and was never sent. We deliberately compute no
        # signature and never log the password. Prefer the API-key (RSA-PSS)
        # signed flow via api_key_id + private_key_path (see _auth_header), which
        # never transmits a password at all.
        if self._use_api_key_auth():
            raise RuntimeError("Kalshi write/login ops require API-key auth; legacy password login is unsupported")
        payload = json.dumps({"email": self.email, "password": self.password}).encode()
        req = urllib.request.Request(
            f"{self.base_url}/login",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "PortfolioOptimizer/1.0",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode())
            self._token = data.get("token", "")
            self._member_id = data.get("member_id", "")
            # Never log the email/password; log only the opaque member id.
            logger.info("Kalshi: logged in (member=%s)", self._member_id)
        except Exception as e:
            logger.warning("Kalshi login failed: %s", e)
            self._token = ""

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, _retries: int = 3) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            parts = []
            for k, v in params.items():
                if v is not None:
                    parts.append(f"{k}={urllib.parse.quote(str(v))}")
            url = f"{url}?{'&'.join(parts)}"
        req = urllib.request.Request(url, headers=self._auth_header("GET", path))
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 401 and self._token:
                self._login()
                return self._get(path, params, _retries)
            # Rate limited (common on the unauthenticated public tier): back off + retry.
            if e.code == 429 and _retries > 0:
                wait = float(e.headers.get("Retry-After", 0) or 0) or (2.0 * (4 - _retries))
                time.sleep(min(wait, 10.0))
                return self._get(path, params, _retries - 1)
            raise

    def _write(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Any:
        """Signed write request (POST/DELETE/PUT) for order/portfolio endpoints.

        Requires API-key auth (RSA-PSS). The signature covers the full request
        path including the base-URL prefix (handled in `_sign_request`).
        """
        if not self._use_api_key_auth():
            raise RuntimeError("Kalshi write ops require API-key auth (api_key_id + private_key_path)")
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        headers = self._auth_header(method, path)
        headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode()
            except Exception:
                pass
            logger.warning("Kalshi %s %s failed: HTTP %s %s", method, path, e.code, detail)
            raise

    def search_markets(self, term: str = "", limit: int = 50,
                       min_volume: float = 0, max_spread: float = 1.0,
                       min_open_interest: float = 0) -> List[KalshiMarket]:
        params: Dict[str, Any] = {"limit": min(limit, 100), "status": "open"}
        if term:
            params["search_term"] = term
        try:
            data = self._get("/markets", params)
        except Exception as e:
            logger.warning("Kalshi search failed: %s", e)
            return []
        raw = data.get("markets", [])
        results = [self._parse_market(m) for m in raw]
        # Filter by volume, open_interest, and spread
        filtered = []
        for m in results:
            if m.volume < min_volume:
                continue
            if m.open_interest < min_open_interest:
                continue
            spread = m.yes_ask - m.yes_bid
            if spread > max_spread:
                continue
            filtered.append(m)
        filtered.sort(key=lambda m: m.volume, reverse=True)
        return filtered[:limit]

    def get_market(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch a single market's raw record by ticker (includes settlement result)."""
        if not ticker:
            return None
        try:
            data = self._get(f"/markets/{ticker}")
        except Exception as e:
            logger.debug("Kalshi get_market failed for %s: %s", ticker, e)
            return None
        return data.get("market") if isinstance(data, dict) else None

    def get_settlement(self, ticker: str) -> Optional[int]:
        """Return 1 if the market settled YES, 0 if settled NO, else None (unresolved)."""
        m = self.get_market(ticker)
        if not m:
            return None
        settled = bool(m.get("settled", False)) or str(m.get("status", "")).lower() in (
            "settled", "finalized", "closed",
        )
        if not settled:
            return None
        result = str(m.get("result", "")).lower()
        if result in ("yes", "y"):
            return 1
        if result in ("no", "n"):
            return 0
        return None

    def get_relevant_markets(self, limit: int = 50,
                              min_volume: float = 1000,
                              max_spread: float = 0.15,
                              min_open_interest: float = 100) -> List[KalshiMarket]:
        """Search events by crypto/macro keywords, get their markets with filters."""
        all_events = self.fetch_all_events()
        event_tickers = self._filter_events_by_keywords(all_events, WATCHED_EVENTS)
        results = []
        seen: set = set()
        for et in event_tickers:
            for m in self.get_event_markets(et):
                if m.ticker not in seen:
                    spread = m.yes_ask - m.yes_bid
                    if m.volume >= min_volume and m.open_interest >= min_open_interest and spread <= max_spread:
                        seen.add(m.ticker)
                        results.append(m)
            if len(results) >= limit:
                break
        # Fall back to market search
        if not results:
            for term in WATCHED_EVENTS:
                markets = self.search_markets(term=term, limit=10,
                                             min_volume=min_volume, max_spread=max_spread,
                                             min_open_interest=min_open_interest)
                for m in markets:
                    if m.ticker not in seen:
                        seen.add(m.ticker)
                        results.append(m)
                if len(results) >= limit:
                    break
        results.sort(key=lambda m: m.volume, reverse=True)
        return results[:limit]

    def search_broad(self, limit: int = 50,
                     min_volume: float = 0, max_spread: float = 1.0,
                     min_open_interest: float = 0) -> List[KalshiMarket]:
        """Search across all event categories with filters."""
        all_events = self.fetch_all_events()
        event_tickers = self._filter_events_by_keywords(all_events, BROAD_SEARCH_TERMS)
        results = []
        seen: set = set()
        for et in event_tickers:
            try:
                for m in self.get_event_markets(et):
                    if m.ticker not in seen:
                        spread = m.yes_ask - m.yes_bid
                        if m.volume >= min_volume and m.open_interest >= min_open_interest and spread <= max_spread:
                            seen.add(m.ticker)
                            results.append(m)
            except Exception:
                continue
            if len(results) >= limit:
                break
        # Fall back to market search
        if len(results) < limit:
            for term in BROAD_SEARCH_TERMS:
                try:
                    markets = self.search_markets(term=term, limit=10,
                                                 min_volume=min_volume, max_spread=max_spread,
                                                 min_open_interest=min_open_interest)
                    for m in markets:
                        if m.ticker not in seen:
                            seen.add(m.ticker)
                            results.append(m)
                    if len(results) >= limit:
                        break
                except Exception:
                    continue
        results.sort(key=lambda m: m.volume, reverse=True)
        return results[:limit]

    def get_event_markets(self, event_ticker: str) -> List[KalshiMarket]:
        """Fetch all open markets belonging to a single event."""
        if not event_ticker:
            return []
        try:
            data = self._get("/markets", {"event_ticker": event_ticker, "status": "open", "limit": 100})
        except Exception as e:
            logger.debug("Kalshi get_event_markets failed for %s: %s", event_ticker, e)
            return []
        raw = data.get("markets", []) if isinstance(data, dict) else []
        return [self._parse_market(m) for m in raw]

    def fetch_events_with_markets(self, limit: int = 200,
                                  categories: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Fetch open events with their nested markets in batched calls.

        Uses `with_nested_markets=true` so each event dict carries its markets
        (avoids an N+1 call per event). Each returned event gains a
        ``parsed_markets`` key (List[KalshiMarket]). Preserves ``category`` and
        ``mutually_exclusive`` from the event record (needed for internal-arb).
        """
        events: List[Dict[str, Any]] = []
        cursor = ""
        want = min(limit, 1000)
        cats = set(categories) if categories else None
        while len(events) < want:
            params: Dict[str, Any] = {
                "limit": min(200, want - len(events)),
                "status": "open",
                "with_nested_markets": "true",
            }
            if cursor:
                params["cursor"] = cursor
            try:
                data = self._get("/events", params)
            except Exception as e:
                logger.debug("Kalshi fetch_events_with_markets failed: %s", e)
                break
            batch = data.get("events", []) if isinstance(data, dict) else []
            if not batch:
                break
            for e in batch:
                if cats and e.get("category", "") not in cats:
                    continue
                e["parsed_markets"] = [self._parse_market(m) for m in e.get("markets", [])]
                events.append(e)
            cursor = data.get("cursor", "") if isinstance(data, dict) else ""
            if not cursor:
                break
        return events

    def get_markets_by_categories(self, priority: Optional[List[str]] = None,
                                  total_event_limit: int = 20) -> Dict[str, List[KalshiMarket]]:
        """Return open markets grouped by Kalshi category, priority-ordered.

        Fetches up to ``total_event_limit`` events (highest-priority categories
        first) and flattens their markets into ``{category: [KalshiMarket, ...]}``.
        """
        priority = priority or [
            "Crypto", "Economics", "Financials", "Politics", "Elections",
            "Sports", "Science and Technology", "Entertainment",
            "World", "Climate and Weather",
        ]
        all_events = self.fetch_events_with_markets(limit=max(total_event_limit * 3, 100))
        by_category: Dict[str, List[Dict[str, Any]]] = {}
        for e in all_events:
            by_category.setdefault(e.get("category", "unknown"), []).append(e)

        ordered_cats = priority + [c for c in by_category if c not in priority]
        result: Dict[str, List[KalshiMarket]] = {}
        seen: set = set()
        count = 0
        for cat in ordered_cats:
            for e in by_category.get(cat, []):
                if count >= total_event_limit:
                    break
                for m in e.get("parsed_markets", []):
                    if m.ticker not in seen:
                        seen.add(m.ticker)
                        result.setdefault(cat, []).append(m)
                count += 1
            if count >= total_event_limit:
                break
        for cat in result:
            result[cat].sort(key=lambda m: m.volume, reverse=True)
        return result


    def fetch_all_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch all open events (batched once, avoid per-keyword calls)."""
        try:
            data = self._get("/events", {"limit": min(limit, 100), "status": "open"})
        except Exception as e:
            logger.debug("Kalshi fetch events failed: %s", e)
            return []
        return data.get("events", [])

    def _filter_events_by_keywords(
        self, events: List[Dict[str, Any]], keywords: List[str]
    ) -> List[str]:
        """Filter events by keyword match on title/ticker, return event tickers.
        Short keywords (< 4 chars) use word-boundary matching.
        """
        matched = []
        for e in events:
            text = (e.get("title", "") + " " + e.get("event_ticker", "")).lower()
            for kw in keywords:
                if len(kw) < 4:
                    if re.search(r'\b' + re.escape(kw) + r'\b', text):
                        matched.append(e.get("event_ticker", ""))
                        break
                elif kw in text:
                    matched.append(e.get("event_ticker", ""))
                    break
        return matched

    def get_order_book(self, ticker: str) -> Dict[str, Any]:
        try:
            return self._get(f"/markets/{ticker}/orderbook")
        except Exception as e:
            logger.debug("Kalshi book failed for %s: %s", ticker, e)
            return {}

    def get_balance(self) -> Dict[str, Any]:
        try:
            return self._get("/portfolio/balance")
        except Exception as e:
            logger.debug("Kalshi balance failed: %s", e)
            return {}

    # ── Trading / portfolio (write) ──────────────────────────────────
    # These require API-key auth. Kalshi v2 trade API:
    #   POST   /portfolio/orders          create an order
    #   DELETE /portfolio/orders/{id}     cancel an order
    #   GET    /portfolio/orders          list orders
    #   GET    /portfolio/positions       list positions
    #   GET    /portfolio/fills           list fills

    def create_order(self, ticker: str, side: str, action: str, count: int,
                     order_type: str = "limit", price: Optional[float] = None,
                     client_order_id: Optional[str] = None,
                     time_in_force: str = "immediate_or_cancel",
                     self_trade_prevention_type: str = "taker_at_cross",
                     post_only: bool = False) -> Dict[str, Any]:
        """Place an order via the Kalshi V2 endpoint (POST /portfolio/events/orders).

        Ergonomic wrapper: `side` in {"yes","no"}, `action` in {"buy","sell"}.
        The V2 book is quoted from the YES leg only ("bid"=buy YES, "ask"=sell
        YES); buying NO at q == selling YES at 1-q. We translate accordingly.

        `price` is dollars in [0,1]. `count` is number of contracts.
        `time_in_force` in {fill_or_kill, good_till_canceled, immediate_or_cancel}.
        For arbitrage we default to IOC (don't rest / leg risk).
        """
        import uuid
        if price is None:
            raise ValueError("price is required (V2 orders are always priced)")
        p = float(price)
        # Map (side, action) -> V2 book side + YES-quoted price.
        if side == "yes":
            book_side = "bid" if action == "buy" else "ask"
            yes_price = p
        elif side == "no":
            # buy NO == sell YES @ 1-p ; sell NO == buy YES @ 1-p
            book_side = "ask" if action == "buy" else "bid"
            yes_price = 1.0 - p
        else:
            raise ValueError("side must be 'yes' or 'no'")
        yes_price = max(0.0, min(1.0, yes_price))
        body: Dict[str, Any] = {
            "ticker": ticker,
            "side": book_side,
            "count": f"{int(count)}",
            "price": f"{yes_price:.4f}",
            "time_in_force": time_in_force,
            "self_trade_prevention_type": self_trade_prevention_type,
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if post_only:
            body["post_only"] = True
        return self._write("POST", "/portfolio/events/orders", body)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        # V2 cancel endpoint (legacy /portfolio/orders/{id} is deprecated / 410).
        return self._write("DELETE", f"/portfolio/events/orders/{order_id}")

    def get_orders(self, ticker: str = "", status: str = "") -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if ticker:
            params["ticker"] = ticker
        if status:
            params["status"] = status
        try:
            data = self._get("/portfolio/orders", params or None)
            return data.get("orders", []) if isinstance(data, dict) else []
        except Exception as e:
            logger.debug("Kalshi get_orders failed: %s", e)
            return []

    def get_positions(self) -> List[Dict[str, Any]]:
        try:
            data = self._get("/portfolio/positions")
            if isinstance(data, dict):
                # v2 returns {"market_positions": [...], "event_positions": [...]}
                return data.get("market_positions", data.get("positions", []))
            return []
        except Exception as e:
            logger.debug("Kalshi get_positions failed: %s", e)
            return []

    def get_fills(self, ticker: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": min(limit, 1000)}
        if ticker:
            params["ticker"] = ticker
        try:
            data = self._get("/portfolio/fills", params)
            return data.get("fills", []) if isinstance(data, dict) else []
        except Exception as e:
            logger.debug("Kalshi get_fills failed: %s", e)
            return []

    def _parse_market(self, raw: dict) -> KalshiMarket:
        # Kalshi v2 API uses _dollars suffix (already in 0-1 range)
        # v1 API returns cents (divide by 100).
        has_v2 = "yes_bid_dollars" in raw
        div = 100 if not has_v2 else 1
        yes_bid = raw.get("yes_bid_dollars") if has_v2 else raw.get("yes_bid", 0)
        yes_ask = raw.get("yes_ask_dollars") if has_v2 else raw.get("yes_ask", 0)
        no_bid = raw.get("no_bid_dollars") if has_v2 else raw.get("no_bid", 0)
        no_ask = raw.get("no_ask_dollars") if has_v2 else raw.get("no_ask", 0)
        volume = raw.get("volume_24h_fp") if has_v2 else raw.get("volume", 0)
        open_interest = raw.get("open_interest_fp") if has_v2 else raw.get("open_interest", 0)
        close_date = raw.get("close_time") if has_v2 else raw.get("close_date", "")
        category = raw.get("category", "")
        return KalshiMarket(
            ticker=raw.get("ticker", ""),
            title=raw.get("title", ""),
            event_ticker=raw.get("event_ticker", ""),
            yes_bid=float(yes_bid or 0) / div,
            yes_ask=float(yes_ask or 0) / div,
            no_bid=float(no_bid or 0) / div,
            no_ask=float(no_ask or 0) / div,
            volume=float(volume or 0),
            open_interest=float(open_interest or 0),
            close_date=close_date,
            status=raw.get("status", ""),
            settled=bool(raw.get("settled", False)),
            category=category,
        )


def format_market(m: KalshiMarket) -> str:
    return (
        f"  [{m.ticker}] {m.title[:80]}\n"
        f"    YES: {m.yes_bid*100:.1f}%/{m.yes_ask*100:.1f}%  "
        f"NO: {m.no_bid*100:.1f}%/{m.no_ask*100:.1f}%\n"
        f"    Volume: ${m.volume:,.0f}  Close: {m.close_date[:10]}  Status: {m.status}"
    )
