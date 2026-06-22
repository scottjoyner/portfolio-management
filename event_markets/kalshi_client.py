"""
Kalshi Trade API client.

Supports both the legacy email/password login flow and the current
API-key flow backed by a PEM private key.
"""

import hashlib
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
    close_date: str
    status: str
    settled: bool


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
        ts = int(time.time() * 1000)
        raw = f"{ts}{self.password}"
        sig = hashlib.sha256(raw.encode()).hexdigest()
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
            logger.info("Kalshi: logged in as %s", self._member_id)
        except Exception as e:
            logger.warning("Kalshi login failed: %s", e)
            self._token = ""

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
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
                return self._get(path, params)
            raise

    def search_markets(self, term: str = "", limit: int = 50) -> List[KalshiMarket]:
        params: Dict[str, Any] = {"limit": min(limit, 100), "status": "open"}
        if term:
            params["search_term"] = term
        try:
            data = self._get("/markets", params)
        except Exception as e:
            logger.warning("Kalshi search failed: %s", e)
            return []
        raw = data.get("markets", [])
        return [self._parse_market(m) for m in raw]

    def get_event_markets(self, event_ticker: str) -> List[KalshiMarket]:
        try:
            data = self._get(f"/events/{event_ticker}")
        except Exception as e:
            logger.debug("Kalshi event %s failed: %s", event_ticker, e)
            return []
        raw = data.get("markets", []) if isinstance(data, dict) else []
        return [self._parse_market(m) for m in raw]

    def get_series(self, limit: int = 50) -> List[KalshiSeries]:
        """Fetch available series (thematic event groups)."""
        try:
            data = self._get("/series", {"limit": min(limit, 100), "status": "open"})
        except Exception as e:
            logger.debug("Kalshi series failed: %s", e)
            return []
        return [
            KalshiSeries(
                series_ticker=s.get("series_ticker", ""),
                title=s.get("title", ""),
                category=s.get("category", ""),
                status=s.get("status", ""),
            )
            for s in data.get("series", [])
        ]

    def search_events_by_category(self, category: str, limit: int = 20) -> List[KalshiMarket]:
        """Fetch events for a specific category and return their markets."""
        try:
            data = self._get("/events", {"limit": min(limit, 100), "status": "open", "category": category})
        except Exception as e:
            logger.debug("Kalshi category %s failed: %s", category, e)
            return []
        event_tickers = [e.get("event_ticker", "") for e in data.get("events", []) if e.get("event_ticker")]
        results = []
        seen: set = set()
        for et in event_tickers:
            try:
                for m in self.get_event_markets(et):
                    if m.ticker not in seen:
                        seen.add(m.ticker)
                        results.append(m)
            except Exception:
                continue
        results.sort(key=lambda m: m.volume, reverse=True)
        return results[:limit]

    def get_markets_by_categories(
        self, total_event_limit: int = 20
    ) -> Dict[str, List[KalshiMarket]]:
        """Fetch all open events, group by Kalshi category, and get markets.

        Limits to *total_event_limit* events across all categories (not per-category)
        to keep API call count manageable. Prioritizes categories most likely to
        overlap with Polymarket: Sports, Entertainment, Politics, Elections.
        """
        all_events = self.fetch_all_events(limit=100)
        # Priority order: categories most likely to match Polymarket first
        priority = ["Sports", "Entertainment", "Politics", "Elections",
                    "Crypto", "Economics", "Financials", "Science and Technology",
                    "Climate and Weather", "Companies", "Social", "World"]
        by_category: Dict[str, List[str]] = {}
        for e in all_events:
            cat = e.get("category", "unknown")
            ticker = e.get("event_ticker", "")
            if ticker:
                by_category.setdefault(cat, []).append(ticker)

        # Flatten by priority up to total_event_limit
        selected: list[tuple[str, str]] = []
        for cat in priority:
            for t in by_category.get(cat, []):
                if len(selected) >= total_event_limit:
                    break
                selected.append((cat, t))
            if len(selected) >= total_event_limit:
                break

        result: Dict[str, List[KalshiMarket]] = {}
        seen: set = set()
        for cat, et in selected:
            try:
                for m in self.get_event_markets(et):
                    if m.ticker not in seen:
                        seen.add(m.ticker)
                        result.setdefault(cat, []).append(m)
            except Exception:
                continue
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

    def get_relevant_markets(self, limit: int = 50) -> List[KalshiMarket]:
        """Search events by crypto/macro keywords, get their markets."""
        all_events = self.fetch_all_events()
        event_tickers = self._filter_events_by_keywords(all_events, WATCHED_EVENTS)
        results = []
        seen: set = set()
        for et in event_tickers:
            for m in self.get_event_markets(et):
                if m.ticker not in seen:
                    seen.add(m.ticker)
                    results.append(m)
            if len(results) >= limit:
                break
        # Fall back to market search
        if not results:
            for term in WATCHED_EVENTS:
                markets = self.search_markets(term=term, limit=10)
                for m in markets:
                    if m.ticker not in seen:
                        seen.add(m.ticker)
                        results.append(m)
                if len(results) >= limit:
                    break
        results.sort(key=lambda m: m.volume, reverse=True)
        return results[:limit]

    def search_broad(self, limit: int = 50) -> List[KalshiMarket]:
        """Search across all event categories."""
        all_events = self.fetch_all_events()
        event_tickers = self._filter_events_by_keywords(all_events, BROAD_SEARCH_TERMS)
        results = []
        seen: set = set()
        for et in event_tickers:
            try:
                for m in self.get_event_markets(et):
                    if m.ticker not in seen:
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
                    markets = self.search_markets(term=term, limit=10)
                    for m in markets:
                        if m.ticker not in seen:
                            seen.add(m.ticker)
                            results.append(m)
                except Exception:
                    continue
                if len(results) >= limit:
                    break
        results.sort(key=lambda m: m.volume, reverse=True)
        return results[:limit]

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
        close_date = raw.get("close_time") if has_v2 else raw.get("close_date", "")
        return KalshiMarket(
            ticker=raw.get("ticker", ""),
            title=raw.get("title", ""),
            event_ticker=raw.get("event_ticker", ""),
            yes_bid=float(yes_bid or 0) / div,
            yes_ask=float(yes_ask or 0) / div,
            no_bid=float(no_bid or 0) / div,
            no_ask=float(no_ask or 0) / div,
            volume=float(volume or 0),
            close_date=close_date,
            status=raw.get("status", ""),
            settled=bool(raw.get("settled", False)),
        )


def format_market(m: KalshiMarket) -> str:
    return (
        f"  [{m.ticker}] {m.title[:80]}\n"
        f"    YES: {m.yes_bid*100:.1f}%/{m.yes_ask*100:.1f}%  "
        f"NO: {m.no_bid*100:.1f}%/{m.no_ask*100:.1f}%\n"
        f"    Volume: ${m.volume:,.0f}  Close: {m.close_date[:10]}  Status: {m.status}"
    )
