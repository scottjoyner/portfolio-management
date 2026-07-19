"""
Polymarket client — uses Gamma API for market data, CLOB API for order book.

References:
    https://docs.polymarket.com/api/rest
"""

import json
import logging
import re
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("polymarket")

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
CLOB_BASE_URL = "https://clob.polymarket.com"
REQUEST_HEADERS = {
    "User-Agent": "PortfolioOptimizer/1.0",
    "Accept": "application/json",
}

# Crypto keywords used to filter relevant markets
CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "crypto", "cryptocurrency", "defi", "token", "coin",
    "avax", "avalanche", "matic", "polygon", "link", "chainlink",
    "doge", "dogecoin", "xrp", "ripple", "ada", "cardano",
    "dot", "polkadot", "atom", "cosmos", "uni", "uniswap",
    "op", "optimism", "arb", "arbitrum", "layer 2", "l2",
    "stablecoin", "usdc", "usdt", "dai",
]

SPORTS_KEYWORDS = [
    "nfl", "nba", "mlb", "nhl", "mls", "ufc", "pga", "lpga",
    "super bowl", "world series", "stanley cup", "final four", "march madness",
    "champions league", "premier league", "la liga", "serie a", "bundesliga",
    "world cup", "grand slam", "wimbledon", "us open", "french open", "australian open",
    "fight", "boxing", "mma", "knockout", "title fight",
    "grand prix", "formula 1", "f1", "nascar", "indycar",
    "champion", "final", "playoff", "semifinal", "quarterfinal",
    "soccer", "football", "basketball", "baseball", "hockey", "tennis", "golf",
    "super bowl lix", "super bowl lx",
    "nfl draft", "nba draft", "mlb draft",
    "olympics", "olympic", "gold medal",
]

POLITICS_KEYWORDS = [
    "election", "president", "presidential", "congress", "senate", "house",
    "democrat", "republican", "independent", "nominee", "nomination",
    "primary", "caucus", "debate", "convention",
    "governor", "mayor", "attorney general", "secretary",
    "trump", "biden", "harris", "desantis", "newsom", "haley", "ramaswamy",
    "vote", "ballot", "poll", "approval rating",
    "impeach", "indict", "convict", "sentence",
    "supreme court", "justice", "judge", "scotus",
    "gop", "dnc", "rnc", "gop primary", "democratic primary",
]

ENTERTAINMENT_KEYWORDS = [
    "oscar", "academy award", "grammy", "emmy", "golden globe", "tony",
    "box office", "gross", "opening weekend", "blockbuster",
    "movie", "film", "actor", "actress", "director", "best picture",
    "album", "single", "billboard", "platinum", "streaming",
    "netflix", "disney", "hbo", "paramount", "amazon", "apple",
    "music", "concert", "tour", "festival",
    "super bowl halftime", "halftime show",
    "eurovision", "song contest",
]

ECONOMICS_KEYWORDS = [
    "fed", "federal reserve", "interest rate", "rate hike", "rate cut",
    "inflation", "cpi", "core cpi", "pce", "core pce",
    "gdp", "gdp growth", "recession", "expansion",
    "unemployment", "jobs", "payroll", "nonfarm",
    "consumer", "retail", "manufacturing", "industrial",
    "debt", "deficit", "default", "bankrupt",
    "tariff", "trade", "import", "export",
    "yield", "spread", "treasury", "bond",
    "housing", "home", "mortgage", "real estate",
]

TECHNOLOGY_KEYWORDS = [
    "gpt", "chatgpt", "openai", "anthropic", "gemini", "claude", "llama",
    "ai", "artificial intelligence", "machine learning", "llm",
    "silicon valley", "startup", "ipo",
    "space", "spacex", "starship", "nasa",
    "hack", "breach", "exploit", "ransomware", "cyber",
    "iphone", "apple", "google", "microsoft", "meta", "amazon",
    "nvidia", "amd", "intel", "tsmc", "samsung",
    "quantum", "compute", "data center",
]

ALL_CATEGORY_KEYWORDS = {
    "crypto": CRYPTO_KEYWORDS,
    "sports": SPORTS_KEYWORDS,
    "politics": POLITICS_KEYWORDS,
    "entertainment": ENTERTAINMENT_KEYWORDS,
    "economics": ECONOMICS_KEYWORDS,
    "technology": TECHNOLOGY_KEYWORDS,
}


@dataclass
class PolymarketMarket:
    condition_id: str
    question: str
    description: str
    outcomes: List[str]
    outcome_prices: Dict[str, float]
    volume: float
    end_date_iso: str
    closed: bool
    accepting_orders: bool
    tokens: List[Dict[str, Any]] = field(default_factory=list)
    ticker: str = ""
    event_slug: str = ""
    yes_bid: float = 0.0
    yes_ask: float = 1.0
    spread: float = 0.0


@dataclass
class PolymarketBook:
    bids: List[tuple] = field(default_factory=list)
    asks: List[tuple] = field(default_factory=list)
    spread: float = 0.0
    mid_price: float = 0.0
    # USD size available within 1% of mid (both sides) — depth proxy.
    liquidity_1pct: float = 0.0
    # 0-1 liquidity score derived from tight-spread depth.
    liquidity_score: float = 0.0


class PolymarketClient:
    """Read-only client for Polymarket — Gamma API for markets, CLOB for order book."""

    def __init__(self, timeout: int = 15):
        self.timeout = timeout

    def _gamma_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{GAMMA_BASE_URL}{path}"
        if params:
            parts = []
            for k, v in params.items():
                if v is not None:
                    parts.append(f"{k}={urllib.parse.quote(str(v))}")
            url = f"{url}?{'&'.join(parts)}"
        req = urllib.request.Request(url, headers=REQUEST_HEADERS)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode())

    def _clob_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{CLOB_BASE_URL}{path}"
        if params:
            parts = []
            for k, v in params.items():
                if v is not None:
                    parts.append(f"{k}={urllib.parse.quote(str(v))}")
            url = f"{url}?{'&'.join(parts)}"
        req = urllib.request.Request(url, headers=REQUEST_HEADERS)
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode())

    def search_markets(self, term: str = "", limit: int = 100, closed: bool = False) -> List[PolymarketMarket]:
        """Search markets by title keyword via Gamma API."""
        params = {"limit": min(limit, 100), "closed": str(closed).lower()}
        if term:
            params["title"] = term
        try:
            data = self._gamma_get("/markets", params)
        except Exception as e:
            logger.warning("Polymarket search failed: %s", e)
            return []
        if not isinstance(data, list):
            data = data.get("data", [])
        return [self._parse_gamma_market(m) for m in data if self._is_valid(m)]

    def fetch_markets(self, limit: int = 100, closed: bool = False) -> List[PolymarketMarket]:
        """Fetch all open markets from Gamma API."""
        params = {"limit": min(limit, 100), "closed": str(closed).lower()}
        try:
            data = self._gamma_get("/markets", params)
        except Exception as e:
            logger.warning("Polymarket Gamma fetch failed: %s", e)
            return []
        if not isinstance(data, list):
            data = data.get("data", [])
        return [self._parse_gamma_market(m) for m in data if self._is_valid(m)]

    def fetch_crypto_events(self, limit: int = 20) -> List[str]:
        """Get event slugs tagged as crypto via Gamma API."""
        params = {"limit": min(limit, 50), "closed": "false", "tag": "crypto"}
        try:
            data = self._gamma_get("/events", params)
        except Exception as e:
            logger.debug("Polymarket crypto events failed: %s", e)
            return []
        if not isinstance(data, list):
            data = data.get("data", [])
        return [e.get("slug", "") for e in data if e.get("slug")]

    def _filter_by_keywords(
        self, markets: List[PolymarketMarket], keywords: List[str], limit: int
    ) -> List[PolymarketMarket]:
        """Filter a list of markets by keyword match on question.
        Short keywords (< 4 chars) use word-boundary matching to avoid false positives.
        """
        def _matches(q: str, kw: str) -> bool:
            if len(kw) < 4:
                return bool(re.search(r'\b' + re.escape(kw) + r'\b', q))
            return kw in q
        matched = [m for m in markets if any(_matches(m.question.lower(), kw) for kw in keywords)]
        matched.sort(key=lambda m: m.volume, reverse=True)
        return matched[:limit]

    def _markets_by_event_slugs(
        self, slugs: List[str], all_markets: List[PolymarketMarket]
    ) -> List[PolymarketMarket]:
        """Filter markets whose event slug is in the given list."""
        slug_set = set(slugs)
        return [m for m in all_markets if m.event_slug in slug_set]

    def get_markets_by_tag(self, tag: str, limit: int = 50) -> List[PolymarketMarket]:
        """Fetch markets by Gamma tag — more efficient than keyword-filtering all markets."""
        params = {"limit": min(limit, 100), "closed": "false", "tag": tag}
        try:
            data = self._gamma_get("/markets", params)
        except Exception as e:
            logger.debug("Polymarket tag %s fetch failed: %s", tag, e)
            return []
        if not isinstance(data, list):
            data = data.get("data", [])
        return [self._parse_gamma_market(m) for m in data if self._is_valid(m)]

    def fetch_market_detail(self, condition_id: str) -> Optional[PolymarketMarket]:
        """Fetch a single market's full detail from Gamma."""
        try:
            data = self._gamma_get(f"/markets/{condition_id}")
        except Exception as e:
            logger.debug("Polymarket market detail failed: %s", e)
            return None
        return self._parse_gamma_market(data) if self._is_valid(data) else None

    def get_crypto_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        all_mkts = self.get_markets_by_tag("crypto", limit=limit)
        matched = self._filter_by_keywords(all_mkts, CRYPTO_KEYWORDS, limit)
        if not matched:
            # Fallback: keyword-filter from all markets
            all_mkts = self.fetch_markets(limit=100)
            matched = self._filter_by_keywords(all_mkts, CRYPTO_KEYWORDS, limit)
            if not matched:
                logger.info("No active Polymarket crypto markets found")
        return matched

    def get_sports_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        results = self.get_markets_by_tag("sports", limit=limit)
        if not results:
            logger.info("No active Polymarket sports markets found")
        return results

    def get_politics_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        results = self.get_markets_by_tag("politics", limit=limit)
        if not results:
            logger.info("No active Polymarket politics markets found")
        return results

    def get_entertainment_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        results = self.get_markets_by_tag("entertainment", limit=limit)
        if not results:
            logger.info("No active Polymarket entertainment markets found")
        return results

    def get_economics_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        results = self.get_markets_by_tag("economics", limit=limit)
        if not results:
            logger.info("No active Polymarket economics markets found")
        return results

    def get_technology_markets(self, limit: int = 100) -> List[PolymarketMarket]:
        results = self.get_markets_by_tag("technology", limit=limit)
        if not results:
            logger.info("No active Polymarket technology markets found")
        return results

    def get_all_category_markets(
        self, limit_per_category: int = 30
    ) -> Dict[str, List[PolymarketMarket]]:
        # Fetch once per tag instead of keyword-filtering the same 100 markets
        results = {}
        for tag in ALL_CATEGORY_KEYWORDS:
            results[tag] = self.get_markets_by_tag(tag, limit=limit_per_category)
            if not results[tag]:
                # Fallback: keyword filter from a single batch fetch
                all_mkts = self.fetch_markets(limit=100)
                results[tag] = self._filter_by_keywords(all_mkts, ALL_CATEGORY_KEYWORDS[tag], limit_per_category)
        return results

    def get_order_book(self, token_id: str, depth: int = 25) -> PolymarketBook:
        try:
            data = self._clob_get("/book", {"token_id": token_id})
        except Exception as e:
            logger.debug("Polymarket book failed for %s: %s", token_id, e)
            return PolymarketBook()
        asks = [(float(a.get("price", 0)), float(a.get("size", 0))) for a in data.get("asks", [])[:depth]]
        bids = [(float(b.get("price", 0)), float(b.get("size", 0))) for b in data.get("bids", [])[:depth]]
        asks.sort(key=lambda x: x[0])
        bids.sort(key=lambda x: x[0], reverse=True)
        best_ask = asks[0][0] if asks else 0.0
        best_bid = bids[0][0] if bids else 0.0

        # Cumulative liquidity: USD available within 1% of mid on each side.
        mid = (best_ask + best_bid) / 2 if best_ask > 0 and best_bid > 0 else 0.0
        if mid > 0:
            ask_liq = sum(p * s for p, s in asks if p <= mid * 1.01)
            bid_liq = sum(p * s for p, s in bids if p >= mid * 0.99)
            liq_1pct = ask_liq + bid_liq
        else:
            liq_1pct = 0.0
        spread = (best_ask - best_bid) if best_ask > 0 and best_bid > 0 else 0.0
        # Score: reward depth (saturates ~$10k within 1%) and penalise wide spreads.
        depth_component = min(liq_1pct / 10000.0, 1.0)
        spread_component = max(0.0, 1.0 - spread * 5)
        liq_score = round(depth_component * spread_component, 4)

        return PolymarketBook(
            bids=bids,
            asks=asks,
            spread=spread,
            mid_price=mid,
            liquidity_1pct=round(liq_1pct, 2),
            liquidity_score=liq_score,
        )

    def _parse_gamma_market(self, raw: dict) -> PolymarketMarket:
        outcomes_raw = raw.get("outcomes", ["Yes", "No"])
        if isinstance(outcomes_raw, str):
            try:
                outcomes = json.loads(outcomes_raw)
            except (json.JSONDecodeError, TypeError):
                outcomes = ["Yes", "No"]
        else:
            outcomes = outcomes_raw
        raw_prices_raw = raw.get("outcomePrices", ["0.5", "0.5"])
        if isinstance(raw_prices_raw, str):
            try:
                raw_prices = json.loads(raw_prices_raw)
            except (json.JSONDecodeError, TypeError):
                raw_prices = ["0.5", "0.5"]
        else:
            raw_prices = raw_prices_raw
        prices = {}
        for i, o in enumerate(outcomes):
            try:
                prices[o] = float(raw_prices[i]) if i < len(raw_prices) else 0.5
            except (ValueError, IndexError):
                prices[o] = 0.5
        vol_raw = raw.get("volume", 0)
        if isinstance(vol_raw, str):
            try:
                vol = float(vol_raw.replace(",", ""))
            except (ValueError, AttributeError):
                vol = 0.0
        else:
            vol = float(vol_raw or 0)
        events = raw.get("events", [])
        event_slug = events[0].get("slug", "") if events else ""
        # Use Gamma's bestBid/bestAsk / spread directly. A missing book
        # (bestAsk absent) means we have NO valid ask => treat as spread=1.0
        # so the liquidity filter rejects it (overstated liquidity guard, P1-8).
        g_spread = float(raw.get("spread", 0) or 0)
        yes_bid = float(raw.get("bestBid", 0) or 0)
        raw_ask = raw.get("bestAsk")
        yes_ask = float(raw_ask) if raw_ask not in (None, "", 0) else 0.0
        if g_spread <= 0 and yes_ask <= 0:
            g_spread = 1.0
        token_ids_raw = raw.get("clobTokenIds") or []
        if isinstance(token_ids_raw, str):
            try:
                token_ids = json.loads(token_ids_raw)
            except (json.JSONDecodeError, TypeError):
                token_ids = []
        else:
            token_ids = token_ids_raw
        return PolymarketMarket(
            condition_id=raw.get("conditionId", raw.get("condition_id", "")),
            question=raw.get("question", ""),
            description=raw.get("description", ""),
            outcomes=outcomes,
            outcome_prices=prices,
            volume=vol,
            end_date_iso=raw.get("endDateIso", raw.get("end_date_iso", "")),
            closed=bool(raw.get("closed", False)),
            accepting_orders=bool(raw.get("acceptingOrders", raw.get("accepting_orders", True))),
            tokens=[{"token_id": tid} for tid in token_ids],
            ticker=raw.get("slug", ""),
            event_slug=event_slug,
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            spread=g_spread,
        )

    def _is_valid(self, raw: dict) -> bool:
        cid = raw.get("conditionId") or raw.get("condition_id")
        q = raw.get("question")
        return bool(cid) and bool(q)


def format_market(m: PolymarketMarket) -> str:
    prices_str = ", ".join(f"{k}: {v*100:.1f}%" for k, v in m.outcome_prices.items())
    return (
        f"  [{m.condition_id[:12]}…] {m.question[:80]}\n"
        f"    Prices: {prices_str}\n"
        f"    Volume: ${m.volume:,.0f}  End: {m.end_date_iso[:10]}\n"
        f"    Accepting: {m.accepting_orders}  Closed: {m.closed}"
    )
