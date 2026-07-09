"""
Unified Prediction Market Client — wraps Kalshi and Polymarket into a single interface.

Consolidates the 4 separate Kalshi implementations and 3 Polymarket implementations
into one authoritative client with a common data model.

Usage:
    client = UnifiedPredictionMarketClient()
    markets = client.search_all(limit=50)
    for m in markets:
        print(m.format())
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .kalshi_client import KalshiClient as _KalshiClient
from .polymarket_client import PolymarketClient as _PolymarketClient
from .polymarket_relayer import PolymarketRelayerClient as _PolymarketRelayerClient

logger = logging.getLogger("prediction_markets")

# Crypto / macro keywords used across both platforms
CRYPTO_KEYWORDS = [
    "bitcoin", "btc", "ethereum", "eth", "solana", "sol",
    "crypto", "cryptocurrency", "defi", "token", "coin",
    "avax", "avalanche", "matic", "polygon", "link", "chainlink",
    "doge", "dogecoin", "xrp", "ripple", "ada", "cardano",
    "dot", "polkadot", "uni", "uniswap",
    "inflation", "cpi", "fed", "interest", "rate",
    "recession", "gdp", "unemployment", "sp500",
]


@dataclass
class PredictionMarket:
    """Unified prediction market data model for both Kalshi and Polymarket."""
    platform: str               # "kalshi" | "polymarket"
    market_id: str              # platform-specific ID (ticker for Kalshi, condition_id for Polymarket)
    question: str               # Human-readable market question
    outcomes: List[str]         # ["YES", "NO"] or similar
    outcome_prices: Dict[str, float]  # {outcome: probability 0-1}
    volume: float               # 24h volume in USD
    end_date: str               # ISO date string
    is_open: bool               # Accepting orders?
    yes_bid: float = 0.0       # Best bid for YES (Kalshi) or outcome0 bid (Polymarket)
    yes_ask: float = 0.0       # Best ask for YES
    spread: float = 0.0        # Bid-ask spread
    liquidity_score: float = 0.0  # 0-1, based on volume + depth
    category: str = "general"     # crypto | sports | politics | entertainment | economics | technology | general
    keywords: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)

    @property
    def mid_price(self) -> float:
        yes = self.outcome_prices.get("YES")
        if yes is None and self.outcome_prices:
            yes = next(iter(self.outcome_prices.values()))
        return float(yes) if yes is not None else 0.0

    @property
    def probability_extremity(self) -> float:
        """0 = neutral (0.5), 1 = extreme (0 or 1)."""
        mp = self.mid_price
        if mp <= 0 or mp >= 1:
            return 0.0
        return abs(mp - 0.5) / 0.5

    @property
    def is_relevant(self) -> bool:
        q = self.question.lower()
        return any(kw in q for kw in CRYPTO_KEYWORDS)

    def format(self) -> str:
        prices = ", ".join(f"{k}: {v*100:.1f}%" for k, v in self.outcome_prices.items())
        return (
            f"[{self.platform:>10s}] {self.question[:90]}\n"
            f"  {prices}  Vol=${self.volume:,.0f}  Spread={self.spread:.2%}  "
            f"Liq={self.liquidity_score:.2f}  Close={self.end_date[:10]}"
        )


class UnifiedPredictionMarketClient:
    """Single client that fetches from both Kalshi and Polymarket."""

    _category_cache: dict[str, Any] = {}
    _category_cache_ts: float = 0

    def __init__(
        self,
        kalshi_email: str = "",
        kalshi_password: str = "",
        kalshi_api_key_id: str = "",
        kalshi_private_key_path: str = "",
        kalshi_base_url: str = "",
        polymarket_relayer_api_key: str = "",
        polymarket_relayer_api_key_address: str = "",
        polymarket_relayer_url: str = "",
        polymarket_relayer_credentials_path: str = "",
        timeout: int = 15,
    ):
        self._kalshi = _KalshiClient(
            email=kalshi_email,
            password=kalshi_password,
            api_key_id=kalshi_api_key_id,
            private_key_path=kalshi_private_key_path,
            base_url=kalshi_base_url,
            timeout=timeout,
        )
        self._polymarket = _PolymarketClient(timeout=timeout)
        self._polymarket_relayer = _PolymarketRelayerClient(
            api_key=polymarket_relayer_api_key,
            api_key_address=polymarket_relayer_api_key_address,
            relayer_url=polymarket_relayer_url,
            credentials_path=polymarket_relayer_credentials_path,
            timeout=timeout,
        )

    # ── Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _detect_category(question: str) -> str:
        import re
        q = question.lower()
        from .polymarket_client import ALL_CATEGORY_KEYWORDS as _cats
        scores: dict = {}
        for cat, kws in _cats.items():
            for kw in kws:
                kwl = kw.lower()
                # Short keywords (< 4 chars) use word-boundary matching to avoid false positives
                if len(kwl) < 4:
                    if re.search(r'\b' + re.escape(kwl) + r'\b', q):
                        scores[cat] = scores.get(cat, 0) + 1
                else:
                    if kwl in q:
                        scores[cat] = scores.get(cat, 0) + len(kwl)
        if not scores:
            return "general"
        return max(scores, key=scores.get)

    def _kalshi_to_unified(
        self,
        raw: List,
        limit: int,
        min_volume: float,
        max_spread: float,
    ) -> List[PredictionMarket]:
        results = []
        for m in raw:
            if m.settled or m.status not in ("open", "active"):
                continue
            if m.volume < min_volume:
                continue
            spread = m.yes_ask - m.yes_bid
            if spread > max_spread:
                continue
            ls = min(m.volume / 50000, 1.0) * max(0, 1 - spread * 3)
            category = self._detect_category(m.title)
            keywords = [kw for kw in CRYPTO_KEYWORDS if kw in m.title.lower()]
            results.append(PredictionMarket(
                platform="kalshi",
                market_id=m.ticker,
                question=m.title,
                outcomes=["YES", "NO"],
                outcome_prices={"YES": m.yes_bid, "NO": m.no_bid},
                volume=m.volume,
                end_date=m.close_date,
                is_open=m.status in ("open", "active"),
                yes_bid=m.yes_bid,
                yes_ask=m.yes_ask,
                spread=spread,
                liquidity_score=ls,
                category=category,
                keywords=keywords,
                raw_data={"event_ticker": m.event_ticker},
            ))
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def _polymarket_to_unified(
        self,
        raw: List,
        limit: int,
        min_volume: float,
        max_spread: float,
    ) -> List[PredictionMarket]:
        results = []
        for pm in raw:
            if not pm.accepting_orders or pm.closed:
                continue
            if pm.volume < min_volume:
                continue
            # Use Gamma's built-in bid/ask/spread; fall back to CLOB order book
            spread = pm.spread
            bid = pm.yes_bid
            ask = pm.yes_ask
            if spread <= 0 and pm.tokens and "token_id" in pm.tokens[0]:
                book = self._polymarket.get_order_book(
                    pm.tokens[0]["token_id"]
                )
                spread = book.spread
                bid = max((p for p, _ in book.bids), default=0)
                ask = min((p for p, _ in book.asks), default=1)
            if spread > max_spread:
                continue
            token_ids = [t["token_id"] for t in pm.tokens if "token_id" in t]
            ls = min(pm.volume / 50000, 1.0) * max(0, 1 - spread * 3)
            results.append(PredictionMarket(
                platform="polymarket",
                market_id=pm.condition_id,
                question=pm.question,
                outcomes=pm.outcomes,
                outcome_prices=pm.outcome_prices,
                volume=pm.volume,
                end_date=pm.end_date_iso,
                is_open=pm.accepting_orders,
                yes_bid=bid,
                yes_ask=ask,
                spread=spread,
                liquidity_score=ls,
                category=self._detect_category(pm.question),
                raw_data={"token_ids": token_ids},
            ))
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    # ── Kalshi ──────────────────────────────────────────────────────

    def search_kalshi(
        self,
        term: str = "",
        limit: int = 30,
        min_volume: float = 1000,
        max_spread: float = 0.15,
    ) -> List[PredictionMarket]:
        """Search Kalshi markets and return unified model."""
        has_email_auth = bool(getattr(self._kalshi, "email", "") and getattr(self._kalshi, "password", ""))
        has_api_key_auth = bool(getattr(self._kalshi, "api_key_id", "") and getattr(self._kalshi, "private_key_path", ""))
        if not (has_email_auth or has_api_key_auth):
            return []
        if term:
            raw = self._kalshi.search_markets(term=term, limit=limit)
        else:
            raw = self._kalshi.get_relevant_markets(limit=limit)
        return self._kalshi_to_unified(raw, limit, min_volume, max_spread)

    def get_kalshi_order_book_depth(self, ticker: str) -> Dict[str, Any]:
        return self._kalshi.get_order_book(ticker)

    # ── Polymarket ──────────────────────────────────────────────────

    def search_polymarket(
        self,
        term: str = "",
        limit: int = 30,
        min_volume: float = 1000,
        max_spread: float = 0.15,
    ) -> List[PredictionMarket]:
        """Search Polymarket markets and return unified model."""
        raw = self._polymarket.search_markets(term=term, limit=limit, closed=False)
        results = self._polymarket_to_unified(raw, limit, min_volume, max_spread)
        cat = self._detect_category(term) if term else "general"
        # Add category + keywords to each result
        for r in results:
            if cat != "general":
                r.category = cat
            else:
                r.category = self._detect_category(r.question)
            r.keywords = [kw for kw in CRYPTO_KEYWORDS if kw in r.question.lower()]
        return results

    def get_polymarket_order_book(self, token_id: str):
        return self._polymarket.get_order_book(token_id)

    def get_polymarket_relayer_keys(self) -> list[dict[str, Any]]:
        return self._polymarket_relayer.list_api_keys()

    # ── Combined ────────────────────────────────────────────────────

    def search_all(
        self,
        term: str = "",
        limit: int = 30,
        min_volume: float = 1000,
        max_spread: float = 0.15,
    ) -> List[PredictionMarket]:
        """Search both platforms and return merged, ranked results."""
        results = []
        try:
            results.extend(self.search_kalshi(term=term, limit=limit, min_volume=min_volume, max_spread=max_spread))
        except Exception as e:
            logger.warning("Kalshi search failed: %s", e)
        try:
            results.extend(self.search_polymarket(term=term, limit=limit, min_volume=min_volume, max_spread=max_spread))
        except Exception as e:
            logger.warning("Polymarket search failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def search_all_categories(
        self,
        limit_per_platform: int = 15,
        min_volume: float = 0,
        max_spread: float = 0.25,
    ) -> Dict[str, List[PredictionMarket]]:
        """Search ALL event categories across both platforms.

        Uses keyword-based categorization since Polymarket Gamma tags don't
        reliably filter by category. Results cached for 45 seconds.
        """
        now = time.time()
        if self._category_cache and (now - self._category_cache_ts) < 45:
            return self._category_cache

        categories = ["crypto", "sports", "politics", "entertainment", "economics", "technology"]
        result: Dict[str, List[PredictionMarket]] = {c: [] for c in categories}

        # Polymarket: fetch once, categorize by keyword matching
        try:
            all_raw = self._polymarket.fetch_markets(limit=limit_per_platform * 4)
            all_poly = self._polymarket_to_unified(all_raw, len(all_raw), min_volume, max_spread)
            for m in all_poly:
                cat = self._detect_category(m.question)
                if cat in result:
                    m.category = cat
                    m.keywords = [kw for kw in CRYPTO_KEYWORDS if kw in m.question.lower()]
                    result[cat].append(m)
        except Exception as e:
            logger.debug("Polymarket category search failed: %s", e)

        # Kalshi: category-based search if API auth available
        KALSHI_CAT_MAP: dict[str, str] = {
            "Sports": "sports",
            "Entertainment": "entertainment",
            "Politics": "politics",
            "Elections": "politics",
            "Economics": "economics",
            "Financials": "economics",
            "Crypto": "crypto",
            "Science and Technology": "technology",
            "Climate and Weather": "general",
        }
        has_api_auth = bool(getattr(self._kalshi, "api_key_id", "") and getattr(self._kalshi, "private_key_path", ""))
        if has_api_auth:
            try:
                by_category = self._kalshi.get_markets_by_categories(total_event_limit=20)
                for kalshi_cat, markets in by_category.items():
                    unified_cat = KALSHI_CAT_MAP.get(kalshi_cat, "general")
                    if unified_cat not in result:
                        continue
                    for m in self._kalshi_to_unified(markets, len(markets), min_volume, max_spread):
                        if m.market_id not in {x.market_id for x in result[unified_cat]}:
                            result[unified_cat].append(m)
            except Exception as e:
                logger.debug("Kalshi category search failed: %s", e)
        else:
            # Fallback: keyword-filter broad search
            try:
                kalshi_raw = self._kalshi.search_broad(limit=limit_per_platform * 4)
                unified = self._kalshi_to_unified(kalshi_raw, len(kalshi_raw), min_volume, max_spread)
                for m in unified:
                    cat = self._detect_category(m.question)
                    if cat in result and m.market_id not in {x.market_id for x in result[cat]}:
                        result[cat].append(m)
            except Exception as e:
                logger.debug("Kalshi broad search failed: %s", e)

        # Sort each category by volume and limit
        for cat in categories:
            result[cat].sort(key=lambda p: p.volume, reverse=True)
            result[cat] = result[cat][:limit_per_platform]

        self._category_cache = result
        self._category_cache_ts = now
        return result

    def get_crypto_markets(self, limit: int = 50) -> List[PredictionMarket]:
        """Get crypto-relevant markets from both platforms."""
        results = []
        try:
            poly_markets = self._polymarket.get_crypto_markets(limit=limit)
            results.extend(self._polymarket_to_unified(poly_markets, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Polymarket crypto markets failed: %s", e)
        try:
            kalshi = self._kalshi.get_relevant_markets(limit=limit)
            results.extend(self._kalshi_to_unified(kalshi, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Kalshi crypto markets failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def get_sports_markets(self, limit: int = 50) -> List[PredictionMarket]:
        results = []
        try:
            raw = self._polymarket.get_sports_markets(limit=limit)
            results.extend(self._polymarket_to_unified(raw, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Polymarket sports markets failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def get_politics_markets(self, limit: int = 50) -> List[PredictionMarket]:
        results = []
        try:
            raw = self._polymarket.get_politics_markets(limit=limit)
            results.extend(self._polymarket_to_unified(raw, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Polymarket politics markets failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def get_entertainment_markets(self, limit: int = 50) -> List[PredictionMarket]:
        results = []
        try:
            raw = self._polymarket.get_entertainment_markets(limit=limit)
            results.extend(self._polymarket_to_unified(raw, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Polymarket entertainment markets failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def get_economics_markets(self, limit: int = 50) -> List[PredictionMarket]:
        results = []
        try:
            raw = self._polymarket.get_economics_markets(limit=limit)
            results.extend(self._polymarket_to_unified(raw, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Polymarket economics markets failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def get_technology_markets(self, limit: int = 50) -> List[PredictionMarket]:
        results = []
        try:
            raw = self._polymarket.get_technology_markets(limit=limit)
            results.extend(self._polymarket_to_unified(raw, limit, 0, 1.0))
        except Exception as e:
            logger.warning("Polymarket technology markets failed: %s", e)
        results.sort(key=lambda p: p.volume, reverse=True)
        return results[:limit]

    def format_markets(self, markets: List[PredictionMarket]) -> str:
        lines = [f"{'Platform':>10s}  {'Question':<90s}  {'Prob':>6s}  {'Vol':>10s}  {'Spread':>7s}  {'Liq':>5s}"]
        lines.append("-" * 140)
        for m in markets:
            mp = m.mid_price
            lines.append(
                f"{m.platform:>10s}  {m.question:<90s}  {mp*100:>5.1f}%  "
                f"${m.volume:>8,.0f}  {m.spread:>6.2%}  {m.liquidity_score:>4.2f}"
            )
        return "\n".join(lines)


def main():
    """CLI: print prediction markets across all categories."""
    import os

    logging.basicConfig(level=logging.INFO)
    client = UnifiedPredictionMarketClient(
        kalshi_email=os.environ.get("KALSHI_EMAIL", ""),
        kalshi_password=os.environ.get("KALSHI_PASSWORD", ""),
        kalshi_api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
        kalshi_private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
        kalshi_base_url=os.environ.get("KALSHI_API_BASE_URL", ""),
        polymarket_relayer_api_key=os.environ.get("RELAYER_API_KEY", ""),
        polymarket_relayer_api_key_address=os.environ.get("RELAYER_API_KEY_ADDRESS", ""),
        polymarket_relayer_url=os.environ.get("POLYMARKET_RELAYER_URL", ""),
        polymarket_relayer_credentials_path=os.environ.get("POLYMARKET_RELAYER_CREDENTIALS_PATH", ""),
    )
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", "-c", default="crypto",
                        choices=["crypto", "sports", "politics", "entertainment", "economics", "technology", "all"])
    parser.add_argument("--limit", "-l", type=int, default=20)
    args = parser.parse_args()

    if args.category == "all":
        cats = client.search_all_categories(limit_per_platform=args.limit)
        total = sum(len(v) for v in cats.values())
        print(f"All Categories: {total} markets\n")
        for cat, markets in cats.items():
            if markets:
                print(f"── {cat.upper()} ({len(markets)}) ──")
                print(client.format_markets(markets[:10]))
                print()
        return

    fn = getattr(client, f"get_{args.category}_markets", None)
    if not fn:
        print(f"Unknown category: {args.category}")
        return
    markets = fn(limit=args.limit)
    print(f"{args.category.title()} Markets ({len(markets)} found):")
    print(client.format_markets(markets))


if __name__ == "__main__":
    main()
