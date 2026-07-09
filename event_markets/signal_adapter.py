"""
Prediction Market Signal Adapter — converts Kalshi/Polymarket data into
AccumulatedSignal objects for the UnifiedSignalAccumulator.

Follows the pattern of NewsSentimentAdapter (unified_signal_accumulator.py):
  - get_signals() returns List[AccumulatedSignal]
  - Each signal maps a prediction market probability to a BUY/SELL direction
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .unified_client import UnifiedPredictionMarketClient, PredictionMarket

logger = logging.getLogger("prediction_market_adapter")

# Order book cache: 30-second TTL
_book_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

# Map non-crypto events to crypto symbols when they have market-moving potential
# Sports / major events → general market sentiment
# Politics → regulatory / macro uncertainty
# Economics → direct macro impact
# Technology → sector-specific
EVENT_SYMBOL_MAP: List[tuple] = [
    # Sports — major events affect general market risk sentiment
    ("super bowl", "BTC-USD"),
    ("world cup", "BTC-USD"),
    ("champions league", "BTC-USD"),
    ("nba champion", "BTC-USD"),
    ("nfl champion", "BTC-USD"),
    ("world series", "BTC-USD"),
    ("stanley cup", "BTC-USD"),
    ("final four", "BTC-USD"),
    ("olympics", "BTC-USD"),
    # Politics — regulatory/macro uncertainty
    ("president", "BTC-USD"),
    ("presidential", "BTC-USD"),
    ("election", "BTC-USD"),
    ("congress", "BTC-USD"),
    ("senate", "BTC-USD"),
    ("regulation", "BTC-USD"),
    ("sec", "BTC-USD"),
    # Economics — direct market impact
    ("fed", "BTC-USD"),
    ("federal reserve", "BTC-USD"),
    ("inflation", "BTC-USD"),
    ("interest rate", "BTC-USD"),
    ("cpi", "BTC-USD"),
    ("gdp", "BTC-USD"),
    ("recession", "BTC-USD"),
    ("unemployment", "BTC-USD"),
    # Technology — sector-specific
    ("ethereum", "ETH-USD"),
    ("eth", "ETH-USD"),
    ("solana", "SOL-USD"),
    ("sol", "SOL-USD"),
    ("ai", "NVDA"),
    ("artificial intelligence", "NVDA"),
    ("nvidia", "NVDA"),
    ("openai", "MSFT"),
    ("spacex", "TSLA"),
    ("starship", "TSLA"),
    # Crypto (direct)
    ("bitcoin", "BTC-USD"),
    ("btc", "BTC-USD"),
    ("dogecoin", "DOGE-USD"),
    ("doge", "DOGE-USD"),
    ("xrp", "XRP-USD"),
    ("ripple", "XRP-USD"),
    ("cardano", "ADA-USD"),
    ("polkadot", "DOT-USD"),
    ("avalanche", "AVAX-USD"),
    ("chainlink", "LINK-USD"),
    ("uniswap", "UNI-USD"),
    ("polygon", "POL-USD"), ("matic", "POL-USD"), ("pol", "POL-USD"),
    ("cosmos", "ATOM-USD"), ("atom", "ATOM-USD"),
    ("litecoin", "LTC-USD"), ("ltc", "LTC-USD"),
    ("bitcoin cash", "BCH-USD"), ("bitcoincash", "BCH-USD"),
    ("near", "NEAR-USD"), ("aptos", "APT-USD"), ("apt", "APT-USD"),
    ("sui", "SUI-USD"), ("arbitrum", "ARB-USD"), ("arb", "ARB-USD"),
    ("optimism", "OP-USD"), ("op", "OP-USD"),
    ("filecoin", "FIL-USD"), ("injective", "INJ-USD"),
    ("sei", "SEI-USD"), ("celestia", "TIA-USD"), ("tia", "TIA-USD"),
    ("shiba", "SHIB-USD"), ("shib", "SHIB-USD"),
    ("pepe", "PEPE-USD"), ("bonk", "BONK-USD"),
    ("trump", "TRUMP-USD"), ("floki", "FLOKI-USD"),
    ("algorand", "ALGO-USD"),
    ("stellar", "XLM-USD"), ("stacks", "STX-USD"),
    ("hedera", "HBAR-USD"),
    ("internet computer", "ICP-USD"), ("grt", "GRT-USD"),
    # Non-crypto commodities / indices
    ("sp500", "SPY"),
    ("s&p 500", "SPY"),
    ("s&p500", "SPY"),
    ("gold", "GLD"),
]

# Categories that get actionable Coinbase trades (vs read-only dashboard)
ACTIONABLE_CATEGORIES = {"crypto", "economics", "technology"}

# Fee constants
KALSHI_FEE = 0.02  # 2% per trade
POLYMARKET_FEE = 0.02  # 2% protocol fee
ESTIMATED_GAS = 0.005  # ~0.5% gas on Polygon


class PredictionMarketAdapter:
    """Adapts Kalshi/Polymarket data into AccumulatedSignal-compatible outputs.

    Handles ALL event categories:
      - crypto: direct symbol mapping, always actionable
      - economics/technology: mapped to crypto/stock symbols, usually actionable
      - sports/politics/entertainment: speculative mapping, dashboard display
    """

    def __init__(
        self,
        kalshi_email: str = "",
        kalshi_password: str = "",
        kalshi_api_key_id: str = "",
        kalshi_private_key_path: str = "",
        min_volume: float = 2000,
        min_extremity: float = 0.2,
        min_open_interest: float = 100,
        max_spread: float = 0.20,
        categories: Optional[List[str]] = None,
    ):
        self._client = UnifiedPredictionMarketClient(
            kalshi_email=kalshi_email or os.environ.get("KALSHI_EMAIL", ""),
            kalshi_password=kalshi_password or os.environ.get("KALSHI_PASSWORD", ""),
            kalshi_api_key_id=kalshi_api_key_id or os.environ.get("KALSHI_API_KEY_ID", ""),
            kalshi_private_key_path=kalshi_private_key_path or os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
        )
        self.min_volume = min_volume
        self.min_extremity = min_extremity
        self.min_open_interest = min_open_interest
        self.max_spread = max_spread
        # Default: only crypto (fast, single keyword list). Enable all with categories=["*"].
        self.categories = categories or ["crypto"]

    def _get_order_book_depth(self, market: PredictionMarket) -> Tuple[float, float]:
        """Fetch and cache order book depth. Returns (bid_depth_1pct, ask_depth_1pct)."""
        cache_key = f"{market.platform}:{market.market_id}"
        now = time.time()
        
        if cache_key in _book_cache:
            ts, data = _book_cache[cache_key]
            if now - ts < 30:  # 30-second TTL
                return data.get("bid_depth", 0), data.get("ask_depth", 0)
        
        try:
            if market.platform == "kalshi":
                book = self._client.get_kalshi_order_book_depth(market.market_id)
                # Kalshi orderbook: bids/asks with price/size
                bids = book.get("bids", [])
                asks = book.get("asks", [])
                mid = market.mid_price
                bid_depth = sum(s for p, s in bids if p >= mid * 0.99)
                ask_depth = sum(s for p, s in asks if p <= mid * 1.01)
            else:  # polymarket
                token_ids = market.raw_data.get("token_ids", [])
                if not token_ids:
                    return 0, 0
                book = self._client.get_polymarket_order_book(token_ids[0])
                mid = market.mid_price
                bid_depth = sum(s for p, s in book.bids if p >= mid * 0.99)
                ask_depth = sum(s for p, s in book.asks if p <= mid * 1.01)
            
            _book_cache[cache_key] = (now, {"bid_depth": bid_depth, "ask_depth": ask_depth})
            return bid_depth, ask_depth
        except Exception as e:
            logger.debug("Order book fetch failed for %s: %s", market.market_id, e)
            return 0, 0

    def _hours_to_expiry(self, end_date: str) -> float:
        """Calculate hours until market expiry."""
        try:
            # Parse ISO format
            end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            hours = (end_dt - now).total_seconds() / 3600
            return max(0, hours)
        except Exception:
            return 168  # Default 1 week

    def get_signals(self, price_map: Optional[Dict[str, float]] = None) -> List[Any]:
        """Fetch prediction market signals for configured categories.

        Returns list of dicts matching AccumulatedSignal fields so callers
        can construct the dataclass directly.  Returns empty list on error.
        """
        signals = []
        use_crypto_only = self.categories == ["crypto"]
        try:
            if use_crypto_only:
                markets = self._client.get_crypto_markets(limit=30)
                for m in markets:
                    if not m.is_open or m.volume < self.min_volume:
                        continue
                    if m.raw_data.get("open_interest", 0) < self.min_open_interest:
                        continue
                    if m.spread > self.max_spread:
                        continue
                    sigs = self._market_to_signals(m)
                    signals.extend(sigs)
            else:
                all_cats = self.categories if self.categories != ["*"] else None
                limit = 20 if all_cats else 12
                categories = self._client.search_all_categories(
                    limit_per_platform=limit, min_volume=self.min_volume, max_spread=0.25
                )
                for cat, mkt_list in categories.items():
                    if all_cats and cat not in all_cats:
                        continue
                    for m in mkt_list:
                        if not m.is_open or m.volume < self.min_volume:
                            continue
                        if m.raw_data.get("open_interest", 0) < self.min_open_interest:
                            continue
                        if m.spread > self.max_spread:
                            continue
                        sigs = self._market_to_signals(m)
                        signals.extend(sigs)
        except Exception as e:
            logger.warning("Prediction market fetch failed: %s", e)
            return []

        # Deduplicate by (symbol, action, strategy_name)
        seen = set()
        unique = []
        for s in signals:
            key = (s["symbol"], s["action"], s.get("strategy_name", ""))
            if key not in seen:
                seen.add(key)
                unique.append(s)
        return unique

    def _market_to_signals(self, m: PredictionMarket) -> List[Dict[str, Any]]:
        """Convert a single prediction market into 0-2 signals."""
        mp = m.mid_price
        if mp <= 0 or mp >= 1:
            return []

        extremity = m.probability_extremity
        if extremity < self.min_extremity:
            return []

        # Order book depth for liquidity validation
        bid_depth, ask_depth = self._get_order_book_depth(m)
        depth_score = min((bid_depth + ask_depth) / (m.volume * 0.01), 1.0) if m.volume > 0 else 0
        
        # Time to expiry weighting
        hours_left = self._hours_to_expiry(m.end_date)
        time_weight = min(1.0, hours_left / 168)  # Cap at 1 week
        
        # Confidence: extremity * liquidity * depth * time_weight
        confidence = min(extremity * m.liquidity_score * 1.5 * depth_score * time_weight, 0.95)
        base_score = confidence * 0.5

        symbol = self._question_to_symbol(m.question, m.category)
        # Lower confidence for non-crypto categories (more speculative signal)
        if m.category not in ACTIONABLE_CATEGORIES:
            confidence *= 0.6

        signals = []
        base_reason = (
            f"{m.platform} [{m.category}]: {m.question[:60]} → {mp*100:.0f}% YES "
            f"(vol=${m.volume:.0f}, liq={m.liquidity_score:.2f}, depth={depth_score:.2f}, "
            f"hours_left={hours_left:.1f})"
        )

        if mp > 0.5 + self.min_extremity * 0.5:
            signals.append(self._make_signal(symbol, "BUY", confidence, base_score, m, base_reason))
        elif mp < 0.5 - self.min_extremity * 0.5:
            signals.append(self._make_signal(symbol, "SELL", confidence, base_score, m, base_reason))

        return signals

    def _make_signal(
        self, symbol: str, action: str, confidence: float, base_score: float,
        m: PredictionMarket, reason: str
    ) -> Dict[str, Any]:
        is_actionable = m.category in ACTIONABLE_CATEGORIES
        
        # Get order book depth
        bid_depth, ask_depth = self._get_order_book_depth(m)
        
        # Kelly fraction for prediction markets: f = (p - q) / (1 - spread)
        # where p = model prob, q = market prob, spread = bid-ask spread + fees
        mp = m.mid_price
        if action == "BUY":
            p = mp if mp > 0.5 else 1 - mp
        else:
            p = mp if mp < 0.5 else 1 - mp
        q = 1 - p
        effective_spread = m.spread + (KALSHI_FEE if m.platform == "kalshi" else POLYMARKET_FEE + ESTIMATED_GAS)
        kelly_f = max(0, (p - q) / (1 - effective_spread)) if effective_spread < 1 else 0
        kelly_f = min(kelly_f, 0.5)  # Cap at half-Kelly

        # Time-to-expiry weighting
        hours_left = self._hours_to_expiry(m.end_date)
        time_weight = min(1.0, hours_left / 168)  # Cap at 1 week
        
        adjusted_confidence = confidence * time_weight

        return {
            "symbol": symbol,
            "action": action,
            "base_confidence": round(confidence, 3),
            "final_confidence": round(adjusted_confidence, 3),
            "opportunity_score": round(base_score * (1 + m.probability_extremity) * time_weight, 4),
            "strategy_name": f"PM:{m.platform}:{m.category}",
            "signal_reason": reason,
            "estimated_volume_usd": round(confidence * m.volume * 0.1, 2),
            "kelly_fraction": round(kelly_f, 4),
            "hours_to_expiry": round(hours_left, 1),
            "market_data": {
                "platform": m.platform,
                "category": m.category,
                "question": m.question,
                "probability": m.mid_price,
                "volume": m.volume,
                "open_interest": m.raw_data.get("open_interest", 0),
                "spread": m.spread,
                "liquidity_score": m.liquidity_score,
                "bid_depth_1pct": round(bid_depth, 2),
                "ask_depth_1pct": round(ask_depth, 2),
                "actionable": is_actionable,
            },
        }

    def _hours_to_expiry(self, end_date: str) -> float:
        """Calculate hours until market expiry."""
        try:
            # Parse ISO format, handle both with and without timezone
            if end_date.endswith("Z"):
                end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            else:
                end_dt = datetime.fromisoformat(end_date)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            delta = end_dt - now
            return max(0, delta.total_seconds() / 3600)
        except Exception:
            return 168  # Default to 1 week

    @staticmethod
    def _question_to_symbol(question: str, category: str = "general") -> str:
        """Map a prediction market question to the most relevant tradeable symbol."""
        import re
        q = question.lower()
        for kw, sym in EVENT_SYMBOL_MAP:
            # Use word-boundary matching to avoid substring false positives
            # e.g. "pol" shouldn't match "politics"
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, q):
                return sym
        # Fallback by category
        cat_map = {
            "crypto": "BTC-USD",
            "economics": "BTC-USD",
            "sports": "BTC-USD",
            "politics": "BTC-USD",
            "entertainment": "BTC-USD",
            "technology": "NVDA",
        }
        return cat_map.get(category, "BTC-USD")
