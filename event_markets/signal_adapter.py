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
        max_spread: float = 0.15,
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
                # Kalshi orderbook returns lists of {"price", "size"} dicts
                # (prices/sizes may be strings); also accept (price, size) tuples.
                bids = book.get("bids", []) if isinstance(book, dict) else []
                asks = book.get("asks", []) if isinstance(book, dict) else []
                mid = market.mid_price
                bid_depth = sum(_depth_size(b) for b in bids
                                if _depth_price(b) >= mid * 0.99)
                ask_depth = sum(_depth_size(b) for b in asks
                                if _depth_price(b) <= mid * 1.01)
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

    def get_signals(
        self,
        price_map: Optional[Dict[str, float]] = None,
        kg_assessments: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        """Fetch prediction market signals for configured categories.

        Returns list of dicts matching AccumulatedSignal fields so callers
        can construct the dataclass directly.  Returns empty list on error.

        ``kg_assessments`` maps ``market.market_id`` -> a
        ``KnowledgeGapAssessment``; when a significant assessment exists it
        overrides the signal side and boosts confidence (see
        ``_kg_side_and_boost``), reconciling the adapter with the optimizer.
        """
        kg_assessments = kg_assessments or {}
        signals = []
        use_crypto_only = self.categories == ["crypto"]
        try:
            if use_crypto_only:
                markets = self._client.get_crypto_markets(limit=30)
                for m in markets:
                    if not m.is_open or not m.is_tradeable or m.volume < self.min_volume:
                        continue
                    oi = m.raw_data.get("open_interest")
                    if oi is not None and oi < self.min_open_interest:
                        continue
                    if m.spread > self.max_spread:
                        continue
                    sigs = self._market_to_signals(m, kg_assessments.get(m.market_id))
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
                        if not m.is_open or not m.is_tradeable or m.volume < self.min_volume:
                            continue
                        oi = m.raw_data.get("open_interest")
                        if oi is not None and oi < self.min_open_interest:
                            continue
                        if m.spread > self.max_spread:
                            continue
                        sigs = self._market_to_signals(m, kg_assessments.get(m.market_id))
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

    def _market_to_signals(
        self, m: PredictionMarket,
        kg_assessment: Any = None,
    ) -> List[Dict[str, Any]]:
        """Convert a single prediction market into 0-2 signals.

        Side derivation reconciles the naive mid-price rule with the knowledge
        gap (KG) direction so the adapter agrees with
        ``portfolio_optimizer._detect_event_markets``:
          - KG undervalued  => market's YES price is too LOW vs evidence  => BUY YES
          - KG overvalued   => market's YES price is too HIGH vs evidence => SELL YES
        When no significant KG is present we fall back to the mid-price rule
        (mp > 0.5 -> BUY, mp < 0.5 -> SELL).
        """
        if not m.is_tradeable:
            return []
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

        # Knowledge-gap override of side + confidence when significant.
        kg_sig = self._kg_side_and_boost(m, kg_assessment)
        if kg_sig is not None:
            kg_side, kg_boost = kg_sig
            confidence = min(confidence * kg_boost, 0.95)
        else:
            kg_side = None

        signals = []
        base_reason = (
            f"{m.platform} [{m.category}]: {m.question[:60]} → {mp*100:.0f}% YES "
            f"(vol=${m.volume:.0f}, liq={m.liquidity_score:.2f}, depth={depth_score:.2f}, "
            f"hours_left={hours_left:.1f})"
        )

        # Decide side: prefer KG direction; else naive mid-price rule.
        if kg_side is not None:
            side = kg_side
            if kg_assessment is not None and getattr(kg_assessment, "is_significant", False):
                base_reason += f" [kg: {kg_assessment.direction} gap={kg_assessment.gap_pct:.0f}%]"
        elif mp > 0.5 + self.min_extremity * 0.5:
            side = "BUY"
        elif mp < 0.5 - self.min_extremity * 0.5:
            side = "SELL"
        else:
            return signals

        signals.append(self._make_signal(symbol, side, confidence, base_score, m, base_reason))
        return signals

    @staticmethod
    def _kg_side_and_boost(m: PredictionMarket, kg: Any) -> Optional[tuple]:
        """Return (side, confidence_boost) when KG is significant and contradicts
        or confirms the mid-price direction, matching the optimizer's logic.

        Returns None when there is no significant KG assessment (caller falls
        back to the naive mid-price rule).
        """
        if kg is None or not getattr(kg, "is_significant", False):
            return None
        mp = m.mid_price
        contradiction = (
            (kg.direction == "overvalued" and mp > 0.5)
            or (kg.direction == "undervalued" and mp < 0.5)
        )
        confirmation = (
            (kg.direction == "overvalued" and mp < 0.5)
            or (kg.direction == "undervalued" and mp > 0.5)
        )
        if contradiction:
            side = "BUY" if kg.direction == "undervalued" else "SELL"
            return (side, 1.4)
        if confirmation:
            side = "BUY" if kg.direction == "undervalued" else "SELL"
            return (side, 1.2)
        return ("BUY" if kg.direction == "undervalued" else "SELL", 1.0)

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
            # SELL = bet NO, so our winning probability is 1 - mp (the NO side).
            p = 1 - mp if mp < 0.5 else mp
        q = 1 - p
        effective_spread = m.spread + (KALSHI_FEE if m.platform == "kalshi" else POLYMARKET_FEE + ESTIMATED_GAS)
        kelly_f = max(0, (p - q) / (1 - effective_spread)) if effective_spread < 1 else 0
        kelly_f = min(kelly_f, 0.5)  # Cap at half-Kelly

        # Time-to-expiry weighting
        hours_left = self._hours_to_expiry(m.end_date)
        time_weight = min(1.0, hours_left / 168)  # Cap at 1 week
        
        adjusted_confidence = confidence * time_weight

        # NOTE: the top-level keys here MUST be exactly the AccumulatedSignal
        # dataclass fields, because the accumulator constructs
        # ``AccumulatedSignal(**signal_dict)``. Platform-specific extras
        # (kelly_fraction, hours_to_expiry, depth, …) live inside market_data
        # so both Kalshi and Polymarket produce an identical, constructible shape.
        return {
            "symbol": symbol,
            "action": action,
            "base_confidence": round(confidence, 3),
            "final_confidence": round(adjusted_confidence, 3),
            "opportunity_score": round(base_score * (1 + m.probability_extremity) * time_weight, 4),
            "strategy_name": f"PM:{m.platform}:{m.category}",
            "signal_reason": reason,
            "estimated_volume_usd": round(confidence * m.volume * 0.1, 2),
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
                "kelly_fraction": round(kelly_f, 4),
                "hours_to_expiry": round(hours_left, 1),
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
        """Map a prediction market question to the most relevant tradeable symbol.

        Uses WORD-BOUNDARY matching exclusively so short tickers don't false-match
        substrings: "pol" != "politics", "eth" != "ethics", "btc" != "botcoin".
        Returns "" when no keyword matches (callers treat that as "no symbol").
        """
        import re
        q = question.lower()
        for kw, sym in EVENT_SYMBOL_MAP:
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, q):
                return sym
        return ""


def _depth_price(level) -> float:
    """Extract the price from a Kalshi order-book level (dict or tuple)."""
    if isinstance(level, dict):
        return float(level.get("price", 0) or 0)
    if isinstance(level, (list, tuple)) and len(level) >= 1:
        return float(level[0])
    return 0.0


def _depth_size(level) -> float:
    """Extract the size from a Kalshi order-book level (dict or tuple)."""
    if isinstance(level, dict):
        return float(level.get("size", 0) or 0)
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return float(level[1])
    return 0.0
