"""
Comparison engine for cross-platform event market opportunities.

Detects:
1. Crypto event probability vs coinbase portfolio position
2. Price-level prediction vs strategy signal direction
3. Cross-platform spread (Polymarket vs Kalshi same/similar event)

Each comparison yields an EventSignal that the PortfolioOptimizer
can surface as a notification or trade opportunity.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .polymarket_client import PolymarketClient, PolymarketMarket, PolymarketBook
from .kalshi_client import KalshiClient, KalshiMarket

logger = logging.getLogger("event_comparison")

# Confidence thresholds
MIN_VOLUME = 1000       # $1K minimum volume to consider
MIN_SPREAD_PROFIT = 0.05  # 5% edge minimum for a signal
MAX_SPREAD = 0.15       # Max 15% bid-ask spread to consider


@dataclass
class EventSignal:
    """A detected opportunity from event market analysis."""
    platform: str
    market_question: str
    market_ticker: str
    outcome: str
    probability: float        # 0-1 market-implied probability
    position_size: float = 0.0  # Notional value if we traded it
    confidence: float = 0.0     # 0-1 our confidence in the signal
    signal_type: str = ""       # "arbitrage", "strategy_divergence", "hedge"
    reason: str = ""
    source_data: Dict[str, Any] = field(default_factory=dict)


class ComparisonEngine:
    """Analyzes event market data against portfolio state for opportunities."""

    def __init__(
        self,
        polymarket: Optional[PolymarketClient] = None,
        kalshi: Optional[KalshiClient] = None,
    ):
        self.polymarket = polymarket or PolymarketClient()
        self.kalshi = kalshi

    def find_opportunities(self, holdings: Dict[str, Any]) -> List[EventSignal]:
        """Run all detectors and return ranked signals."""
        signals: List[EventSignal] = []

        # 1. Crypto prediction vs portfolio position
        pm_markets = self.polymarket.get_crypto_markets(limit=50)
        signals.extend(self._compare_with_holdings(pm_markets, holdings))

        # 2. Polymarket order book analysis
        for m in pm_markets[:10]:
            if not m.accepting_orders or m.closed:
                continue
            for outcome in m.outcomes:
                token_id = ""
                for t in m.tokens:
                    if t.get("outcome") == outcome:
                        token_id = t.get("token_id", "")
                        break
                if not token_id:
                    continue
                book = self.polymarket.get_order_book(token_id)
                sig = self._analyze_book(m, outcome, book)
                if sig:
                    signals.append(sig)

        # 3. Kalshi relevant markets
        if self.kalshi:
            try:
                kalshi_markets = self.kalshi.get_relevant_markets(limit=30)
                for m in kalshi_markets:
                    sig = self._analyze_kalshi(m)
                    if sig:
                        signals.append(sig)
            except Exception as e:
                logger.warning("Kalshi analysis failed: %s", e)

        signals.sort(key=lambda s: s.confidence, reverse=True)
        return signals[:20]

    def _compare_with_holdings(
        self,
        markets: List[PolymarketMarket],
        holdings: Dict[str, Any],
    ) -> List[EventSignal]:
        """Match Polymarket crypto price predictions against our holdings."""
        signals = []
        for cur, h in holdings.items():
            cur_upper = cur.upper()
            for m in markets:
                q = m.question.lower()
                if cur_upper not in q.upper():
                    continue
                if not m.accepting_orders or m.closed:
                    continue
                # Found a relevant price prediction market
                for outcome, price in m.outcome_prices.items():
                    outcome_lower = outcome.lower()
                    prob = float(price)
                    if prob <= 0 or prob >= 1:
                        continue
                    # Estimate strategy confidence from holding data
                    change_24h = float(h.get("change_24h", 0) or 0)
                    # If our holding is up and the market says low probability of upside → divergence
                    if change_24h > 2 and outcome_lower in ("yes", "up"):
                        divergence = prob - 0.5  # negative if market is bearish but we're up
                        if abs(divergence) > 0.2:
                            signals.append(EventSignal(
                                platform="polymarket",
                                market_question=m.question,
                                market_ticker=m.ticker,
                                outcome=outcome,
                                probability=prob,
                                confidence=min(abs(divergence), 0.9),
                                signal_type="strategy_divergence",
                                reason=f"{cur} up {change_24h:+.1f}% today, "
                                       f"but Polymarket says {outcome}={prob*100:.0f}%",
                                source_data={"currency": cur, "change_24h": change_24h},
                            ))
        return signals

    def _analyze_book(
        self,
        market: PolymarketMarket,
        outcome: str,
        book: PolymarketBook,
    ) -> Optional[EventSignal]:
        """Check if the order book shows a mispricing opportunity."""
        if not book.asks and not book.bids:
            return None
        if book.spread > MAX_SPREAD:
            return None
        mid = book.mid_price
        if mid <= 0 or mid >= 1:
            return None
        # Check for deep liquidity at a price far from 0.5
        total_bid_size = sum(s for _, s in book.bids[:5])
        total_ask_size = sum(s for _, s in book.asks[:5])
        total_liquidity = total_bid_size + total_ask_size
        if total_liquidity < 100:
            return None
        # Strong directional bias with deep liquidity = potential trade signal
        bias = abs(mid - 0.5) / 0.5  # 0 = neutral, 1 = extreme
        if bias > 0.3:
            confidence = min(bias * total_liquidity / 1000, 0.8)
            direction = outcome if mid > 0.5 else f"NOT {outcome}"
            return EventSignal(
                platform="polymarket",
                market_question=market.question,
                market_ticker=market.ticker,
                outcome=direction,
                probability=mid,
                position_size=min(total_liquidity, 10000),
                confidence=confidence,
                signal_type="book_signal",
                reason=f"Book shows {bias*100:.0f}% directional bias toward {direction} "
                       f"(mid={mid*100:.1f}%, liq=${total_liquidity:.0f})",
                source_data={
                    "mid_price": mid,
                    "spread": book.spread,
                    "total_liquidity": total_liquidity,
                    "bids": len(book.bids),
                    "asks": len(book.asks),
                },
            )
        return None

    def _analyze_kalshi(self, market: KalshiMarket) -> Optional[EventSignal]:
        """Check Kalshi market for interesting opportunities."""
        if market.settled or market.volume < MIN_VOLUME:
            return None
        if market.status != "open":
            return None
        yes_mid = (market.yes_bid + market.yes_ask) / 2
        no_mid = (market.no_bid + market.no_ask) / 2
        if yes_mid <= 0 or yes_mid >= 1:
            return None
        spread = market.yes_ask - market.yes_bid
        if spread > MAX_SPREAD:
            return None
        # Interesting if probability is extreme (>70% or <30%) with volume
        bias = abs(yes_mid - 0.5) / 0.5
        if bias > 0.3 and market.volume > MIN_VOLUME:
            direction = "YES" if yes_mid > 0.5 else "NO"
            return EventSignal(
                platform="kalshi",
                market_question=market.title,
                market_ticker=market.ticker,
                outcome=direction,
                probability=yes_mid,
                position_size=min(market.volume * 0.1, 10000),
                confidence=min(bias * market.volume / 10000, 0.7),
                signal_type="directional_bias",
                reason=f"Kalshi {market.ticker}: {yes_mid*100:.0f}% probability "
                       f"(vol=${market.volume:,.0f}, spread={spread*100:.1f}%)",
                source_data={
                    "yes_bid": market.yes_bid,
                    "yes_ask": market.yes_ask,
                    "no_bid": market.no_bid,
                    "no_ask": market.no_ask,
                    "volume": market.volume,
                },
            )
        return None


def format_signal(s: EventSignal) -> str:
    return (
        f"  [{s.platform}] {s.signal_type.upper()}\n"
        f"    Market: {s.market_question[:100]}\n"
        f"    Outcome: {s.outcome} @ {s.probability*100:.1f}%\n"
        f"    Confidence: {s.confidence:.0%}  Size: ${s.position_size:,.0f}\n"
        f"    {s.reason}"
    )
