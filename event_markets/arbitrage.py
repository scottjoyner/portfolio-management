"""Cross-platform prediction market arbitrage scanner.

Finds same-event mismatches between Kalshi and Polymarket and flags
opposite-leg combinations that produce positive lock-in after a fee buffer.
"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .unified_client import PredictionMarket, UnifiedPredictionMarketClient

logger = logging.getLogger("event_arbitrage")

ROOT = Path(__file__).resolve().parent.parent
PAPER_TRADES_PATH = ROOT / "data" / "paper-trades.json"
MAX_PAPER_AGE_DAYS = 7

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "can", "could",
    "do", "does", "for", "from", "future", "if", "in", "is", "it", "may",
    "might", "of", "on", "or", "should", "that", "the", "their", "there",
    "this", "to", "will", "with", "would", "when", "what", "who", "where",
    "which", "why", "how", "over", "under", "before", "after", "during",
    "will", "not", "yes", "no", "up", "down", "more", "less", "than",
}

ALIASES = {
    "btc": "bitcoin",
    "xbt": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "dec": "december",
    "sept": "september",
    "sep": "september",
    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "oct": "october",
    "nov": "november",
    "eoy": "yearend",
    # World Cup / soccer country aliases
    "usa": "united states",
    "u.s.a": "united states",
    "u.s.": "united states",
    "uk": "england",
    "eng": "england",
    "arg": "argentina",
    "fra": "france",
    "bra": "brazil",
    "esp": "spain",
    "ger": "germany",
    "ita": "italy",
    "por": "portugal",
    "ned": "netherlands",
    "holland": "netherlands",
    "bel": "belgium",
    "cro": "croatia",
    "mci": "morocco",
    "jpn": "japan",
    "aus": "australia",
    "mex": "mexico",
    "col": "colombia",
    "uru": "uruguay",
    "sui": "switzerland",
    "den": "denmark",
    "swe": "sweden",
    "nor": "norway",
    "pol": "poland",
    "tur": "turkey",
    "ksa": "saudi arabia",
    "qat": "qatar",
    "mor": "morocco",
    "chi": "chile",
    "per": "peru",
    "ecu": "ecuador",
    "par": "paraguay",
    "sen": "senegal",
    "gha": "ghana",
    "nig": "nigeria",
}


@dataclass(slots=True)
class ArbitrageLeg:
    platform: str
    market_id: str
    question: str
    outcome: str
    side: str
    price: float
    category: str = "general"


@dataclass(slots=True)
class ArbitrageOpportunity:
    event_key: str
    category: str
    platform_buy: str
    platform_hedge: str
    leg_buy: ArbitrageLeg
    leg_hedge: ArbitrageLeg
    buy_yes_price: float
    hedge_yes_price: float
    total_cost: float
    guaranteed_payout: float
    edge: float
    edge_pct: float
    confidence: float
    reason: str
    source_markets: Dict[str, Any] = field(default_factory=dict)


def _tokenize(question: str) -> List[str]:
    q = question.lower()
    q = re.sub(r"[$€£,()\[\]{}:;!?/\\\-]", " ", q)
    q = re.sub(r"\b\d{1,4}(?:\.\d+)?\b", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    tokens = []
    for token in q.split():
        if len(token) <= 1 or token in STOPWORDS:
            continue
        tokens.append(ALIASES.get(token, token))
    return tokens


def normalize_question(question: str) -> str:
    tokens = _tokenize(question)
    return " ".join(sorted(dict.fromkeys(tokens)))


def _similarity(left: Sequence[str], right: Sequence[str]) -> float:
    if not left or not right:
        return 0.0
    a = set(left)
    b = set(right)
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _yes_price(market: PredictionMarket) -> float:
    yes = market.outcome_prices.get("YES")
    if yes is not None:
        return float(yes)
    return float(market.mid_price)


class EventArbitrageScanner:
    """Scan Kalshi and Polymarket for same-event arbitrage.

    Matches events across categories (since Kalshi and Polymarket may
    label the same event under different categories) and estimates
    slippage from available order book depth and daily volume.
    """

    def __init__(
        self,
        client: Optional[UnifiedPredictionMarketClient] = None,
        min_edge: float = 0.015,
        fee_buffer: float = 0.015,
        min_volume: float = 1000,
        similarity_threshold: float = 0.30,
        record_paper_trades: bool = True,
    ):
        self.client = client or UnifiedPredictionMarketClient()
        self.min_edge = min_edge
        self.fee_buffer = fee_buffer
        self.min_volume = min_volume
        self.similarity_threshold = similarity_threshold
        self.record_paper_trades = record_paper_trades

    def scan(self, limit_per_category: int = 20) -> List[ArbitrageOpportunity]:
        categories = self.client.search_all_categories(
            limit_per_platform=limit_per_category, min_volume=self.min_volume, max_spread=0.30
        )
        markets: List[PredictionMarket] = []
        for items in categories.values():
            markets.extend(items)
        return self.scan_markets(markets)

    @staticmethod
    def _slippage_bps(market: PredictionMarket, trade_size_usd: float = 1000) -> float:
        """Estimated price impact in basis points for a given trade size.

        Uses daily volume as a liquidity proxy. A $1k trade on a $50k
        daily-volume market costs ~20 bps in price impact.
        """
        if market.volume <= 0:
            return 50.0
        impact = (trade_size_usd / market.volume) * 10000.0
        half_spread = (market.spread / 2.0) * 10000.0
        return min(impact + half_spread, 200.0)

    def scan_markets(self, markets: Sequence[PredictionMarket]) -> List[ArbitrageOpportunity]:
        # Collect ALL markets by platform, ignoring category boundaries
        by_platform: Dict[str, List[PredictionMarket]] = {}
        for market in markets:
            if not market.is_open or market.volume < self.min_volume:
                continue
            by_platform.setdefault(market.platform, []).append(market)

        kalshi_mkts = by_platform.get("kalshi", [])
        poly_mkts = by_platform.get("polymarket", [])

        if not kalshi_mkts or not poly_mkts:
            return []

        # Tokenize once per market for performance
        kalshi_tokenized = [(m, _tokenize(m.question)) for m in kalshi_mkts]
        poly_tokenized = [(m, _tokenize(m.question)) for m in poly_mkts]

        opportunities: List[ArbitrageOpportunity] = []
        for left, left_tokens in kalshi_tokenized:
            for right, right_tokens in poly_tokenized:
                sim = _similarity(left_tokens, right_tokens)
                if sim < self.similarity_threshold:
                    continue
                opp = self._pair_to_arb(left, right, sim)
                if opp:
                    opportunities.append(opp)

        opportunities.sort(key=lambda o: (o.edge_pct, o.confidence), reverse=True)
        return opportunities

    def _pair_to_arb(
        self,
        left: PredictionMarket,
        right: PredictionMarket,
        similarity: float,
    ) -> Optional[ArbitrageOpportunity]:
        left_yes = _yes_price(left)
        right_yes = _yes_price(right)
        if left_yes <= 0 or right_yes <= 0:
            return None

        spread_cost = (left.spread if left.spread else 0.0) + (right.spread if right.spread else 0.0)
        if abs(left_yes - right_yes) <= self.min_edge + spread_cost:
            return None

        if left_yes < right_yes:
            buy_market, hedge_market = left, right
            buy_yes, hedge_yes = left_yes, right_yes
        else:
            buy_market, hedge_market = right, left
            buy_yes, hedge_yes = right_yes, left_yes

        # Slippage estimate: volume-based price impact + spread + fees
        trade_size = 1000.0  # standard paper trade size
        buy_slip = self._slippage_bps(buy_market, trade_size) / 10000.0
        hedge_slip = self._slippage_bps(hedge_market, trade_size) / 10000.0
        total_slippage = buy_slip + hedge_slip + self.fee_buffer
        total_cost = buy_yes + (1.0 - hedge_yes) + total_slippage
        guaranteed_payout = 1.0
        edge = guaranteed_payout - total_cost
        if edge <= self.min_edge:
            return None

        edge_pct = edge / max(total_cost, 1e-9)

        # Confidence: volume-based liquidity quality + spread penalty
        liq_quality = min(buy_market.liquidity_score + hedge_market.liquidity_score, 2.0) / 2.0
        spread_penalty = max(0, 1.0 - spread_cost * 5.0)
        vol_quality = min(
            min(buy_market.volume, hedge_market.volume) / 10000.0, 1.0
        )
        confidence = min(0.95, max(0.15, edge * 10.0 * liq_quality * spread_penalty * vol_quality))

        buy_leg = ArbitrageLeg(
            platform=buy_market.platform,
            market_id=buy_market.market_id,
            question=buy_market.question,
            outcome="YES",
            side="BUY",
            price=buy_yes,
            category=buy_market.category,
        )
        hedge_leg = ArbitrageLeg(
            platform=hedge_market.platform,
            market_id=hedge_market.market_id,
            question=hedge_market.question,
            outcome="NO",
            side="BUY",
            price=1.0 - hedge_yes,
            category=hedge_market.category,
        )

        reason = (
            f"{buy_market.platform} YES at {buy_yes:.2f} vs {hedge_market.platform} NO at {1.0 - hedge_yes:.2f}; "
            f"locked edge {edge_pct*100:.1f}% (slippage {total_slippage*100:.1f}%, vol {buy_market.volume:,.0f}/{hedge_market.volume:,.0f})"
        )
        event_key = self._event_key(left, right)
        category = f"{buy_market.category}/{hedge_market.category}" if buy_market.category != hedge_market.category else (buy_market.category or "general")

        create_trade = edge_pct >= self.min_edge * 2 and confidence >= 0.3 and self.record_paper_trades
        if create_trade:
            self._record_paper_trade(
                event_key, category,
                buy_market, buy_yes,
                hedge_market, hedge_yes,
                edge, edge_pct, confidence,
            )

        return ArbitrageOpportunity(
            event_key=event_key,
            category=category,
            platform_buy=buy_market.platform,
            platform_hedge=hedge_market.platform,
            leg_buy=buy_leg,
            leg_hedge=hedge_leg,
            buy_yes_price=buy_yes,
            hedge_yes_price=hedge_yes,
            total_cost=total_cost,
            guaranteed_payout=guaranteed_payout,
            edge=edge,
            edge_pct=edge_pct,
            confidence=confidence,
            reason=reason,
            source_markets={
                buy_market.platform: {
                    "market_id": buy_market.market_id,
                    "question": buy_market.question,
                    "mid_price": buy_market.mid_price,
                    "volume": buy_market.volume,
                    "spread": buy_market.spread,
                    "liquidity_score": buy_market.liquidity_score,
                },
                hedge_market.platform: {
                    "market_id": hedge_market.market_id,
                    "question": hedge_market.question,
                    "mid_price": hedge_market.mid_price,
                    "volume": hedge_market.volume,
                    "spread": hedge_market.spread,
                    "liquidity_score": hedge_market.liquidity_score,
                },
            },
        )

    @staticmethod
    def _event_key(left: PredictionMarket, right: PredictionMarket) -> str:
        left_key = normalize_question(left.question)
        right_key = normalize_question(right.question)
        return f"{left_key or left.market_id}::{right_key or right.market_id}"

    @staticmethod
    def _load_paper_trades() -> list[dict]:
        if not PAPER_TRADES_PATH.exists():
            return []
        try:
            return json.loads(PAPER_TRADES_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            return []

    @staticmethod
    def _save_paper_trades(trades: list[dict]) -> None:
        PAPER_TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = PAPER_TRADES_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(trades, indent=2))
        tmp.replace(PAPER_TRADES_PATH)

    def _record_paper_trade(
        self,
        event_key: str,
        category: str,
        buy_market: PredictionMarket,
        buy_yes: float,
        hedge_market: PredictionMarket,
        hedge_yes: float,
        edge: float,
        edge_pct: float,
        confidence: float,
    ) -> dict | None:
        trades = self._load_paper_trades()
        # Deduplicate by event_key — don't re-record the same event within 24h
        now_ts = time.time()
        for t in trades[-50:]:
            ts = t.get("timestamp", 0)
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
                except (ValueError, TypeError):
                    ts = 0
            if t.get("event_key") == event_key and (now_ts - ts) < 86400:
                return None
        total_cost = buy_yes + (1.0 - hedge_yes)
        trade: dict[str, Any] = {
            "event_key": event_key,
            "category": category,
            "type": "arbitrage",
            "platform_buy": buy_market.platform,
            "platform_hedge": hedge_market.platform,
            "buy_market_id": buy_market.market_id,
            "hedge_market_id": hedge_market.market_id,
            "buy_question": buy_market.question[:120],
            "hedge_question": hedge_market.question[:120],
            "buy_yes_price": round(buy_yes, 4),
            "hedge_yes_price": round(hedge_yes, 4),
            "total_cost": round(total_cost, 4),
            "guaranteed_payout": 1.0,
            "edge": round(edge, 4),
            "edge_pct": round(edge_pct, 4),
            "confidence": round(confidence, 3),
            "notional": 1000.0,
            "expected_profit": round(1000.0 * edge, 2),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": "open",
        }
        trades.append(trade)
        self._save_paper_trades(trades)
        return trade


def format_arbitrage(opp: ArbitrageOpportunity) -> str:
    return (
        f"[{opp.category}] edge={opp.edge_pct*100:.1f}% confidence={opp.confidence:.0%}\n"
        f"  Buy YES:   {opp.platform_buy} @ {opp.buy_yes_price:.2f}\n"
        f"  Hedge NO:   {opp.platform_hedge} @ {1.0 - opp.hedge_yes_price:.2f}\n"
        f"  Cost={opp.total_cost:.2f} Payout={opp.guaranteed_payout:.2f}\n"
        f"  {opp.reason}"
    )
