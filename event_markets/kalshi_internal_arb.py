"""Kalshi single-venue (internal) arbitrage scanner.

Unlike the cross-venue scanner (Kalshi x Polymarket in ``arbitrage.py``), this
detects *locked* mispricings **within a single Kalshi event** whose markets are
mutually exclusive. No second venue is required, so it works with Kalshi alone.

Two strategies (both operate on an event flagged ``mutually_exclusive``):

1. ``mutex_no`` — GUARANTEED. Buy NO on every outcome. Because at most one
   outcome can resolve YES, at least ``N-1`` of the NO positions pay $1, so the
   worst-case payout is ``N-1``. Cost is ``sum(no_ask_i) = N - sum(yes_bid_i)``.
   Worst-case profit = ``(N-1) - (N - sum(yes_bid)) = sum(yes_bid) - 1``.
   => Profitable (before fees) whenever ``sum(yes_bid) > 1``. This relies ONLY
   on mutual exclusivity, which Kalshi settlement enforces — a true lock.

2. ``mutex_yes`` — CONDITIONAL on collective exhaustiveness. Buy YES on every
   outcome for ``sum(yes_ask) < 1``; the single winner pays $1. BUT this is only
   a lock if one of the listed outcomes MUST win (exhaustive). Many Kalshi
   "who will be the next X" markets list only front-runners, so an unlisted
   outcome could win and the whole basket pays $0. We surface these as
   NON-guaranteed opportunities (``guaranteed=False``) and never auto-execute
   them; a human must confirm the market set is exhaustive.

NOTE: a single Kalshi market's YES and NO share one linked order book
(``no_ask = 1 - yes_bid``), so ``yes_ask + no_ask = 1 + spread >= 1`` always —
there is no single-market YES+NO lock. Internal arbitrage only exists across the
sibling markets of a mutually-exclusive event.

Fees: Kalshi charges ``ceil(rate * C * P * (1-P))`` per leg. We report an
amortized per-contract fee estimate (``rate * sum(P_i*(1-P_i))``) so the net
edge shown is realistic; the executor recomputes exact ceil-rounded fees at the
chosen contract size.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger("kalshi_internal_arb")


@dataclass
class InternalArbLeg:
    ticker: str
    title: str
    side: str            # "no" | "yes"
    action: str          # "buy"
    price: float         # ask price paid per contract
    yes_bid: float
    yes_ask: float
    volume: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class InternalArbOpportunity:
    event_ticker: str
    event_title: str
    category: str
    strategy: str                 # "mutex_no" | "mutex_yes"
    n_outcomes: int
    legs: List[InternalArbLeg]
    total_cost: float             # cost per contract-set (sum of ask prices)
    guaranteed_payout: float      # worst-case payout per set
    gross_edge: float             # guaranteed_payout - total_cost
    est_fees: float               # amortized per-contract fee estimate
    net_edge: float               # gross_edge - est_fees
    edge_pct: float               # net_edge / total_cost
    guaranteed: bool
    confidence: float
    min_volume: float             # min leg volume (liquidity proxy)
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["legs"] = [l.to_dict() if isinstance(l, InternalArbLeg) else l for l in self.legs]
        d["type"] = "kalshi_internal"
        d["event_key"] = self.event_ticker
        return d


class KalshiInternalArbScanner:
    def __init__(self, kalshi_client=None, *,
                 min_net_edge: float = 0.005,
                 min_volume: float = 0.0,
                 max_legs: int = 40,
                 fee_rate: Optional[float] = None,
                 include_yes_side: bool = True):
        self.kalshi = kalshi_client
        self.min_net_edge = min_net_edge
        self.min_volume = min_volume
        self.max_legs = max_legs
        self.fee_rate = fee_rate if fee_rate is not None else float(
            os.environ.get("KALSHI_FEE_RATE", "0.07") or 0.07)
        self.include_yes_side = include_yes_side

    # ── fee model ────────────────────────────────────────────────────
    def _amortized_fee(self, prices: List[float]) -> float:
        """Per-contract fee estimate = rate * sum(p*(1-p)) over legs."""
        return round(self.fee_rate * sum(p * (1.0 - p) for p in prices), 4)

    # ── scanning ─────────────────────────────────────────────────────
    def scan(self, limit_events: int = 300,
             categories: Optional[List[str]] = None) -> List[InternalArbOpportunity]:
        if self.kalshi is None:
            return []
        try:
            events = self.kalshi.fetch_events_with_markets(limit=limit_events, categories=categories)
        except Exception as e:  # pragma: no cover - network
            logger.debug("fetch_events_with_markets failed: %s", e)
            return []
        opps: List[InternalArbOpportunity] = []
        for e in events:
            if not e.get("mutually_exclusive"):
                continue
            markets = [m for m in e.get("parsed_markets", [])
                       if getattr(m, "yes_bid", 0) > 0 and getattr(m, "yes_ask", 0) > 0
                       and getattr(m, "no_ask", 0) > 0]
            if len(markets) < 2 or len(markets) > self.max_legs:
                continue
            no_opp = self._check_no_side(e, markets)
            if no_opp:
                opps.append(no_opp)
            if self.include_yes_side:
                yes_opp = self._check_yes_side(e, markets)
                if yes_opp:
                    opps.append(yes_opp)
        # guaranteed first, then by net edge
        opps.sort(key=lambda o: (o.guaranteed, o.net_edge), reverse=True)
        return opps

    def _check_no_side(self, event: Dict[str, Any], markets: List[Any]) -> Optional[InternalArbOpportunity]:
        """Buy NO on every outcome — guaranteed by mutual exclusivity."""
        n = len(markets)
        no_prices = [float(m.no_ask) for m in markets]
        total_cost = sum(no_prices)
        guaranteed_payout = float(n - 1)          # worst case: exactly one YES
        gross_edge = guaranteed_payout - total_cost
        # gross_edge == sum(yes_bid) - 1
        if gross_edge <= 0:
            return None
        est_fees = self._amortized_fee(no_prices)
        net_edge = round(gross_edge - est_fees, 4)
        if net_edge < self.min_net_edge:
            return None
        min_vol = min(float(m.volume) for m in markets)
        if min_vol < self.min_volume:
            return None
        legs = [InternalArbLeg(ticker=m.ticker, title=m.title, side="no", action="buy",
                               price=float(m.no_ask), yes_bid=float(m.yes_bid),
                               yes_ask=float(m.yes_ask), volume=float(m.volume))
                for m in markets]
        edge_pct = round(net_edge / total_cost, 4) if total_cost else 0.0
        confidence = self._confidence(net_edge, min_vol, n, guaranteed=True)
        return InternalArbOpportunity(
            event_ticker=event.get("event_ticker", ""),
            event_title=event.get("title", "") or event.get("sub_title", ""),
            category=event.get("category", ""),
            strategy="mutex_no", n_outcomes=n, legs=legs,
            total_cost=round(total_cost, 4), guaranteed_payout=guaranteed_payout,
            gross_edge=round(gross_edge, 4), est_fees=est_fees, net_edge=net_edge,
            edge_pct=edge_pct, guaranteed=True, confidence=confidence,
            min_volume=min_vol,
            reason=(f"Buy NO on all {n} mutually-exclusive outcomes; "
                    f"worst-case payout {n-1} vs cost {total_cost:.3f} "
                    f"(Σyes_bid={sum(m.yes_bid for m in markets):.3f}>1)"),
        )

    def _check_yes_side(self, event: Dict[str, Any], markets: List[Any]) -> Optional[InternalArbOpportunity]:
        """Buy YES on every outcome — a lock ONLY if outcomes are exhaustive."""
        n = len(markets)
        yes_prices = [float(m.yes_ask) for m in markets]
        total_cost = sum(yes_prices)
        guaranteed_payout = 1.0                    # if exhaustive, exactly one YES pays $1
        gross_edge = guaranteed_payout - total_cost
        if gross_edge <= 0:
            return None
        est_fees = self._amortized_fee(yes_prices)
        net_edge = round(gross_edge - est_fees, 4)
        if net_edge < self.min_net_edge:
            return None
        min_vol = min(float(m.volume) for m in markets)
        if min_vol < self.min_volume:
            return None
        legs = [InternalArbLeg(ticker=m.ticker, title=m.title, side="yes", action="buy",
                               price=float(m.yes_ask), yes_bid=float(m.yes_bid),
                               yes_ask=float(m.yes_ask), volume=float(m.volume))
                for m in markets]
        edge_pct = round(net_edge / total_cost, 4) if total_cost else 0.0
        # Not guaranteed: heavily discount confidence (exhaustiveness unknown).
        confidence = round(0.5 * self._confidence(net_edge, min_vol, n, guaranteed=False), 3)
        return InternalArbOpportunity(
            event_ticker=event.get("event_ticker", ""),
            event_title=event.get("title", "") or event.get("sub_title", ""),
            category=event.get("category", ""),
            strategy="mutex_yes", n_outcomes=n, legs=legs,
            total_cost=round(total_cost, 4), guaranteed_payout=guaranteed_payout,
            gross_edge=round(gross_edge, 4), est_fees=est_fees, net_edge=net_edge,
            edge_pct=edge_pct, guaranteed=False, confidence=confidence,
            min_volume=min_vol,
            reason=(f"Buy YES on all {n} outcomes for Σ={total_cost:.3f}<1. "
                    f"ONLY a lock if the outcome set is collectively exhaustive — "
                    f"verify manually; an unlisted winner pays $0."),
        )

    @staticmethod
    def _confidence(net_edge: float, min_vol: float, n_legs: int, guaranteed: bool) -> float:
        # edge component (saturates ~5%), liquidity component, leg-count penalty
        edge_c = min(1.0, net_edge / 0.05)
        liq_c = min(1.0, min_vol / 1000.0)
        leg_pen = max(0.3, 1.0 - 0.05 * (n_legs - 2))   # more legs => more exec risk
        base = 0.5 * edge_c + 0.3 * liq_c + 0.2
        return round(max(0.0, min(1.0, base * leg_pen)), 3)


def format_internal_opp(o: InternalArbOpportunity) -> str:
    tag = "GUARANTEED" if o.guaranteed else "conditional(exhaustive?)"
    return (f"[{o.strategy}/{tag}] {o.event_ticker} ({o.category}) n={o.n_outcomes} "
            f"cost={o.total_cost:.3f} payout={o.guaranteed_payout:.0f} "
            f"net_edge={o.net_edge:.3f} ({o.edge_pct*100:.1f}%) conf={o.confidence:.0%}")
