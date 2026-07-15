"""Shared helpers for event_markets coverage tests.

Provides a fake ``urllib.request``-compatible response object and factory
functions that build real dataclass instances from the event_markets modules
so tests can exercise logic without touching the network.
"""

import json
from typing import Any, Dict

from event_markets.kalshi_client import KalshiMarket
from event_markets.polymarket_client import PolymarketMarket, PolymarketBook


class FakeResp:
    """Minimal stand-in for a context-managed ``urlopen`` response."""

    def __init__(self, payload: Any, raise_on_read: bool = False):
        if isinstance(payload, (bytes, bytearray)):
            self._data = payload
        else:
            self._data = json.dumps(payload).encode("utf-8")
        self.raise_on_read = raise_on_read

    def read(self) -> bytes:
        if self.raise_on_read:
            raise OSError("read failed")
        return self._data

    def __enter__(self) -> "FakeResp":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


def make_kalshi_market(
    ticker="KALS-1",
    title="Will BTC hit $100k?",
    event_ticker="BTC",
    yes_bid=0.4,
    yes_ask=0.45,
    no_bid=0.55,
    no_ask=0.6,
    volume=50000.0,
    open_interest=1000.0,
    close_date="2026-12-31T00:00:00Z",
    status="open",
    settled=False,
    category="Crypto",
):
    return KalshiMarket(
        ticker=ticker,
        title=title,
        event_ticker=event_ticker,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        volume=volume,
        open_interest=open_interest,
        close_date=close_date,
        status=status,
        settled=settled,
        category=category,
    )


def make_polymarket_market(
    condition_id="cond-1",
    question="Will ETH reach $5000?",
    outcomes=None,
    outcome_prices=None,
    volume=40000.0,
    end_date_iso="2026-12-31T00:00:00Z",
    closed=False,
    accepting_orders=True,
    token_ids=None,
    yes_bid=0.42,
    yes_ask=0.46,
    spread=0.04,
    slug="eth-5000",
    event_slug="eth",
):
    outcomes = outcomes or ["YES", "NO"]
    outcome_prices = outcome_prices or {"YES": 0.45, "NO": 0.55}
    tokens = [{"token_id": t} for t in (token_ids or ["tok-1", "tok-2"])]
    return PolymarketMarket(
        condition_id=condition_id,
        question=question,
        description="desc",
        outcomes=outcomes,
        outcome_prices=outcome_prices,
        volume=volume,
        end_date_iso=end_date_iso,
        closed=closed,
        accepting_orders=accepting_orders,
        tokens=tokens,
        ticker=slug,
        event_slug=event_slug,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        spread=spread,
    )


def make_book(bids=((0.4, 100),), asks=((0.6, 100),), spread=0.2, mid=0.5):
    return PolymarketBook(
        bids=[(float(p), float(s)) for p, s in bids],
        asks=[(float(p), float(s)) for p, s in asks],
        spread=spread,
        mid_price=mid,
    )


class UrlRouter:
    """Routes ``urlopen`` calls to canned JSON by URL substring."""

    def __init__(self, mapping: Dict[str, Any], default=None):
        self.mapping = mapping
        self.default = default

    def __call__(self, req, *args, **kwargs):
        url = getattr(req, "full_url", None)
        if url is None:
            url = str(req)
        for substr, payload in self.mapping.items():
            if substr in url:
                return FakeResp(payload)
        if self.default is not None:
            return FakeResp(self.default)
        raise AssertionError(f"Unexpected urlopen URL: {url}")
