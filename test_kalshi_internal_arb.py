"""Unit tests for the Kalshi internal (single-venue) arbitrage scanner."""
from dataclasses import dataclass

import pytest

from event_markets.kalshi_internal_arb import KalshiInternalArbScanner


@dataclass
class FakeMarket:
    ticker: str
    yes_bid: float
    yes_ask: float
    volume: float = 5000.0
    title: str = "outcome"

    @property
    def no_ask(self):
        # Kalshi: buying NO == 1 - yes_bid
        return round(1.0 - self.yes_bid, 4)

    @property
    def no_bid(self):
        return round(1.0 - self.yes_ask, 4)


def _event(ticker, markets, mutually_exclusive=True, category="Politics"):
    return {
        "event_ticker": ticker,
        "title": ticker,
        "category": category,
        "mutually_exclusive": mutually_exclusive,
        "parsed_markets": markets,
    }


class FakeKalshi:
    def __init__(self, events):
        self._events = events

    def fetch_events_with_markets(self, limit=300, categories=None):
        return self._events


def test_mutex_no_guaranteed_detected():
    # Two outcomes, yes_bid 0.60 each => sum 1.20 > 1 => NO-side lock.
    mkts = [FakeMarket("A", yes_bid=0.60, yes_ask=0.62),
            FakeMarket("B", yes_bid=0.60, yes_ask=0.62)]
    s = KalshiInternalArbScanner(FakeKalshi([_event("EVT", mkts)]), min_net_edge=0.0)
    opps = s.scan()
    no = [o for o in opps if o.strategy == "mutex_no"]
    assert len(no) == 1
    o = no[0]
    assert o.guaranteed is True
    # cost = sum(no_ask) = 0.4+0.4 = 0.8 ; payout = n-1 = 1 ; gross = 0.2
    assert o.total_cost == pytest.approx(0.8)
    assert o.guaranteed_payout == 1.0
    assert o.gross_edge == pytest.approx(0.2)
    # fee = 0.07 * (0.4*0.6 + 0.4*0.6) = 0.0336 ; net = 0.1664
    assert o.est_fees == pytest.approx(0.0336, abs=1e-4)
    assert o.net_edge == pytest.approx(0.1664, abs=1e-3)


def test_mutex_no_absent_when_sum_yes_bid_below_1():
    # Fairly priced: sum yes_bid = 0.98 < 1 => no NO-side lock.
    mkts = [FakeMarket("A", yes_bid=0.49, yes_ask=0.51),
            FakeMarket("B", yes_bid=0.49, yes_ask=0.51)]
    s = KalshiInternalArbScanner(FakeKalshi([_event("EVT", mkts)]), min_net_edge=0.0)
    assert [o for o in s.scan() if o.strategy == "mutex_no"] == []


def test_mutex_yes_conditional_flagged_not_guaranteed():
    # sum yes_ask = 0.30 < 1 => YES-side candidate, but NOT guaranteed.
    mkts = [FakeMarket("A", yes_bid=0.10, yes_ask=0.15),
            FakeMarket("B", yes_bid=0.10, yes_ask=0.15)]
    s = KalshiInternalArbScanner(FakeKalshi([_event("EVT", mkts)]), min_net_edge=0.0)
    yes = [o for o in s.scan() if o.strategy == "mutex_yes"]
    assert len(yes) == 1
    assert yes[0].guaranteed is False
    assert yes[0].total_cost == pytest.approx(0.30)
    assert yes[0].confidence <= 0.5   # discounted for unknown exhaustiveness


def test_non_mutually_exclusive_skipped():
    mkts = [FakeMarket("A", yes_bid=0.60, yes_ask=0.62),
            FakeMarket("B", yes_bid=0.60, yes_ask=0.62)]
    s = KalshiInternalArbScanner(FakeKalshi([_event("EVT", mkts, mutually_exclusive=False)]),
                                 min_net_edge=0.0)
    assert s.scan() == []


def test_min_net_edge_filter():
    mkts = [FakeMarket("A", yes_bid=0.505, yes_ask=0.51),
            FakeMarket("B", yes_bid=0.505, yes_ask=0.51)]
    # sum yes_bid = 1.01 -> gross 0.01, fees ~0.035 -> net negative -> filtered
    s = KalshiInternalArbScanner(FakeKalshi([_event("EVT", mkts)]), min_net_edge=0.005)
    assert [o for o in s.scan() if o.strategy == "mutex_no"] == []


def test_to_dict_shape():
    mkts = [FakeMarket("A", yes_bid=0.60, yes_ask=0.62),
            FakeMarket("B", yes_bid=0.60, yes_ask=0.62)]
    s = KalshiInternalArbScanner(FakeKalshi([_event("EVT", mkts)]), min_net_edge=0.0)
    d = s.scan()[0].to_dict()
    assert d["type"] == "kalshi_internal"
    assert d["event_key"] == "EVT"
    assert isinstance(d["legs"], list) and d["legs"][0]["side"] == "no"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
