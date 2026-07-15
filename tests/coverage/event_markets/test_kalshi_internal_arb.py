"""Coverage tests for event_markets.kalshi_internal_arb (network mocked)."""

from unittest.mock import MagicMock

import pytest

from event_markets.kalshi_internal_arb import (
    KalshiInternalArbScanner,
    InternalArbOpportunity,
    InternalArbLeg,
    format_internal_opp,
)
from event_markets.kalshi_client import KalshiMarket


def _km(ticker, yes_bid=0.4, yes_ask=0.1, no_ask=0.1, no_bid=0.55, volume=50000.0,
        title="Market"):
    return KalshiMarket(
        ticker=ticker, title=title, event_ticker="E1",
        yes_bid=yes_bid, yes_ask=yes_ask, no_bid=no_bid, no_ask=no_ask,
        volume=volume, open_interest=1000.0, close_date="2026-12-31T00:00:00Z",
        status="open", settled=False, category="Crypto",
    )


def _event(markets, mutually_exclusive=True, **kw):
    e = {
        "mutually_exclusive": mutually_exclusive,
        "parsed_markets": markets,
        "event_ticker": kw.get("event_ticker", "E1"),
        "title": kw.get("title", "Event One"),
        "sub_title": kw.get("sub_title", ""),
        "category": kw.get("category", "Crypto"),
    }
    return e


def test_scanner_no_client():
    s = KalshiInternalArbScanner(kalshi_client=None)
    assert s.scan() == []


def test_amortized_fee():
    s = KalshiInternalArbScanner()
    assert s._amortized_fee([0.5, 0.5]) == pytest.approx(0.07 * (0.25 + 0.25), abs=1e-6)
    assert s._amortized_fee([]) == 0.0


def test_scan_no_side_and_yes_side():
    mkts = [_km("K1", no_ask=0.1, yes_bid=0.9), _km("K2", no_ask=0.1, yes_bid=0.9),
            _km("K3", no_ask=0.1, yes_bid=0.9)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client)
    opps = s.scan()
    assert len(opps) == 2
    types = {o.strategy for o in opps}
    assert "mutex_no" in types and "mutex_yes" in types
    no_opp = [o for o in opps if o.strategy == "mutex_no"][0]
    assert no_opp.guaranteed is True
    yes_opp = [o for o in opps if o.strategy == "mutex_yes"][0]
    assert yes_opp.guaranteed is False
    # guaranteed sorted first
    assert opps[0].guaranteed


def test_scan_not_mutually_exclusive_skip():
    mkts = [_km("K1", no_ask=0.1), _km("K2", no_ask=0.1)]
    events = [_event(mkts, mutually_exclusive=False)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client)
    assert s.scan() == []


def test_scan_no_side_unprofitable_skip():
    # no_ask large -> gross_edge <= 0
    mkts = [_km("K1", no_ask=0.9), _km("K2", no_ask=0.9)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False)
    opps = s.scan()
    # yes side also: yes_ask 0.45*2=0.9 <1 -> profitable yes. so mutex_yes remains.
    assert all(o.strategy == "mutex_yes" for o in opps)


def test_scan_market_count_bounds():
    # single market -> len < 2 -> skip
    mkts = [_km("K1", no_ask=0.1)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False)
    assert s.scan() == []
    # too many legs
    many = [_km(f"K{i}", no_ask=0.1) for i in range(50)]
    events2 = [_event(many)]
    client.fetch_events_with_markets.return_value = events2
    s2 = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False, max_legs=10)
    assert s2.scan() == []


def test_scan_min_volume_filter():
    mkts = [_km("K1", no_ask=0.1, volume=5), _km("K2", no_ask=0.1, volume=5)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False,
                                 min_volume=1000.0)
    assert s.scan() == []


def test_scan_market_price_validation_skip():
    # markets with zero prices filtered out -> < 2 valid -> skip
    bad = [_km("K1", no_ask=0.0, yes_bid=0.0, yes_ask=0.0), _km("K2", no_ask=0.0)]
    events = [_event(bad)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False)
    assert s.scan() == []


def test_scan_category_filter_and_limit():
    mkts = [_km("K1", no_ask=0.1), _km("K2", no_ask=0.1)]
    events = [_event(mkts, category="Crypto")]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    # no_side gross edge = 1 - 0.2 = 0.8; net ~0.79. min_net_edge=0.9 skips both.
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False,
                                 min_net_edge=0.9)
    assert s.scan() == []


def test_scan_include_yes_side_false():
    mkts = [_km("K1", no_ask=0.1), _km("K2", no_ask=0.1)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=False)
    opps = s.scan()
    assert opps and all(o.strategy == "mutex_no" for o in opps)


def test_yes_side_unprofitable_skip():
    # yes_ask large -> gross_edge <= 0 -> yes side returns None
    mkts = [_km("K1", yes_ask=0.9), _km("K2", yes_ask=0.9)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=True)
    opps = s.scan()
    # no_side: no_ask=0.1 -> profitable; yes_side unprofitable -> only mutex_no
    assert opps and all(o.strategy == "mutex_no" for o in opps)


def test_yes_side_net_below_min_edge():
    mkts = [_km("K1", yes_ask=0.45), _km("K2", yes_ask=0.45)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=True,
                                 min_net_edge=0.5)
    opps = s.scan()
    assert all(o.strategy == "mutex_no" for o in opps)  # yes net ~0.09 < 0.5


def test_yes_side_min_volume_filter():
    mkts = [_km("K1", yes_ask=0.1, volume=5), _km("K2", yes_ask=0.1, volume=5)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=True,
                                 min_volume=1000.0)
    opps = s.scan()
    assert all(o.strategy == "mutex_no" for o in opps)  # yes filtered by min vol


def test_yes_side_profitable_full():
    mkts = [_km("K1", yes_ask=0.1), _km("K2", yes_ask=0.1), _km("K3", yes_ask=0.1)]
    events = [_event(mkts)]
    client = MagicMock()
    client.fetch_events_with_markets.return_value = events
    s = KalshiInternalArbScanner(kalshi_client=client, include_yes_side=True)
    opps = s.scan()
    yes = [o for o in opps if o.strategy == "mutex_yes"]
    assert yes and yes[0].guaranteed is False
    assert yes[0].legs[0].side == "yes"


def test_scan_fetch_exception():
    client = MagicMock()
    client.fetch_events_with_markets.side_effect = RuntimeError("boom")
    s = KalshiInternalArbScanner(kalshi_client=client)
    assert s.scan() == []


def test_confidence_and_to_dict():
    leg = InternalArbLeg(ticker="K1", title="M", side="no", action="buy",
                         price=0.6, yes_bid=0.4, yes_ask=0.45, volume=50000.0)
    opp = InternalArbOpportunity(
        event_ticker="E1", event_title="T", category="Crypto",
        strategy="mutex_no", n_outcomes=2, legs=[leg], total_cost=1.2,
        guaranteed_payout=1.0, gross_edge=0.2, est_fees=0.01, net_edge=0.19,
        edge_pct=0.1, guaranteed=True, confidence=0.5, min_volume=50000.0,
        reason="r",
    )
    d = opp.to_dict()
    assert d["type"] == "kalshi_internal"
    assert d["event_key"] == "E1"
    assert d["legs"][0]["ticker"] == "K1"
    leg_d = leg.to_dict()
    assert leg_d["ticker"] == "K1"
    assert format_internal_opp(opp)


def test_confidence_static():
    c = KalshiInternalArbScanner._confidence(0.05, 1000.0, 2, guaranteed=True)
    assert 0.0 <= c <= 1.0
    c2 = KalshiInternalArbScanner._confidence(0.05, 1000.0, 40, guaranteed=False)
    assert 0.0 <= c2 <= 1.0
