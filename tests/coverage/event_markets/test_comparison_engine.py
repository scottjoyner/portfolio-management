"""Coverage tests for event_markets.comparison_engine (network mocked)."""

from unittest.mock import MagicMock, patch

import pytest

from event_markets import comparison_engine as ce
from event_markets.comparison_engine import (
    ComparisonEngine, EventSignal, format_signal,
)
from event_markets.polymarket_client import PolymarketMarket, PolymarketBook
from em_helpers import make_polymarket_market, make_kalshi_market, make_book


def _pm(**kw):
    return make_polymarket_market(**kw)


def _book(bids=(), asks=(), spread=0.02, mid=0.5):
    return PolymarketBook(
        bids=[(float(p), float(s)) for p, s in bids],
        asks=[(float(p), float(s)) for p, s in asks],
        spread=spread,
        mid_price=mid,
    )


# ── __init__ ─────────────────────────────────────────────────────────

def test_init_defaults_create_polymarket():
    eng = ComparisonEngine()
    assert eng.polymarket is not None
    assert eng.kalshi is None


def test_init_with_clients():
    poly = MagicMock()
    kal = MagicMock()
    eng = ComparisonEngine(polymarket=poly, kalshi=kal)
    assert eng.polymarket is poly
    assert eng.kalshi is kal


# ── _compare_with_holdings ───────────────────────────────────────────

def test_compare_with_holdings_divergence_signal():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm(condition_id="c1", question="Will BTC go up today?",
            outcomes=["Yes", "No"], outcome_prices={"Yes": 0.2, "No": 0.8},
            accepting_orders=True, closed=False)
    sigs = eng._compare_with_holdings([m], {"BTC": {"change_24h": 5.0}})
    assert len(sigs) == 1
    assert sigs[0].signal_type == "strategy_divergence"
    assert sigs[0].outcome == "Yes"


def test_compare_with_holdings_skips_various():
    eng = ComparisonEngine(polymarket=MagicMock())
    # currency not in question -> skip
    m_norel = _pm(condition_id="a", question="Will ETH go up?",
                  outcome_prices={"Yes": 0.2}, accepting_orders=True)
    # closed market -> skip
    m_closed = _pm(condition_id="b", question="Will BTC go up?",
                   outcome_prices={"Yes": 0.2}, accepting_orders=True, closed=True)
    # prob out of range -> continue
    m_extreme = _pm(condition_id="c", question="Will BTC go up?",
                    outcome_prices={"Yes": 0.0, "No": 1.0}, accepting_orders=True)
    # change small -> no signal
    m_small = _pm(condition_id="d", question="Will BTC go up?",
                  outcome_prices={"Yes": 0.2}, accepting_orders=True)
    sigs = eng._compare_with_holdings(
        [m_norel, m_closed, m_extreme, m_small],
        {"BTC": {"change_24h": 1.0}},
    )
    assert sigs == []


def test_compare_with_holdings_small_divergence_no_signal():
    eng = ComparisonEngine(polymarket=MagicMock())
    # divergence = 0.6 - 0.5 = 0.1, abs <= 0.2 -> no signal
    m = _pm(condition_id="c1", question="Will BTC go up?",
            outcome_prices={"Yes": 0.6}, accepting_orders=True)
    sigs = eng._compare_with_holdings([m], {"BTC": {"change_24h": 5.0}})
    assert sigs == []


# ── _analyze_book ────────────────────────────────────────────────────

def test_analyze_book_empty():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm()
    assert eng._analyze_book(m, "YES", _book(bids=[], asks=[])) is None


def test_analyze_book_wide_spread():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm()
    b = _book(bids=[(0.4, 100)], asks=[(0.6, 100)], spread=0.5, mid=0.5)
    assert eng._analyze_book(m, "YES", b) is None


def test_analyze_book_mid_out_of_range():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm()
    b = _book(bids=[(0.0, 100)], asks=[(0.0, 100)], spread=0.01, mid=0.0)
    assert eng._analyze_book(m, "YES", b) is None


def test_analyze_book_low_liquidity():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm()
    b = _book(bids=[(0.8, 10)], asks=[(0.82, 10)], spread=0.02, mid=0.81)
    assert eng._analyze_book(m, "YES", b) is None


def test_analyze_book_low_bias_none():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm()
    # mid=0.55 -> bias=0.1 <= 0.3 -> None
    b = _book(bids=[(0.54, 200)], asks=[(0.56, 200)], spread=0.02, mid=0.55)
    assert eng._analyze_book(m, "YES", b) is None


def test_analyze_book_signal_up():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm(question="Q", slug="tick")
    b = _book(bids=[(0.80, 300)], asks=[(0.82, 300)], spread=0.02, mid=0.81)
    sig = eng._analyze_book(m, "YES", b)
    assert sig is not None
    assert sig.signal_type == "book_signal"
    assert sig.outcome == "YES"  # mid > 0.5


def test_analyze_book_signal_down():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = _pm()
    b = _book(bids=[(0.18, 300)], asks=[(0.20, 300)], spread=0.02, mid=0.19)
    sig = eng._analyze_book(m, "YES", b)
    assert sig is not None
    assert sig.outcome == "NOT YES"  # mid < 0.5


# ── _analyze_kalshi ──────────────────────────────────────────────────

def test_analyze_kalshi_settled_or_low_volume():
    eng = ComparisonEngine(polymarket=MagicMock())
    assert eng._analyze_kalshi(make_kalshi_market(settled=True)) is None
    assert eng._analyze_kalshi(make_kalshi_market(volume=10)) is None


def test_analyze_kalshi_not_open():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = make_kalshi_market(status="closed", volume=50000)
    assert eng._analyze_kalshi(m) is None


def test_analyze_kalshi_mid_out_of_range():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = make_kalshi_market(yes_bid=0.0, yes_ask=0.0, no_bid=1.0, no_ask=1.0,
                           volume=50000, status="open")
    assert eng._analyze_kalshi(m) is None


def test_analyze_kalshi_wide_spread():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = make_kalshi_market(yes_bid=0.6, yes_ask=0.9, no_bid=0.1, no_ask=0.4,
                           volume=50000, status="open")
    assert eng._analyze_kalshi(m) is None


def test_analyze_kalshi_low_bias_none():
    eng = ComparisonEngine(polymarket=MagicMock())
    # yes_mid ~ 0.55 -> bias 0.1 -> None
    m = make_kalshi_market(yes_bid=0.54, yes_ask=0.56, no_bid=0.44, no_ask=0.46,
                           volume=50000, status="open")
    assert eng._analyze_kalshi(m) is None


def test_analyze_kalshi_signal_yes():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = make_kalshi_market(ticker="K1", yes_bid=0.80, yes_ask=0.82,
                           no_bid=0.18, no_ask=0.20, volume=50000, status="open")
    sig = eng._analyze_kalshi(m)
    assert sig is not None
    assert sig.outcome == "YES"
    assert sig.signal_type == "directional_bias"


def test_analyze_kalshi_signal_no():
    eng = ComparisonEngine(polymarket=MagicMock())
    m = make_kalshi_market(ticker="K2", yes_bid=0.18, yes_ask=0.20,
                           no_bid=0.80, no_ask=0.82, volume=50000, status="open")
    sig = eng._analyze_kalshi(m)
    assert sig is not None
    assert sig.outcome == "NO"


# ── find_opportunities ───────────────────────────────────────────────

def test_find_opportunities_full():
    poly = MagicMock()
    # market with a token carrying an outcome mapping -> book gets analyzed
    m = _pm(condition_id="mkt", question="Will BTC go up?",
            outcomes=["Yes", "No"], outcome_prices={"Yes": 0.2, "No": 0.8},
            accepting_orders=True, closed=False)
    m.tokens = [{"outcome": "Yes", "token_id": "tokA"},
                {"outcome": "No", "token_id": "tokB"}]
    poly.get_crypto_markets.return_value = [m]
    poly.get_order_book.return_value = _book(
        bids=[(0.80, 300)], asks=[(0.82, 300)], spread=0.02, mid=0.81)

    kal = MagicMock()
    kal.get_relevant_markets.return_value = [
        make_kalshi_market(ticker="K1", yes_bid=0.80, yes_ask=0.82,
                           no_bid=0.18, no_ask=0.20, volume=50000, status="open")
    ]
    eng = ComparisonEngine(polymarket=poly, kalshi=kal)
    sigs = eng.find_opportunities({"BTC": {"change_24h": 5.0}})
    assert len(sigs) >= 1
    types = {s.signal_type for s in sigs}
    assert "book_signal" in types
    assert "directional_bias" in types
    # sorted by confidence descending
    confs = [s.confidence for s in sigs]
    assert confs == sorted(confs, reverse=True)


def test_find_opportunities_skips_closed_and_missing_token():
    poly = MagicMock()
    m_closed = _pm(condition_id="c", question="Will BTC go up?",
                   accepting_orders=False, closed=True)
    m_notoken = _pm(condition_id="n", question="Will BTC go up?",
                    outcomes=["Yes"], accepting_orders=True, closed=False)
    m_notoken.tokens = [{"token_id": "x"}]  # no "outcome" key -> token_id ""
    poly.get_crypto_markets.return_value = [m_closed, m_notoken]
    eng = ComparisonEngine(polymarket=poly, kalshi=None)
    sigs = eng.find_opportunities({})
    assert sigs == []


def test_find_opportunities_book_and_kalshi_no_signal():
    # book analysis returns None (low bias) and kalshi analysis returns None
    # -> exercises the `if sig:` false loop-back branches.
    poly = MagicMock()
    m = _pm(condition_id="mkt", question="Will BTC go up?",
            outcomes=["Yes"], outcome_prices={"Yes": 0.5},
            accepting_orders=True, closed=False)
    m.tokens = [{"outcome": "Yes", "token_id": "tokA"}]
    poly.get_crypto_markets.return_value = [m]
    poly.get_order_book.return_value = _book(
        bids=[(0.54, 200)], asks=[(0.56, 200)], spread=0.02, mid=0.55)
    kal = MagicMock()
    kal.get_relevant_markets.return_value = [
        make_kalshi_market(ticker="K1", yes_bid=0.54, yes_ask=0.56,
                           no_bid=0.44, no_ask=0.46, volume=50000, status="open")
    ]
    eng = ComparisonEngine(polymarket=poly, kalshi=kal)
    sigs = eng.find_opportunities({})
    assert sigs == []


def test_find_opportunities_kalshi_exception():
    poly = MagicMock()
    poly.get_crypto_markets.return_value = []
    kal = MagicMock()
    kal.get_relevant_markets.side_effect = RuntimeError("boom")
    eng = ComparisonEngine(polymarket=poly, kalshi=kal)
    sigs = eng.find_opportunities({})
    assert sigs == []


def test_find_opportunities_no_kalshi():
    poly = MagicMock()
    poly.get_crypto_markets.return_value = []
    eng = ComparisonEngine(polymarket=poly, kalshi=None)
    assert eng.find_opportunities({}) == []


# ── format_signal ────────────────────────────────────────────────────

def test_format_signal():
    s = EventSignal(
        platform="kalshi",
        market_question="Will BTC hit 100k?",
        market_ticker="K1",
        outcome="YES",
        probability=0.8,
        position_size=1000.0,
        confidence=0.5,
        signal_type="directional_bias",
        reason="because",
    )
    out = format_signal(s)
    assert "kalshi" in out
    assert "DIRECTIONAL_BIAS" in out
    assert "because" in out
