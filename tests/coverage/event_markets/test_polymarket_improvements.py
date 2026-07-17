"""Tests for Polymarket integration improvements.

Covers, per mandate:
  1. Data correctness — CLOB order-book liquidity_score / depth metrics.
  2. Crypto-market mapping — question->symbol word-boundary parity.
  3. Spread/liquidity filtering — sentinel-spread rejection + book scoring.
  4. Adapter unification — Kalshi/Polymarket produce an identical, directly
     constructible AccumulatedSignal shape.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from event_markets.polymarket_client import PolymarketClient, PolymarketBook
from event_markets.unified_client import (
    UnifiedPredictionMarketClient, PredictionMarket,
)
from event_markets.signal_adapter import PredictionMarketAdapter
from em_helpers import make_polymarket_market, make_kalshi_market, UrlRouter, make_book


# Make the accumulator (which owns AccumulatedSignal) importable.
_GAB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "graph-alpha-bot",
)
if _GAB not in sys.path:
    sys.path.insert(0, _GAB)


def _client():
    c = UnifiedPredictionMarketClient()
    c._kalshi = MagicMock()
    c._polymarket = MagicMock()
    c._polymarket_relayer = MagicMock()
    return c


def _adapter(**kw):
    a = PredictionMarketAdapter(**kw)
    a._client = MagicMock()
    return a


# ── 1. Data correctness: CLOB order-book metrics ───────────────────

def test_order_book_computes_liquidity_score_and_depth():
    book_json = {
        "asks": [{"price": "0.502", "size": "6000"}, {"price": "0.60", "size": "9000"}],
        "bids": [{"price": "0.498", "size": "6000"}, {"price": "0.40", "size": "9000"}],
    }
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/book": book_json})):
        b = c.get_order_book("tok1")
    assert b.mid_price == pytest.approx(0.50)
    assert b.spread == pytest.approx(0.004)
    # 0.502 <= 0.505 and 0.498 >= 0.495 => both count toward 1% depth.
    assert b.liquidity_1pct > 0
    assert 0.0 <= b.liquidity_score <= 1.0


def test_order_book_deep_tight_market_scores_high():
    book_json = {
        "asks": [{"price": "0.501", "size": "20000"}],
        "bids": [{"price": "0.499", "size": "20000"}],
    }
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/book": book_json})):
        b = c.get_order_book("tok1")
    # Deep ($>10k each side) + tight spread => score saturates near 1.0.
    assert b.liquidity_score > 0.9


def test_order_book_empty_has_zero_metrics():
    c = PolymarketClient()
    with patch("urllib.request.urlopen", UrlRouter({"/book": {"asks": [], "bids": []}})):
        b = c.get_order_book("tok1")
    assert b.mid_price == 0.0
    assert b.liquidity_1pct == 0.0
    assert b.liquidity_score == 0.0


# ── 2. Crypto-market mapping parity ────────────────────────────────

@pytest.mark.parametrize("question,expected", [
    ("Will Bitcoin reach 100k?", "BTC-USD"),
    ("Will Solana hit $500?", "SOL-USD"),
    ("Who wins the presidential election?", "BTC-USD"),
    ("What is politics?", ""),          # 'pol' must not match 'politics'
    ("ethics committee ruling", ""),    # 'eth' must not match 'ethics'
])
def test_question_to_symbol_word_boundary(question, expected):
    assert PredictionMarketAdapter._question_to_symbol(question) == expected


def test_polymarket_unified_populates_keywords_like_kalshi():
    """Parity: both platforms populate .keywords from the crypto keyword list."""
    c = _client()
    poly_raw = [make_polymarket_market(condition_id="p1",
                                       question="Will Bitcoin reach 100k?",
                                       spread=0.03, volume=99999)]
    poly = c._polymarket_to_unified(poly_raw, 10, 0, 1.0)
    kalshi_raw = [make_kalshi_market(ticker="k1", title="Will Bitcoin reach 100k?",
                                     volume=99999, yes_bid=0.4, yes_ask=0.45)]
    kalshi = c._kalshi_to_unified(kalshi_raw, 10, 0, 1.0)
    assert "bitcoin" in poly[0].keywords
    assert poly[0].keywords == kalshi[0].keywords


# ── 3. Spread / liquidity filtering ────────────────────────────────

def test_sentinel_spread_market_is_rejected_even_with_permissive_max_spread():
    """A market with spread==1.0 (no valid book sentinel) must never pass,
    even when the crypto path uses max_spread=1.0."""
    c = _client()
    bad = make_polymarket_market(condition_id="nobook", question="Will BTC moon?",
                                 spread=1.0, volume=99999)
    good = make_polymarket_market(condition_id="ok", question="Will BTC moon?",
                                  spread=0.03, volume=99999)
    out = c._polymarket_to_unified([bad, good], 10, min_volume=0, max_spread=1.0)
    ids = {m.market_id for m in out}
    assert "nobook" not in ids
    assert "ok" in ids


def test_book_fallback_uses_richer_liquidity_score():
    c = _client()
    pm = make_polymarket_market(condition_id="c1", question="Will BTC reach 100k?",
                                spread=0.0, token_ids=["tok1"], volume=1000)
    book = make_book(bids=((0.49, 100),), asks=((0.51, 100),), spread=0.02, mid=0.50)
    book.liquidity_score = 0.85
    c._polymarket.get_order_book.return_value = book
    out = c._polymarket_to_unified([pm], 10, min_volume=0, max_spread=1.0)
    assert out and out[0].liquidity_score >= 0.85


def test_low_volume_polymarket_filtered_by_adapter():
    a = _adapter()
    a._client.get_crypto_markets.return_value = [
        PredictionMarket(platform="polymarket", market_id="lowvol",
                         question="Will BTC reach 100k?", outcomes=["YES", "NO"],
                         outcome_prices={"YES": 0.8}, volume=100,  # < min_volume
                         end_date="2030-01-01T00:00:00Z", is_open=True,
                         yes_bid=0.78, yes_ask=0.82, spread=0.04,
                         liquidity_score=0.5, category="crypto",
                         raw_data={"token_ids": ["t1"]}),
    ]
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.78, 100)], asks=[(0.82, 100)])
    assert a.get_signals() == []


def test_wide_spread_polymarket_filtered_by_adapter():
    a = _adapter()
    a._client.get_crypto_markets.return_value = [
        PredictionMarket(platform="polymarket", market_id="wide",
                         question="Will BTC reach 100k?", outcomes=["YES", "NO"],
                         outcome_prices={"YES": 0.8}, volume=50000,
                         end_date="2030-01-01T00:00:00Z", is_open=True,
                         yes_bid=0.6, yes_ask=0.95, spread=0.35,  # > max_spread(0.15)
                         liquidity_score=0.5, category="crypto",
                         raw_data={"token_ids": ["t1"]}),
    ]
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.6, 100)], asks=[(0.95, 100)])
    assert a.get_signals() == []


def test_adapter_default_max_spread_tightened():
    assert PredictionMarketAdapter().max_spread == 0.15


# ── 4. Adapter unification / AccumulatedSignal parity ──────────────

def _poly_market():
    return PredictionMarket(
        platform="polymarket", market_id="P1", question="Will Bitcoin reach 100k?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.85, "NO": 0.15},
        volume=50000, end_date="2030-01-01T00:00:00Z", is_open=True,
        yes_bid=0.84, yes_ask=0.86, spread=0.02, liquidity_score=0.9,
        category="crypto", raw_data={"token_ids": ["t1"]})


def _kalshi_market():
    return PredictionMarket(
        platform="kalshi", market_id="K1", question="Will Bitcoin reach 100k?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.85, "NO": 0.15},
        volume=50000, end_date="2030-01-01T00:00:00Z", is_open=True,
        yes_bid=0.84, yes_ask=0.86, spread=0.02, liquidity_score=0.9,
        category="crypto", raw_data={"event_ticker": "btc", "open_interest": 500})


def _signal_for(m):
    a = _adapter()
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.84, 500)], asks=[(0.86, 500)])
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [{"price": "0.84", "size": "500"}],
        "asks": [{"price": "0.86", "size": "500"}],
    }
    sigs = a._market_to_signals(m)
    assert len(sigs) == 1
    return sigs[0]


def test_adapter_signals_have_identical_top_level_shape():
    poly = _signal_for(_poly_market())
    kalshi = _signal_for(_kalshi_market())
    assert set(poly.keys()) == set(kalshi.keys())
    # Extras must be nested, not top-level.
    assert "kelly_fraction" not in poly
    assert "hours_to_expiry" not in poly
    assert "kelly_fraction" in poly["market_data"]
    assert "hours_to_expiry" in poly["market_data"]


def test_adapter_signal_dict_constructs_accumulated_signal():
    """Regression: AccumulatedSignal(**sig) must succeed for BOTH platforms.
    Previously top-level kelly_fraction/hours_to_expiry raised TypeError."""
    from app.strategies.unified_signal_accumulator import AccumulatedSignal
    for m in (_poly_market(), _kalshi_market()):
        sig = _signal_for(m)
        acc = AccumulatedSignal(**sig)
        assert acc.symbol == "BTC-USD"
        assert acc.action == "BUY"
        assert acc.market_data["platform"] == m.platform
