"""Coverage tests for event_markets.signal_adapter (PredictionMarketAdapter)."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from event_markets import signal_adapter as sa
from event_markets.signal_adapter import PredictionMarketAdapter
from event_markets.unified_client import PredictionMarket
from event_markets.polymarket_client import PolymarketBook


def _pm(question="Will Bitcoin reach 100k?", price=0.8, platform="kalshi",
        category="crypto", volume=5000, spread=0.05, liquidity=0.5,
        open_interest=1000, token_ids=None, is_open=True, end_date="2027-12-31T00:00:00Z",
        market_id="m1"):
    raw = {"open_interest": open_interest}
    if token_ids is not None:
        raw["token_ids"] = token_ids
    return PredictionMarket(
        platform=platform, market_id=market_id, question=question,
        outcomes=["YES", "NO"], outcome_prices={"YES": price},
        volume=volume, end_date=end_date, is_open=is_open,
        yes_bid=price - 0.02, yes_ask=price + 0.02, spread=spread,
        liquidity_score=liquidity, category=category, raw_data=raw,
    )


def _adapter(**kw):
    a = PredictionMarketAdapter(**kw)
    a._client = MagicMock()
    return a


def test_init_defaults_and_categories(monkeypatch):
    monkeypatch.setenv("KALSHI_EMAIL", "")
    a = PredictionMarketAdapter()
    assert a.categories == ["crypto"]
    a2 = PredictionMarketAdapter(categories=["*"])
    assert a2.categories == ["*"]
    a3 = PredictionMarketAdapter(categories=["crypto", "sports"])
    assert a3.categories == ["crypto", "sports"]


def test_hours_to_expiry():
    a = _adapter()
    # Z suffix
    h = a._hours_to_expiry("2027-01-01T00:00:00Z")
    assert h > 0
    # no Z, naive -> treated as utc
    h2 = a._hours_to_expiry("2027-01-01T00:00:00")
    assert h2 > 0
    # past date -> clamped to 0
    h3 = a._hours_to_expiry("2000-01-01T00:00:00Z")
    assert h3 == 0
    # invalid -> default 168
    assert a._hours_to_expiry("not-a-date") == 168


def test_get_order_book_depth_kalshi_cache():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [(0.5, 100)], "asks": [(0.9, 100)],
    }
    bid, ask = a._get_order_book_depth(_pm())
    assert bid == 0 and ask == 0  # prices far from mid*0.99/1.01
    # cache hit: no further call
    a._client.get_kalshi_order_book_depth.reset_mock()
    bid2, ask2 = a._get_order_book_depth(_pm())
    a._client.get_kalshi_order_book_depth.assert_not_called()
    assert bid2 == 0


def test_get_order_book_depth_kalshi_nonzero():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [(0.99, 50), (0.5, 100)], "asks": [(1.01, 60)],
    }
    bid, ask = a._get_order_book_depth(_pm(price=1.0, market_id="nz1"))
    assert bid == 50 and ask == 60


def test_get_order_book_depth_kalshi_real_dict_format():
    # Kalshi's REST orderbook returns lists of {"price","size"} dicts (prices
    # are strings). The adapter must parse these, not unpack tuples.
    sa._book_cache.clear()
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [{"price": "0.99", "size": "40"}, {"price": "0.50", "size": "10"}],
        "asks": [{"price": "1.01", "size": "55"}],
    }
    bid, ask = a._get_order_book_depth(_pm(platform="kalshi", price=1.0, market_id="kd1"))
    assert bid == 40 and ask == 55


def test_get_order_book_depth_kalshi_empty_book():
    sa._book_cache.clear()
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {}
    bid, ask = a._get_order_book_depth(_pm(platform="kalshi", price=1.0, market_id="ke1"))
    assert bid == 0 and ask == 0


def test_get_order_book_depth_polymarket():
    sa._book_cache.clear()
    a = _adapter()
    a._client.get_polymarket_order_book.return_value = PolymarketBook(
        bids=[(0.99, 50)], asks=[(1.01, 60)])
    bid, ask = a._get_order_book_depth(_pm(platform="polymarket", token_ids=["tok1"], price=1.0, market_id="p1"))
    assert bid == 50 and ask == 60
    # no token ids -> 0,0
    b2, a2 = a._get_order_book_depth(_pm(platform="polymarket", token_ids=[], market_id="p2"))
    assert b2 == 0 and a2 == 0


def test_get_order_book_depth_exception():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.side_effect = RuntimeError("boom")
    assert a._get_order_book_depth(_pm()) == (0, 0)


def test_question_to_symbol():
    assert PredictionMarketAdapter._question_to_symbol("Will Bitcoin reach 100k?") == "BTC-USD"
    assert PredictionMarketAdapter._question_to_symbol("Will DOGE moon?") == "DOGE-USD"
    # word boundary: "pol" should not match "politics", "eth" not "ethics", "btc" not "botcoin"
    assert PredictionMarketAdapter._question_to_symbol("What is politics?") == ""
    assert PredictionMarketAdapter._question_to_symbol("ethics debate") == ""
    assert PredictionMarketAdapter._question_to_symbol("botcoin launch") == ""
    # no keyword match -> empty (caller treats as "no symbol"), no false-positive fallback
    assert PredictionMarketAdapter._question_to_symbol("random words here", "sports") == ""
    assert PredictionMarketAdapter._question_to_symbol("random words", "technology") == ""


def test_market_to_signals_buy_and_sell():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    buy = a._market_to_signals(_pm(price=0.8, category="crypto"))
    assert len(buy) == 1 and buy[0]["action"] == "BUY"
    sell = a._market_to_signals(_pm(price=0.2, category="crypto"))
    assert sell[0]["action"] == "SELL"
    # non-actionable category confidence reduced
    nonact = a._market_to_signals(_pm(price=0.8, category="sports"))
    assert nonact and nonact[0]["market_data"]["actionable"] is False


def test_market_to_signals_skip_conditions():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    # mp out of range
    assert a._market_to_signals(_pm(price=0.0)) == []
    # insufficient extremity
    assert a._market_to_signals(_pm(price=0.55)) == []
    # (spread/volume are filtered at get_signals level, not here)


def test_make_signal_kelly_branches():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    buy = a._market_to_signals(_pm(price=0.8, category="crypto"))[0]
    assert "kelly_fraction" in buy
    # SELL path
    sell = a._market_to_signals(_pm(price=0.2, category="crypto"))[0]
    assert sell["action"] == "SELL"
    assert "kelly_fraction" in sell


def test_make_signal_kelly_sell_uses_no_probability():
    # Regression: SELL bets NO, so its Kelly edge must come from the NO side
    # (1 - mp). A low mp (e.g. 0.2) means NO is likely (0.8) => positive Kelly.
    sa._book_cache.clear()
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [{"price": "0.19", "size": "100"}],
        "asks": [{"price": "0.21", "size": "100"}],
    }
    sell = a._make_signal("BTC-USD", "SELL", 0.5, 0.25,
                          _pm(platform="kalshi", price=0.2, market_id="ksell"), "reason")
    assert sell["kelly_fraction"] > 0
    buy = a._make_signal("BTC-USD", "BUY", 0.5, 0.25,
                         _pm(platform="kalshi", price=0.8, market_id="kbuy"), "reason")
    assert buy["kelly_fraction"] > 0


def test_get_signals_crypto_only():
    a = _adapter()
    a._client.get_crypto_markets.return_value = [
        _pm(price=0.8), _pm(price=0.2, open_interest=10),  # second filtered by open_interest
    ]
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    sigs = a.get_signals()
    assert len(sigs) == 1
    assert sigs[0]["action"] == "BUY"


def test_get_signals_open_and_spread_filters():
    a = _adapter()
    a._client.get_crypto_markets.return_value = [
        _pm(price=0.8, is_open=False),
        _pm(price=0.8, volume=10),
    ]
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    assert a.get_signals() == []


def test_get_signals_all_categories():
    a = _adapter(categories=["crypto", "sports"])
    a._client.search_all_categories.return_value = {
        "crypto": [_pm(price=0.8)],
        "sports": [_pm(price=0.8, category="sports", question="Who wins super bowl?")],
        "politics": [],
    }
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    a._client.get_polymarket_order_book.return_value = MagicMock(bids=[(0.99, 50)], asks=[(1.01, 60)])
    sigs = a.get_signals()
    assert len(sigs) == 2


def test_get_signals_wildcard_categories():
    a = _adapter(categories=["*"])
    a._client.search_all_categories.return_value = {
        "crypto": [_pm(price=0.8)],
    }
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    sigs = a.get_signals()
    assert len(sigs) == 1


def test_get_signals_exception_returns_empty():
    a = _adapter()
    a._client.get_crypto_markets.side_effect = RuntimeError("boom")
    assert a.get_signals() == []


def test_get_signals_dedup():
    a = _adapter()
    m = _pm(price=0.8)
    a._client.get_crypto_markets.return_value = [m, m]
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    sigs = a.get_signals()
    assert len(sigs) == 1


def test_constants_present():
    assert "BTC-USD" in [s for _, s in sa.EVENT_SYMBOL_MAP]
    assert sa.ACTIONABLE_CATEGORIES == {"crypto", "economics", "technology"}
    assert sa.KALSHI_FEE == 0.02


def test_get_signals_real_market_without_open_interest():
    # Regression: unified_client never populated raw_data["open_interest"],
    # so the min_open_interest filter dropped every real market and the
    # adapter produced zero signals. A market built exactly as
    # unified_client._kalshi_to_unified would must still yield a signal.
    sa._book_cache.clear()
    pm = PredictionMarket(
        platform="kalshi", market_id="K1", question="Will BTC hit 100k?",
        outcomes=["YES", "NO"], outcome_prices={"YES": 0.85, "NO": 0.15},
        volume=50000, end_date="2030-01-01T00:00:00Z", is_open=True,
        yes_bid=0.84, yes_ask=0.86, spread=0.02, liquidity_score=0.9,
        category="crypto", raw_data={"event_ticker": "btc"},
    )
    a = _adapter()
    a._client.get_crypto_markets.return_value = [pm]
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [{"price": "0.84", "size": "100"}],
        "asks": [{"price": "0.86", "size": "100"}],
    }
    sigs = a.get_signals()
    assert len(sigs) == 1
    assert sigs[0]["action"] == "BUY"


# ── supplementary branch coverage ──────────────────────────────────
def test_get_order_book_depth_cache_hit():
    sa._book_cache.clear()
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {
        "bids": [(0.99, 50)], "asks": [(1.01, 60)],
    }
    a._get_order_book_depth(_pm(price=1.0, market_id="ch1"))
    a._client.get_kalshi_order_book_depth.reset_mock()
    bid, ask = a._get_order_book_depth(_pm(price=1.0, market_id="ch1"))
    a._client.get_kalshi_order_book_depth.assert_not_called()
    assert bid == 50 and ask == 60


def test_get_signals_crypto_spread_filter():
    a = _adapter()
    a._client.get_crypto_markets.return_value = [_pm(price=0.8, spread=0.5)]
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    assert a.get_signals() == []


def test_get_signals_allcats_oi_filter():
    a = _adapter(categories=["crypto", "sports"])
    a._client.search_all_categories.return_value = {
        "sports": [_pm(price=0.8, category="sports", open_interest=10,
                       question="Who wins the super bowl?")],
    }
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    assert a.get_signals() == []


def test_get_signals_allcats_spread_filter():
    a = _adapter(categories=["crypto", "sports"])
    a._client.search_all_categories.return_value = {
        "sports": [_pm(price=0.8, category="sports", spread=0.5,
                       question="Who wins the super bowl?")],
    }
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    assert a.get_signals() == []


def test_market_to_signals_no_direction():
    a = _adapter()
    a._client.get_kalshi_order_book_depth.return_value = {"bids": [(0.99, 50)], "asks": [(1.01, 60)]}
    # mp in neutral band -> no BUY/SELL signal
    assert a._market_to_signals(_pm(price=0.5)) == []


def test_depth_price_and_size_helpers():
    assert sa._depth_price((0.5, 100)) == 0.5
    assert sa._depth_size((0.5, 100)) == 100
    # non-dict/non-tuple -> 0.0
    assert sa._depth_price(5) == 0.0
    assert sa._depth_size(5) == 0.0
