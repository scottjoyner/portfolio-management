"""Coverage tests for event_markets.settlement (network mocked)."""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import event_markets.settlement as st
from event_markets.settlement import SettlementTracker
from event_markets.polymarket_client import PolymarketMarket


def _pm(closed=False, yes_price=1.0):
    return PolymarketMarket(
        condition_id="C1", question="Q", description="", outcomes=["YES", "NO"],
        outcome_prices={"YES": yes_price, "NO": 1 - yes_price}, volume=1000.0,
        end_date_iso="2026-12-31T00:00:00Z", closed=closed, accepting_orders=True,
        tokens=[{"token_id": "t1"}], ticker="c1", event_slug="e",
        yes_bid=yes_price - 0.02, yes_ask=yes_price + 0.02, spread=0.04,
    )


def _make_client(buy_res=1, hedge_res=0, poly_closed=True, poly_yes=1.0):
    client = MagicMock()
    pm = _pm(closed=poly_closed, yes_price=poly_yes)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = hedge_res
    return client, buy_res


def _trade(**kw):
    base = {
        "event_key": "E1", "category": "crypto", "type": "arbitrage",
        "platform_buy": "polymarket", "buy_market_id": "C1",
        "platform_hedge": "kalshi", "hedge_market_id": "K1",
        "buy_yes_price": 0.4, "hedge_yes_price": 0.6, "total_cost": 0.7,
        "guaranteed_payout": 1.0, "edge": 0.1, "edge_pct": 0.1,
        "confidence": 0.5, "notional": 1000.0, "contracts": 1000,
        "expected_profit": 100.0, "estimated_fees": 0.0,
        "net_expected_profit": 100.0, "status": "open",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    base.update(kw)
    return base


@pytest.fixture
def tracker(tmp_path):
    p = tmp_path / "paper-trades.json"
    client = MagicMock()
    t = SettlementTracker(client=client, trades_path=p)
    return t, p, client


def test_load_save_roundtrip(tracker):
    t, p, _ = tracker
    trades = [_trade()]
    t._save(trades)
    assert t._load() == trades
    # corrupt
    p.write_text("{bad")
    assert t._load() == []
    # nonexistent
    t2 = SettlementTracker(trades_path=p.parent / "nope.json")
    assert t2._load() == []


def test_settle_both_resolved(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 0
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["settled"] == 1
    assert res["realized_pnl"] > 0
    loaded = t._load()
    assert loaded[0]["status"] == "settled"
    # matched pair resolves oppositely -> hedge held (no divergence warning)
    assert loaded[0]["resolution"]["hedge_held"] is True


def test_settle_diverged_legs(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=0.0)  # buy resolved NO (0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 0  # hedge resolved NO
    # buy_yes_res=0 (YES price 0) and hedge_yes_res=0 -> both NO resolved
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["settled"] == 1
    # both legs resolved NO (mismatch) -> hedge_held False, flagged diverged
    loaded = t._load()
    assert loaded[0]["resolution"]["hedge_held"] is False


def test_settle_kalshi_buy_resolved(tracker):
    t, p, client = tracker
    trades = [_trade(platform_buy="kalshi", buy_market_id="K1",
                     platform_hedge="polymarket", hedge_market_id="C1")]
    t._save(trades)
    client._kalshi.get_settlement.return_value = 1  # buy YES
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm  # hedge YES
    res = t.settle_open_trades()
    assert res["settled"] == 1


def test_settle_unresolved_stays_open(tracker):
    t, p, client = tracker
    # buy resolves, hedge not
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = None
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["settled"] == 0
    assert res["still_open"] == 1


def test_settle_expired_fallback(tracker):
    t, p, client = tracker
    # both unresolved but very old -> expired
    old = time.time() - 86400 * 30
    # both legs unresolved -> expired fallback (mocks must return None, not MagicMock)
    client._polymarket.fetch_market_detail.return_value = None
    client._kalshi.get_settlement.return_value = None
    trades = [_trade(timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(old)))]
    t._save(trades)
    res = t.settle_open_trades(max_age_days=7)
    assert res["expired"] == 1
    loaded = t._load()
    assert loaded[0]["status"] == "expired"


def test_settle_live_trade_multiplier(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 0
    trades = [_trade(status="live_open", mode="live", contracts=500,
                     net_expected_profit=50.0)]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["settled"] == 1
    loaded = t._load()
    assert loaded[0]["resolution"]["live"] is True


def test_settle_skips_non_arbitrage(tracker):
    t, p, client = tracker
    trades = [_trade(type="kalshi_internal", status="open")]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["still_open"] == 0
    assert res["settled"] == 0


def test_settle_polymarket_unresolved_not_closed(tracker):
    t, p, client = tracker
    pm = _pm(closed=False)  # not closed -> unresolved
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 1
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["settled"] == 0


def test_settle_polymarket_no_price_fallback(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=0.0)
    pm.outcome_prices = {"OTHER": 0.0}  # no YES key -> fallback to next(iter)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 0
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["settled"] == 1


def test_settle_poly_detail_none(tracker):
    t, p, client = tracker
    client._polymarket.fetch_market_detail.return_value = None
    client._kalshi.get_settlement.return_value = 1
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    # buy unresolved -> still open
    assert res["still_open"] == 1


def test_settle_poly_exception(tracker):
    t, p, client = tracker
    client._polymarket.fetch_market_detail.side_effect = RuntimeError("x")
    client._kalshi.get_settlement.return_value = 1
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["still_open"] == 1


def test_settle_kalshi_exception(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.side_effect = RuntimeError("x")
    trades = [_trade()]
    t._save(trades)
    res = t.settle_open_trades()
    assert res["still_open"] == 1


def test_summary(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 0
    trades = [_trade(), _trade(status="settled", realized_pnl=10.0)]
    t._save(trades)
    t.settle_open_trades()
    s = t.summary()
    assert "by_status" in s
    assert s["total_trades"] == 2
    assert s["realized_pnl"] > 0


def test_parse_ts_edge():
    from event_markets.settlement import _parse_ts
    assert _parse_ts(None) == 0.0
    assert _parse_ts("not-a-date") == 0.0
    assert _parse_ts(123.0) == 123.0
    assert _parse_ts(5) == 5.0
    assert _parse_ts("2020-01-01T00:00:00Z") > 0


def test_resolve_yes_unknown_and_empty(tracker):
    t, p, client = tracker
    assert t._resolve_yes("foo", "X") is None
    assert t._resolve_yes("polymarket", "") is None
    assert t._resolve_yes("kalshi", "") is None


def test_summary_with_open_and_diverged(tracker):
    t, p, client = tracker
    pm = _pm(closed=True, yes_price=1.0)
    client._polymarket.fetch_market_detail.return_value = pm
    client._kalshi.get_settlement.return_value = 0
    # matched pair (hedge_held True -> not diverged)
    trades = [_trade(), _trade(status="settled", realized_pnl=50.0)]
    # an open trade
    trades.append(_trade(status="open"))
    # a diverged settled pair (both YES)
    div = _trade(status="settled", realized_pnl=-5.0)
    div["resolution"] = {"hedge_held": False}
    trades.append(div)
    t._save(trades)
    s = t.summary()
    assert s["total_trades"] == 4
    assert s["diverged_pairs"] == 1
    assert s["open_expected_pnl"] > 0
