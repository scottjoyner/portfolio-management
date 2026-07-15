"""Coverage for EventTraderV4 scan-loop single-pass bodies (news/macro/pair/onchain/funding)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from coinbase.src.run_trader_v4 import EventTraderV4  # noqa: E402


def _make_trader(**kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    return EventTraderV4(mode=mode, products=["BTC-USD", "ETH-USD", "SOL-USD"], **kw)


class _Series:
    def __init__(self, data):
        self._d = list(data)

    def to_list(self):
        return self._d


class _Buf:
    def __init__(self, closes, volumes):
        self.closes = _Series(closes)
        self.volumes = _Series(volumes)


def test_news_sentiment_scan():
    t = _make_trader()
    sig = MagicMock()
    sig.to_opportunity.return_value = {"product_id": "BTC-USD"}
    t._news_sentiment = MagicMock()
    t._news_sentiment.get_signals.return_value = [sig]
    t._paper_execute = MagicMock()
    t._last_price = {"BTC-USD": 100.0}
    t._news_sentiment_scan()
    t._paper_execute.assert_called_once()


def test_macro_risk_scan():
    t = _make_trader()
    sig = MagicMock()
    sig.to_opportunity.return_value = {"action": "BUY"}
    t._macro_risk = MagicMock()
    t._macro_risk.get_signal.return_value = sig
    t._paper_execute = MagicMock()
    t._last_price = {"BTC-USD": 100.0}
    t._macro_risk_scan()
    t._paper_execute.assert_called_once()


def test_macro_risk_scan_none():
    t = _make_trader()
    t._macro_risk = MagicMock()
    t._macro_risk.get_signal.return_value = None
    t._paper_execute = MagicMock()
    t._macro_risk_scan()
    t._paper_execute.assert_not_called()


def test_pair_trade_scan():
    t = _make_trader()
    t._pair_trading = MagicMock()
    t._pair_trading.on_prices.return_value = [{"product_id": "BTC-USD", "action": "BUY"}]
    t._paper_execute = MagicMock()
    t._last_price = {"BTC-USD": 100.0}
    t._pair_trade_scan()
    t._paper_execute.assert_called_once()


def test_onchain_flow_scan():
    t = _make_trader()
    t._onchain_flow = MagicMock()
    t._onchain_flow.get_signals.return_value = [{"product_id": "BTC-USD"}]
    t._paper_execute = MagicMock()
    t._last_price = {"BTC-USD": 100.0}
    t._onchain_flow_scan()
    t._paper_execute.assert_called_once()


def test_funding_scan_bullish_floored():
    # bias->BUY branch is taken, but conf=confidence*0.3 (<0.35 floor) forces HOLD
    t = _make_trader()
    closes = [100.0 + i for i in range(60)]
    t.streaming = MagicMock()
    t.streaming.try_get = lambda pid: _Buf(closes, [1.0] * len(closes))
    t._last_macro_signal = SimpleNamespace(bias="bullish", confidence=0.6)
    t._last_price = {"BTC-USD": 159.0, "ETH-USD": 200.0, "SOL-USD": 50.0}
    t._paper_execute = MagicMock()
    t._funding_scan()
    t._paper_execute.assert_not_called()


def test_funding_scan_bearish_floored():
    # last close below ma20 -> not trend_up -> SELL branch (also floored to HOLD)
    t = _make_trader()
    closes = [200.0 - i for i in range(60)]
    t.streaming = MagicMock()
    t.streaming.try_get = lambda pid: _Buf(closes, [1.0] * len(closes))
    t._last_macro_signal = SimpleNamespace(bias="bearish", confidence=0.6)
    t._last_price = {"BTC-USD": 141.0}
    t._paper_execute = MagicMock()
    t._funding_scan()
    t._paper_execute.assert_not_called()


def test_funding_scan_vol_spike_holds():
    # vol spike gates out both bullish/bearish -> else HOLD
    t = _make_trader()
    closes = [100.0 + i for i in range(60)]
    vols = [1.0] * 59 + [3.0]
    t.streaming = MagicMock()
    t.streaming.try_get = lambda pid: _Buf(closes, vols)
    t._last_macro_signal = SimpleNamespace(bias="bullish", confidence=0.6)
    t._last_price = {"BTC-USD": 159.0}
    t._paper_execute = MagicMock()
    t._funding_scan()
    t._paper_execute.assert_not_called()


def test_funding_scan_short_closes():
    t = _make_trader()
    t.streaming = MagicMock()
    t.streaming.try_get = lambda pid: _Buf([1.0, 2.0], [1.0, 1.0])
    t._last_macro_signal = SimpleNamespace(bias="bullish", confidence=0.6)
    t._last_price = {"BTC-USD": 159.0}
    t._paper_execute = MagicMock()
    t._funding_scan()
    t._paper_execute.assert_not_called()


def test_funding_scan_no_macro():
    t = _make_trader()
    t._last_macro_signal = None
    t._paper_execute = MagicMock()
    t._funding_scan()
    t._paper_execute.assert_not_called()
