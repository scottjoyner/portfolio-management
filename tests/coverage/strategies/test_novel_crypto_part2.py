"""Coverage tests for novel_crypto_strategies_part2.py (on-chain / flow strategies)."""
from __future__ import annotations

import pytest

from trading_system.strategies.base import OHLCVBar
import trading_system.strategies.novel_crypto_strategies_part2 as nc2


def obars(n, start=100.0, up=True):
    out = []
    p = start
    for i in range(n):
        p *= 1.01 if up else 0.99
        out.append(OHLCVBar(timestamp=i, open=p * 0.99, high=p * 1.02,
                            low=p * 0.98, close=p, volume=1e6 + i))
    return out


def test_sign_helper():
    assert nc2._sign(5) == 1.0
    assert nc2._sign(-5) == -1.0
    assert nc2._sign(0) == 0.0


# ---------------------------------------------------------------------------
# OnChainRegimeStrategy
# ---------------------------------------------------------------------------

def test_onchain_regime():
    s = nc2.OnChainRegimeStrategy()
    # on_bar before mock data -> None
    s.nvt_history = []
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    # setup with explicit nvt_data
    from trading_system.strategies.novel_crypto_strategies_part2 import OnChainMetrics
    nvt = [OnChainMetrics(timestamp=i, nvt_ratio=30 + i) for i in range(40)]
    s.setup(obars(40), nvt_data=nvt)
    assert len(s.nvt_history) == 40

    # setup with mock data path
    s2 = nc2.OnChainRegimeStrategy()
    s2.setup(obars(60))
    assert s2.nvt_history

    # drive on_bar across bars
    for b in obars(20):
        s2.on_bar(b)

    # force buy branch (low percentile threshold high so current < threshold)
    sbuy = nc2.OnChainRegimeStrategy(percentile_threshold_low=100.0)
    sbuy.setup(obars(60))
    r = sbuy.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                             close=100.5, volume=1e6))
    assert r[0] in (True, None)

    # force sell branch (threshold_high very low)
    ssell = nc2.OnChainRegimeStrategy(percentile_threshold_high=-1.0)
    ssell.setup(obars(60))
    r = ssell.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                              close=100.5, volume=1e6))
    assert r[0] in (False, None)

    # bar.open == 0 branch in current_nvt computation
    s2.on_bar(OHLCVBar(timestamp=1, open=0, high=1, low=0, close=1, volume=1))

    # get_nvt_signal + error branch
    assert "current_nvt" in s2.get_nvt_signal()
    empty = nc2.OnChainRegimeStrategy()
    empty.nvt_history = []
    assert "error" in empty.get_nvt_signal()

    # percentile with < 30 history -> 50.0
    assert s2._calculate_nvt_percentile(s2.nvt_history[-1]) >= 0
    tiny = nc2.OnChainRegimeStrategy()
    tiny.nvt_history = [1.0]
    assert tiny._calculate_nvt_percentile(1.0) == 50.0

    assert s2.get_strategy_info()["name"] == "OnChainRegimeStrategy"


# ---------------------------------------------------------------------------
# WhaleFlowMomentumStrategy
# ---------------------------------------------------------------------------

def test_whale_flow_momentum():
    s = nc2.WhaleFlowMomentumStrategy()
    s.setup(obars(40))
    # not enough flows
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    s._simulate_whale_transactions()
    assert s.exchange_flows

    # flow momentum with < 5 flows -> 0.0
    s2 = nc2.WhaleFlowMomentumStrategy()
    s2.exchange_flows = [1.0, 2.0]
    assert s2._calculate_flow_momentum() == 0.0

    # bullish: strong positive flow + positive price
    sb = nc2.WhaleFlowMomentumStrategy()
    sb.setup(obars(20))
    sb.exchange_flows = [1e8] * 12  # big positive flow momentum
    r = sb.on_bar(OHLCVBar(timestamp=0, open=100, high=110, low=99,
                           close=105, volume=1e6))
    assert r[0] in (True, None)

    # bearish: strong negative flow + negative price
    sbear = nc2.WhaleFlowMomentumStrategy()
    sbear.setup(obars(20))
    sbear.exchange_flows = [-1e8] * 12
    r = sbear.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=90,
                              close=95, volume=1e6))
    assert r[0] in (False, None)

    # analysis
    assert "signal" in sb.get_flow_analysis()
    empty = nc2.WhaleFlowMomentumStrategy()
    assert "error" in empty.get_flow_analysis()
    # accumulation (net<0)
    accn = nc2.WhaleFlowMomentumStrategy()
    accn.exchange_flows = [-1e7] * 10
    assert accn.get_flow_analysis()["signal"] == "accumulation"

    assert sb.get_strategy_info()["name"] == "WhaleFlowMomentumStrategy"


# ---------------------------------------------------------------------------
# OrderFlowImbalanceStrategy
# ---------------------------------------------------------------------------

def test_order_flow_imbalance():
    s = nc2.OrderFlowImbalanceStrategy(imbalance_threshold=1e-9)
    s.setup(obars(40))
    # not enough imbalances
    assert s.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                             close=100.0, volume=1e6)) == (None, None)

    # _estimate_market_depth default (short history)
    short = nc2.OrderFlowImbalanceStrategy()
    short.ohlcv = obars(3)
    assert short._estimate_market_depth(obars(1)[0]) == (1e6, 1e6)

    # _calculate_order_imbalance with < 5 ohlcv -> 0
    tiny = nc2.OrderFlowImbalanceStrategy()
    tiny.ohlcv = obars(3)
    assert tiny._calculate_order_imbalance(obars(1)[0]) == 0.0

    # buy branch: positive imbalances + positive price change
    sb = nc2.OrderFlowImbalanceStrategy(imbalance_threshold=1e-12)
    sb.ohlcv = obars(20)
    sb.order_imbalances = [1e-6] * 12
    r = sb.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                           close=200000, volume=1e6))
    assert r[0] in (True, None)

    # sell branch: negative imbalances + negative price change
    ss = nc2.OrderFlowImbalanceStrategy(imbalance_threshold=1e-12)
    ss.ohlcv = obars(20)
    ss.order_imbalances = [-1e-6] * 12
    r = ss.on_bar(OHLCVBar(timestamp=0, open=200000, high=200001, low=99,
                           close=100, volume=1e6))
    assert r[0] in (False, None)

    # analysis
    assert "signal" in sb.get_imbalance_analysis()
    empty = nc2.OrderFlowImbalanceStrategy()
    assert "error" in empty.get_imbalance_analysis()
    selling = nc2.OrderFlowImbalanceStrategy()
    selling.order_imbalances = [-1.0] * 20
    assert selling.get_imbalance_analysis()["signal"] == "selling_pressure"

    assert sb.get_strategy_info()["name"] == "OrderFlowImbalanceStrategy"


# ---------------------------------------------------------------------------
# CrossExchangeMicrostructureArb
# ---------------------------------------------------------------------------

def test_cross_exchange_arb():
    s = nc2.CrossExchangeMicrostructureArb(min_profit_pct=0.0)
    s.setup(obars(40))
    # no prices yet
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)
    assert s.get_arb_opportunity() is None

    s._fetch_exchange_prices()
    assert len(s.exchange_prices) >= 2

    # _calculate_fee_adjusted_profit with <2 prices -> 0
    tiny = nc2.CrossExchangeMicrostructureArb()
    tiny.exchange_prices = {"a": 100.0}
    assert tiny._calculate_fee_adjusted_profit("a", "a") == 0.0

    # signal path (min_profit 0 -> best_profit 0 >= 0)
    r = s.on_bar(OHLCVBar(timestamp=0, open=100, high=101, low=99,
                          close=100, volume=1e6))
    assert r[0] in (True, None)

    # profitable opportunity via manual price spread
    sp = nc2.CrossExchangeMicrostructureArb(min_profit_pct=0.0,
                                            exchanges=["binance", "coinbase"])
    sp.exchange_prices = {"binance": 100.0, "coinbase": 120.0}
    opp = sp.get_arb_opportunity()
    assert opp is not None and opp["profit_pct"] > 0

    # unprofitable -> None
    sp2 = nc2.CrossExchangeMicrostructureArb(min_profit_pct=0.5,
                                             exchanges=["binance", "coinbase"])
    sp2.exchange_prices = {"binance": 100.0, "coinbase": 100.0}
    assert sp2.get_arb_opportunity() is None

    assert s.get_strategy_info()["name"] == "CrossExchangeMicrostructureArb"


# ---------------------------------------------------------------------------
# SentimentRegimeDetector
# ---------------------------------------------------------------------------

def test_sentiment_regime(monkeypatch):
    s = nc2.SentimentRegimeDetector(extreme_threshold=0.5)
    s.setup(obars(40))
    # not enough sentiment
    assert s.on_bar(OHLCVBar(timestamp=0, close=100.0)) == (None, None)

    # _simulate_sentiment_score with short history -> 0
    tiny = nc2.SentimentRegimeDetector()
    tiny.ohlcv = obars(2)
    assert tiny._simulate_sentiment_score() == 0.0

    # _calculate_sentiment_extremity: short -> 0
    assert s._calculate_sentiment_extremity() == 0.0
    # range zero -> 0
    s.sentiment_history = [0.5] * 25
    assert s._calculate_sentiment_extremity() == 0.0

    # sell branch: positive extreme sentiment, weak price
    ssell = nc2.SentimentRegimeDetector(extreme_threshold=0.5)
    ssell.setup(obars(20))
    ssell.sentiment_history = [-0.5] + [0.0] * 24  # min -0.5, will append 0.9 -> max
    monkeypatch.setattr(ssell, "_simulate_sentiment_score", lambda: 0.9)
    r = ssell.on_bar(OHLCVBar(timestamp=0, open=100, high=100.1, low=99.9,
                              close=100.0, volume=1e6))
    assert r[0] in (False, None)

    # buy branch: negative extreme sentiment, strong price
    sbuy = nc2.SentimentRegimeDetector(extreme_threshold=0.5)
    sbuy.setup(obars(20))
    sbuy.sentiment_history = [0.5] + [0.0] * 24
    monkeypatch.setattr(sbuy, "_simulate_sentiment_score", lambda: -0.9)
    r = sbuy.on_bar(OHLCVBar(timestamp=0, open=100, high=110, low=99,
                             close=105, volume=1e6))
    assert r[0] in (True, None)

    # analysis: bullish / bearish / neutral labels
    sa = nc2.SentimentRegimeDetector()
    sa.sentiment_history = [0.8] * 30
    assert sa.get_sentiment_analysis()["sentiment_label"] == "extremely_bullish"
    sa.sentiment_history = [-0.8] * 30
    assert sa.get_sentiment_analysis()["sentiment_label"] == "extremely_bearish"
    sa.sentiment_history = [0.0] * 30
    assert sa.get_sentiment_analysis()["sentiment_label"] == "neutral"
    empty = nc2.SentimentRegimeDetector()
    assert "error" in empty.get_sentiment_analysis()

    assert sa.get_strategy_info()["name"] == "SentimentRegimeDetector"

    # simulate score real path (>=5 bars)
    s.ohlcv = obars(10)
    val = s._simulate_sentiment_score()
    assert -1.0 <= val <= 1.0
