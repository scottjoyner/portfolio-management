"""Coverage tests for trading_system.strategies.novel_crypto_strategies_part2."""
from __future__ import annotations

from strategies.base import OHLCVBar

from trading_system.strategies.novel_crypto_strategies_part2 import (
    OnChainRegimeStrategy,
    WhaleFlowMomentumStrategy,
    OrderFlowImbalanceStrategy,
    CrossExchangeMicrostructureArb,
    SentimentRegimeDetector,
    OnChainMetrics,
)


def mk(close, high=None, low=None, open_=None, volume=1000.0, ts=0):
    return OHLCVBar(
        open=open_ if open_ is not None else close,
        high=high if high is not None else close,
        low=low if low is not None else close,
        close=close,
        volume=volume,
        timestamp=ts,
    )


def bars(n, start=100.0, step=1.0):
    return [mk(round(start + i * step, 2)) for i in range(n)]


# --------------------------------------------------------------------------
# OnChainRegimeStrategy
# --------------------------------------------------------------------------
def test_onchain_init_and_info():
    s = OnChainRegimeStrategy()
    assert s.get_strategy_info()["name"] == "OnChainRegimeStrategy"


def test_onchain_setup_mock_and_short():
    s = OnChainRegimeStrategy()
    s.setup(bars(40))
    assert s.on_bar(mk(100.0)) == (None, None)  # nvt_history < 30 guard


def test_onchain_with_nvt_data():
    s = OnChainRegimeStrategy()
    nvt = [OnChainMetrics(timestamp=i, nvt_ratio=40.0 + i) for i in range(40)]
    s.setup(bars(40), nvt_data=nvt)
    assert len(s.nvt_history) == 40


def test_onchain_buy_signal():
    s = OnChainRegimeStrategy()
    s.setup(bars(60))
    # history mostly high, last low -> current NVT near the low -> low percentile
    s.nvt_history = [200.0] * 59 + [10.0]
    s.whale_flow_buffer = [1e6] * 7
    sig = s.on_bar(mk(100.0))
    assert sig[0] is True


def test_onchain_sell_signal():
    s = OnChainRegimeStrategy()
    s.setup(bars(60))
    s.nvt_history = [10.0] * 59 + [200.0]
    s.whale_flow_buffer = [-1e6] * 7
    sig = s.on_bar(mk(100.0))
    assert sig[0] is False


def test_onchain_helpers():
    s = OnChainRegimeStrategy()
    s.setup(bars(60))
    assert "avg_nvt_50d" in s.get_nvt_signal()


# --------------------------------------------------------------------------
# WhaleFlowMomentumStrategy
# --------------------------------------------------------------------------
def test_whale_init_and_info():
    s = WhaleFlowMomentumStrategy()
    assert s.get_strategy_info()["name"] == "WhaleFlowMomentumStrategy"


def test_whale_short():
    s = WhaleFlowMomentumStrategy()
    s.setup(bars(10))
    assert s.on_bar(mk(100.0)) == (None, None)


def test_whale_buy():
    s = WhaleFlowMomentumStrategy()
    s.setup(bars(30))
    s._simulate_whale_transactions()
    for b in bars(15, start=100.0, step=2.0):
        s.on_bar(b)  # rising -> buy once flow momentum positive
    # force a clearly positive flow + rising bar to guarantee a buy
    s.exchange_flows = [5e7] * 30
    sig = s.on_bar(mk(120.0, open_=100.0))
    assert sig[0] is True


def test_whale_sell():
    s = WhaleFlowMomentumStrategy()
    s.setup(bars(30))
    s._simulate_whale_transactions()
    s.exchange_flows = [-5e7] * 30
    sig = s.on_bar(mk(80.0, open_=100.0))
    assert sig[0] is False


def test_whale_helpers():
    s = WhaleFlowMomentumStrategy()
    s.setup(bars(30))
    s._simulate_whale_transactions()
    assert "signal" in s.get_flow_analysis()


# --------------------------------------------------------------------------
# OrderFlowImbalanceStrategy
# --------------------------------------------------------------------------
def test_orderflow_init_and_info():
    s = OrderFlowImbalanceStrategy()
    assert s.get_strategy_info()["name"] == "OrderFlowImbalanceStrategy"


def test_orderflow_short():
    s = OrderFlowImbalanceStrategy()
    s.setup(bars(10))
    assert s.on_bar(mk(100.0)) == (None, None)


def test_orderflow_no_signal_tiny_imbalance():
    s = OrderFlowImbalanceStrategy()
    s.setup(bars(30))
    # feed bars; imbalance is normalised by huge depth so never breaches threshold
    for b in bars(40, start=100.0, step=1.0):
        s.on_bar(b)
    assert s.get_imbalance_analysis()["signal"] in ("buying_pressure", "selling_pressure")
    # depth estimation branches
    assert s._estimate_market_depth(mk(100.0, high=101.0, low=99.0))[0] > 0


# --------------------------------------------------------------------------
# CrossExchangeMicrostructureArb
# --------------------------------------------------------------------------
def test_cross_init_and_info():
    s = CrossExchangeMicrostructureArb()
    assert s.get_strategy_info()["name"] == "CrossExchangeMicrostructureArb"


def test_cross_no_prices_short():
    s = CrossExchangeMicrostructureArb()
    s.setup(bars(5))
    assert s.on_bar(mk(100.0)) == (None, None)


def test_cross_fee_profit_and_opportunity():
    s = CrossExchangeMicrostructureArb(min_profit_pct=0.0001)
    s.setup(bars(30))
    s._fetch_exchange_prices()
    # exercise fee-adjusted profit + arb loop
    assert s._calculate_fee_adjusted_profit("coinbase", "binance") >= 0.0
    s.on_bar(mk(100.0))
    opp = s.get_arb_opportunity()
    assert opp is None or "buy_exchange" in opp


# --------------------------------------------------------------------------
# SentimentRegimeDetector
# --------------------------------------------------------------------------
def test_sentiment_init_and_info():
    s = SentimentRegimeDetector()
    assert s.get_strategy_info()["name"] == "SentimentRegimeDetector"


def test_sentiment_short():
    s = SentimentRegimeDetector()
    s.setup(bars(10))
    assert s.on_bar(mk(100.0)) == (None, None)


def test_sentiment_buy(monkeypatch):
    s = SentimentRegimeDetector()
    s.setup(bars(30))
    s.sentiment_history = [-1.0] * 10 + [1.0] * 10
    monkeypatch.setattr(s, "_simulate_sentiment_score", lambda: -0.9)
    sig = s.on_bar(mk(120.0, open_=100.0))  # rising price
    assert sig[0] is True


def test_sentiment_sell(monkeypatch):
    s = SentimentRegimeDetector()
    s.setup(bars(30))
    s.sentiment_history = [-1.0] * 10 + [1.0] * 10
    monkeypatch.setattr(s, "_simulate_sentiment_score", lambda: 0.9)
    sig = s.on_bar(mk(100.0, open_=100.0))  # flat price
    assert sig[0] is False


def test_sentiment_helpers():
    s = SentimentRegimeDetector()
    s.setup(bars(30))
    s.sentiment_history = [-0.5] * 25
    assert "extremity_score" in s.get_sentiment_analysis()
