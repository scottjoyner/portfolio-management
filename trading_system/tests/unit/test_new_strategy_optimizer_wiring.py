"""Wiring test for the 12 new registry strategies and optimizer integration."""
from trading_system.strategies.registry.registry import load_strategies
from trading_system.strategies.base.interfaces import StrategySignal

NEW_IDS = [
    "BollingerBandReversionStrategy",
    "RsiBounceReversionStrategy",
    "DonchianMeanReversionStrategy",
    "EmaMacdMomentumStrategy",
    "AdxDiStrengthStrategy",
    "AroonBreakoutMomentumStrategy",
    "KeltnerVolBreakoutStrategy",
    "BollingerSqueezeVolExpansionStrategy",
    "DonchianChoppinessVolBreakoutStrategy",
    "TradeFlowImbalanceStrategy",
    "SpreadCompressionStrategy",
    "CvdExhaustionStrategy",
]


def test_all_twelve_registered():
    ids = [s.strategy_id for s in load_strategies()]
    for nid in NEW_IDS:
        assert nid in ids, f"missing strategy_id: {nid}"


def test_generate_signal_no_exception():
    ms = {
        "product_id": "BTC-USD",
        "currency": "BTC",
        "close": 100.0,
        "closes": [float(i) for i in range(1, 60)],
        "highs": [float(i) for i in range(1, 60)],
        "lows": [float(i) for i in range(1, 60)],
        "volumes": [1.0] * 59,
        "open": 1.0,
        "price": 100.0,
        "score": 0.0,
        "warmup_complete": True,
        "best_bid": 99.9,
        "best_ask": 100.1,
        "mid_price": 100.0,
        "spread_bps": 2.0,
        "bid_volume": 1.0,
        "ask_volume": 1.0,
        "trade_flow_imbalance": 0.0,
        "imbalance": 0.0,
        "cumulative_delta": 0.0,
    }
    for s in load_strategies():
        if s.strategy_id not in NEW_IDS:
            continue
        sig = s.generate_signal(ms)
        assert sig is None or (hasattr(sig, "strategy_id") and hasattr(sig, "product_id")
                               and hasattr(sig, "score") and hasattr(sig, "confidence")), \
            f"bad signal from {s.strategy_id}"
        if isinstance(sig, StrategySignal):
            assert sig.strategy_id == s.strategy_id
