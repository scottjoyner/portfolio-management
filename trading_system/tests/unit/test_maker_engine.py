from execution.maker_engine.engine import MakerConfig, MakerQuoteEngine, MakerState
from market_data.microstructure.features import TopOfBook, ToxicFlowEstimator, TradePrint


def test_maker_inventory_drift_notional_signals_imbalance():
    engine = MakerQuoteEngine(MakerConfig(inventory_target=0.0))
    book = TopOfBook(100.0, 2.0, 100.1, 2.0)
    pos_state = MakerState(inventory=1.0)
    neg_state = MakerState(inventory=-1.0)
    assert engine.inventory_drift(pos_state, book) > 0
    assert engine.inventory_drift(neg_state, book) < 0


def test_toxic_flow_accumulates_and_triggers_proxy():
    est = ToxicFlowEstimator(bucket_volume=2.0)
    v1 = est.update(TradePrint(side="BUY", size=1.0, price=100.0))
    v2 = est.update(TradePrint(side="BUY", size=1.0, price=100.1))
    assert v1 == 0.0
    assert v2 >= 0.9


def test_quote_fade_timing_and_toxic_conditions():
    cfg = MakerConfig(fade_after_ms=250, toxic_flow_threshold=0.6)
    engine = MakerQuoteEngine(cfg)
    state_old = MakerState(quote_age_ms=300)
    state_new = MakerState(quote_age_ms=100)
    assert engine.should_fade_quotes(state_old, toxic_flow=0.2, microprice_drift_bps=0.1)
    assert engine.should_fade_quotes(state_new, toxic_flow=0.7, microprice_drift_bps=0.1)
    assert not engine.should_fade_quotes(state_new, toxic_flow=0.2, microprice_drift_bps=0.1)


def test_cancel_replace_pressure_saturates():
    cfg = MakerConfig(max_cancel_replace_per_sec=20)
    engine = MakerQuoteEngine(cfg)
    low = engine.cancel_replace_pressure(MakerState(cancel_replace_count=5))
    high = engine.cancel_replace_pressure(MakerState(cancel_replace_count=40))
    assert 0 < low < 1
    assert high == 1.0
