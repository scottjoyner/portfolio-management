from market_data.microstructure.features import TopOfBook

from trading_system.execution.maker_engine.engine import (
    MakerQuoteEngine,
    MakerConfig,
    MakerState,
    QuoteLevel,
)


def _book(bid_px=100.0, bid_sz=1.0, ask_px=101.0, ask_sz=2.0):
    return TopOfBook(bid_px=bid_px, bid_sz=bid_sz, ask_px=ask_px, ask_sz=ask_sz)


def _state(inventory=0.0, cancel_replace_count=0, quote_age_ms=0.0):
    return MakerState(inventory=inventory, cancel_replace_count=cancel_replace_count,
                      quote_age_ms=quote_age_ms)


def test_dynamic_spread_bps_clamped_low():
    e = MakerQuoteEngine(MakerConfig())
    spread = e._dynamic_spread_bps(0.0, 0.0)
    assert spread == 2.0


def test_dynamic_spread_bps_high_volatility_and_toxic():
    e = MakerQuoteEngine(MakerConfig())
    spread = e._dynamic_spread_bps(100.0, 1.0)
    assert spread > 2.0


def test_dynamic_spread_bps_capped():
    e = MakerQuoteEngine(MakerConfig())
    spread = e._dynamic_spread_bps(100000.0, 1.0)
    assert spread == 30.0


def test_inventory_skew_bps():
    e = MakerQuoteEngine(MakerConfig(inventory_target=0.0, inventory_skew_per_unit_bps=3.0))
    assert e._inventory_skew_bps(1.0) == 3.0
    assert e._inventory_skew_bps(-2.0) == -6.0


def test_build_ladder():
    e = MakerQuoteEngine(MakerConfig(levels=4, base_order_size=0.01))
    quotes, queue = e.build_ladder(_book(), _state(), 5.0, 0.1, 1.0, 10.0, 5.0)
    assert len(quotes) == 8
    assert all(isinstance(q, QuoteLevel) for q in quotes)
    buys = [q for q in quotes if q.side == "BUY"]
    sells = [q for q in quotes if q.side == "SELL"]
    assert len(buys) == 4 and len(sells) == 4
    # deeper levels further from mid
    assert buys[0].price > buys[3].price
    assert sells[0].price < sells[3].price
    assert queue.fill_probability >= 0.0


def test_build_ladder_zero_levels():
    e = MakerQuoteEngine(MakerConfig(levels=0))
    quotes, _ = e.build_ladder(_book(), _state(), 5.0, 0.1, 1.0, 10.0, 5.0)
    assert quotes == []


def test_should_fade_quotes_age():
    e = MakerQuoteEngine(MakerConfig(fade_after_ms=400.0))
    assert e.should_fade_quotes(_state(quote_age_ms=500.0), 0.0, 0.0) is True


def test_should_fade_quotes_toxic():
    e = MakerQuoteEngine(MakerConfig(toxic_flow_threshold=0.65))
    assert e.should_fade_quotes(_state(), 0.9, 0.0) is True


def test_should_fade_quotes_drift():
    e = MakerQuoteEngine(MakerConfig(min_spread_bps=2.0))
    assert e.should_fade_quotes(_state(), 0.0, 5.0) is True


def test_should_fade_quotes_false():
    e = MakerQuoteEngine(MakerConfig(fade_after_ms=400.0, toxic_flow_threshold=0.65,
                                     min_spread_bps=2.0))
    assert e.should_fade_quotes(_state(quote_age_ms=100.0), 0.1, 1.0) is False


def test_cancel_replace_pressure():
    e = MakerQuoteEngine(MakerConfig(max_cancel_replace_per_sec=25))
    assert e.cancel_replace_pressure(_state(cancel_replace_count=25)) == 1.0
    assert e.cancel_replace_pressure(_state(cancel_replace_count=12)) == 12 / 25


def test_cancel_replace_pressure_zero_cfg():
    e = MakerQuoteEngine(MakerConfig(max_cancel_replace_per_sec=0))
    assert e.cancel_replace_pressure(_state(cancel_replace_count=5)) == 1.0


def test_inventory_drift_positive_microprice():
    e = MakerQuoteEngine(MakerConfig(inventory_target=0.0))
    drift = e.inventory_drift(_state(inventory=2.0), _book())
    assert drift > 0.0


def test_inventory_drift_zero_microprice():
    e = MakerQuoteEngine(MakerConfig(inventory_target=0.0))
    book = TopOfBook(bid_px=0.0, bid_sz=0.0, ask_px=0.0, ask_sz=0.0)
    assert e.inventory_drift(_state(inventory=2.0), book) == 0.0


def test_default_queue_model():
    e = MakerQuoteEngine(MakerConfig())
    assert e.queue_model is not None
