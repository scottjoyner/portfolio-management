"""Extended edge-case tests for the four hybrid/maker target modules.

These exercise boundary and negative-input branches on top of the existing
coverage so every public function and branch is exercised explicitly.
"""

from __future__ import annotations

from decimal import Decimal

from market_data.microstructure.features import TopOfBook

from trading_system.execution.hybrid.exposure_sync import validate_hybrid_exposure
from trading_system.execution.hybrid.sequencing import classify_hybrid_atomicity
from trading_system.execution.hybrid.unwind_planner import (
    plan_unwind_sequence,
    rollback_after_failed_hedge,
)
from trading_system.execution.maker_engine.engine import (
    MakerConfig,
    MakerQuoteEngine,
    MakerState,
    QuoteLevel,
)


# --- exposure_sync ---------------------------------------------------------

def test_exposure_cex_negative():
    snap = validate_hybrid_exposure(Decimal("1000"), Decimal("-200"))
    assert snap.net_delta_usd == Decimal("800")
    assert snap.hedge_coverage_ratio == Decimal("0.2")


def test_exposure_both_negative():
    snap = validate_hybrid_exposure(Decimal("-100"), Decimal("-500"))
    # 500/100 = 5 -> capped at 2
    assert snap.net_delta_usd == Decimal("-600")
    assert snap.hedge_coverage_ratio == Decimal("2")


def test_exposure_onchain_negative_ratio():
    snap = validate_hybrid_exposure(Decimal("-1000"), Decimal("200"))
    assert snap.hedge_coverage_ratio == Decimal("0.2")


def test_exposure_ratio_exactly_two():
    snap = validate_hybrid_exposure(Decimal("100"), Decimal("200"))
    assert snap.hedge_coverage_ratio == Decimal("2")


def test_exposure_zero_cex():
    snap = validate_hybrid_exposure(Decimal("100"), Decimal("0"))
    assert snap.hedge_coverage_ratio == Decimal("0")
    assert snap.net_delta_usd == Decimal("100")


# --- sequencing ------------------------------------------------------------

def test_sequencing_boundary_strong():
    # joint exactly 0.8 is NOT > 0.8
    assert classify_hybrid_atomicity(Decimal("0.8"), Decimal("1.0")) == "SEMI_ATOMIC_MODERATE"
    assert classify_hybrid_atomicity(Decimal("0.81"), Decimal("1.0")) == "SEMI_ATOMIC_STRONG"


def test_sequencing_boundary_moderate():
    # joint exactly 0.5 is NOT > 0.5
    assert classify_hybrid_atomicity(Decimal("0.5"), Decimal("1.0")) == "NON_ATOMIC"
    assert classify_hybrid_atomicity(Decimal("0.51"), Decimal("1.0")) == "SEMI_ATOMIC_MODERATE"


def test_sequencing_zero():
    assert classify_hybrid_atomicity(Decimal("0"), Decimal("0")) == "NON_ATOMIC"


def test_sequencing_negative():
    assert classify_hybrid_atomicity(Decimal("-1"), Decimal("1")) == "NON_ATOMIC"


# --- unwind_planner --------------------------------------------------------

def test_plan_cex_first_default():
    seq = plan_unwind_sequence(prefer_cex_first=True)
    assert seq[0] == "close_cex_hedge"
    assert seq[-1] == "remove_onchain_liquidity"


def test_plan_onchain_first_default():
    seq = plan_unwind_sequence(prefer_cex_first=False)
    assert seq[0] == "remove_onchain_liquidity"


def test_rollback_length():
    assert len(rollback_after_failed_hedge()) == 3


# --- maker engine ----------------------------------------------------------

class _FakeQueueModel:
    def estimate(self, **kwargs):
        from execution.queue_model.models import QueueEstimate

        return QueueEstimate(
            fill_probability=0.9,
            expected_queue_time_ms=1.0,
            expected_fill_size=1.0,
            adverse_selection_bps=10.0,
            stale_quote_decay=0.5,
        )


def test_default_config_values():
    cfg = MakerConfig()
    assert cfg.levels == 4
    assert cfg.base_order_size == 0.01
    assert cfg.min_spread_bps == 2.0
    assert cfg.max_spread_bps == 30.0
    assert cfg.inventory_skew_per_unit_bps == 3.0
    assert cfg.toxic_flow_threshold == 0.65
    assert cfg.fade_after_ms == 400.0
    assert cfg.max_cancel_replace_per_sec == 25


def test_dataclass_instances():
    q = QuoteLevel(side="BUY", price=100.0, size=1.0, level=1)
    assert q.side == "BUY" and q.level == 1
    s = MakerState()
    assert s.inventory == 0.0 and s.cancel_replace_count == 0 and s.quote_age_ms == 0.0


def test_build_ladder_custom_queue_model():
    e = MakerQuoteEngine(MakerConfig(levels=3), queue_model=_FakeQueueModel())
    quotes, queue = e.build_ladder(
        _book(), _state(inventory=0.0), 5.0, 0.1, 1.0, 10.0, 5.0
    )
    assert len(quotes) == 6
    assert queue.fill_probability == 0.9


def test_build_ladder_negative_inventory_skew():
    e = MakerQuoteEngine(MakerConfig(levels=2, inventory_skew_per_unit_bps=5.0))
    mid = 100.0
    quotes_neutral, _ = e.build_ladder(
        _book(bid_px=mid, ask_px=mid + 1), _state(inventory=0.0), 1.0, 0.0, 1.0, 1.0, 1.0
    )
    quotes_neg, _ = e.build_ladder(
        _book(bid_px=mid, ask_px=mid + 1), _state(inventory=-4.0), 1.0, 0.0, 1.0, 1.0, 1.0
    )
    buys_neutral = [q for q in quotes_neutral if q.side == "BUY"]
    buys_neg = [q for q in quotes_neg if q.side == "BUY"]
    sells_neutral = [q for q in quotes_neutral if q.side == "SELL"]
    sells_neg = [q for q in quotes_neg if q.side == "SELL"]
    # negative inventory -> bids (and the whole ladder) shifted upward vs neutral
    assert buys_neg[0].price > buys_neutral[0].price
    assert sells_neg[0].price > sells_neutral[0].price
    assert len(buys_neg) == 2 and len(sells_neg) == 2


def test_build_ladder_size_floor():
    e = MakerQuoteEngine(MakerConfig(base_order_size=0.0), queue_model=_FakeQueueModel())
    quotes, _ = e.build_ladder(_book(), _state(), 0.0, 0.0, 1.0, 1.0, 1.0)
    # size floored to 0.0001
    assert all(q.size == 0.0001 for q in quotes)


def test_dynamic_spread_exact_min():
    e = MakerQuoteEngine(MakerConfig(min_spread_bps=2.0))
    assert e._dynamic_spread_bps(0.0, 0.0) == 2.0


def test_should_fade_quotes_all_conditions_false_via_branch():
    e = MakerQuoteEngine(MakerConfig(fade_after_ms=400.0, toxic_flow_threshold=0.65,
                                     min_spread_bps=2.0))
    # age just below, toxic just below, drift just below
    assert e.should_fade_quotes(_state(quote_age_ms=399.0), 0.64, 3.99) is False


def _book(bid_px=100.0, bid_sz=1.0, ask_px=101.0, ask_sz=2.0):
    return TopOfBook(bid_px=bid_px, bid_sz=bid_sz, ask_px=ask_px, ask_sz=ask_sz)


def _state(inventory=0.0, cancel_replace_count=0, quote_age_ms=0.0):
    return MakerState(inventory=inventory, cancel_replace_count=cancel_replace_count,
                      quote_age_ms=quote_age_ms)
