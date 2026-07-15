import time
from decimal import Decimal

from trading_system.execution.smart_execution.service import (
    TWAPOrder,
    TWAPSlice,
    IcebergOrder,
    PeggedOrder,
    SmartExecutionEngine,
    SmartOrderType,
)


def test_twap_post_init():
    o = TWAPOrder(total_size=Decimal("10"), slices=4, interval_seconds=10)
    assert o.remaining_slices == 4
    assert len(o.slices_data) == 4
    assert o.slices_data[0].size == Decimal("2.5")


def test_twap_next_slice_first():
    o = TWAPOrder(total_size=Decimal("10"), slices=2, interval_seconds=10)
    o.started_at = time.time()
    s = o.next_slice()
    assert s == Decimal("5")
    assert o.slices_data[0].executed
    assert o.remaining_slices == 1


def test_twap_next_slice_already_executed_returns_none():
    o = TWAPOrder(total_size=Decimal("10"), slices=2, interval_seconds=10)
    o.started_at = time.time()
    o.next_slice()
    assert o.next_slice() is None


def test_twap_next_slice_second():
    o = TWAPOrder(total_size=Decimal("10"), slices=2, interval_seconds=10)
    o.started_at = time.time() - 15
    assert o.next_slice() == Decimal("5")
    assert o.next_slice() == Decimal("5")


def test_twap_next_slice_all_consumed():
    o = TWAPOrder(total_size=Decimal("10"), slices=2, interval_seconds=10)
    o.started_at = time.time() - 25
    assert o.next_slice() is None


def test_twap_next_slice_future_start_negative_expected():
    o = TWAPOrder(total_size=Decimal("10"), slices=2, interval_seconds=10)
    o.started_at = time.time() + 100
    assert o.next_slice() is None


def test_twap_next_slice_without_start_sets_started_at():
    o = TWAPOrder(total_size=Decimal("10"), slices=2, interval_seconds=10)
    assert o.started_at == 0
    o.next_slice()
    assert o.started_at != 0


def test_iceberg_next_chunk():
    o = IcebergOrder(total_size=Decimal("10"), visible_size=Decimal("3"))
    assert o.next_chunk() == Decimal("3")
    o.update_filled(Decimal("3"))
    assert o.next_chunk() == Decimal("3")
    o.update_filled(Decimal("3"))
    assert o.next_chunk() == Decimal("3")
    o.update_filled(Decimal("3"))
    assert o.next_chunk() == Decimal("1")
    o.update_filled(Decimal("1"))
    assert o.next_chunk() is None


def test_iceberg_next_chunk_fully_filled():
    o = IcebergOrder(total_size=Decimal("5"), visible_size=Decimal("3"))
    o.update_filled(Decimal("5"))
    assert o.next_chunk() is None


def test_pegged_buy():
    p = PeggedOrder(product_id="BTC-USD", size=Decimal("1"), offset_bps=Decimal("10"),
                    side="buy")
    tp = p.target_price(Decimal("10000"))
    assert tp == Decimal("9990")
    assert p.last_price == Decimal("10000")
    assert p.last_placed_price == Decimal("9990")


def test_pegged_sell():
    p = PeggedOrder(product_id="BTC-USD", size=Decimal("1"), offset_bps=Decimal("10"),
                    side="sell")
    tp = p.target_price(Decimal("10000"))
    assert tp == Decimal("10010")


def test_smart_engine_create():
    e = SmartExecutionEngine()
    tw = e.create_twap("t1", Decimal("10"), 5, 2.0)
    assert isinstance(tw, TWAPOrder)
    ib = e.create_iceberg("i1", Decimal("20"), Decimal("5"))
    assert isinstance(ib, IcebergOrder)


def test_smart_order_type_enum():
    assert SmartOrderType.TWAP.value == "twap"
    assert SmartOrderType.ICBERG.value == "iceberg"
    assert SmartOrderType.PEGGED.value == "pegged"


def test_twap_slice_defaults():
    s = TWAPSlice(size=Decimal("1"))
    assert s.executed is False
