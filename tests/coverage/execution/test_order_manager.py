from datetime import datetime, timezone

from trading_system.execution.order_manager.service import (
    OrderManager,
    TrackedOrder,
    OrderStatus,
    OrderSide,
    OrderType,
)


def _order(oid="o1", product="BTC-USD", status=OrderStatus.PENDING, size=10.0):
    return TrackedOrder(
        order_id=oid,
        product_id=product,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        size=size,
        status=status,
    )


def test_add_and_get():
    m = OrderManager()
    o = _order()
    m.add_order(o)
    assert m.get_order("o1") is o


def test_get_missing():
    m = OrderManager()
    assert m.get_order("nope") is None


def test_update_status_found_and_missing():
    m = OrderManager()
    m.add_order(_order())
    m.update_status("o1", OrderStatus.OPEN)
    assert m.get_order("o1").status == OrderStatus.OPEN
    # missing
    m.update_status("nope", OrderStatus.OPEN)


def test_update_fill_first_fill():
    m = OrderManager()
    m.add_order(_order(size=10.0))
    m.update_fill("o1", 4.0, 100.0)
    o = m.get_order("o1")
    assert o.filled_size == 4.0
    assert o.avg_fill_price == 100.0
    assert o.status == OrderStatus.PARTIALLY_FILLED


def test_update_fill_full_fill():
    m = OrderManager()
    m.add_order(_order(size=10.0))
    m.update_fill("o1", 10.0, 50.0)
    o = m.get_order("o1")
    assert o.filled_size == 10.0
    assert o.status == OrderStatus.FILLED


def test_update_fill_subsequent():
    m = OrderManager()
    m.add_order(_order(size=10.0))
    m.update_fill("o1", 4.0, 100.0)
    m.update_fill("o1", 4.0, 200.0)
    o = m.get_order("o1")
    # avg = (100*4 + 200*4)/8 = 150
    assert o.avg_fill_price == 150.0
    assert o.status == OrderStatus.PARTIALLY_FILLED


def test_update_fill_zero_filled_branch():
    m = OrderManager()
    m.add_order(_order(size=10.0))
    m.update_fill("o1", 0.0, 123.0)
    o = m.get_order("o1")
    assert o.filled_size == 0.0
    assert o.avg_fill_price == 123.0


def test_update_fill_missing():
    m = OrderManager()
    m.update_fill("nope", 1.0, 10.0)


def test_cancel_pending():
    m = OrderManager()
    m.add_order(_order(status=OrderStatus.PENDING))
    m.cancel_order("o1")
    assert m.get_order("o1").status == OrderStatus.CANCELLED


def test_cancel_open():
    m = OrderManager()
    m.add_order(_order(status=OrderStatus.OPEN))
    m.cancel_order("o1")
    assert m.get_order("o1").status == OrderStatus.CANCELLED


def test_cancel_filled_skipped():
    m = OrderManager()
    m.add_order(_order(status=OrderStatus.FILLED))
    m.cancel_order("o1")
    assert m.get_order("o1").status == OrderStatus.FILLED


def test_cancel_missing():
    m = OrderManager()
    m.cancel_order("nope")


def test_open_orders_includes_and_excludes():
    m = OrderManager()
    m.add_order(_order(oid="a", status=OrderStatus.OPEN))
    m.add_order(_order(oid="b", status=OrderStatus.FILLED))
    m.add_order(_order(oid="c", status=OrderStatus.PARTIALLY_FILLED))
    m.add_order(_order(oid="d", status=OrderStatus.CANCELLED))
    ids = {o.order_id for o in m.open_orders()}
    assert ids == {"a", "c"}


def test_orders_for_product():
    m = OrderManager()
    m.add_order(_order(oid="a", product="BTC-USD"))
    m.add_order(_order(oid="b", product="ETH-USD"))
    assert {o.order_id for o in m.orders_for_product("BTC-USD")} == {"a"}
    assert m.orders_for_product("ZZZ") == []


def test_tracked_order_defaults():
    o = TrackedOrder(order_id="x", product_id="BTC-USD", side=OrderSide.BUY,
                     order_type=OrderType.LIMIT, size=1.0)
    assert o.price is None
    assert o.stop_price is None
    assert o.status == OrderStatus.PENDING
    assert isinstance(o.created_at, datetime)
    assert o.metadata == {}
