from execution.order_manager.service import OrderManager, TrackedOrder, OrderStatus, OrderSide, OrderType
from execution.trade_lifecycle.service import TradeLifecycleManager, TradeRecord, TradeState


def test_order_manager_create():
    om = OrderManager()
    order = TrackedOrder(order_id="test-1", product_id="BTC-USD", side=OrderSide.BUY, order_type=OrderType.LIMIT, size=1.0, price=50000.0)
    om.add_order(order)
    assert om.get_order("test-1") is not None


def test_order_manager_update_status():
    om = OrderManager()
    order = TrackedOrder(order_id="test-2", product_id="BTC-USD", side=OrderSide.SELL, order_type=OrderType.MARKET, size=0.5)
    om.add_order(order)
    om.update_status("test-2", OrderStatus.OPEN)
    assert om.get_order("test-2").status == OrderStatus.OPEN


def test_order_manager_fill():
    om = OrderManager()
    order = TrackedOrder(order_id="test-3", product_id="ETH-USD", side=OrderSide.BUY, order_type=OrderType.LIMIT, size=2.0, price=3000.0)
    om.add_order(order)
    om.update_fill("test-3", 1.0, 3010.0)
    assert om.get_order("test-3").filled_size == 1.0
    assert om.get_order("test-3").avg_fill_price == 3010.0


def test_order_manager_full_fill():
    om = OrderManager()
    order = TrackedOrder(order_id="test-4", product_id="ETH-USD", side=OrderSide.BUY, order_type=OrderType.LIMIT, size=1.0, price=3000.0)
    om.add_order(order)
    om.update_fill("test-4", 1.0, 3005.0)
    assert om.get_order("test-4").status == OrderStatus.FILLED


def test_order_manager_cancel():
    om = OrderManager()
    order = TrackedOrder(order_id="test-5", product_id="BTC-USD", side=OrderSide.BUY, order_type=OrderType.LIMIT, size=1.0, price=50000.0)
    om.add_order(order)
    om.cancel_order("test-5")
    assert om.get_order("test-5").status == OrderStatus.CANCELLED


def test_trade_lifecycle():
    tlm = TradeLifecycleManager()
    record = TradeRecord(trade_id="trade-1", signal_id="sig-1", strategy_id="strat-1", product_id="BTC-USD", side="buy", size=1.0, price=50000.0)
    tlm.start_trade(record)
    assert tlm.get_trade("trade-1").state == TradeState.SIGNAL_RECEIVED


def test_trade_lifecycle_full():
    tlm = TradeLifecycleManager()
    record = TradeRecord(trade_id="trade-2", signal_id="sig-2", strategy_id="strat-2", product_id="ETH-USD", side="sell", size=10.0, price=3000.0)
    tlm.start_trade(record)
    tlm.mark_risk_checked("trade-2", True)
    tlm.mark_placed("trade-2", "exch-1")
    tlm.mark_filled("trade-2", 3010.0, 10.0)
    trade = tlm.get_trade("trade-2")
    assert trade.state == TradeState.FILLED
    assert trade.fill_price == 3010.0


def test_trade_lifecycle_risk_fail():
    tlm = TradeLifecycleManager()
    record = TradeRecord(trade_id="trade-3", signal_id="sig-3", strategy_id="strat-3", product_id="BTC-USD", side="buy", size=1.0, price=50000.0)
    tlm.start_trade(record)
    tlm.mark_risk_checked("trade-3", False)
    assert tlm.get_trade("trade-3").state == TradeState.FAILED


def test_active_trades():
    tlm = TradeLifecycleManager()
    t1 = TradeRecord(trade_id="t1", signal_id="s1", strategy_id="st1", product_id="BTC-USD", side="buy", size=1.0, price=50000.0)
    t2 = TradeRecord(trade_id="t2", signal_id="s2", strategy_id="st2", product_id="ETH-USD", side="sell", size=2.0, price=3000.0)
    tlm.start_trade(t1)
    tlm.start_trade(t2)
    tlm.mark_settled("t2", 100.0)
    active = tlm.active_trades()
    assert len(active) == 1
    assert active[0].trade_id == "t1"
