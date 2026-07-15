from trading_system.execution.trade_lifecycle.service import (
    TradeLifecycleManager,
    TradeRecord,
    TradeState,
)


def _record(tid="t1", state=TradeState.SIGNAL_RECEIVED):
    return TradeRecord(
        trade_id=tid,
        signal_id="s1",
        strategy_id="strat",
        product_id="BTC-USD",
        side="buy",
        size=1.0,
        price=100.0,
        state=state,
    )


def test_start_trade():
    m = TradeLifecycleManager()
    r = m.start_trade(_record())
    assert r.state == TradeState.SIGNAL_RECEIVED
    assert len(r.events) == 1
    assert m.get_trade("t1") is r


def test_get_missing():
    m = TradeLifecycleManager()
    assert m.get_trade("nope") is None


def test_mark_risk_checked_pass():
    m = TradeLifecycleManager()
    m.start_trade(_record())
    m.mark_risk_checked("t1", True)
    assert m.get_trade("t1").state == TradeState.RISK_CHECKED


def test_mark_risk_checked_fail():
    m = TradeLifecycleManager()
    m.start_trade(_record())
    m.mark_risk_checked("t1", False)
    assert m.get_trade("t1").state == TradeState.FAILED


def test_mark_risk_checked_missing():
    m = TradeLifecycleManager()
    m.mark_risk_checked("nope", True)


def test_mark_placed():
    m = TradeLifecycleManager()
    m.start_trade(_record())
    m.mark_placed("t1", "ex123")
    assert m.get_trade("t1").state == TradeState.ORDER_PLACED


def test_mark_placed_missing():
    m = TradeLifecycleManager()
    m.mark_placed("nope", "x")


def test_mark_filled():
    m = TradeLifecycleManager()
    m.start_trade(_record())
    m.mark_filled("t1", 101.0, 1.0)
    r = m.get_trade("t1")
    assert r.state == TradeState.FILLED
    assert r.fill_price == 101.0
    assert r.filled_size == 1.0


def test_mark_filled_missing():
    m = TradeLifecycleManager()
    m.mark_filled("nope", 1.0, 1.0)


def test_mark_settled():
    m = TradeLifecycleManager()
    m.start_trade(_record())
    m.mark_settled("t1", 5.0)
    r = m.get_trade("t1")
    assert r.state == TradeState.SETTLED
    assert r.pnl == 5.0


def test_mark_settled_missing():
    m = TradeLifecycleManager()
    m.mark_settled("nope", 0.0)


def test_mark_failed():
    m = TradeLifecycleManager()
    m.start_trade(_record())
    m.mark_failed("t1", "boom")
    assert m.get_trade("t1").state == TradeState.FAILED


def test_mark_failed_missing():
    m = TradeLifecycleManager()
    m.mark_failed("nope", "boom")


def test_active_trades_filter():
    m = TradeLifecycleManager()
    m.start_trade(_record(tid="a"))
    m.start_trade(_record(tid="b"))
    m.start_trade(_record(tid="c"))
    m.mark_settled("c", 1.0)
    m.mark_failed("a", "x")
    m.get_trade("b").state = TradeState.FILLED
    active = {t.trade_id for t in m.active_trades()}
    assert active == {"b"}
