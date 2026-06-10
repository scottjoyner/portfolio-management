from trading_system.core.runtime.models import (
    AccountSnapshot,
    ExecutionStatus,
    OrderIntent,
    RuntimeStatus,
    StrategyStatus,
    TradingEvent,
)


def test_strategy_status_serializes_to_dict():
    status = StrategyStatus(strategy_id="triplema", name="Triple MA", category="trend_following")

    data = status.to_dict()

    assert data["strategy_id"] == "triplema"
    assert data["name"] == "Triple MA"
    assert data["category"] == "trend_following"
    assert data["enabled"] is False
    assert data["mode"] == "paper"


def test_trading_event_has_required_fields_and_serializes_payload():
    event = TradingEvent(source="worker", event_type="strategy_tick", payload={"product_id": "BTC-USD"})

    data = event.to_dict()

    assert data["timestamp"]
    assert data["source"] == "worker"
    assert data["event_type"] == "strategy_tick"
    assert data["payload"] == {"product_id": "BTC-USD"}


def test_order_intent_requires_product_side_and_sizing():
    intent = OrderIntent(
        strategy_id="manual",
        product_id="BTC-USD",
        side="BUY",
        order_type="market",
        quote_size=100.0,
    )

    data = intent.to_dict()

    assert data["strategy_id"] == "manual"
    assert data["product_id"] == "BTC-USD"
    assert data["side"] == "BUY"
    assert data["quote_size"] == 100.0


def test_order_intent_rejects_missing_sizing():
    try:
        OrderIntent(strategy_id="manual", product_id="BTC-USD", side="BUY", order_type="market")
    except ValueError as exc:
        assert "quote_size or base_size" in str(exc)
    else:
        raise AssertionError("OrderIntent should require quote_size or base_size")


def test_runtime_status_reports_control_plane_state():
    status = RuntimeStatus(
        mode="paper",
        live_trading_enabled=False,
        coinbase_connected=True,
        worker_status="running",
    )

    data = status.to_dict()

    assert data["mode"] == "paper"
    assert data["live_trading_enabled"] is False
    assert data["coinbase_connected"] is True
    assert data["worker_status"] == "running"


def test_account_snapshot_and_execution_status_shapes():
    snapshot = AccountSnapshot(accounts=[{"currency": "USDC", "available": "100"}])
    execution = ExecutionStatus(
        order_id="local-1",
        client_order_id="client-1",
        strategy_id="manual",
        product_id="BTC-USD",
        side="BUY",
        order_type="market",
        status="previewed",
    )

    assert snapshot.to_dict()["account_count"] == 1
    assert execution.to_dict()["client_order_id"] == "client-1"
