from trading_system.apps.api.main import runtime_status


def test_runtime_status_endpoint_shape(monkeypatch):
    monkeypatch.setenv("TRADING_MODE", "PAPER")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")

    data = runtime_status()

    assert data["mode"] in {"paper", "PAPER"}
    assert data["live_trading_enabled"] is False
    assert "coinbase_connected" in data
    assert "worker_status" in data
    assert "event_log_status" in data
    assert "timestamp" in data
