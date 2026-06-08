from trading_system.apps.ui import main


def test_dashboard_html_contains_core_sections(monkeypatch):
    def fake_fetch(path):
        payloads = {
            "/runtime/status": {"mode": "paper", "live_trading_enabled": False, "coinbase_connected": True},
            "/coinbase/status": {"connected": True, "account_count": 16},
            "/coinbase/balances": {"accounts": [{"currency": "USDC", "available": "100", "hold": "0"}]},
            "/strategies/catalog": {"strategies": [{"name": "triplema", "category": "trend_following", "status": "production"}]},
            "/events?limit=20": {"events": [{"timestamp": "now", "source": "worker", "event_type": "strategy_tick", "payload": {}}]},
        }
        return payloads[path]

    monkeypatch.setattr(main, "fetch_json", fake_fetch)

    response = main.dashboard()
    html = response.body.decode()

    assert "Trading Network Dashboard" in html
    assert "Runtime Status" in html
    assert "Coinbase Account" in html
    assert "Strategy Catalog" in html
    assert "Recent Events" in html
    assert "USDC" in html
    assert "triplema" in html


def test_dashboard_health():
    assert main.health() == {"status": "ok", "service": "trading-ui"}
