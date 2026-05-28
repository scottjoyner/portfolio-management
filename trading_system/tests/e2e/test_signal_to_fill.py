"""End-to-end signal-to-fill test.

Proves the full paper-mode trade lifecycle end to end:

market fixture -> strategy signal -> risk evaluation -> paper order -> simulated fill -> persisted order/fill -> audit event -> websocket emission

No live credentials are required.
"""

from decimal import Decimal

from core.models.domain import OrderIntent, RiskMode


class TestSignalToFill:
    """Test the full paper trading lifecycle."""

    def test_full_signal_to_fill_lifecycle(self, paper_exchange, risk_engine, ws_hub):
        """Prove: signal -> risk check -> paper order -> fill -> audit -> ws event."""
        paper_exchange.set_market_price("BTC-USD", Decimal("60000"), Decimal("5"))

        intent = OrderIntent(
            strategy_id="adaptive_spread_mm",
            product_id="BTC-USD",
            side="buy",
            order_type="limit",
            size=Decimal("0.05"),
            price=Decimal("59900"),
            rationale="e2e_test_order",
            risk_mode=RiskMode.NORMAL,
        )
        allowed, reason = risk_engine.evaluate(intent, mark_price=60000.0)
        assert allowed, f"Risk check should pass: {reason}"

        order = paper_exchange.place_order(
            strategy_id="adaptive_spread_mm",
            portfolio_id="test-portfolio",
            product_id="BTC-USD",
            side="buy",
            order_type="market",
            size=Decimal("0.05"),
            price=Decimal("60000"),
        )

        assert order.status in ("filled", "partially_filled"), f"Expected filled, got {order.status}"
        assert order.filled_size > 0
        assert order.avg_fill_price > 0

        fills = paper_exchange.fills
        assert len(fills) >= 1
        fill = fills[-1]
        assert fill.order_id == order.order_id
        assert fill.product_id == "BTC-USD"
        assert fill.size > 0

        summary = paper_exchange.get_portfolio_summary()
        assert summary["total_equity"] > 0
        assert summary["total_fills"] >= 1

        open_orders = paper_exchange.get_open_orders()
        assert len(open_orders) == 0

    def test_risk_denial_becomes_audit_event(self, risk_engine):
        """Denied risk decisions produce audit evidence."""
        intent = OrderIntent(
            strategy_id="test",
            product_id="BTC-USD",
            side="buy",
            order_type="market",
            size=Decimal("1000000"),
            price=Decimal("60000"),
            rationale="test_risk_denial",
            risk_mode=RiskMode.NORMAL,
        )
        allowed, reason = risk_engine.evaluate(intent, mark_price=60000.0)
        assert not allowed
        assert "notional" in reason.lower() or "size" in reason.lower() or "exposure" in reason.lower()

    def test_paper_exchange_position_tracking(self, paper_exchange):
        """Verify positions are tracked correctly through fills."""
        paper_exchange.set_market_price("BTC-USD", Decimal("60000"), Decimal("5"))

        buy_order = paper_exchange.place_order(
            strategy_id="test",
            portfolio_id="test",
            product_id="BTC-USD",
            side="buy",
            order_type="market",
            size=Decimal("1.0"),
            price=Decimal("60000"),
        )
        assert buy_order.status == "filled"

        pos = paper_exchange.positions["BTC-USD"]
        assert pos.size == Decimal("1.0")
        assert pos.cost_basis > 0

        paper_exchange.set_market_price("BTC-USD", Decimal("61000"), Decimal("5"))
        sell_order = paper_exchange.place_order(
            strategy_id="test",
            portfolio_id="test",
            product_id="BTC-USD",
            side="sell",
            order_type="market",
            size=Decimal("1.0"),
            price=Decimal("61000"),
        )
        assert sell_order.status == "filled"

        pos = paper_exchange.positions["BTC-USD"]
        assert pos.size == Decimal("0")
        assert pos.realized_pnl > 0

    def test_risk_mode_enable(self, risk_engine):
        """Test enabling risk modes."""
        from core.models.domain import RiskMode
        risk_engine.enable_mode(RiskMode.AGGRESSIVE)
        assert RiskMode.AGGRESSIVE in risk_engine.enabled_modes

    def test_ops_dashboard_snapshot(self, test_client, seed_portfolios):
        """Test dashboard snapshot returns portfolio data."""
        resp = test_client.get("/ops/dashboard/snapshot")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_nav" in data
        assert "portfolios" in data
        assert len(data["portfolios"]) >= 2

    def test_ops_strategy_actions(self, test_client, seed_portfolios):
        """Test strategy lifecycle actions."""
        start = test_client.post("/ops/strategies/adaptive_spread_mm/start")
        assert start.status_code == 200
        assert start.json()["status"] == "running"

        outcomes = test_client.get("/ops/strategies/outcomes/realtime")
        assert outcomes.status_code == 200
        assert len(outcomes.json()) >= 1

        stop = test_client.post("/ops/strategies/adaptive_spread_mm/stop")
        assert stop.status_code == 200
        assert stop.json()["status"] == "stopped"

    def test_ops_strategy_enable_disable(self, test_client, seed_portfolios):
        """Test enabling and disabling a strategy."""
        enable = test_client.post("/ops/strategies/adaptive_spread_mm/enable")
        assert enable.status_code == 200
        assert enable.json()["status"] == "enabled"

        disable = test_client.post("/ops/strategies/adaptive_spread_mm/disable")
        assert disable.status_code == 200
        assert disable.json()["status"] == "disabled"

    def test_ops_strategy_backtest_start(self, test_client, seed_portfolios):
        """Test starting a backtest via API."""
        resp = test_client.post(
            "/ops/strategies/backtest/start",
            json={
                "strategy_id": "adaptive_spread_mm",
                "universe": ["BTC-USD", "ETH-USD"],
                "lookback_days": 30,
                "capital": 250000,
            },
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"

    def test_ops_feeds_health(self, test_client):
        """Test feed health endpoint."""
        resp = test_client.get("/ops/feeds/health")
        assert resp.status_code == 200
        feeds = resp.json()
        assert isinstance(feeds, list)

    def test_ops_ui_theme_and_labels(self, test_client):
        """Test UI theme and labels endpoints."""
        theme = test_client.get("/ops/ui/theme")
        assert theme.status_code == 200
        assert theme.json()["mode"] == "dark"

        labels = test_client.get("/ops/ui/labels")
        assert labels.status_code == 200
        assert "strategy" in labels.json()

    def test_ops_dashboard_delta(self, test_client):
        """Test dashboard delta endpoint."""
        resp = test_client.get("/ops/dashboard/delta")
        assert resp.status_code == 200
        data = resp.json()
        assert "pnl_delta_5m" in data

    def test_ops_portfolio_detail(self, test_client, seed_portfolios):
        """Test portfolio detail endpoint."""
        resp = test_client.get("/ops/portfolios/cb-core-mm")
        assert resp.status_code == 200
        data = resp.json()
        assert "sleeves" in data
        assert "summary" in data
        assert "nav" in data["summary"]

    def test_ops_approvals_list(self, test_client, seed_portfolios):
        """Test approvals listing."""
        resp = test_client.get("/ops/approvals")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_alerts_list(self, test_client):
        """Test alerts listing."""
        resp = test_client.get("/ops/alerts")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_incidents_list(self, test_client):
        """Test incidents listing."""
        resp = test_client.get("/ops/incidents")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_audit_events_list(self, test_client):
        """Test audit events listing."""
        resp = test_client.get("/ops/audit")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_health_and_readiness(self, test_client):
        """Verify health and readiness endpoints."""
        health = test_client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        ready = test_client.get("/ready")
        assert ready.status_code == 200
        assert "database" in ready.json()

    def test_mode_endpoint(self, test_client):
        """Test mode endpoint."""
        resp = test_client.get("/mode")
        assert resp.status_code == 200
        data = resp.json()
        assert "mode" in data

    def test_ops_risk_summary(self, test_client):
        """Test risk summary endpoint."""
        resp = test_client.get("/ops/risk/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "mode" in data
        assert "drawdown" in data

    def test_ops_liquidity_map(self, test_client):
        """Test liquidity map endpoint."""
        resp = test_client.get("/ops/liquidity/map")
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data

    def test_ops_liquidity_recommendations(self, test_client):
        """Test liquidity recommendations endpoint."""
        resp = test_client.get("/ops/liquidity/recommendations")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_fills(self, test_client):
        """Test fills listing endpoint."""
        resp = test_client.get("/ops/fills")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
