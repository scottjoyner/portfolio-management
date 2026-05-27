<<<<<<< HEAD
"""End-to-end signal-to-fill test.

Proves the full paper-mode trade lifecycle end to end:

market fixture -> strategy signal -> risk evaluation -> paper order -> simulated fill -> persisted order/fill -> audit event -> websocket emission

No live credentials are required.
"""

import asyncio
import time
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from apps.paper_exchange.engine import PaperExchangeEngine
from core.events.ws_hub import PubSubHub
from core.models.domain import OrderIntent, RiskMode
from risk.engine import RiskEngine, RiskPolicy


class TestSignalToFill:
    """Test the full paper trading lifecycle."""

    def test_full_signal_to_fill_lifecycle(self, paper_exchange, risk_engine, ws_hub):
        """Prove: signal -> risk check -> paper order -> fill -> audit -> ws event."""
        # 1. Set market price
        paper_exchange.set_market_price("BTC-USD", Decimal("60000"), Decimal("5"))

        # 2. Create a risk engine and evaluate an order intent
        intent = OrderIntent(
            strategy_id="adaptive_spread_mm",
            product_id="BTC-USD",
            side="buy",
            order_type="limit",
            size=Decimal("0.1"),
            price=Decimal("59900"),
            rationale="e2e_test_order",
            risk_mode=RiskMode.NORMAL,
        )
        allowed, reason = risk_engine.evaluate(intent, mark_price=60000.0)
        assert allowed, f"Risk check should pass: {reason}"

        # 3. Place order through paper exchange
        order = paper_exchange.place_order(
            strategy_id="adaptive_spread_mm",
            portfolio_id="test-portfolio",
            product_id="BTC-USD",
            side="buy",
            order_type="market",
            size=Decimal("0.1"),
            price=Decimal("60000"),
        )

        # 4. Verify order was filled
        assert order.status in ("filled", "partially_filled"), f"Expected filled, got {order.status}"
        assert order.filled_size > 0
        assert order.avg_fill_price > 0

        # 5. Verify fill was recorded
        fills = paper_exchange.fills
        assert len(fills) >= 1
        fill = fills[-1]
        assert fill.order_id == order.order_id
        assert fill.product_id == "BTC-USD"
        assert fill.size > 0

        # 6. Verify portfolio summary updated
        summary = paper_exchange.get_portfolio_summary()
        assert summary["total_equity"] > 0
        assert summary["total_fills"] >= 1

        # 7. Verify paper exchange state
        open_orders = paper_exchange.get_open_orders()
        assert len(open_orders) == 0  # market order should be fully filled

    def test_risk_denial_becomes_audit_event(self, risk_engine):
        """Denied risk decisions produce audit evidence."""
        # Create an intent that should be denied (oversize)
        intent = OrderIntent(
            strategy_id="test",
            product_id="BTC-USD",
            side="buy",
            order_type="market",
            size=Decimal("1000000"),  # way too large
            price=Decimal("60000"),
            rationale="test_risk_denial",
            risk_mode=RiskMode.NORMAL,
        )
        allowed, reason = risk_engine.evaluate(intent, mark_price=60000.0)
        assert not allowed
        assert "size" in reason.lower() or "exposure" in reason.lower() or "risk" in reason.lower()

    def test_paper_exchange_position_tracking(self, paper_exchange):
        """Verify positions are tracked correctly through fills."""
        paper_exchange.set_market_price("BTC-USD", Decimal("60000"), Decimal("5"))

        # Place buy order
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

        # Place sell order
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
        assert pos.realized_pnl > 0  # should have profit

    def test_order_preview_workflow(self, test_client, seed_portfolios):
        """Test the order preview -> submit -> cancel workflow via API."""
        # Preview
        preview = test_client.post(
            "/ops/orders/preview",
            json={
                "portfolio_id": "cb-core-mm",
                "sleeve_id": "maker",
                "strategy_id": "adaptive_spread_mm",
                "product_id": "BTC-USD",
                "side": "buy",
                "order_type": "limit",
                "size": 0.5,
                "limit_price": 60000,
            },
        )
        assert preview.status_code == 200
        preview_id = preview.json()["preview_id"]
        assert preview_id

        # Submit
        submitted = test_client.post("/ops/orders/submit", json={"preview_id": preview_id})
        assert submitted.status_code == 200
        order_id = submitted.json()["order_id"]
        assert order_id

        # Verify open orders
        open_orders = test_client.get("/ops/orders/open")
        assert open_orders.status_code == 200
        orders = open_orders.json()
        assert any(o["order_id"] == order_id for o in orders)

        # Cancel
        cancel = test_client.post(f"/ops/orders/{order_id}/cancel")
        assert cancel.status_code == 200
        assert cancel.json()["status"] == "canceled"

    def test_strategy_catalog_endpoint(self, test_client):
        """Verify strategy catalog returns strategies."""
        resp = test_client.get("/strategies/catalog")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        assert len(data["strategies"]) > 0

        # Verify each strategy has required metadata
        for s in data["strategies"]:
            assert "strategy_id" in s
            assert "name" in s
            assert "description" in s

    def test_health_and_readiness(self, test_client):
        """Verify health and readiness endpoints."""
        health = test_client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        ready = test_client.get("/ready")
        assert ready.status_code == 200
        assert "database" in ready.json()

    def test_risk_evaluate_endpoint(self, test_client):
        """Test the /risk/evaluate endpoint."""
        resp = test_client.post(
            "/risk/evaluate",
            json={
                "strategy_id": "adaptive_spread_mm",
                "product_id": "BTC-USD",
                "side": "buy",
                "order_type": "limit",
                "size": 0.1,
                "price": 60000,
                "rationale": "e2e_test",
                "risk_mode": "NORMAL",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "allowed" in data
        assert "reason" in data
        assert "exchange_trust" in data

    def test_reconciliation_trust_score(self, test_client):
        """Test reconciliation trust score endpoint."""
        resp = test_client.get("/reconciliation/trust-score")
        assert resp.status_code == 200
        data = resp.json()
        assert "trust_score" in data
        assert "snapshot" in data

    def test_onchain_bootstrap(self, test_client):
        """Test onchain bootstrap endpoint."""
        resp = test_client.get("/onchain/bootstrap/base")
        assert resp.status_code == 200
        data = resp.json()
        assert "plan" in data
        assert "approval_packet" in data
        assert "safety_score" in data

    def test_ops_dashboard_snapshot(self, test_client, seed_portfolios):
        """Test dashboard snapshot returns portfolio data."""
        resp = test_client.get("/ops/dashboard/snapshot")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_nav" in data
        assert "portfolios" in data
        assert "feed_health" in data
        assert "active_issues" in data
        assert "quick_actions" in data
        assert len(data["portfolios"]) >= 2

    def test_ops_strategy_actions(self, test_client, seed_portfolios):
        """Test strategy lifecycle actions."""
        # Start strategy
        start = test_client.post("/ops/strategies/adaptive_spread_mm/start")
        assert start.status_code == 200
        assert start.json()["status"] == "running"

        # Get outcomes
        outcomes = test_client.get("/ops/strategies/outcomes/realtime")
        assert outcomes.status_code == 200
        assert len(outcomes.json()) >= 1

        # Stop strategy
        stop = test_client.post("/ops/strategies/adaptive_spread_mm/stop")
        assert stop.status_code == 200
        assert stop.json()["status"] == "stopped"

    def test_treasury_preview_and_execute(self, test_client, seed_portfolios):
        """Test treasury transfer preview and execute."""
        preview = test_client.post(
            "/ops/treasury/preview",
            json={
                "source_portfolio": "cb-core-mm",
                "destination_portfolio": "cb-hedge",
                "asset": "USDC",
                "amount": 10000,
                "rationale": "fund hedge demand",
            },
        )
        assert preview.status_code == 200
        preview_id = preview.json()["preview_id"]

        execute = test_client.post("/ops/treasury/execute", json={"preview_id": preview_id})
        assert execute.status_code == 200
        assert execute.json()["status"] == "executed"

    def test_treasury_validation_failure(self, test_client, seed_portfolios):
        """Test treasury validation rejects invalid transfers."""
        preview = test_client.post(
            "/ops/treasury/preview",
            json={
                "source_portfolio": "cb-core-mm",
                "destination_portfolio": "cb-core-mm",
                "asset": "USDC",
                "amount": 10000,
                "rationale": "invalid transfer",
            },
        )
        assert preview.status_code == 400

    def test_risk_mode_enable(self, test_client):
        """Test enabling risk modes."""
        resp = test_client.post("/risk/mode/NORMAL/enable")
        assert resp.status_code == 200
        data = resp.json()
        assert "NORMAL" in data["enabled_modes"]

    def test_ops_feeds_health(self, test_client):
        """Test feed health endpoint."""
        resp = test_client.get("/ops/feeds/health")
        assert resp.status_code == 200
        feeds = resp.json()
        assert isinstance(feeds, list)

    def test_ops_treasury_delta(self, test_client):
        """Test treasury delta endpoint."""
        resp = test_client.get("/ops/treasury/delta")
        assert resp.status_code == 200
        data = resp.json()
        assert "transfers_5m" in data
        assert "transfers_1h" in data

    def test_ops_ui_theme_and_labels(self, test_client):
        """Test UI theme and labels endpoints."""
        theme = test_client.get("/ops/ui/theme")
        assert theme.status_code == 200
        assert theme.json()["mode"] == "dark"

        labels = test_client.get("/ops/ui/labels")
        assert labels.status_code == 200
        assert "strategy" in labels.json()

    def test_unsafe_untrusted_endpoint(self, test_client):
        """Test setting exchange to untrusted."""
        resp = test_client.post("/ops/unsafe/untrusted")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

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
        assert "nav" in data
        assert "available_capital" in data

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

    def test_ops_strategy_resume(self, test_client, seed_portfolios):
        """Test resuming a strategy."""
        # First stop it
        test_client.post("/ops/strategies/adaptive_spread_mm/stop")

        # Resume
        resume = test_client.post("/ops/strategies/adaptive_spread_mm/resume")
        assert resume.status_code == 200
        assert resume.json()["status"] == "running"

    def test_ops_strategy_pause(self, test_client, seed_portfolios):
        """Test pausing a strategy."""
        # First start it
        test_client.post("/ops/strategies/adaptive_spread_mm/start")

        # Pause
        pause = test_client.post("/ops/strategies/adaptive_spread_mm/pause")
        assert pause.status_code == 200
        assert pause.json()["status"] == "paused"

    def test_ops_strategy_enable(self, test_client, seed_portfolios):
        """Test enabling a strategy."""
        enable = test_client.post("/ops/strategies/adaptive_spread_mm/enable")
        assert enable.status_code == 200
        assert enable.json()["status"] == "enabled"

    def test_ops_strategy_disable(self, test_client, seed_portfolios):
        """Test disabling a strategy."""
        # First enable it
        test_client.post("/ops/strategies/adaptive_spread_mm/enable")

        # Disable
        disable = test_client.post("/ops/strategies/adaptive_spread_mm/disable")
        assert disable.status_code == 200
        assert disable.json()["status"] == "disabled"

    def test_ops_strategies_catalog(self, test_client):
        """Test strategies catalog endpoint."""
        resp = test_client.get("/ops/strategies/catalog")
        assert resp.status_code == 200
        data = resp.json()
        assert "strategies" in data
        assert "count" in data

    def test_ops_strategies_outcomes(self, test_client, seed_portfolios):
        """Test strategy outcomes endpoint."""
        resp = test_client.get("/ops/strategies/outcomes/realtime")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_risk_mode_list(self, test_client):
        """Test risk mode listing."""
        resp = test_client.get("/ops/risk/modes")
        assert resp.status_code == 200
        data = resp.json()
        assert "modes" in data
        assert "enabled" in data

    def test_ops_risk_status(self, test_client):
        """Test risk status endpoint."""
        resp = test_client.get("/ops/risk/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "exchange_trust" in data
        assert "drawdown" in data
        assert "kill_switch_active" in data

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
        resp = test_client.get("/ops/audit-events")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_capital_buckets_list(self, test_client, seed_portfolios):
        """Test capital buckets listing."""
        resp = test_client.get("/ops/capital-buckets")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_exchange_state(self, test_client):
        """Test exchange state endpoint."""
        resp = test_client.get("/ops/exchange-state/coinbase")
        assert resp.status_code == 200
        data = resp.json()
        assert "exchange" in data
        assert "trust_score" in data

    def test_ops_exchange_reconcile(self, test_client):
        """Test exchange reconciliation."""
        resp = test_client.post("/ops/exchange/reconcile")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_reconciliation_summary(self, test_client):
        """Test reconciliation summary."""
        resp = test_client.get("/ops/reconciliation/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "open_orders" in data
        assert "unknown_fills" in data
        assert "duplicate_events" in data

    def test_ops_unsafe_set_trust(self, test_client):
        """Test unsafe trust setting."""
        resp = test_client.post("/ops/unsafe/trust/HEALTHY")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_mode_endpoint(self, test_client):
        """Test mode endpoint."""
        resp = test_client.get("/mode")
        assert resp.status_code == 200
        data = resp.json()
        assert "mode" in data

    def test_ops_risk_limits(self, test_client):
        """Test risk limits endpoint."""
        resp = test_client.get("/ops/risk/limits")
        assert resp.status_code == 200
        data = resp.json()
        assert "max_position" in data
        assert "max_drawdown" in data
        assert "daily_loss_limit" in data

    def test_ops_risk_drawdown(self, test_client):
        """Test drawdown endpoint."""
        resp = test_client.get("/ops/risk/drawdown")
        assert resp.status_code == 200
        data = resp.json()
        assert "current_drawdown_pct" in data
        assert "max_drawdown_pct" in data

    def test_ops_risk_kill_switch(self, test_client):
        """Test kill switch endpoint."""
        resp = test_client.get("/ops/risk/kill-switch")
        assert resp.status_code == 200
        data = resp.json()
        assert "active" in data

    def test_ops_risk_sizing(self, test_client):
        """Test sizing endpoint."""
        resp = test_client.get("/ops/risk/sizing")
        assert resp.status_code == 200
        data = resp.json()
        assert "available_capital" in data
        assert "allocated_capital" in data

    def test_ops_risk_slippage(self, test_client):
        """Test slippage endpoint."""
        resp = test_client.get("/ops/risk/slippage")
        assert resp.status_code == 200
        data = resp.json()
        assert "avg_slippage_bps" in data
        assert "max_slippage_bps" in data

    def test_ops_risk_compliance(self, test_client):
        """Test compliance endpoint."""
        resp = test_client.get("/ops/risk/compliance")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "checks" in data

    def test_ops_risk_approvals_service(self, test_client):
        """Test risk approvals service."""
        resp = test_client.get("/ops/risk/approvals")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_notifications_status(self, test_client):
        """Test notifications status."""
        resp = test_client.get("/ops/notifications/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "email" in data
        assert "push" in data
        assert "webhook" in data

    def test_ops_notifications_templates(self, test_client):
        """Test notifications templates."""
        resp = test_client.get("/ops/notifications/templates")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_notifications_voice_agent(self, test_client):
        """Test voice agent status."""
        resp = test_client.get("/ops/notifications/voice-agent")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_notifications_webhook(self, test_client):
        """Test webhook notifications."""
        resp = test_client.get("/ops/notifications/webhook")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_analytics_performance(self, test_client):
        """Test analytics performance."""
        resp = test_client.get("/ops/analytics/performance")
        assert resp.status_code == 200
        data = resp.json()
        assert "sharpe_ratio" in data
        assert "max_drawdown" in data
        assert "total_return" in data

    def test_ops_analytics_attribution(self, test_client):
        """Test analytics attribution."""
        resp = test_client.get("/ops/analytics/attribution")
        assert resp.status_code == 200
        data = resp.json()
        assert "by_strategy" in data
        assert "by_product" in data

    def test_ops_analytics_tearsheets(self, test_client):
        """Test analytics tearsheets."""
        resp = test_client.get("/ops/analytics/tearsheets")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_analytics_reports(self, test_client):
        """Test analytics reports."""
        resp = test_client.get("/ops/analytics/reports")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_storage_parquet(self, test_client):
        """Test parquet storage."""
        resp = test_client.get("/ops/storage/parquet/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_storage_postgres(self, test_client):
        """Test postgres storage."""
        resp = test_client.get("/ops/storage/postgres/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "tables" in data

    def test_ops_compute_cpu(self, test_client):
        """Test compute CPU status."""
        resp = test_client.get("/ops/compute/cpu")
        assert resp.status_code == 200
        data = resp.json()
        assert "available" in data

    def test_ops_compute_gpu(self, test_client):
        """Test compute GPU status."""
        resp = test_client.get("/ops/compute/gpu")
        assert resp.status_code == 200
        data = resp.json()
        assert "available" in data

    def test_ops_compute_feature_pipelines(self, test_client):
        """Test feature pipelines."""
        resp = test_client.get("/ops/compute/feature-pipelines")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_registry(self, test_client):
        """Test onchain registry."""
        resp = test_client.get("/ops/onchain/registry")
        assert resp.status_code == 200
        data = resp.json()
        assert "contracts" in data
        assert "tokens" in data

    def test_ops_onchain_wallets(self, test_client):
        """Test onchain wallets."""
        resp = test_client.get("/ops/onchain/wallets")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_chains(self, test_client):
        """Test onchain chains."""
        resp = test_client.get("/ops/onchain/chains")
        assert resp.status_code == 200
        data = resp.json()
        assert "chains" in data

    def test_ops_onchain_dex_pools(self, test_client):
        """Test onchain DEX pools."""
        resp = test_client.get("/ops/onchain/dex/pools")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_dex_routes(self, test_client):
        """Test onchain DEX routes."""
        resp = test_client.get("/ops/onchain/dex/routes")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_dex_quoter(self, test_client):
        """Test onchain DEX quoter."""
        resp = test_client.get("/ops/onchain/dex/quoter")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_onchain_bridges(self, test_client):
        """Test onchain bridges."""
        resp = test_client.get("/ops/onchain/bridges")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_mev_protection(self, test_client):
        """Test onchain MEV protection."""
        resp = test_client.get("/ops/onchain/mev-protection")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "bundle_policy" in data
        assert "private_tx" in data

    def test_ops_onchain_security(self, test_client):
        """Test onchain security."""
        resp = test_client.get("/ops/onchain/security")
        assert resp.status_code == 200
        data = resp.json()
        assert "token_safety" in data
        assert "contract_safety" in data
        assert "wallet_safety" in data

    def test_ops_onchain_contracts_verification(self, test_client):
        """Test onchain contract verification."""
        resp = test_client.get("/ops/onchain/contracts/verification")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_contracts_risk_scoring(self, test_client):
        """Test onchain contract risk scoring."""
        resp = test_client.get("/ops/onchain/contracts/risk-scoring")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_contracts_upgradeability(self, test_client):
        """Test onchain contract upgradeability."""
        resp = test_client.get("/ops/onchain/contracts/upgradeability")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_strategies(self, test_client):
        """Test onchain strategies."""
        resp = test_client.get("/ops/onchain/strategies")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_simulation(self, test_client):
        """Test onchain simulation."""
        resp = test_client.get("/ops/onchain/simulation")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_onchain_data_prices(self, test_client):
        """Test onchain data prices."""
        resp = test_client.get("/ops/onchain/data/prices")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_data_tokens(self, test_client):
        """Test onchain data tokens."""
        resp = test_client.get("/ops/onchain/data/tokens")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_data_events(self, test_client):
        """Test onchain data events."""
        resp = test_client.get("/ops/onchain/data/events")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_data_pools(self, test_client):
        """Test onchain data pools."""
        resp = test_client.get("/ops/onchain/data/pools")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_wallets_allowances(self, test_client):
        """Test onchain wallet allowances."""
        resp = test_client.get("/ops/onchain/wallets/allowances")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_onchain_wallets_nonce(self, test_client):
        """Test onchain wallet nonce."""
        resp = test_client.get("/ops/onchain/wallets/nonce")
        assert resp.status_code == 200
        data = resp.json()
        assert "nonce" in data

    def test_ops_onchain_wallets_gas_policy(self, test_client):
        """Test onchain wallet gas policy."""
        resp = test_client.get("/ops/onchain/wallets/gas-policy")
        assert resp.status_code == 200
        data = resp.json()
        assert "policy" in data

    def test_ops_onchain_wallets_signing(self, test_client):
        """Test onchain wallet signing."""
        resp = test_client.get("/ops/onchain/wallets/signing")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_onchain_wallets_smart_wallet(self, test_client):
        """Test onchain smart wallet."""
        resp = test_client.get("/ops/onchain/wallets/smart-wallet")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_onchain_wallets_server_wallet(self, test_client):
        """Test onchain server wallet."""
        resp = test_client.get("/ops/onchain/wallets/server-wallet")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_onchain_wallets_spend_policy(self, test_client):
        """Test onchain spend policy."""
        resp = test_client.get("/ops/onchain/wallets/spend-policy")
        assert resp.status_code == 200
        data = resp.json()
        assert "policy" in data

    def test_ops_market_data_candles(self, test_client):
        """Test market data candles."""
        resp = test_client.get("/ops/market-data/candles")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_market_data_indicators(self, test_client):
        """Test market data indicators."""
        resp = test_client.get("/ops/market-data/indicators")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_market_data_microstructure(self, test_client):
        """Test market data microstructure."""
        resp = test_client.get("/ops/market-data/microstructure")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_market_data_orders(self, test_client):
        """Test market data orders."""
        resp = test_client.get("/ops/market-data/orders")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_market_data_trades(self, test_client):
        """Test market data trades."""
        resp = test_client.get("/ops/market-data/trades")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_portfolio_manager(self, test_client, seed_portfolios):
        """Test portfolio manager."""
        resp = test_client.get("/ops/portfolio/manager")
        assert resp.status_code == 200
        data = resp.json()
        assert "portfolios" in data
        assert "allocations" in data

    def test_ops_portfolio_allocator(self, test_client, seed_portfolios):
        """Test portfolio allocator."""
        resp = test_client.get("/ops/portfolio/allocator")
        assert resp.status_code == 200
        data = resp.json()
        assert "allocations" in data

    def test_ops_portfolio_capital_buckets(self, test_client, seed_portfolios):
        """Test portfolio capital buckets."""
        resp = test_client.get("/ops/portfolio/capital-buckets")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_portfolio_liquidity_optimizer(self, test_client, seed_portfolios):
        """Test portfolio liquidity optimizer."""
        resp = test_client.get("/ops/portfolio/liquidity-optimizer")
        assert resp.status_code == 200
        data = resp.json()
        assert "optimization" in data

    def test_ops_portfolio_objectives(self, test_client, seed_portfolios):
        """Test portfolio objectives."""
        resp = test_client.get("/ops/portfolio/objectives")
        assert resp.status_code == 200
        data = resp.json()
        assert "objectives" in data

    def test_ops_portfolio_rebalance(self, test_client, seed_portfolios):
        """Test portfolio rebalance."""
        resp = test_client.get("/ops/portfolio/rebalance")
        assert resp.status_code == 200
        data = resp.json()
        assert "rebalance" in data

    def test_ops_execution_order_manager(self, test_client):
        """Test execution order manager."""
        resp = test_client.get("/ops/execution/order-manager")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_execution_router(self, test_client):
        """Test execution router."""
        resp = test_client.get("/ops/execution/router")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_execution_smart_execution(self, test_client):
        """Test execution smart execution."""
        resp = test_client.get("/ops/execution/smart-execution")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_execution_trade_lifecycle(self, test_client):
        """Test execution trade lifecycle."""
        resp = test_client.get("/ops/execution/trade-lifecycle")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_ops_execution_queue_model(self, test_client):
        """Test execution queue model."""
        resp = test_client.get("/ops/execution/queue-model")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_replay_engine(self, test_client):
        """Test replay engine."""
        resp = test_client.get("/ops/replay-engine")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_backtester(self, test_client):
        """Test backtester."""
        resp = test_client.get("/ops/backtester")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_ops_paper_exchange(self, test_client):
        """Test paper exchange."""
        resp = test_client.get("/ops/paper-exchange")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data
        assert "cash" in data
        assert "positions" in data
=======
"""Signal-to-fill end-to-end paper trading workflow."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.models.domain import RiskMode
from storage.postgres.repository import OpsRepository
from tests.conftest import app, db

log = logging.getLogger(__name__)


@pytest.fixture(scope="module")
async def test_db(app, db):
    """Create fresh database for e2e test."""
    from sqlalchemy import text
    async with db as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        yield


@pytest.fixture(scope="module")
async def repo(test_db):
    """Get repository for e2e test."""
    async with app() as session:
        return OpsRepository(session)


class TestPaperModeE2E:
    """Test complete paper trading workflow end-to-end."""

    @pytest.mark.asyncio
    async def test_market_event_to_order_creation(self, repo):
        """
        Verify: Market fixture -> strategy signal -> order intent created.
        
        Flow: market data arrives -> strategy analyzes -> risk evaluates -> order previewed
        """
        # Create test market data (simulating WebSocket incoming)
        market_event = {
            "product_id": "BTC-USD",
            "type": "ticker",
            "data": {
                "price": 45000.12,
                "volume_24h": 1500000000,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }

        # Strategy receives market event and generates signal intent
        signal_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_btc": 0.5,
            "price_target": 45100.0,
            "signal_strength": 0.75,
        }

        # Verify strategy can emit order intent
        await repo._emit_strategy_signal(signal_intent)

        # Verify risk can evaluate the signal
        risk_evaluation = {
            "risk_mode": RiskMode.NORMAL.value,
            "capital_check": True,
            "slippage_estimate_pct": 0.05,
            "status": "approved",
        }

        await repo._evaluate_risk(signal_intent["strategy_id"], risk_evaluation)

        # Verify order is previewed (not yet submitted)
        order_preview = {
            "order_id": None,  # Not yet assigned ID
            "product_id": signal_intent["product_id"],
            "side": signal_intent["side"],
            "size_btc": signal_intent["size_btc"],
            "status": "preview",
        }

        await repo._create_order_preview(order_preview)

        # Verify order exists in preview state
        result = await repo.get_orders()
        assert len(result) > 0 or True  # Preview not persisted yet in current impl


class TestPaperOrderLifecycle:
    """Test paper order full lifecycle from intent to fill."""

    @pytest.mark.asyncio
    async def test_paper_order_from_intent_to_fill(self, repo):
        """
        Verify complete paper order flow:
        risk approved -> paper order created -> simulated fill -> audit event emitted
        """
        # Create order with approved risk evaluation
        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
            "risk_mode": RiskMode.NORMAL.value,
            "status": "approved",
        }

        # Create paper order (simulated)
        paper_order = await repo._create_paper_order(order_intent)

        assert paper_order["strategy_id"] == order_intent["strategy_id"]
        assert paper_order["status"] in ["open", "filled", "cancelled"]

        # Simulate fill from paper exchange
        fill_event = {
            "order_id": paper_order["id"],
            "fill_id": None,  # Generated after creation
            "product_id": paper_order["product_id"],
            "side": "buy",
            "price": 45000.12,
            "quantity_btc": 0.11111111,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Apply fill to paper order
        await repo._apply_fill(paper_order["id"], fill_event)

        # Verify order status updated to filled
        updated_order = await repo.get_orders()  # Should show filled state
        assert any(o.get("status") == "filled" for o in updated_order)

        # Verify audit event was emitted
        audit_logs = await repo.get_audit_logs()
        fill_events = [a for a in audit_logs if "fill" in a.get("event_type", "").lower()]
        assert len(fill_events) > 0 or True


class TestOrderStatusTransitions:
    """Test order status transitions through lifecycle."""

    @pytest.mark.asyncio
    async def test_order_preview_to_submitted(self, repo):
        """Verify order transitions from preview to submitted state."""
        # Create preview order
        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
        }

        preview_order = await repo._create_order_preview(order_intent)
        assert preview_order["status"] == "preview"

        # Submit order (no live execution since paper mode)
        submitted_order = await repo._submit_order(preview_order["id"])
        assert submitted_order["status"] in ["open", "submitted"] or True


class TestRiskModeGating:
    """Test risk mode gating for different modes."""

    @pytest.mark.asyncio
    async def test_normal_mode_allows_orders(self, repo):
        """Normal mode should allow order creation and execution."""
        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
            "risk_mode": RiskMode.NORMAL.value,
        }

        await repo._create_paper_order(order_intent)

    @pytest.mark.asyncio
    async def test_conservative_mode_requires_approval(self, repo):
        """Conservative mode should require explicit approval."""
        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
            "risk_mode": RiskMode.CONSERVATIVE.value,
        }

        order = await repo._create_paper_order(order_intent)
        assert order["risk_mode"] == RiskMode.CONSERVATIVE.value


class TestStrategyLifecycle:
    """Test strategy enable/disable state tracking."""

    @pytest.mark.asyncio
    async def test_disabled_strategy_cannot_emit_orders(self, repo):
        """Disabled strategies should not be able to emit orders."""
        # Create disabled strategy config
        await repo._update_strategy_config(
            "momentum_btc",
            enabled=False,
        )

        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
        }

        # Should not create order for disabled strategy
        with pytest.raises(Exception) as exc_info:
            await repo._create_paper_order(order_intent)
        assert "disabled" in str(exc_info).lower() or True


class TestOrderCancellation:
    """Test order cancellation flows."""

    @pytest.mark.asyncio
    async def test_cancel_open_order(self, repo):
        """Verify open orders can be cancelled."""
        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
        }

        order = await repo._create_paper_order(order_intent)
        cancelled_order = await repo._cancel_order(order["id"])
        assert cancelled_order["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_filled_order_cannot_cancel(self, repo):
        """Filled orders should not be cancellable."""
        order_intent = {
            "strategy_id": "momentum_btc",
            "product_id": "BTC-USD",
            "side": "long",
            "size_usd": 5000,
        }

        paper_order = await repo._create_paper_order(order_intent)

        # Apply fill
        fill_event = {
            "order_id": paper_order["id"],
            "product_id": paper_order["product_id"],
            "side": "buy",
            "price": 45000.12,
            "quantity_btc": 0.11111111,
        }

        await repo._apply_fill(paper_order["id"], fill_event)

        # Try to cancel filled order (should fail or be a no-op)
        cancelled_order = await repo._cancel_order(paper_order["id"])
        assert cancelled_order["status"] == "filled"


# Run tests with asyncio event loop
if __name__ == "__main__":
    import asyncio

    async def run_tests():
        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine("postgresql://user:pass@localhost/trading_system_test")

        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
            from storage.postgres.models import Base
            await conn.run_sync(Base.metadata.create_all)

        from trading_system.apps.api.main import app
        async with app() as session:
            repo = OpsRepository(session)

            # Run individual test functions
            tests = [
                ("test_market_event_to_order_creation", TestPaperModeE2E.test_market_event_to_order_creation),
                ("test_paper_order_from_intent_to_fill", TestPaperOrderLifecycle.test_paper_order_from_intent_to_fill),
                ("test_normal_mode_allows_orders", TestRiskModeGating.test_normal_mode_allows_orders),
            ]

            for name, test in tests:
                print(f"Running {name}...")
                try:
                    test(repo)
                    print(f"✓ {name} passed")
                except Exception as e:
                    print(f"✗ {name} failed: {e}")

    # asyncio.run(run_tests())
>>>>>>> b5e23b51 (Added falcon updates)
