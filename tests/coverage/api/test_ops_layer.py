from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from trading_system.apps.api import ops_layer


class _Row:
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)


def _portfolio(pid, available_capital=1_000_000, **kw):
    return SimpleNamespace(
        id=pid,
        name=f"port-{pid}",
        objective="grow",
        nav=2_000_000,
        available_capital=available_capital,
        locked_capital=0,
        realized_pnl=1000,
        unrealized_pnl=2000,
        liquidity_score=0.9,
        capital_efficiency=0.8,
        **kw,
    )


def _feed(name, state):
    return SimpleNamespace(
        feed_name=name,
        state=state,
        freshness_ms=100,
        update_rate_hz=1.0,
        dropped_messages_1m=0,
        failover_active=False,
    )


def _order(status):
    return SimpleNamespace(
        order_id="o1",
        preview_id="p1",
        strategy_id="s1",
        portfolio_id="cb",
        sleeve_id="m",
        product_id="BTC-USD",
        side="buy",
        size=0.5,
        remaining_size=0.5,
        order_type="limit",
        status=status,
        maker_taker_expectation="maker",
        queue_age_s=0,
        created_at=datetime.now(timezone.utc),
    )


def _build_repo():
    repo = MagicMock()
    repo.list_portfolios.return_value = [
        _portfolio("cb-core-mm", available_capital=1_000_000),
        _portfolio("cb-small", available_capital=100_000),  # triggers low-capital issue
    ]
    repo.list_orders.return_value = [_order("open"), _order("closed")]
    repo.list_fills.return_value = [SimpleNamespace(
        fill_id="f1", order_id="o1", product_id="BTC-USD", size=0.5, price=100.0,
        slippage_bps=1.0, fee=0.1, created_at=datetime.now(timezone.utc))]
    repo.list_feed_health.return_value = [
        _feed("coinbase", "healthy"),
        _feed("kraken", "degraded"),  # triggers stale feed issue
    ]
    repo.list_approvals.return_value = [SimpleNamespace(
        approval_id="a1", approval_type="trade", summary="s", capital_affected=1000.0,
        liquidity_impact="low", risk_impact="med", expires_at=None)]
    repo.list_alerts.return_value = [SimpleNamespace(
        alert_id="al1", severity="high", summary="x", acknowledged=False)]
    repo.list_incidents.return_value = [SimpleNamespace(
        incident_id="i1", severity="low", summary="y", status="open")]
    repo.list_audit_events.return_value = [SimpleNamespace(
        event_type="e", actor="a", resource_type="r", resource_id="rid",
        details="d", created_at=datetime.now(timezone.utc))]
    repo.get_portfolio.return_value = _portfolio("cb-core-mm")
    repo.update_order.return_value = _order("canceled")
    repo.db.query.return_value.filter.return_value.order_by.return_value.first.return_value = (
        SimpleNamespace(task_id="t1", status="running", queued_at="2026-01-01T00:00:00+00:00")
    )
    return repo


@pytest.fixture
def env(monkeypatch):
    repo = _build_repo()
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    monkeypatch.setattr(ops_layer, "PortfolioSleeve", SimpleNamespace(portfolio_id="x"))

    class _SRM:
        # Class-level attributes used by the ORM query construction.
        strategy_id = MagicMock()
        status = MagicMock()
        queued_at = MagicMock()

        def __init__(self, **kw):
            self.__dict__.update(kw)
            if "queued_at" not in kw:
                self.queued_at = datetime.now(timezone.utc)

    monkeypatch.setattr(ops_layer, "StrategyRunModel", _SRM)

    class _OrderRow(_Row):
        def __init__(self, **kw):
            kw.setdefault("created_at", datetime.now(timezone.utc))
            super().__init__(**kw)

    monkeypatch.setattr(ops_layer, "OrderModel", _OrderRow)

    class _Run:
        def __init__(self, task_id="t", status="queued", queued_at=None):
            self.task_id = task_id
            self.status = status
            self.queued_at = queued_at or datetime.now(timezone.utc)

    class _Mgr:
        def __init__(self, repo=None):
            self.repo = repo

        def start(self, sid):
            return _Run(status="queued")

        def stop(self, tid):
            return _Run(status="stopped")

        def pause(self, tid):
            return _Run(status="paused")

        def resume(self, tid):
            return _Run(status="running")

        def enable(self, sid):
            return True

        def disable(self, sid):
            return True

    monkeypatch.setattr(ops_layer, "StrategyLifecycleManager", _Mgr)
    env = SimpleNamespace(repo=repo)
    return env


# ----- dashboard / read endpoints -----

def test_repo_factory():
    # Exercises the real `_repo` factory (otherwise always monkeypatched away).
    r = ops_layer._repo(MagicMock())
    assert r is not None


def test_dashboard_snapshot(env):
    snap = ops_layer.dashboard_snapshot(db=MagicMock())
    assert snap.total_nav == 4_000_000
    # Both a degraded feed and a low-capital portfolio issues should appear.
    issue_types = {i.severity for i in snap.active_issues}
    assert "medium" in issue_types
    assert "high" in issue_types
    assert len(snap.capital_buckets) == 5
    assert snap.risk_mode == "MARKET_MAKING_PRO"


def test_dashboard_snapshot_no_issues(env, monkeypatch):
    repo = _build_repo()
    repo.list_portfolios.return_value = [_portfolio("p", available_capital=1_000_000)]
    repo.list_feed_health.return_value = [_feed("c", "healthy")]
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    snap = ops_layer.dashboard_snapshot(db=MagicMock())
    assert snap.active_issues == []


def test_dashboard_delta():
    d = ops_layer.dashboard_delta()
    assert d.nav_delta_5m == 21_200


def test_feeds_health(env):
    feeds = ops_layer.feeds_health(db=MagicMock())
    assert len(feeds) == 2
    assert feeds[0].state.value == "healthy"


def test_list_portfolios(env):
    ps = ops_layer.list_portfolios(db=MagicMock())
    assert len(ps) == 2


def test_get_portfolio_detail_found(env):
    db = MagicMock()
    db.query.return_value.filter.return_value.all.return_value = [
        SimpleNamespace(name="sleeve1", weight=0.3)]
    d = ops_layer.get_portfolio_detail("cb-core-mm", db=db)
    assert d.summary.portfolio_id == "cb-core-mm"
    assert d.sleeves == {"sleeve1": 0.3}


def test_get_portfolio_detail_not_found(env, monkeypatch):
    repo = _build_repo()
    repo.get_portfolio.return_value = None
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    with pytest.raises(Exception):
        ops_layer.get_portfolio_detail("missing", db=MagicMock())


def test_liquidity_map():
    assert len(ops_layer.liquidity_map().nodes) == 4


def test_liquidity_recommendations():
    recs = ops_layer.liquidity_recommendations()
    assert len(recs) == 2


def test_open_orders(env):
    os = ops_layer.open_orders(db=MagicMock())
    assert len(os) == 1
    assert os[0].status == "open"


def test_fills(env):
    f = ops_layer.fills(db=MagicMock())
    assert len(f) == 1


def test_approvals(env):
    a = ops_layer.approvals(db=MagicMock())
    assert a[0]["approval_id"] == "a1"


def test_alerts(env):
    assert ops_layer.alerts(db=MagicMock())[0]["alert_id"] == "al1"


def test_incidents(env):
    assert ops_layer.incidents(db=MagicMock())[0]["incident_id"] == "i1"


def test_audit_events(env):
    e = ops_layer.audit_events(db=MagicMock())
    assert e[0]["event_type"] == "e"


# ----- treasury -----

def test_treasury_preview_same_source_dest(env):
    req = ops_layer.TreasuryTransferPreviewRequest(
        source_portfolio="a", destination_portfolio="a", asset="USD", amount=10, rationale="test")
    with pytest.raises(Exception):
        ops_layer.treasury_preview(req)


def test_treasury_preview_and_execute(env, monkeypatch):
    repo = _build_repo()
    repo.get_portfolio.return_value = _portfolio("cb-core-mm", available_capital=1_000_000)
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)

    preview = ops_layer.treasury_preview(ops_layer.TreasuryTransferPreviewRequest(
        source_portfolio="src", destination_portfolio="dst", asset="USD", amount=1000, rationale="test"))
    assert preview.preview_id.startswith("tr-prev-")
    res = ops_layer.treasury_execute(
        ops_layer.TreasuryTransferExecuteRequest(preview_id=preview.preview_id), db=MagicMock())
    assert res["status"] == "executed"
    repo.upsert_portfolio.assert_called()
    repo.create_audit_event.assert_called()


def test_treasury_execute_missing_preview(env):
    with pytest.raises(Exception):
        ops_layer.treasury_execute(
            ops_layer.TreasuryTransferExecuteRequest(preview_id="nope"), db=MagicMock())


def test_treasury_execute_missing_portfolio(env, monkeypatch):
    repo = _build_repo()
    repo.get_portfolio.return_value = None
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    preview = ops_layer.treasury_preview(ops_layer.TreasuryTransferPreviewRequest(
        source_portfolio="src", destination_portfolio="dst", asset="USD", amount=1000, rationale="test"))
    with pytest.raises(Exception):
        ops_layer.treasury_execute(
            ops_layer.TreasuryTransferExecuteRequest(preview_id=preview.preview_id), db=MagicMock())


def test_treasury_execute_insufficient_capital(env, monkeypatch):
    repo = _build_repo()
    repo.get_portfolio.return_value = _portfolio("cb-core-mm", available_capital=500)
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    preview = ops_layer.treasury_preview(ops_layer.TreasuryTransferPreviewRequest(
        source_portfolio="src", destination_portfolio="dst", asset="USD", amount=1000, rationale="test"))
    with pytest.raises(Exception):
        ops_layer.treasury_execute(
            ops_layer.TreasuryTransferExecuteRequest(preview_id=preview.preview_id), db=MagicMock())


# ----- orders -----

def test_preview_order_limit_requires_price(env):
    req = ops_layer.OrderPreviewRequest(
        portfolio_id="p", sleeve_id="s", strategy_id="st", product_id="BTC-USD",
        side="buy", order_type="limit", size=1.0, limit_price=None)
    with pytest.raises(Exception):
        ops_layer.preview_order(req)


def test_preview_order_and_submit(env, monkeypatch):
    repo = _build_repo()
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    preview = ops_layer.preview_order(ops_layer.OrderPreviewRequest(
        portfolio_id="p", sleeve_id="s", strategy_id="st", product_id="BTC-USD",
        side="buy", order_type="market", size=1.0))
    assert preview.preview_id.startswith("ord-prev-")
    rec = ops_layer.submit_order(
        ops_layer.SubmitOrderRequest(preview_id=preview.preview_id), db=MagicMock())
    assert rec.order_id.startswith("ord-")
    repo.create_order.assert_called()
    repo.create_audit_event.assert_called()


def test_submit_order_missing_preview(env):
    with pytest.raises(Exception):
        ops_layer.submit_order(
            ops_layer.SubmitOrderRequest(preview_id="nope"), db=MagicMock())


def test_cancel_order(env):
    res = ops_layer.cancel_order("o1", db=MagicMock())
    assert res["status"] == "canceled"
    env.repo.create_audit_event.assert_called()


def test_cancel_order_not_found(env, monkeypatch):
    repo = _build_repo()
    repo.update_order.return_value = None
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    with pytest.raises(Exception):
        ops_layer.cancel_order("o1", db=MagicMock())


def test_start_backtest(env):
    res = ops_layer.start_backtest(ops_layer.BacktestRequest(
        strategy_id="st", universe=["BTC-USD"], lookback_days=10, capital=1000), db=MagicMock())
    assert res.task_id.startswith("bt-")


# ----- strategy lifecycle -----

def test_start_strategy(env):
    res = ops_layer.start_strategy("st", db=MagicMock())
    assert res.status == "queued"


def test_stop_strategy(env):
    res = ops_layer.stop_strategy("st", db=MagicMock())
    assert res.status == "stopped"


def test_stop_strategy_no_run(env, monkeypatch):
    repo = _build_repo()
    repo.db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    with pytest.raises(Exception):
        ops_layer.stop_strategy("st", db=MagicMock())


def test_pause_strategy(env):
    assert ops_layer.pause_strategy("st", db=MagicMock()).status == "paused"


def test_pause_strategy_no_run(env, monkeypatch):
    repo = _build_repo()
    repo.db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    with pytest.raises(Exception):
        ops_layer.pause_strategy("st", db=MagicMock())


def test_resume_strategy(env):
    assert ops_layer.resume_strategy("st", db=MagicMock()).status == "running"


def test_resume_strategy_no_run(env, monkeypatch):
    repo = _build_repo()
    repo.db.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
    monkeypatch.setattr(ops_layer, "_repo", lambda db: repo)
    with pytest.raises(Exception):
        ops_layer.resume_strategy("st", db=MagicMock())


def test_enable_strategy(env):
    assert ops_layer.enable_strategy("st", db=MagicMock())["status"] == "enabled"


def test_enable_strategy_not_found(env, monkeypatch):
    class _Mgr2:
        def __init__(self, repo=None):
            pass

        def enable(self, sid):
            return False

    monkeypatch.setattr(ops_layer, "StrategyLifecycleManager", _Mgr2)
    with pytest.raises(Exception):
        ops_layer.enable_strategy("st", db=MagicMock())


def test_disable_strategy(env):
    assert ops_layer.disable_strategy("st", db=MagicMock())["status"] == "disabled"


def test_disable_strategy_not_found(env, monkeypatch):
    class _Mgr2:
        def __init__(self, repo=None):
            pass

        def disable(self, sid):
            return False

    monkeypatch.setattr(ops_layer, "StrategyLifecycleManager", _Mgr2)
    with pytest.raises(Exception):
        ops_layer.disable_strategy("st", db=MagicMock())


# ----- misc -----

def test_strategy_outcomes_realtime():
    out = ops_layer.strategy_outcomes_realtime()
    assert len(out) == 2


def test_theme_settings():
    assert ops_layer.theme_settings().mode.value == "dark"


def test_ui_labels():
    assert ops_layer.ui_labels()["portfolio"] == "portfolios"


def test_risk_summary():
    assert ops_layer.risk_summary()["mode"] == "MARKET_MAKING_PRO"
