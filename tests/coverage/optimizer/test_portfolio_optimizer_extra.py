"""Additional coverage for portfolio_optimizer.py targeting state fetching,
cost bases, the tick orchestrator, pending-approval handling and approved
execution paths. All external I/O is mocked; no background threads are spawned.
"""

import json
import math
import time
from types import SimpleNamespace
from unittest import mock

import pytest

import portfolio_optimizer as P
from conftest import make_state, holding


@pytest.fixture(autouse=True)
def _disable_smart_feed_thread(monkeypatch):
    monkeypatch.setattr(P, "_HAS_SMART_FEED", False)


def _cli():
    cli = mock.MagicMock()
    cli.environment = "live"
    cli.best_product.side_effect = lambda c, s: f"{c}-USD"
    cli.get_price.return_value = {"price": 100.0, "price_percentage_change_24h": 1.0, "volume_24h": 5_000_000}
    cli.get_products.return_value = {}
    cli.get_candles.return_value = []
    cli.get_balances.return_value = []
    cli.get_fees.return_value = {"advanced_trade_only_volume": 0}
    cli.get_fills.return_value = []
    cli.preview_order.return_value = {"total_fee": 1.0, "total_cost": 100.0}
    cli.create_order.return_value = {"id": "ord1"}
    return cli


@pytest.fixture
def o(opt):
    opt.cli = _cli()
    opt._feed_mgr = None
    opt._lock_fd = None
    opt.neo4j_store = None
    for _attr in ("_funding_contrarian", "_onchain_flow", "_order_flow_engine",
                 "_smart_money_flow", "_ensemble_blender", "_cross_asset_regime",
                 "_macro_risk", "_arb_scanner", "_knowledge_gap", "_pm_client",
                 "notifier"):
        setattr(opt, _attr, None)
    return opt


# ---------------------------------------------------------------------------
# _compute_cost_bases
# ---------------------------------------------------------------------------

def test_compute_cost_bases(o):
    o.cli.get_fills.return_value = [
        {"product_id": "BTC-USD", "side": "BUY", "size": "1", "price": "100"},
        {"product_id": "BTC-USD", "side": "BUY", "size": "1", "price": "200"},
        {"product_id": "BTC-USD", "side": "SELL", "size": "0.5", "price": "300"},
        {"product_id": "", "currency": "ETH", "side": "BUY", "size": "0", "price": "0"},  # skipped
        {"product_id": "SOL-USD", "side": "SELL", "size": "1", "price": "50"},  # no prior buy -> ignored
    ]
    cb = o._compute_cost_bases()
    assert "BTC" in cb
    assert cb["BTC"] > 0
    assert "SOL" not in cb


# ---------------------------------------------------------------------------
# _fetch_state
# ---------------------------------------------------------------------------

def test_fetch_state_success(o):
    # NOTE: BTC must be processed before USDC — see test_fetch_state_stablecoin_first_bug.
    o.cli.get_balances.return_value = [
        {"currency": "BTC", "available_balance": {"value": "1"}, "hold": {"value": "0"}},
        {"currency": "USDC", "available_balance": {"value": "5000"}, "hold": {"value": "0"}},
        {"currency": "ZERO", "available_balance": {"value": "0"}, "hold": {"value": "0"}},  # skipped
    ]
    o.cli.get_price.return_value = {"price": "30000", "price_percentage_change_24h": "2.0", "volume_24h": "1000000"}
    o.cli.get_fees.return_value = {"advanced_trade_only_volume": 0}
    o.cli.get_fills.return_value = []
    o._fetch_state()
    assert o.state is not None
    assert o.state.usdc_balance == 5000.0
    assert "BTC" in o.state.holdings
    assert o.state.total_value > 0
    # allocation percentages computed
    assert o.state.holdings["BTC"]["allocation_pct"] > 0


def test_fetch_state_stablecoin_first(o):
    """VERIFIES FIX (portfolio_optimizer.py:2639-2642): a stablecoin holding now
    sets price_info, so the holding dict builds correctly even when the
    stablecoin is the FIRST non-zero balance processed (no UnboundLocalError)."""
    o.cli.get_balances.return_value = [
        {"currency": "USDC", "available_balance": {"value": "5000"}, "hold": {"value": "0"}},
    ]
    o.cli.get_fills.return_value = []
    o._fetch_state()  # must not raise
    assert o.state is not None
    assert o.state.usdc_balance == 5000.0
    assert "USDC" in o.state.holdings
    assert o.state.holdings["USDC"]["change_24h"] == 0.0
    assert o.state.holdings["USDC"]["volume_24h"] == 0.0


def test_fetch_state_balances_fail(o):
    o.cli.get_balances.side_effect = RuntimeError("api down")
    o._fetch_state()
    # falls back to empty portfolio
    assert o.state is not None
    assert o.state.total_value == 0.0
    assert o.state.holdings == {}


# ---------------------------------------------------------------------------
# _tick orchestrator (detection mocked to isolate orchestration)
# ---------------------------------------------------------------------------

def test_tick(o, monkeypatch):
    o.state = make_state({"BTC": holding("BTC", 5000, "safe")}, usdc=90000.0)
    monkeypatch.setattr(o, "_fetch_state", lambda: None)
    monkeypatch.setattr(o, "_check_pending_approvals", lambda: None)
    monkeypatch.setattr(o, "_apply_bear_market_policy", lambda: None)
    opp = P.Opportunity(opp_type=P.OpportunityType.STRATEGY_SIGNAL, currency="SOL",
                        side="BUY", size_usd=100, reason="r", priority=0.5,
                        entry_price_est=100, stop_loss_pct=5, meta={})
    monkeypatch.setattr(o, "_detect_opportunities", lambda: [opp])
    monkeypatch.setattr(o, "_apply_cross_asset_risk_filter", lambda ops: ops)
    monkeypatch.setattr(o, "_process_opportunity", mock.MagicMock())
    monkeypatch.setattr(o, "_save_state", lambda: None)
    monkeypatch.setattr(o, "_write_trade_plans", lambda ops: None)
    monkeypatch.setattr(o, "_write_signal_cache", lambda ops: None)
    monkeypatch.setattr(o, "_write_enhanced_state", lambda: None)
    o._tick()
    assert o._last_detected_opportunities == [opp]
    o._process_opportunity.assert_called_once_with(opp)


# ---------------------------------------------------------------------------
# _check_pending_approvals + _execute_approved
# ---------------------------------------------------------------------------

def test_check_pending_approvals_executes_approved(o, tmp_path):
    o.require_approval = True
    o.dry_run = True
    o.state = make_state({"BTC": holding("BTC", 5000, "safe")}, usdc=90000.0)
    pend = {
        "tok1": {"status": "approved", "side": "BUY", "currency": "SOL", "size_usd": 100,
                 "product_id": "SOL-USD", "reason": "ok", "capital_bucket": "opportunity",
                 "type": "strategy", "priority": 0.5},
        "tok2": {"status": "pending", "side": "BUY", "currency": "ETH", "size_usd": 100},
    }
    o.pending_file = str(tmp_path / "pend.json")
    with open(o.pending_file, "w") as f:
        json.dump(pend, f)
    o._check_pending_approvals()
    # approved token consumed, pending token remains
    with open(o.pending_file) as f:
        remaining = json.load(f)
    assert "tok1" not in remaining
    assert "tok2" in remaining


def test_check_pending_approvals_bad_json(o, tmp_path):
    o.require_approval = True
    o.pending_file = str(tmp_path / "bad.json")
    with open(o.pending_file, "w") as f:
        f.write("{not json")
    assert o._check_pending_approvals() is None


def test_execute_approved_sell(o):
    o.dry_run = True
    o.require_approval = False
    o._bracket_mgr = None
    o.state = make_state({"BTC": holding("BTC", 5000, "safe", price=100)}, usdc=90000.0)
    entry = {"side": "SELL", "currency": "BTC", "size_usd": 1000, "product_id": "BTC-USD",
             "reason": "approved", "type": "strategy"}
    before = len(o.trade_log)
    o._execute_approved(entry)
    assert len(o.trade_log) == before + 1


def test_execute_approved_preview_fail(o):
    o.dry_run = True
    o._bracket_mgr = None
    o.state = make_state({"BTC": holding("BTC", 5000, "safe")}, usdc=90000.0)
    o.cli.preview_order.return_value = None
    entry = {"side": "BUY", "currency": "SOL", "size_usd": 100, "product_id": "SOL-USD",
             "reason": "approved", "capital_bucket": "opportunity", "type": "strategy"}
    before = len(o.trade_log)
    o._execute_approved(entry)
    assert len(o.trade_log) == before  # nothing recorded


# ---------------------------------------------------------------------------
# summary aggregation
# ---------------------------------------------------------------------------

def test_summary_multi_trades(o):
    o.trade_log = [
        {"type": "tlh", "size_usd": 100, "fee": 1.0},
        {"type": "rebalance", "size_usd": 200, "fee": 2.0},
        {"type": "tlh", "size_usd": 50, "fee": 0.5},
    ]
    s = o.summary()
    assert s["total_trades"] == 3
    assert s["total_volume"] == 350.0


# ---------------------------------------------------------------------------
# _detect_coinbase_universe_signals — full momentum path
# ---------------------------------------------------------------------------

def test_detect_coinbase_universe_generates_ops(o):
    o.last_execution.clear()
    o.state = make_state({"SOL": holding("SOL", 5000, "growth", price=100)},
                         total_value=100000.0, usdc=90000.0)
    o._feed_mgr = None
    o.cli.get_products.return_value = {
        "SOL-USD": {"product_id": "SOL-USD", "trading_disabled": False, "volume_24h": 200_000_000},
    }
    # Strong up-trend with realistic pullbacks (so RSI is not saturated to 100,
    # which the quality filter would reject as exhaustion) -> high momentum.
    trend = [{
        "time": str(i),
        "open": 100.0 * (1.03 ** i) * (1 + 0.08 * math.sin(2 * math.pi * i / 6.0)),
        "high": 100.0 * (1.03 ** i) * (1.08 + 0.08 * math.sin(2 * math.pi * i / 6.0)),
        "low": 100.0 * (1.03 ** i) * (0.92 + 0.08 * math.sin(2 * math.pi * i / 6.0)),
        "close": 100.0 * (1.03 ** i) * (1 + 0.08 * math.sin(2 * math.pi * i / 6.0)),
        "volume": 1000.0 + i * 50.0,
    } for i in range(60)]
    o.cli.get_candles.return_value = trend
    ops = o._detect_coinbase_universe_signals()
    assert isinstance(ops, list)
    assert any(o_.currency == "SOL" for o_ in ops)
    assert ops[0].opp_type in (P.OpportunityType.STRATEGY_SIGNAL, P.OpportunityType.NEW_LISTING_MOMENTUM)
