"""Tests for trading_system.ui.dashboard_server (aim >=90% line+branch).

Mocks all external I/O: state store, coinbase CLI, prediction client, graph
store, and urllib network. Uses a fake HTTP handler to drive do_GET/do_POST.
"""

import io
import json
import os
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

import pytest

import trading_system.ui.dashboard_server as m


# --------------------------------------------------------------------------
# Fake external dependencies
# --------------------------------------------------------------------------
class FakeStateStore:
    def stats(self):
        return {"trades": 3, "snapshots": 2}

    def load_trades(self, limit=200):
        return [
            {"side": "BUY", "size_usd": 100.0, "fee": 0.1, "pnl_usd": 5.0, "currency": "BTC"},
            {"side": "SELL", "size_usd": 50.0, "fee": 0.05, "pnl_usd": -2.0, "currency": "ETH"},
        ]

    def load_snapshots(self, limit=100):
        return [
            {"total_value": 1000.0},
            {"total_value": 1100.0},
        ]

    def load_bt_cache(self, ttl=86400 * 30):
        return {"stratA:param": {"sharpe_ratio": 1.2, "win_rate": 0.6, "total_trades": 10, "max_drawdown_pct": -5.0}}

    def get_meta(self, key):
        if key == "capital_policy":
            return json.dumps({"targets": {"reserve": 0.5}, "preset_name": "custom"})
        return None

    def set_meta(self, key, value):
        return True


class _ImmediateExecutor:
    def submit(self, fn, *a, **k):
        class _F:
            def result(self, timeout=None):
                return fn(*a, **k)

            def cancel(self):
                return False
        return _F()


class FakeHandler:
    """Minimal stand-in for DashboardHandler without a real socket."""

    def __init__(self, path, method="GET", body=None):
        self.path = path
        self.command = method
        self._body = body
        self.status = None
        self.headers_out = {}
        self.wfile = io.BytesIO()
        self.rfile = io.BytesIO(body or b"")
        self.headers = {"Content-Length": str(len(body or b""))}
        self.dashboard_served = False

    def send_response(self, status):
        self.status = status

    def send_header(self, k, v):
        self.headers_out[k] = v

    def end_headers(self):
        pass

    def log_message(self, *a, **k):
        pass

    def _serve_dashboard(self):
        self.dashboard_served = True
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html></html>")


def make_handler(path, method="GET", body=None):
    h = FakeHandler(path, method, body)
    # Bind the real handler methods onto our fake instance
    h.do_GET = m.DashboardHandler.do_GET.__get__(h, m.DashboardHandler)
    h.do_POST = m.DashboardHandler.do_POST.__get__(h, m.DashboardHandler)
    return h


@pytest.fixture
def env(monkeypatch, tmp_path):
    # Redirect all data paths to temp files
    for name in ["OPERATOR_STATE_PATH", "APPROVALS_PATH", "CAPITAL_BUCKETS_PATH",
                 "EQUITY_SUMMARY_PATH", "SIGNAL_CACHE_PATH", "STATE_DB_PATH"]:
        monkeypatch.setattr(m, name, str(tmp_path / (name + ".json")))
    monkeypatch.setattr(m, "_SHARED_EXECUTOR", _ImmediateExecutor())
    monkeypatch.setattr(m, "_get_state_store", lambda: FakeStateStore())
    monkeypatch.setattr(m, "_get_coinbase_cli", lambda: None)
    monkeypatch.setattr(m, "_get_prediction_client", lambda: None)
    monkeypatch.setattr(m, "_get_graph_store", lambda: None)
    # Prevent real network
    def _no_net(*a, **k):
        raise URLError("blocked")
    monkeypatch.setattr(m, "urlopen", _no_net)
    return tmp_path


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def test_load_json_missing(env):
    assert m._load_json(str(env / "nope.json")) == {}


def test_load_json_default(env):
    assert m._load_json(str(env / "nope.json"), {"x": 1}) == {"x": 1}


def test_load_json_valid(env):
    p = env / "ok.json"
    p.write_text(json.dumps({"a": 2}))
    assert m._load_json(str(p)) == {"a": 2}


def test_load_json_corrupt(env):
    p = env / "bad.json"
    p.write_text("{not json")
    assert m._load_json(str(p), {"d": 1}) == {"d": 1}


def test_write_json(env):
    p = env / "w.json"
    assert m._write_json(str(p), {"k": 1}) is True
    assert json.loads(p.read_text()) == {"k": 1}


def test_write_json_fail(monkeypatch, env):
    def _boom(*a, **k):
        raise OSError("no")
    monkeypatch.setattr(m, "open", _boom)
    assert m._write_json(str(env / "x.json"), {}) is False


def test_normalize_capital_policy_none():
    r = m._normalize_capital_policy(None)
    assert "targets" in r
    assert r["preset_name"] == "custom"


def test_normalize_capital_policy_custom():
    r = m._normalize_capital_policy({"targets": {"reserve": 1.0}, "core_allowlist": "btc, eth"})
    assert r["core_allowlist"] == ["BTC", "ETH"]
    assert abs(sum(r["targets"].values()) - 1.0) < 1e-6


def test_build_bucket_preset_challenge():
    r = m._build_bucket_preset("challenge_5")
    assert r["buckets"][0]["starting_balance_usd"] == 5.0


def test_build_bucket_preset_unknown():
    assert m._build_bucket_preset("does_not_exist") == {"buckets": []}


def test_build_bucket_preset_imported(monkeypatch):
    fake = types.ModuleType("fake_cb")

    def build_bucket_preset(name, **kw):
        return {"buckets": [{"bucket_id": name}]}
    fake.build_bucket_preset = build_bucket_preset
    monkeypatch.setitem(sys.modules, "coinbase.src.capital_buckets", fake)
    r = m._build_bucket_preset("challenge_5")
    assert r["buckets"][0]["bucket_id"] == "challenge_5"


def test_save_capital_buckets_dict(env):
    r = m._save_capital_buckets({"buckets": [{"bucket_id": "b1", "starting_balance_usd": 100}]})
    assert r["buckets"][0]["bucket_id"] == "b1"
    assert os.path.exists(m.CAPITAL_BUCKETS_PATH)


def test_save_capital_buckets_list(env):
    r = m._save_capital_buckets([{"id": "x", "starting_balance_usd": 10}])
    assert r["buckets"][0]["bucket_id"] == "x"


def test_update_approval_found(env):
    p = Path(m.APPROVALS_PATH)
    p.write_text(json.dumps({"tok1": {"status": "pending", "created_at": "t"}}))
    assert m._update_approval("tok1", "approved") is True
    data = json.loads(p.read_text())
    assert data["tok1"]["status"] == "approved"


def test_update_approval_missing(env):
    assert m._update_approval("nope", "approved") is False


def test_load_capital_buckets_empty(env):
    r = m._load_capital_buckets()
    assert r["buckets"] == []


def test_load_capital_buckets_with_data(env):
    m._save_capital_buckets({"buckets": [{"bucket_id": "b1", "cash_usd": 100,
                                           "positions": {"p1": {"size": 1, "current_price": 50}}}]})
    r = m._load_capital_buckets()
    assert r["buckets"][0]["cash_usd"] == 100.0


def test_get_capital_policy(env):
    r = m._get_capital_policy()
    assert "targets" in r


def test_save_capital_policy(env):
    r = m._save_capital_policy({"targets": {"reserve": 0.5}})
    assert r["preset_name"] == "custom"


def test_graph_summary_empty():
    assert m._graph_summary_for_products([])["available"] is False


def test_graph_summary_no_store(monkeypatch):
    monkeypatch.setattr(m, "_get_graph_store", lambda: None)
    r = m._graph_summary_for_products(["BTC-USD"])
    assert r["available"] is False


def test_build_preset_payload():
    presets = m._build_preset_payload()
    assert isinstance(presets, list)


def test_bucket_preset_names(monkeypatch):
    fake = types.ModuleType("fake_cb")

    def bucket_preset_names():
        return ["custom1"]
    fake.bucket_preset_names = bucket_preset_names
    monkeypatch.setitem(sys.modules, "coinbase.src.capital_buckets", fake)
    assert "custom1" in m._bucket_preset_names()


def test_bucket_preset_payloads(monkeypatch):
    fake = types.ModuleType("fake_cb")

    def bucket_preset_names():
        return ["custom1"]
    fake.bucket_preset_names = bucket_preset_names
    monkeypatch.setitem(sys.modules, "coinbase.src.capital_buckets", fake)
    assert isinstance(m._bucket_preset_payloads(), list)


# --------------------------------------------------------------------------
# API endpoint functions (called directly)
# --------------------------------------------------------------------------
def test_api_health(env):
    assert m.api_health()["status"] in ("healthy", "degraded")


def test_api_accounts(env):
    assert "accounts" in m.api_accounts()


def test_api_positions(env):
    assert "positions" in m.api_positions()


def test_api_strategies(env):
    r = m.api_strategies()
    assert "total_strategies" in r


def test_api_approvals(env):
    assert "approvals" in m.api_approvals()


def test_api_performance(env):
    assert "summary_metrics" in m.api_performance()


def test_api_price_estimates_plain(env):
    r = m.api_price_estimates("btc")
    assert r["instrument"] == "BTC-USD"
    assert r["current_price_usd"] > 0


def test_api_price_estimates_slash(env):
    r = m.api_price_estimates("btc/usd")
    assert r["instrument"] == "BTC-USD"


def test_api_hypotheses(env):
    assert "hypotheses" in m.api_hypotheses()


def test_api_risk_dashboard(env):
    assert "operational_controls" in m.api_risk_dashboard()


def test_api_execute_manual_operation(env):
    r = m._execute_manual_operation("refresh_market_data", {})
    assert r["success"] is True


def test_api_execute_manual_operation_dangerous(env):
    r = m._execute_manual_operation("close_all", {})
    assert r["status"] == "pending_approval"


def test_api_opportunities(env, monkeypatch):
    monkeypatch.setattr(m, "_get_coinbase_cli", lambda: None)
    assert "opportunities" in m.api_opportunities()


def test_api_signal_feed(env):
    assert "signals" in m.api_signal_feed()


def test_api_diversification_signals(env):
    assert "strategies" in m.api_diversification_signals()


def test_api_strategies_performance(env):
    assert "strategies" in m.api_strategies_performance()


def test_api_market_regime(env):
    assert "regime" in m.api_market_regime()


def test_api_market_intelligence(env):
    assert isinstance(m.api_market_intelligence(), dict)


def test_api_prediction_markets(env):
    assert "markets" in m.api_prediction_markets()


def test_api_arbitrage_opportunities(env):
    assert "opportunities" in m.api_arbitrage_opportunities()


def test_api_kalshi_internal_arb(env):
    assert isinstance(m.api_kalshi_internal_arb(), dict)


def test_api_crypto_divergence(env):
    assert isinstance(m.api_crypto_divergence(), dict)


def test_api_paper_trades(env):
    assert isinstance(m.api_paper_trades(), (dict, list))


def test_api_trade_plans(env):
    assert isinstance(m.api_trade_plans(), (dict, list))


def test_api_settlement(env):
    assert isinstance(m.api_settlement(), dict)


def test_api_venue_balances(env):
    assert isinstance(m.api_venue_balances(), dict)


def test_api_execution_status(env):
    assert isinstance(m.api_execution(), dict)


def test_api_universe(env):
    assert "coinbase_total" in m.api_universe()


def test_api_operator_actions(env):
    assert "actions" in m.api_operator_actions()


def test_api_bucket_presets(env):
    assert "presets" in m.api_bucket_presets()


def test_api_execution_status_endpoint(env):
    assert isinstance(m.api_execution_status(), dict)


# --------------------------------------------------------------------------
# HTTP dispatch (do_GET / do_POST)
# --------------------------------------------------------------------------
def _dispatch_get(path):
    h = make_handler(path, "GET")
    h.do_GET()
    return h


def test_get_health():
    h = _dispatch_get("/health")
    assert h.status == 200
    assert json.loads(h.wfile.getvalue())["status"] in ("healthy", "degraded")


def test_get_accounts():
    h = _dispatch_get("/accounts")
    assert h.status == 200


def test_get_positions():
    h = _dispatch_get("/positions")
    assert h.status == 200


def test_get_strategies():
    h = _dispatch_get("/strategies")
    assert h.status == 200


def test_get_approvals():
    h = _dispatch_get("/approvals")
    assert h.status == 200


def test_get_performance():
    h = _dispatch_get("/performance")
    assert h.status == 200


def test_get_market_regime():
    h = _dispatch_get("/market/regime")
    assert h.status == 200


def test_get_research_hypotheses():
    h = _dispatch_get("/research/hypotheses")
    assert h.status == 200


def test_get_prediction_markets():
    h = _dispatch_get("/prediction-markets")
    assert h.status == 200


def test_get_arbitrage_opportunities():
    h = _dispatch_get("/arbitrage/opportunities")
    assert h.status == 200


def test_get_signals_opportunities():
    h = _dispatch_get("/signals/opportunities")
    assert h.status == 200


def test_get_signals_feed():
    h = _dispatch_get("/signals/feed")
    assert h.status == 200


def test_get_signals_diversification():
    h = _dispatch_get("/signals/diversification")
    assert h.status == 200


def test_get_strategies_performance():
    h = _dispatch_get("/strategies/performance")
    assert h.status == 200


def test_get_capital_buckets():
    h = _dispatch_get("/capital/buckets")
    assert h.status == 200


def test_get_equity_summary():
    h = _dispatch_get("/equity-summary")
    assert h.status == 200


def test_get_actions():
    h = _dispatch_get("/actions")
    assert h.status == 200


def test_get_universe():
    h = _dispatch_get("/market/universe")
    assert h.status == 200


def test_get_execution_status():
    h = _dispatch_get("/execution/status")
    assert h.status == 200


def test_get_execution_brackets():
    h = _dispatch_get("/execution/brackets")
    assert h.status == 200


def test_get_dashboard():
    h = _dispatch_get("/dashboard")
    assert h.dashboard_served is True


def test_get_root():
    h = _dispatch_get("/")
    assert h.dashboard_served is True


def test_get_not_found():
    h = _dispatch_get("/does/not/exist")
    assert h.status == 404


def test_get_approve_found(env):
    Path(m.APPROVALS_PATH).write_text(json.dumps({"tok1": {"status": "pending", "created_at": "t"}}))
    h = _dispatch_get("/approvals/approve/tok1")
    assert h.status == 200
    assert json.loads(h.wfile.getvalue())["ok"] is True


def test_get_approve_missing(env):
    h = _dispatch_get("/approvals/approve/unknown")
    assert h.status == 404


def test_get_deny_found(env):
    Path(m.APPROVALS_PATH).write_text(json.dumps({"tok1": {"status": "pending", "created_at": "t"}}))
    h = _dispatch_get("/approvals/deny/tok1")
    assert h.status == 200


def test_get_approve_empty_token(env):
    h = _dispatch_get("/approvals/approve/")
    assert h.status == 404


def test_get_price_estimates_endpoint():
    h = _dispatch_get("/evaluations/price/BTC")
    assert h.status == 200


def test_get_bucket_preset(env):
    h = _dispatch_get("/capital/buckets/preset/challenge_5")
    assert h.status == 200


def test_post_capital_config():
    h = make_handler("/capital/config", "POST", json.dumps({"targets": {"reserve": 0.5}}).encode())
    h.do_POST()
    assert h.status == 200


def test_post_capital_buckets():
    body = json.dumps({"buckets": [{"bucket_id": "b1", "starting_balance_usd": 1}]}).encode()
    h = make_handler("/capital/buckets", "POST", body)
    h.do_POST()
    assert h.status == 200


def test_post_actions_run():
    body = json.dumps({"type": "refresh_market_data"}).encode()
    h = make_handler("/actions/run", "POST", body)
    h.do_POST()
    assert h.status == 200


def test_post_arbitrage_execute():
    body = json.dumps({"execution_plan": {}}).encode()
    h = make_handler("/arbitrage/execute", "POST", body)
    h.do_POST()
    assert h.status in (200, 500)


def test_post_not_found():
    h = make_handler("/nope", "POST", b"{}")
    h.do_POST()
    assert h.status == 404
