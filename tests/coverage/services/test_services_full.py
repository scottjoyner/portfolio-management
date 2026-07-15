"""Coverage tests for services modules: confidence_matrix, state_store,
approval_server, notification, market_universe, neo4j_store."""
import io
import json
import os
import sqlite3
import threading
from types import SimpleNamespace
from unittest import mock

import pytest

import confidence_matrix as cm
from confidence_matrix import ConfidenceMatrix, AggregatedSignal, format_aggregated
from strategy_engine import Signal


# ───────────────────────── confidence_matrix ─────────────────────────

def _sig(strategy, action, confidence, reason=""):
    return Signal(action=action, price=100.0, confidence=confidence,
                  reason=reason, strategy=strategy)


def test_cm_group_lookup():
    assert cm.STRATEGY_GROUP["ema_cross"] == "trend"
    assert cm.STRATEGY_GROUP["rsi_revert"] == "momentum"
    assert cm.STRATEGY_GROUP["kalshi"] == "prediction_market"
    assert "not_a_strategy" not in cm.STRATEGY_GROUP


def test_cm_empty():
    assert ConfidenceMatrix().aggregate([]) == []


def test_cm_aggregate_rust_buy_sell():
    sigs = [_sig("ema_cross", "BUY", 0.9, "b"), _sig("rsi_revert", "SELL", 0.5, "s")]
    out = ConfidenceMatrix().aggregate(sigs, asset_class="growth", currency="ETH-USD")
    assert {o.direction for o in out} == {"BUY", "SELL"}
    confs = [o.confidence for o in out]
    assert confs == sorted(confs, reverse=True)


def test_cm_aggregate_rust_bt_cache_branches():
    bt_cache = {
        "ema_cross/BTC-USD": {"win_rate": 0.6, "sharpe_ratio": 1.2},
        "rsi_revert/BTC-USD": {"win_rate": 0.0, "sharpe_ratio": 0.0},
    }
    sigs = [_sig("ema_cross", "BUY", 0.7, "a"), _sig("rsi_revert", "BUY", 0.7, "b")]
    out = ConfidenceMatrix(bt_cache=bt_cache).aggregate(
        sigs, asset_class="growth", currency="BTC-USD")
    assert out[0].agreeing_groups >= 2
    assert out[0].confidence >= 0.0


def test_cm_aggregate_py_path():
    with mock.patch.object(cm, "_HAS_RUST_CONFIDENCE", False):
        sigs = [
            _sig("ema_cross", "BUY", 0.6, "trend"),
            _sig("rsi_revert", "BUY", 0.6, "momentum"),
            _sig("boll_break", "BUY", 0.6, "vol"),
            _sig("vol_mom", "BUY", 0.6, "vol2"),
        ]
        out = ConfidenceMatrix().aggregate(sigs, asset_class="safe", currency="BTC-USD")
        assert out[0].direction == "BUY"
        assert out[0].agreeing_groups >= 2
        assert out[0].confidence >= out[0].raw_confidence - 1e-9
        assert out[0].strategy_count >= 3


def test_cm_strategy_weight_branches():
    with mock.patch.object(cm, "_HAS_RUST_CONFIDENCE", False):
        m = ConfidenceMatrix(bt_cache={"x/BTC-USD": {"win_rate": 0.8, "sharpe_ratio": 2.0}})
        w_cached = m._strategy_weight("x", "BTC-USD")
        assert w_cached > 0
        w_default = m._strategy_weight("unknown_strat", "BTC-USD")
        assert w_default == 0.5
        m2 = ConfidenceMatrix(bt_cache={"y/BTC-USD": {"win_rate": 0, "sharpe_ratio": 0}})
        w_bad = m2._strategy_weight("y", "BTC-USD")
        assert w_bad == 0.5


def test_cm_class_boost():
    m = ConfidenceMatrix()
    assert m._class_boost("ema_cross", "safe") == 1.3
    assert m._class_boost("ema_cross", "nope") == 1.1
    assert m._class_boost("kalshi", "growth") == 1.0


def test_cm_format_aggregated():
    s = _sig("ema_cross", "BUY", 0.9, "reason")
    out = ConfidenceMatrix().aggregate([s], asset_class="growth", currency="BTC-USD")
    txt = format_aggregated(out[0])
    assert "BUY" in txt


# ───────────────────────── state_store ─────────────────────────

def _fake_state():
    return SimpleNamespace(
        holdings={
            "BTC": {"currency": "BTC", "total": 1.0, "price": 100.0,
                    "value": 100.0, "classification": "safe", "allocation_pct": 50.0},
        },
        total_value=200.0,
        usdc_balance=100.0,
        fee_volume_30d=5000.0,
        fee_tier=(1000.0, 0.001, 0.002),
    )


class _FakeVerdict:
    def __init__(self):
        self.strategy = "ema_cross"
        self.currency = "BTC-USD"
        self.total_trades = 10
        self.winning_trades = 6
        self.losing_trades = 4
        self.win_rate = 0.6
        self.total_return_pct = 12.0
        self.sharpe_ratio = 1.5
        self.profit_factor = 1.8
        self.max_drawdown_pct = -5.0
        self.regime = "trend"
        self.passed = True
        self.reason = "good"


def test_state_store_trades(tmp_path):
    db = str(tmp_path / "s.db")
    s = __import__("state_store").StateStore(db)
    s.save_trade({"type": "rebalance", "side": "BUY", "currency": "BTC",
                  "size_usd": 100, "fee": 1, "symbol": "BTC", "price": 100,
                  "quantity": 1, "strategy": "ema", "pnl_usd": 5,
                  "reason": "r", "order_id": "o1", "dry_run": True})
    rows = s.load_trades()
    assert len(rows) == 1
    assert rows[0]["type"] == "rebalance"
    assert s.stats()["trades"] == 1


def test_state_store_snapshots_and_btcache(tmp_path):
    db = str(tmp_path / "s.db")
    s = __import__("state_store").StateStore(db)
    res = s.save_snapshot(_fake_state())
    assert "id" in res
    snaps = s.load_snapshots()
    assert len(snaps) == 1
    assert snaps[0]["holdings"]["BTC"]["value"] == 100.0

    v = _FakeVerdict()
    s.save_bt_cache("ema_cross/BTC-USD", v)
    cache = s.load_bt_cache()
    assert cache["ema_cross/BTC-USD"]["win_rate"] == 0.6
    s.prune_bt_cache(ttl=0)
    assert s.load_bt_cache() == {}


def test_state_store_positions_and_meta(tmp_path):
    db = str(tmp_path / "s.db")
    s = __import__("state_store").StateStore(db)
    s.save_position_ages({"BTC": 3.5, "ETH": 1.0})
    ages = s.load_position_ages()
    assert ages["BTC"] == 3.5
    s.set_meta("k", "v")
    assert s.get_meta("k") == "v"
    assert s.get_meta("missing", "def") == "def"
    assert s.stats()["db_path"] == db


# ───────────────────────── notification ─────────────────────────

def test_notification_subject_and_html():
    from notification import TradeNotifier
    n = TradeNotifier(smtp_user="a@b.com", to_addr="a@b.com")
    subj = n._build_subject({"side": "BUY", "type": "rebalance", "size_usd": 100,
                             "currency": "BTC"})
    assert "BUY" in subj
    subj2 = n._build_subject({"side": "SELL", "type": "x", "size_usd": 1, "currency": "ETH"})
    assert "SELL" in subj2
    html = n._build_html({"side": "BUY", "currency": "BTC", "size_usd": 100,
                          "type": "rebalance", "expected_fee": 1, "priority": 0.5,
                          "reason": "r"},
                         {"total_value": 1000, "usdc_balance": 200}, None, "tok")
    assert "Approve" in html
    verdict = {"win_rate": 0.6, "sharpe_ratio": 1.2, "profit_factor": 1.5,
               "max_drawdown_pct": -3.0, "strategy": "ema", "currency": "BTC"}
    html2 = n._build_html({"side": "BUY", "currency": "BTC", "size_usd": 100,
                           "type": "rebalance", "expected_fee": 1, "priority": 0.5,
                           "reason": "r"},
                          {"total_value": 1000, "usdc_balance": 200}, verdict, "tok")
    assert "Win Rate" in html2


def test_notification_risk_reward_branches():
    from notification import TradeNotifier
    n = TradeNotifier()
    rr = n._compute_risk_reward({"size_usd": 100, "expected_fee": 2},
                                {"win_rate": 0.6, "profit_factor": 1.5,
                                 "max_drawdown_pct": -3.0})
    assert rr["ev"] != 0
    rr2 = n._compute_risk_reward({"size_usd": 0, "expected_fee": 0},
                                 {"win_rate": 0, "profit_factor": 0})
    assert rr2["ev"] == 0.0
    rr3 = n._compute_risk_reward({"size_usd": 100, "expected_fee": 0},
                                 {"win_rate": 0, "profit_factor": 1.0,
                                  "max_drawdown_pct": 0})
    assert rr3["ev"] == 0.0


def test_notification_send_success_and_failure():
    from notification import TradeNotifier
    n = TradeNotifier(smtp_user="u", smtp_password="p", to_addr="t")
    with mock.patch("notification.smtplib.SMTP_SSL") as sm:
        assert n.send_trade_alert({"side": "BUY", "type": "x", "size_usd": 1,
                                   "currency": "BTC"}, {}, None, "tok") is True
        assert sm.return_value.__enter__.called
    n2 = TradeNotifier(smtp_user="u", smtp_password="p", to_addr="t")
    with mock.patch("notification.smtplib.SMTP_SSL", side_effect=RuntimeError("boom")):
        assert n2.send_trade_alert({"side": "BUY"}, {}, None, "tok") is False


# ───────────────────────── market_universe ─────────────────────────

def test_quote_priority():
    from market_universe import _quote_priority, COINBASE_QUOTE_PRIORITY
    assert _quote_priority("USD") == 0
    assert _quote_priority("btc") == COINBASE_QUOTE_PRIORITY["BTC"]
    assert _quote_priority("ZZZ") == len(COINBASE_QUOTE_PRIORITY) + 1


def test_discover_coinbase_products():
    from market_universe import discover_coinbase_products
    products = [
        {"product_id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD",
         "status": "online"},
        {"product_id": "ETH-BTC", "base_currency": "ETH", "quote_currency": "BTC"},
        {"trading_disabled": True, "product_id": "X-USD"},
        {"status": "online"},  # no product_id
    ]
    conn = SimpleNamespace(list_products=lambda t: products)
    res = discover_coinbase_products(conn)
    assert len(res) == 2
    assert res[0].symbol == "BTC-USD"
    res2 = discover_coinbase_products(conn, max_pairs=1)
    assert len(res2) == 1
    conn3 = SimpleNamespace(list_products=lambda t: {"products": products[:2]})
    assert len(discover_coinbase_products(conn3)) == 2


def test_discover_prediction_markets():
    from market_universe import discover_prediction_markets
    mk = SimpleNamespace(platform="polymarket", market_id="m1", question="Will BTC rise?",
                         mid_price=0.6, volume=1000.0, liquidity_score=0.5,
                         spread=0.1, market_kind="prediction")
    client = SimpleNamespace(search_all_categories=lambda **k: {"crypto": [mk],
                                                                "sports": []})
    out = discover_prediction_markets(client)
    assert out["crypto"][0].actionable is True
    assert out["sports"] == []


def test_discover_stock_watchlist_and_master():
    from market_universe import discover_stock_watchlist, build_master_universe
    wl = discover_stock_watchlist()
    assert wl[0].symbol == "AAPL"
    conn = SimpleNamespace(list_products=lambda t: [])
    uni = build_master_universe(conn)
    assert "coinbase" in uni and "stocks" in uni
    assert uni["prediction_markets"] == {}
    pc = SimpleNamespace(search_all_categories=lambda **k: {})
    uni2 = build_master_universe(conn, prediction_market_client=pc, max_coinbase_pairs=5)
    assert uni2["prediction_markets"] == {}


def test_market_universe_main(capsys, monkeypatch):
    import market_universe as mu
    import sys
    monkeypatch.setattr(sys, "argv", ["market_universe"])
    mu.main()
    out = capsys.readouterr().out
    assert "watchlist" in out


# ───────────────────────── approval_server ─────────────────────────

def _make_handler(path, pending_file, auth_header=None):
    import approval_server as ap
    h = ap.ApprovalHandler.__new__(ap.ApprovalHandler)
    h.path = path
    h.headers = {"Authorization": auth_header} if auth_header else {}
    h.pending_file = pending_file
    h.server = None
    h.wfile = io.BytesIO()
    h._status = None

    def send_response(code, *a):
        h._status = code

    h.send_response = send_response
    h.send_header = lambda *a, **k: None
    h.end_headers = lambda: None
    h.log_message = lambda *a, **k: None
    return h


def test_approval_index(tmp_path):
    import approval_server as ap
    pf = tmp_path / "pending.json"
    pf.write_text("{}")
    h = _make_handler("/", str(pf))
    h.do_GET()
    assert h._status == 200


def test_approval_approve_deny(tmp_path):
    import approval_server as ap
    pf = tmp_path / "pending.json"
    pf.write_text(json.dumps({"tok1": {"side": "BUY", "currency": "BTC",
                                       "size_usd": 100, "type": "rebalance"}}))
    h = _make_handler("/approve/tok1", str(pf))
    h.do_GET()
    assert h._status == 200
    data = json.loads(pf.read_text())
    assert data["tok1"]["status"] == "approved"

    h2 = _make_handler("/approve/missing", str(pf))
    h2.do_GET()
    assert h2._status == 404

    h4 = _make_handler("/deny/tok1", str(pf))
    h4.do_GET()
    assert h4._status == 200
    data = json.loads(pf.read_text())
    assert data["tok1"]["status"] == "denied"

    h5 = _make_handler("/deny/missing", str(pf))
    h5.do_GET()
    assert h5._status == 404


def test_approval_status_and_api(tmp_path):
    import approval_server as ap
    pf = tmp_path / "pending.json"
    pf.write_text(json.dumps({"t": {"side": "BUY", "currency": "BTC",
                                    "size_usd": 100, "type": "x",
                                    "status": "approved",
                                    "stop_price": 1.0, "target_price": 2.0}}))
    ap.ApprovalHandler._auth_token = "secret"
    h = _make_handler("/status", str(pf))
    h.do_GET()
    assert h._status == 200

    ha = _make_handler("/api/status", str(pf), auth_header="Bearer secret")
    ha.do_GET()
    assert ha._status == 200

    hf = _make_handler("/api/status", str(pf))
    hf.do_GET()
    assert hf._status == 403

    hn = _make_handler("/nope", str(pf))
    hn.do_GET()
    assert hn._status == 404


def test_approval_render_helpers():
    import approval_server as ap
    h = ap.ApprovalHandler.__new__(ap.ApprovalHandler)
    assert h._parse_token("/approve/abc", "/approve/") == "abc"
    assert h._parse_token("/x", "/approve/") == ""
    assert h._bracket_detail_html({"bracket": True, "stop_price": 1.0,
                                   "target_price": 2.0}) != ""
    assert h._bracket_detail_html({}) == ""
    page = h._render_page("T", "M", "#28a745", {"bracket": True})
    assert "T" in page
    status = h._render_status({"t": {"created_at": "1", "status": "approved",
                                     "type": "x", "side": "BUY", "currency": "BTC",
                                     "size_usd": 100, "stop_price": 1.0,
                                      "target_price": 2.0, "reason": "r"}})
    assert "APPROVED" in status


def test_approval_serve_and_auth_token(monkeypatch, tmp_path):
    import approval_server as ap

    class FakeServer:
        def __init__(self, *a, **k):
            pass

        def serve_forever(self):
            raise KeyboardInterrupt()

        def shutdown(self):
            pass

    monkeypatch.setattr(ap, "HTTPServer", FakeServer)
    monkeypatch.delenv("APPROVAL_TOKEN", raising=False)
    pf = str(tmp_path / "p.json")
    ap.serve(pending_file=pf, port=9, host="127.0.0.1")
    assert os.path.exists(pf)

    monkeypatch.setenv("APPROVAL_TOKEN", "mytoken")
    ap.ApprovalHandler._auth_token = ""
    ap.serve(pending_file=pf, port=9, host="127.0.0.1")
    assert ap.ApprovalHandler._auth_token == "mytoken"


def test_approval_main(monkeypatch, tmp_path):
    import approval_server as ap
    import sys
    monkeypatch.setattr(ap, "serve", lambda **k: None)
    monkeypatch.setattr(sys, "argv", ["approval_server", "--pending-file",
                                      str(tmp_path / "p.json"), "--port", "9"])
    ap.main()
    # missing token generation branch
    monkeypatch.delenv("APPROVAL_TOKEN", raising=False)
    monkeypatch.setattr(ap, "serve", lambda **k: None)
    ap.main()


# ───────────────────────── neo4j_store ─────────────────────────

class _FakeRecord:
    def __init__(self, d):
        self._d = d

    def get(self, k, default=None):
        return self._d.get(k, default)

    def __getitem__(self, k):
        return self._d[k]


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        for r in self._rows:
            yield _FakeRecord(r)

    def single(self):
        return _FakeRecord(self._rows[0]) if self._rows else None

    def consume(self):
        return None


class _FakeSession:
    def __init__(self):
        self.plan = {}
        self.queries = []

    def set_plan(self, substr, rows):
        self.plan[substr] = rows

    def run(self, query, **params):
        self.queries.append(query)
        for k, v in self.plan.items():
            if k in query:
                return _FakeResult(v)
        return _FakeResult([])

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeDriver:
    def __init__(self, *a, **k):
        self.system_session = _FakeSession()
        self.default_session = _FakeSession()

    def session(self, database="neo4j"):
        s = _FakeSession() if database == "system" else self.default_session
        self._last = s
        return s

    def close(self):
        pass


def _make_store(monkeypatch):
    import neo4j_store
    drv = _FakeDriver()
    monkeypatch.setattr(neo4j_store.GraphDatabase, "driver", lambda *a, **k: drv)
    s = neo4j_store.Neo4jStore(uri="bolt://x", user="u", password="p",
                               database="trading")
    return s, drv


def test_neo4j_trades(monkeypatch):
    import neo4j_store
    s, drv = _make_store(monkeypatch)
    s.save_trade({"type": "rebalance", "side": "BUY", "currency": "BTC",
                  "size_usd": 100, "fee": 1, "reason": "r", "order_id": "o1",
                  "dry_run": True})
    drv.default_session.set_plan("RETURN t.id AS id",
                                 [{"id": "o1", "timestamp": "", "type": "rebalance",
                                   "side": "BUY", "currency": "BTC", "size_usd": 100,
                                   "fee": 1, "reason": "r", "order_id": "o1",
                                   "dry_run": 1}])
    rows = s.load_trades()
    assert rows[0]["currency"] == "BTC"


def test_neo4j_snapshots(monkeypatch):
    import neo4j_store
    s, drv = _make_store(monkeypatch)
    drv.default_session.set_plan("RETURN s.id AS id",
                                 [{"id": "1", "timestamp": "", "total_value": 200,
                                   "holding_count": 1, "usdc_balance": 50,
                                   "fee_volume_30d": 1, "fee_tier_min_volume": 1,
                                   "fee_tier_maker": 2, "fee_tier_taker": 3,
                                   "holdings_json": json.dumps({"BTC": {"currency": "BTC"}})}])
    s.save_snapshot(_fake_state())
    snaps = s.load_snapshots()
    assert snaps[0]["total_value"] == 200
    assert snaps[0]["holdings"]["BTC"]["currency"] == "BTC"


def test_neo4j_btcache(monkeypatch):
    import neo4j_store
    s, drv = _make_store(monkeypatch)
    v = _FakeVerdict()
    s.save_bt_cache("k", v)
    drv.default_session.set_plan("RETURN b.key AS key",
                                 [{"key": "k", "strategy": "ema", "currency": "BTC",
                                   "total_trades": 1, "winning_trades": 1,
                                   "losing_trades": 0, "win_rate": 0.6,
                                   "total_return_pct": 1, "sharpe_ratio": 1,
                                   "profit_factor": 1.5, "max_drawdown_pct": -1,
                                   "regime": "trend", "passed": True, "reason": "r"}])
    cache = s.load_bt_cache()
    assert cache["k"]["win_rate"] == 0.6
    s.prune_bt_cache(ttl=0)
    assert True


def test_neo4j_positions_meta_stats(monkeypatch):
    import neo4j_store
    s, drv = _make_store(monkeypatch)
    s.save_position_ages({"BTC": 3.0})
    drv.default_session.set_plan("RETURN p.currency AS currency",
                                 [{"currency": "BTC", "age": 3.0}])
    ages = s.load_position_ages()
    assert ages["BTC"] == 3.0
    s.set_meta("k", "v")
    drv.default_session.set_plan("RETURN m.value AS value",
                                 [{"value": "v"}])
    assert s.get_meta("k") == "v"
    drv.default_session.plan.clear()
    assert s.get_meta("missing", "d") == "d"
    drv.default_session.set_plan("RETURN count(t) AS c", [{"c": 5}])
    drv.default_session.set_plan("RETURN count(s) AS c", [{"c": 2}])
    drv.default_session.set_plan("RETURN count(b) AS c", [{"c": 1}])
    st = s.stats()
    assert st["trades"] == 5
    assert st["uri"] == "bolt://x"


def test_neo4j_ensure_database_fallback(monkeypatch):
    import neo4j_store

    class BadDriver(_FakeDriver):
        def session(self, database="neo4j"):
            if database == "system":
                raise RuntimeError("no system")
            return super().session(database=database)

    drv = BadDriver()
    monkeypatch.setattr(neo4j_store.GraphDatabase, "driver", lambda *a, **k: drv)
    s = neo4j_store.Neo4jStore(uri="bolt://x", user="u", password="p")
    assert s._database == "neo4j"
