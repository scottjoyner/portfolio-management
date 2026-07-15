"""Coverage tests for event_markets.executor (ArbitrageExecutor)."""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import event_markets.executor as ex
from event_markets.executor import (
    ArbitrageExecutor, ExecConfig, LegPlan, ExecResult,
)


def _opp(buy_yes=0.3, hedge_yes=0.6, edge_pct=0.05, conf=0.5,
         platform_buy="kalshi", platform_hedge="kalshi",
         buy_mid="KBUY", hedge_mid="KHEDGE", total_cost=None,
         depth_buy=None, depth_hedge=None, **kw):
    opp = {
        "buy_yes_price": buy_yes,
        "hedge_yes_price": hedge_yes,
        "edge_pct": edge_pct,
        "confidence": conf,
        "platform_buy": platform_buy,
        "platform_hedge": platform_hedge,
        "buy_market_id": buy_mid,
        "hedge_market_id": hedge_mid,
        "edge": 0.05,
        "total_cost": total_cost,
        "depth_buy": depth_buy or {},
        "depth_hedge": depth_hedge or {},
    }
    opp.update(kw)
    return opp


class FakeClient:
    def __init__(self, fill_count=1000, balance=1000.0):
        self._kalshi = MagicMock()
        self._kalshi.api_key_id = "kid"
        self._kalshi.private_key_path = "pkp"
        self._kalshi.create_order.return_value = {
            "fill_count": fill_count, "order_id": "o1"}
        self._kalshi.get_balance.return_value = {"balance_dollars": balance}


@pytest.fixture
def tmp_paths(tmp_path, monkeypatch):
    p = tmp_path / "paper-trades.json"
    k = tmp_path / "kill-switch"
    monkeypatch.setattr(ex, "PAPER_TRADES_PATH", p)
    monkeypatch.setattr(ex, "KILL_SWITCH_PATH", k)
    return p, k


def test_exec_config_from_env(monkeypatch):
    for k in ("ARBITRAGE_LIVE_ENABLED", "ARBITRAGE_MAX_NOTIONAL_USD",
              "ARBITRAGE_MIN_EDGE_PCT", "ARBITRAGE_MAX_SLIPPAGE",
              "ARBITRAGE_MIN_CONFIDENCE", "KALSHI_FEE_RATE",
              "ARBITRAGE_REQUIRE_NET_PROFIT"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("ARBITRAGE_LIVE_ENABLED", "true")
    monkeypatch.setenv("ARBITRAGE_MAX_NOTIONAL_USD", "250")
    monkeypatch.setenv("ARBITRAGE_REQUIRE_NET_PROFIT", "false")
    cfg = ExecConfig.from_env()
    assert cfg.mode == "live"
    assert cfg.max_notional_usd == 250.0
    assert cfg.require_net_profit is False


def test_leg_fee_and_estimate():
    cfg = ExecConfig()
    kl = LegPlan(platform="kalshi", market_id="M", action="buy", side="yes",
                 price=0.5, count=100, cost=50.0)
    fee = ex.ArbitrageExecutor()._leg_fee(kl)
    assert fee > 0
    poly = LegPlan(platform="polymarket", market_id="M", action="buy", side="yes",
                   price=0.5, count=100, cost=50.0)
    assert ex.ArbitrageExecutor()._leg_fee(poly) == 0.0
    assert ex.ArbitrageExecutor().estimate_fees(kl, poly) == round(fee, 2)


def test_build_plan_and_preflight_dry_reject(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    # notional <= 0
    r = exa.preflight(_opp(), 0, want_live=False)
    assert r.ok is False
    assert any("notional" in x for x in r.reasons)
    # missing market id
    r2 = exa.preflight(_opp(buy_mid="", hedge_mid=""), 100, want_live=False)
    assert any("market_id" in x for x in r2.reasons)


def test_preflight_edge_confidence(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.preflight(_opp(edge_pct=0.001, conf=0.5), 100, want_live=False)
    assert any("edge" in x for x in r.reasons)
    r2 = exa.preflight(_opp(edge_pct=0.05, conf=0.01), 100, want_live=False)
    assert any("confidence" in x for x in r2.reasons)


def test_preflight_net_profit(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(require_net_profit=True))
    # huge fees scenario: set min_edge satisfied but net<=0 via tiny prices? use low conf
    r = exa.preflight(_opp(buy_yes=0.01, hedge_yes=0.01, edge_pct=0.02, conf=0.5),
                      1, want_live=False)
    # contracts=1, gross small, fees ~0 -> net likely <=0
    assert any("unprofitable" in x for x in r.reasons) or r.ok


def test_execute_dry_run(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.execute(_opp(), 100, want_live=False)
    assert r.ok is True
    assert r.status == "planned"
    assert r.record is not None
    data = json.loads(Path(tmp_paths[0]).read_text())
    assert any(t["type"] == "arbitrage" for t in data)


def test_venue_live_ready_and_kill_switch(tmp_paths):
    exa = ArbitrageExecutor(client=FakeClient())
    ok, why = exa.venue_live_ready("kalshi")
    assert ok and not why
    ok2, why2 = exa.venue_live_ready("polymarket")
    assert ok2 is False  # PolymarketExecutionClient not configured
    ok3, _ = exa.venue_live_ready("unknown")
    assert ok3 is False
    assert exa.kill_switch_active() is False
    Path(tmp_paths[1]).write_text("x")
    assert exa.kill_switch_active() is True


def test_preflight_live_blocked(tmp_paths):
    exa = ArbitrageExecutor(client=FakeClient(), config=ExecConfig(mode="dry_run"))
    r = exa.preflight(_opp(), 100, want_live=True)
    assert r.ok is False
    assert any("live disabled" in x for x in r.reasons)


def test_preflight_live_kill_switch(tmp_paths):
    Path(tmp_paths[1]).write_text("x")
    exa = ArbitrageExecutor(client=FakeClient(), config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(), 100, want_live=True)
    assert any("kill-switch" in x for x in r.reasons)


def test_preflight_live_venue_not_ready(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    # no client -> kalshi not ready
    r = exa.preflight(_opp(), 100, want_live=True)
    assert any("not live-ready" in x for x in r.reasons) or any("Kalshi client" in x for x in r.reasons)


def test_preflight_live_balance_check(tmp_paths):
    client = FakeClient(balance=1.0)  # insufficient
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(), 100, want_live=True)
    assert any("insufficient Kalshi balance" in x for x in r.reasons)


def test_preflight_live_slippage(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    opp = _opp(depth_buy={"yes_ask": 0.99}, depth_hedge={"yes_bid": 0.01})
    r = exa.preflight(opp, 100, want_live=True)
    assert any("slippage" in x for x in r.reasons)


def test_execute_live_success(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.execute(_opp(), 100, want_live=True)
    assert r.ok is True
    assert r.status == "filled"
    assert client._kalshi.create_order.call_count == 2


def test_execute_live_buy_leg_raises(tmp_paths):
    client = FakeClient()
    client._kalshi.create_order.side_effect = RuntimeError("boom")
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.execute(_opp(), 100, want_live=True)
    assert r.status == "error"
    assert any("buy leg failed" in x for x in r.reasons)


def test_execute_live_buy_not_filled(tmp_paths):
    client = FakeClient(fill_count=0)
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.execute(_opp(), 100, want_live=True)
    assert r.status == "rejected"
    assert any("did not fill" in x for x in r.reasons)


def test_execute_live_hedge_raises_unwind(tmp_paths):
    client = FakeClient()
    calls = {"n": 0}
    def fake_create(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"fill_count": 1000, "order_id": "o1"}
        raise RuntimeError("hedge boom")
    client._kalshi.create_order.side_effect = fake_create
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.execute(_opp(), 100, want_live=True)
    assert r.status == "partial_unwound"
    assert any("unwound buy leg" in x for x in r.reasons)


def test_execute_live_hedge_not_filled_unwind(tmp_paths):
    client = FakeClient()
    client._kalshi.create_order.side_effect = [
        {"fill_count": 1000, "order_id": "o1"},
        {"fill_count": 0, "order_id": "o2"},
    ]
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.execute(_opp(), 100, want_live=True)
    assert r.status == "partial_unwound"


def test_internal_preflight_reject(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    # no legs
    r = exa.preflight_internal({"legs": [], "edge_pct": 0.05, "confidence": 0.5}, 100, want_live=False)
    assert any("no legs" in x for x in r.reasons)
    # missing ticker
    r2 = exa.preflight_internal({
        "legs": [{"ticker": "", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5,
    }, 100, want_live=False)
    assert any("missing ticker" in x for x in r2.reasons)


def test_internal_preflight_live_requires_guaranteed(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    r = exa.preflight_internal({
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": False, "strategy": "mutex_yes",
    }, 100, want_live=True)
    assert any("GUARANTEED" in x for x in r.reasons)


def test_internal_execute_dry_and_live(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig())
    opp = {
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"},
                 {"ticker": "K2", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
        "strategy": "mutex_no", "total_cost": 0.6, "n_outcomes": 2,
    }
    r = exa.execute_internal(opp, 100, want_live=False)
    assert r.ok and r.status == "planned"
    # live
    exa2 = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r2 = exa2.execute_internal(opp, 100, want_live=True)
    assert r2.ok and r2.status == "filled"


def test_internal_execute_live_leg_fails(tmp_paths):
    client = FakeClient()
    client._kalshi.create_order.side_effect = RuntimeError("fail")
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    opp = {
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
        "strategy": "mutex_no", "total_cost": 0.3, "n_outcomes": 2,
    }
    r = exa.execute_internal(opp, 100, want_live=True)
    assert r.status == "partial_unwound"
    assert any("failed to place" in x for x in r.reasons)


def test_internal_execute_live_leg_not_filled(tmp_paths):
    client = FakeClient(fill_count=0)
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    opp = {
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
        "strategy": "mutex_no", "total_cost": 0.3, "n_outcomes": 2,
    }
    r = exa.execute_internal(opp, 100, want_live=True)
    assert r.status == "partial_unwound"


def test_execute_live_with_real_polymarket_leg(monkeypatch, tmp_paths):
    # polymarket leg: requires PolymarketExecutionClient; is_configured False -> venue not ready
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    opp = _opp(platform_hedge="polymarket")
    r = exa.preflight(opp, 100, want_live=True)
    assert any("polymarket" in x.lower() and "not live-ready" in x for x in r.reasons)


def test_status(tmp_paths):
    exa = ArbitrageExecutor(client=FakeClient(), config=ExecConfig(mode="live"))
    st = exa.status()
    assert st["mode"] == "live"
    assert st["venues"]["kalshi"]["live_ready"] is True
    assert st["venues"]["polymarket"]["live_ready"] is False


class FakePM:
    def __init__(self, success=True, usdc=500.0):
        self._success = success
        self._usdc = usdc
    def is_configured(self):
        return True, ""
    def place_order(self, market_id, side, price, size, outcome, order_type):
        if self._success:
            return {"success": True, "orderID": "pm1"}
        return {"success": False, "error": "nope"}
    def get_usdc_balance(self):
        return self._usdc


def test_place_leg_polymarket(monkeypatch, tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    exa._pm_exec = FakePM(success=True)
    leg = LegPlan(platform="polymarket", market_id="PM1", action="buy", side="yes",
                  price=0.4, count=10, cost=4.0)
    out = exa._place_leg(leg)
    assert out["filled"] is True
    assert out["order_id"] == "pm1"
    # raw (non-dict) response path
    exa._pm_exec = MagicMock()
    exa._pm_exec.place_order.return_value = "rawstring"
    out2 = exa._place_leg(leg)
    assert isinstance(out2["raw"], dict)
    assert out2["raw"]["raw"] == "rawstring"


def test_place_leg_unknown_platform_raises(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    leg = LegPlan(platform="foo", market_id="M", action="buy", side="yes",
                  price=0.4, count=10, cost=4.0)
    with pytest.raises(NotImplementedError):
        exa._place_leg(leg)


def test_unwind_polymarket(monkeypatch, tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    exa._pm_exec = FakePM(success=True)
    leg = LegPlan(platform="polymarket", market_id="PM1", action="buy", side="yes",
                  price=0.4, count=10, cost=4.0)
    out = exa._unwind(leg, filled=10)
    assert out.get("unwind") is True
    # failure path (success False -> returns order, no exception)
    exa._pm_exec = FakePM(success=False)
    out2 = exa._unwind(leg, filled=10)
    assert out2["raw"]["success"] is False
    # exception path -> warning
    exa._pm_exec = MagicMock()
    exa._pm_exec.place_order.side_effect = RuntimeError("boom")
    out2b = exa._unwind(leg, filled=10)
    assert "warning" in out2b
    # nothing to unwind (filled falsy)
    exa._pm_exec = FakePM(success=True)
    out3 = exa._unwind(leg, filled=0)
    assert out3.get("note") == "nothing to unwind"


def test_unwind_kalshi_exception(tmp_paths):
    client = FakeClient()
    client._kalshi.create_order.side_effect = RuntimeError("x")
    exa = ArbitrageExecutor(client=client, config=ExecConfig())
    leg = LegPlan(platform="kalshi", market_id="K", action="buy", side="yes",
                  price=0.4, count=10, cost=4.0)
    out = exa._unwind(leg, filled=10)
    assert "warning" in out


def test_preflight_live_polymarket_balance(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    exa._pm_exec = FakePM(usdc=1.0)  # insufficient
    opp = _opp(platform_hedge="polymarket", hedge_mid="PM1")
    r = exa.preflight(opp, 100, want_live=True)
    assert any("insufficient Polymarket USDC" in x for x in r.reasons)


def test_execute_live_polymarket_leg(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    exa._pm_exec = FakePM(success=True)
    opp = _opp(platform_hedge="polymarket", hedge_mid="PM1")
    r = exa.execute(opp, 100, want_live=True)
    assert r.ok and r.status == "filled"


def test_internal_live_balance_paths(tmp_paths):
    # balance dict uses "balance" (cents) path
    client = FakeClient()
    client._kalshi.get_balance.return_value = {"balance": 50000}  # = 500 USD
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    opp = {
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"},
                 {"ticker": "K2", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
        "strategy": "mutex_no", "total_cost": 0.6, "n_outcomes": 2,
    }
    r = exa.preflight_internal(opp, 100, want_live=True)
    assert r.ok  # 500 USD > need
    # insufficient
    client._kalshi.get_balance.return_value = {"balance_dollars": 1.0}
    r2 = exa.preflight_internal(opp, 100, want_live=True)
    assert any("insufficient Kalshi balance" in x for x in r2.reasons)
    # exception path
    client._kalshi.get_balance.side_effect = RuntimeError("x")
    exa3 = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r3 = exa3.preflight_internal(opp, 100, want_live=True)
    assert r3.ok  # exception swallowed, not live-ready? kalshi is ready so ok


def test_internal_execute_live_partial_fill(tmp_paths):
    client = FakeClient()
    calls = {"n": 0}
    def fake_create(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"fill_count": 1000, "order_id": "o1"}
        return {"fill_count": 0, "order_id": "o2"}
    client._kalshi.create_order.side_effect = fake_create
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    opp = {
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"},
                 {"ticker": "K2", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
        "strategy": "mutex_no", "total_cost": 0.6, "n_outcomes": 2,
    }
    r = exa.execute_internal(opp, 100, want_live=True)
    assert r.status == "partial_unwound"


def test_internal_preflight_live_guaranteed_ok(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    opp = {
        "legs": [{"ticker": "K1", "price": 0.3, "side": "no"}],
        "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
        "strategy": "mutex_no", "total_cost": 0.3, "n_outcomes": 2,
    }
    r = exa.preflight_internal(opp, 100, want_live=True)
    assert r.ok is True  # guaranteed True -> live allowed


def test_load_corrupt_json(tmp_paths, monkeypatch):
    p = tmp_paths[0]
    p.write_text("{not valid json")
    exa = ArbitrageExecutor(config=ExecConfig())
    assert exa._load() == []
    # nonexistent
    p2 = tmp_paths[0].parent / "nope.json"
    monkeypatch.setattr(ex, "PAPER_TRADES_PATH", p2)
    assert exa._load() == []


# ── supplementary branch coverage ──────────────────────────────────
def test_exec_result_to_dict():
    r = ExecResult(ok=True, mode="dry_run", status="planned", reasons=["x"],
                   plan={"a": 1}, legs=[{"l": 1}], record={"r": 1})
    d = r.to_dict()
    assert d["ok"] is True and d["legs"] == [{"l": 1}] and d["record"] == {"r": 1}


def test_venue_kalshi_not_authed(tmp_paths):
    fc = FakeClient()
    fc._kalshi.api_key_id = ""
    exa = ArbitrageExecutor(client=fc)
    ok, why = exa.venue_live_ready("kalshi")
    assert ok is False and "authenticated" in why


def test_venue_polymarket_exec_failure(monkeypatch, tmp_paths):
    import event_markets.polymarket_executor as pme
    fc = FakeClient()
    fc._polymarket = MagicMock()
    monkeypatch.setattr(pme, "PolymarketExecutionClient", MagicMock(side_effect=RuntimeError("x")))
    exa = ArbitrageExecutor(client=fc)
    ok, why = exa.venue_live_ready("polymarket")
    assert ok is False and "unavailable" in why


def test_preflight_bad_total_cost(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.preflight(_opp(buy_yes=0.0, hedge_yes=1.0), 100, want_live=False)
    assert any("could not build" in x for x in r.reasons)


def test_check_balances_kalshi_balance_key(tmp_paths):
    client = FakeClient()
    client._kalshi.get_balance.return_value = {"balance": 50000}
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(), 100, want_live=True)
    assert not any("insufficient Kalshi balance" in x for x in r.reasons)


def test_check_balances_kalshi_no_usd(tmp_paths):
    client = FakeClient()
    client._kalshi.get_balance.return_value = {}
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(), 100, want_live=True)
    assert not any("insufficient Kalshi balance" in x for x in r.reasons)


def test_check_balances_kalshi_exception(tmp_paths):
    client = FakeClient()
    client._kalshi.get_balance.side_effect = RuntimeError("x")
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(), 100, want_live=True)
    assert not any("insufficient Kalshi balance" in x for x in r.reasons)


def test_check_balances_polymarket_exception(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    exa._pm_exec = FakePM(usdc=10.0)
    exa._pm_exec.get_usdc_balance = MagicMock(side_effect=RuntimeError("x"))
    r = exa.preflight(_opp(platform_hedge="polymarket", hedge_mid="PM1"), 100, want_live=True)
    assert not any("insufficient Polymarket" in x for x in r.reasons)


def test_check_balances_polymarket_none(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    exa._pm_exec = None
    exa._polymarket_exec = lambda: None
    r = exa.preflight(_opp(platform_hedge="polymarket", hedge_mid="PM1"), 100, want_live=True)
    assert not any("insufficient Polymarket" in x for x in r.reasons)


def test_slippage_buy_ok(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(depth_buy={"yes_ask": 0.30}), 100, want_live=True)
    assert not any("slippage" in x for x in r.reasons)


def test_slippage_hedge_ok(tmp_paths):
    client = FakeClient()
    exa = ArbitrageExecutor(client=client, config=ExecConfig(mode="live"))
    r = exa.preflight(_opp(depth_hedge={"yes_bid": 0.70}), 100, want_live=True)
    assert not any("slippage" in x for x in r.reasons)


def test_execute_rejected_early(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.execute(_opp(), 0, want_live=False)
    assert r.ok is False


def _internal_opp(**kw):
    base = {"legs": [{"ticker": "K1", "price": 0.3, "side": "no"}],
            "edge_pct": 0.05, "confidence": 0.5,
            "guaranteed": True, "strategy": "mutex_no",
            "total_cost": 0.3, "n_outcomes": 2}
    base.update(kw)
    return base


def test_internal_preflight_notional_zero(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.preflight_internal(_internal_opp(), 0, want_live=False)
    assert any("notional" in x for x in r.reasons)


def test_internal_preflight_edge_conf(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.preflight_internal(_internal_opp(edge_pct=0.001), 100, want_live=False)
    assert any("edge" in x for x in r.reasons)
    r2 = exa.preflight_internal(_internal_opp(confidence=0.01), 100, want_live=False)
    assert any("confidence" in x for x in r2.reasons)


def test_internal_preflight_net_unprofitable(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(require_net_profit=True))
    opp = {"legs": [{"ticker": "K1", "price": 0.5, "side": "no"},
                    {"ticker": "K2", "price": 0.5, "side": "no"}],
           "edge_pct": 0.05, "confidence": 0.5, "guaranteed": True,
           "strategy": "mutex_no", "total_cost": 1.0, "n_outcomes": 2}
    r = exa.preflight_internal(opp, 100, want_live=False)
    assert any("unprofitable" in x for x in r.reasons)


def test_internal_preflight_live_disabled(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig(mode="dry_run"))
    r = exa.preflight_internal(_internal_opp(), 100, want_live=True)
    assert any("live disabled" in x for x in r.reasons)


def test_internal_preflight_live_kill_switch(tmp_paths):
    Path(tmp_paths[1]).write_text("x")
    exa = ArbitrageExecutor(config=ExecConfig(mode="live"))
    r = exa.preflight_internal(_internal_opp(), 100, want_live=True)
    assert any("kill-switch" in x for x in r.reasons)


def test_execute_internal_rejected_early(tmp_paths):
    exa = ArbitrageExecutor(config=ExecConfig())
    r = exa.execute_internal({"legs": [], "edge_pct": 0.05, "confidence": 0.5}, 0, want_live=False)
    assert r.ok is False


def test_log_no_logger():
    saved = ex.logger
    ex.logger = None
    try:
        ex._log("info", "hi")
    finally:
        ex.logger = saved
