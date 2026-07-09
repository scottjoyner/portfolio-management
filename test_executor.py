"""Unit tests for the arbitrage executor (event_markets/executor.py).

These exercise the safety-critical logic (planning, preflight gating, dry-run
recording, live-mode refusal, one-leg unwind) with fully mocked venue clients —
NO network calls and NO real orders.

Run: python3 -m pytest test_executor.py -q
"""
import os
import tempfile
import types
from pathlib import Path

import pytest

import event_markets.executor as ex_mod
from event_markets.executor import ArbitrageExecutor, ExecConfig, LegPlan


# ── fakes ────────────────────────────────────────────────────────────
class FakeKalshi:
    def __init__(self, balance_usd=1000.0, fill=True, raise_on=None):
        self.api_key_id = "test-key-id"
        self.private_key_path = "/tmp/fake.pem"
        self._bal = balance_usd
        self._fill = fill
        self._raise_on = raise_on or set()
        self.orders = []

    def get_balance(self):
        return {"balance_dollars": self._bal}

    def create_order(self, ticker, side, action, count, price, time_in_force=None):
        self.orders.append((ticker, side, action, count, price, time_in_force))
        if action in self._raise_on:
            raise RuntimeError("boom")
        fc = count if self._fill else 0
        return {"order_id": f"k-{len(self.orders)}", "fill_count": fc}


class FakePMExec:
    def __init__(self, configured=True, usdc=1000.0, fill=True):
        self._configured = configured
        self._usdc = usdc
        self._fill = fill
        self.orders = []

    def is_configured(self):
        return (True, "") if self._configured else (False, "no key")

    def get_usdc_balance(self):
        return self._usdc

    def place_order(self, market_id, side, price, size, outcome="yes", order_type="FOK"):
        self.orders.append((market_id, side, price, size, outcome))
        return {"success": self._fill, "orderID": f"pm-{len(self.orders)}"}

    def address(self):
        return "0xTEST"


class FakeUnified:
    def __init__(self, kalshi):
        self._kalshi = kalshi
        self._polymarket = None


def _opp():
    return {
        "event_key": "EVT", "category": "crypto",
        "platform_buy": "kalshi", "platform_hedge": "polymarket",
        "buy_market_id": "KX-T", "hedge_market_id": "0xcond",
        "buy_yes_price": 0.40, "hedge_yes_price": 0.55, "total_cost": 0.85,
        "edge": 0.15, "edge_pct": 0.15, "confidence": 0.8,
    }


@pytest.fixture(autouse=True)
def _tmp_trades(monkeypatch, tmp_path):
    # Redirect the paper-trades sink so tests never touch real data.
    p = tmp_path / "paper-trades.json"
    monkeypatch.setattr(ex_mod, "PAPER_TRADES_PATH", p)
    monkeypatch.setattr(ex_mod, "KILL_SWITCH_PATH", tmp_path / "kill")
    return p


def _executor(kalshi=None, pm=None, mode="dry_run", **cfg):
    kalshi = kalshi or FakeKalshi()
    ex = ArbitrageExecutor(client=FakeUnified(kalshi), config=ExecConfig(mode=mode, **cfg))
    if pm is not None:
        ex._pm_exec = pm
    return ex


# ── planning ─────────────────────────────────────────────────────────
def test_build_plan_sizes_by_total_cost():
    ex = _executor()
    buy, hedge, n = ex.build_plan(_opp(), notional_usd=85)
    assert n == 100  # 85 / 0.85
    assert buy.side == "yes" and buy.price == 0.40
    assert hedge.side == "no" and hedge.price == pytest.approx(0.45)  # 1 - 0.55


def test_notional_capped_at_max():
    ex = _executor(max_notional_usd=10)
    r = ex.preflight(_opp(), notional_usd=1000, want_live=False)
    assert r.plan["notional_usd"] == 10.0


# ── preflight gating ─────────────────────────────────────────────────
def test_reject_low_edge():
    ex = _executor(min_edge_pct=0.20)
    r = ex.preflight(_opp(), 50, want_live=False)
    assert not r.ok and any("edge" in x for x in r.reasons)


def test_reject_low_confidence():
    ex = _executor(min_confidence=0.9)
    r = ex.preflight(_opp(), 50, want_live=False)
    assert not r.ok and any("confidence" in x for x in r.reasons)


def test_live_refused_when_mode_dry_run():
    ex = _executor(pm=FakePMExec(configured=True), mode="dry_run")
    r = ex.preflight(_opp(), 50, want_live=True)
    assert not r.ok and any("live disabled" in x for x in r.reasons)


def test_live_refused_when_pm_not_configured():
    ex = _executor(pm=FakePMExec(configured=False), mode="live")
    r = ex.preflight(_opp(), 50, want_live=True)
    assert not r.ok and any("polymarket" in x.lower() for x in r.reasons)


def test_live_refused_insufficient_balance():
    ex = _executor(kalshi=FakeKalshi(balance_usd=1.0),
                   pm=FakePMExec(configured=True, usdc=1.0), mode="live")
    r = ex.preflight(_opp(), 50, want_live=True)
    assert not r.ok
    assert any("Kalshi balance" in x for x in r.reasons)
    assert any("Polymarket USDC" in x for x in r.reasons)


def test_kill_switch_blocks_live(monkeypatch):
    ex = _executor(pm=FakePMExec(configured=True), mode="live")
    ex_mod.KILL_SWITCH_PATH.write_text("stop")
    r = ex.preflight(_opp(), 50, want_live=True)
    assert not r.ok and any("kill-switch" in x for x in r.reasons)


# ── dry-run execution ────────────────────────────────────────────────
def test_dry_run_records_and_places_nothing(_tmp_trades):
    kalshi = FakeKalshi()
    ex = _executor(kalshi=kalshi, pm=FakePMExec(configured=True))
    r = ex.execute(_opp(), notional_usd=50, want_live=False)
    assert r.ok and r.mode == "dry_run" and r.status == "planned"
    assert kalshi.orders == []           # nothing placed
    assert _tmp_trades.exists()
    import json
    recs = json.loads(_tmp_trades.read_text())
    assert len(recs) == 1 and recs[0]["mode"] == "dry_run"


# ── live execution (mocked venues) ───────────────────────────────────
def test_live_both_legs_fill():
    kalshi = FakeKalshi(fill=True)
    pm = FakePMExec(configured=True, fill=True)
    ex = _executor(kalshi=kalshi, pm=pm, mode="live")
    r = ex.execute(_opp(), notional_usd=50, want_live=True)
    assert r.ok and r.status == "filled"
    assert len(kalshi.orders) == 1 and len(pm.orders) == 1


def test_live_hedge_fails_triggers_unwind():
    kalshi = FakeKalshi(fill=True)
    pm = FakePMExec(configured=True, fill=False)  # hedge won't fill
    ex = _executor(kalshi=kalshi, pm=pm, mode="live")
    r = ex.execute(_opp(), notional_usd=50, want_live=True)
    assert not r.ok and r.status == "partial_unwound"
    # buy leg placed once, then an unwind sell on kalshi
    actions = [o[2] for o in kalshi.orders]
    assert "buy" in actions and "sell" in actions


def test_live_buy_leg_no_fill_no_hedge():
    kalshi = FakeKalshi(fill=False)
    pm = FakePMExec(configured=True, fill=True)
    ex = _executor(kalshi=kalshi, pm=pm, mode="live")
    r = ex.execute(_opp(), notional_usd=50, want_live=True)
    assert not r.ok and r.status == "rejected"
    assert len(pm.orders) == 0           # never placed hedge


def test_live_kalshi_legs_use_fill_or_kill():
    # FOK (all-or-nothing) prevents a partial fill that would be mis-read as
    # unfilled and left as a naked, un-hedged, un-unwound position.
    kalshi = FakeKalshi(fill=True)
    pm = FakePMExec(configured=True, fill=True)
    ex = _executor(kalshi=kalshi, pm=pm, mode="live")
    ex.execute(_opp(), notional_usd=50, want_live=True)
    tifs = [o[5] for o in kalshi.orders]
    assert tifs and all(t == "fill_or_kill" for t in tifs)


# ── fee awareness ────────────────────────────────────────────────────
def test_leg_fee_kalshi_and_polymarket():
    ex = _executor()
    # Kalshi: ceil(0.07 * C * P * (1-P)) rounded up to next cent.
    kalshi_leg = LegPlan(platform="kalshi", market_id="K", action="buy",
                         side="yes", price=0.50, count=100, cost=50.0)
    # 0.07 * 100 * 0.5 * 0.5 = 1.75 -> 1.75
    assert ex._leg_fee(kalshi_leg) == pytest.approx(1.75)
    # Polymarket charges no trading fee.
    pm_leg = LegPlan(platform="polymarket", market_id="P", action="buy",
                     side="no", price=0.50, count=100, cost=50.0)
    assert ex._leg_fee(pm_leg) == 0.0


def test_plan_includes_fee_and_net_profit():
    ex = _executor()
    r = ex.preflight(_opp(), notional_usd=100, want_live=False)
    assert "estimated_fees" in r.plan and "net_expected_profit" in r.plan
    assert r.plan["net_expected_profit"] == pytest.approx(
        r.plan["expected_profit"] - r.plan["estimated_fees"]
    )


def test_reject_unprofitable_after_fees():
    # edge just clears min_edge_pct but Kalshi fee exceeds the gross profit.
    opp = _opp()
    opp.update(buy_yes_price=0.50, hedge_yes_price=0.51, total_cost=0.99,
               edge=0.01, edge_pct=0.01, confidence=0.8)
    ex = _executor()
    r = ex.preflight(opp, notional_usd=100, want_live=False)
    assert not r.ok
    assert any("unprofitable after fees" in x for x in r.reasons)


def test_allow_unprofitable_when_net_check_disabled():
    opp = _opp()
    opp.update(buy_yes_price=0.50, hedge_yes_price=0.51, total_cost=0.99,
               edge=0.01, edge_pct=0.01, confidence=0.8)
    ex = _executor(require_net_profit=False)
    r = ex.preflight(opp, notional_usd=100, want_live=False)
    assert r.ok


# ── Kalshi internal (N-leg) arbitrage ────────────────────────────────
def _internal_opp(strategy="mutex_no", guaranteed=True):
    # 2-outcome mutex_no lock: buy NO on both @0.40; payout n-1=1; cost 0.80.
    return {
        "type": "kalshi_internal", "event_ticker": "EVT", "event_title": "Event",
        "category": "Politics", "strategy": strategy, "guaranteed": guaranteed,
        "n_outcomes": 2, "total_cost": 0.80, "net_edge": 0.166, "edge_pct": 0.20,
        "confidence": 0.8,
        "legs": [
            {"ticker": "A", "side": "no", "price": 0.40},
            {"ticker": "B", "side": "no", "price": 0.40},
        ],
    }


def test_internal_dry_run_records_no_orders(_tmp_trades):
    kalshi = FakeKalshi()
    ex = _executor(kalshi=kalshi)
    r = ex.execute_internal(_internal_opp(), notional_usd=80, want_live=False)
    assert r.ok and r.mode == "dry_run" and r.status == "planned"
    assert kalshi.orders == []
    import json
    recs = json.loads(_tmp_trades.read_text())
    assert recs[0]["type"] == "kalshi_internal" and recs[0]["strategy"] == "mutex_no"


def test_internal_live_all_legs_fill():
    kalshi = FakeKalshi(fill=True)
    ex = _executor(kalshi=kalshi, mode="live")
    r = ex.execute_internal(_internal_opp(), notional_usd=80, want_live=True)
    assert r.ok and r.status == "filled"
    # 2 legs, both bought on kalshi
    assert len([o for o in kalshi.orders if o[2] == "buy"]) == 2
    assert all(o[5] == "fill_or_kill" for o in kalshi.orders)


def test_internal_live_leg_fail_unwinds_prior():
    # First leg fills, second fails to fill -> unwind the first.
    class PartialKalshi(FakeKalshi):
        def create_order(self, ticker, side, action, count, price, time_in_force=None):
            self.orders.append((ticker, side, action, count, price, time_in_force))
            # buy on A fills, buy on B does not; sells (unwind) succeed
            if action == "buy":
                fc = count if ticker == "A" else 0
            else:
                fc = count
            return {"order_id": f"k-{len(self.orders)}", "fill_count": fc}
    kalshi = PartialKalshi(fill=True)
    ex = _executor(kalshi=kalshi, mode="live")
    r = ex.execute_internal(_internal_opp(), notional_usd=80, want_live=True)
    assert not r.ok and r.status == "partial_unwound"
    actions = [o[2] for o in kalshi.orders]
    assert "sell" in actions   # unwound the filled A leg


def test_internal_live_blocked_when_not_guaranteed():
    kalshi = FakeKalshi()
    ex = _executor(kalshi=kalshi, mode="live")
    r = ex.preflight_internal(_internal_opp(strategy="mutex_yes", guaranteed=False),
                              notional_usd=80, want_live=True)
    assert not r.ok
    assert any("GUARANTEED" in x for x in r.reasons)


def test_internal_live_insufficient_balance():
    kalshi = FakeKalshi(balance_usd=1.0)
    ex = _executor(kalshi=kalshi, mode="live")
    r = ex.preflight_internal(_internal_opp(), notional_usd=80, want_live=True)
    assert not r.ok
    assert any("Kalshi balance" in x for x in r.reasons)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
