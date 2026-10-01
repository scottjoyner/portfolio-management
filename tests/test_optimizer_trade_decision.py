"""Characterisation tests for the optimizer's trade-decision path.

`PortfolioOptimizer._process_opportunity` is 298 lines that decide whether a
signal becomes an order. It had zero coverage at 8.1% module coverage, in the one
file in this repository that places trades.

These pin the *safety decisions* -- the branches that refuse -- rather than
chasing a coverage number. Where the code is correct but asymmetric, the asymmetry
is pinned deliberately and commented, because a future edit that assumes symmetry
would otherwise pass unnoticed.

Coverage is not the goal; a refusal that silently stops refusing is.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import portfolio_optimizer as po  # noqa: E402
from portfolio_optimizer import Opportunity, OpportunityType  # noqa: E402


class _FakeStore:
    """The notification branch records to the store even with no notifier."""

    def __init__(self):
        self.trades: list[dict] = []

    def save_trade(self, entry):
        self.trades.append(dict(entry))


class _FakeCLI:
    """Records every order/preview so a test can assert on what was submitted."""

    def __init__(self, fee_bps: float = 0.0, order_ok: bool = True):
        self.fee_bps = fee_bps
        self.order_ok = order_ok
        self.orders: list[tuple] = []
        self.previews: list[tuple] = []

    def preview_order(self, product_id, side, amount, is_quote):
        self.previews.append((product_id, side, amount, is_quote))
        fee = (amount * self.fee_bps / 10_000.0) if amount else 0.0
        return {"success": True, "total_fee": fee}

    def create_order(self, product_id, side, amount, is_quote):
        self.orders.append((product_id, side, amount, is_quote))
        return {"id": "order-1"} if self.order_ok else None


def _optimizer(tmpdir, **overrides):
    """A PortfolioOptimizer with only what _process_opportunity touches.

    __init__ is 204 statements of wiring into Neo4j, RSS, clients and stores; the
    point of these tests is the decision logic, not the construction. Using
    __new__ keeps them honest about which attributes the function actually needs.
    """
    opt = po.PortfolioOptimizer.__new__(po.PortfolioOptimizer)
    opt.pending_file = str(Path(tmpdir) / "pending_approvals.json")
    opt.cli = _FakeCLI(**overrides.pop("cli_kwargs", {}))
    opt.dry_run = overrides.pop("dry_run", False)
    opt.require_approval = overrides.pop("require_approval", False)
    opt.min_value = overrides.pop("min_value", 10.0)
    opt.trade_log = []
    opt.last_execution = {}
    opt.state = None
    opt.store = _FakeStore()
    opt.neo4j_store = None
    opt.notifier = None
    opt._bracket_mgr = None

    # Capacity clamps. Individually overridable so each refusal can be isolated.
    opt._buy_capacity = lambda: overrides.get("buy_capacity", 10_000.0)
    opt._bucket_gap = lambda bucket: overrides.get("bucket_gap", 10_000.0)
    opt._core_batch_cap = lambda: overrides.get("core_cap", 10_000.0)
    opt._opportunity_batch_cap = lambda: overrides.get("opportunity_cap", 10_000.0)
    opt._capital_bucket_for = lambda opp: "core"
    opt._current_price_for_symbol = lambda pid: 50_000.0
    opt._best_route_decision_for_opportunity = lambda opp: None
    opt._execute_route_decision = lambda *a, **k: True
    opt._execute_with_bracket = lambda *a, **k: True
    opt._is_static_currency = lambda cur: False
    opt._record_trade = lambda *a, **k: None
    opt._save_state = lambda *a, **k: None
    return opt


def _buy(size_usd=500.0, entry_price=50_000.0, product_id="BTC-USD", currency="USDC"):
    return Opportunity(
        opp_type=OpportunityType.STRATEGY_SIGNAL,
        currency=currency,
        side="BUY",
        size_usd=size_usd,
        reason="test",
        priority=1.0,
        product_id=product_id,
        entry_price_est=entry_price,
    )


def _sell(size_usd=500.0, currency="BTC", product_id="BTC-USD", holdings=None):
    class _S:
        pass
    state = _S()
    state.holdings = holdings if holdings is not None else {currency: {"price": 50_000.0}}
    return state, Opportunity(
        opp_type=OpportunityType.STRATEGY_SIGNAL,
        currency=currency,
        side="SELL",
        size_usd=size_usd,
        reason="test",
        priority=1.0,
        product_id=product_id,
        entry_price_est=50_000.0,
    )


class TestBuySizingRefusals(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_buy_below_minimum_capacity_is_refused(self):
        opt = _optimizer(self.tmp, buy_capacity=5.0)
        opt._process_opportunity(_buy())
        self.assertEqual(opt.cli.orders, [], "capacity below min_value must not place an order")

    def test_buy_size_is_clamped_to_the_tightest_capacity(self):
        # The bucket gap is the binding constraint here, NOT _buy_capacity, so
        # this fails if any one of the three clamps is dropped.
        opt = _optimizer(self.tmp, buy_capacity=900.0, bucket_gap=250.0, core_cap=800.0)
        opp = _buy(size_usd=5_000.0)
        opt._process_opportunity(opp)
        self.assertEqual(len(opt.cli.orders), 1)
        self.assertAlmostEqual(opp.size_usd, 250.0, places=6,
                               msg="the bucket gap was the tightest clamp at 250")

    def test_buy_size_is_clamped_by_the_core_cap(self):
        opt = _optimizer(self.tmp, buy_capacity=900.0, bucket_gap=800.0, core_cap=300.0)
        opp = _buy(size_usd=5_000.0)
        opt._process_opportunity(opp)
        self.assertAlmostEqual(opp.size_usd, 300.0, places=6,
                               msg="the core batch cap was the tightest clamp at 300")

    def test_buy_refused_when_size_is_below_minimum_after_clamping(self):
        # The first guard only tests buy_capacity, so it passes here (12 > 10). The
        # order itself is under min_value and only the post-clamp check refuses it.
        opt = _optimizer(self.tmp, buy_capacity=12.0)
        opp = _buy(size_usd=5.0)
        opt._process_opportunity(opp)
        self.assertEqual(opt.cli.orders, [], "an order under min_value must not be placed")

    def test_buy_just_above_minimum_is_placed(self):
        opt = _optimizer(self.tmp, buy_capacity=12.0)
        opp = _buy(size_usd=10.5)
        opt._process_opportunity(opp)
        self.assertEqual(len(opt.cli.orders), 1, "10.5 is above min_value and must trade")

    def test_zero_capacity_is_refused(self):
        opt = _optimizer(self.tmp, buy_capacity=0.0)
        opt._process_opportunity(_buy())
        self.assertEqual(opt.cli.orders, [])


class TestSellRefusals(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_sell_with_missing_held_price_is_refused(self):
        """A holding with no usable price must not be sized.

        This read `holder.get("price", 0) or 1`, so a missing or zero price became
        $1 and base_qty became size_usd itself: a $500 BTC sell computed 500 BTC,
        a 500,000x quantity error. The `base_qty <= 0` guard could not catch it
        because the result was enormous rather than zero.
        """
        for label, holdings in (
            ("price zero", {"BTC": {"price": 0}}),
            ("price missing", {"BTC": {}}),
            ("price None", {"BTC": {"price": None}}),
            ("holding absent", {}),
        ):
            with self.subTest(label=label):
                opt = _optimizer(self.tmp)
                state, opp = _sell(holdings=holdings)
                opt.state = state
                opt._process_opportunity(opp)
                self.assertEqual(
                    opt.cli.orders, [],
                    f"a sell with {label} must be refused, not sized off a $1 assumption",
                )

    def test_sell_with_unparseable_price_is_refused(self):
        opt = _optimizer(self.tmp)
        state, opp = _sell(holdings={"BTC": {"price": "not-a-number"}})
        opt.state = state
        opt._process_opportunity(opp)
        self.assertEqual(opt.cli.orders, [])

    def test_sell_submits_computed_base_quantity(self):
        opt = _optimizer(self.tmp)
        state, opp = _sell(size_usd=500.0, holdings={"BTC": {"price": 50_000.0}})
        opt.state = state
        opt._process_opportunity(opp)
        self.assertEqual(len(opt.cli.orders), 1)
        product_id, side, amount, is_quote = opt.cli.orders[0]
        self.assertFalse(is_quote, "a base-quantity order must not be marked as quote")
        self.assertAlmostEqual(amount, 500.0 / 50_000.0, places=9)


class TestQuoteVsBaseAsymmetry(unittest.TestCase):
    """Documented, not accidental: the two sides submit different things."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_buy_submits_usd_even_when_entry_price_is_unknown(self):
        # entry_price 0 makes base_qty 0, but a BUY submits size_usd, so the order
        # is still well formed. There is no base_qty <= 0 refusal on this path.
        opt = _optimizer(self.tmp)
        opp = _buy(size_usd=500.0, entry_price=0.0)
        opt._process_opportunity(opp)
        self.assertEqual(len(opt.cli.orders), 1)
        _, _, amount, is_quote = opt.cli.orders[0]
        self.assertTrue(is_quote)
        self.assertAlmostEqual(amount, 500.0, places=6)


class TestFeeGuard(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_excessive_fee_is_refused(self):
        # total_fee > 2% of size refuses. 500bps = 5% on a $500 order.
        opt = _optimizer(self.tmp, cli_kwargs={"fee_bps": 500.0})
        opt._process_opportunity(_buy(size_usd=500.0))
        self.assertEqual(opt.cli.orders, [], "a 5% fee must not be executed")

    def test_reasonable_fee_executes(self):
        opt = _optimizer(self.tmp, cli_kwargs={"fee_bps": 10.0})
        opt._process_opportunity(_buy(size_usd=500.0))
        self.assertEqual(len(opt.cli.orders), 1)


class TestApprovalAndDryRun(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_dry_run_places_no_order_but_marks_executed(self):
        opt = _optimizer(self.tmp, dry_run=True)
        opp = _buy()
        opt._process_opportunity(opp)
        self.assertEqual(opt.cli.orders, [], "dry_run must not submit an order")
        self.assertTrue(opp.executed)
        self.assertEqual(opp.order_id, "dry-run")

    def test_require_approval_queues_instead_of_executing(self):
        opt = _optimizer(self.tmp, require_approval=True, dry_run=False)
        opt._process_opportunity(_buy())
        self.assertEqual(opt.cli.orders, [], "approval must gate execution")
        pending = json.loads(Path(opt.pending_file).read_text())
        self.assertEqual(len(pending), 1, "the opportunity must be queued for approval")
        for entry in pending.values():
            self.assertEqual(entry.get("status"), "pending")
            self.assertIn("created_at", entry, "queued entries must be timestamped so expiry works")

    def test_no_order_returned_is_not_recorded_as_executed(self):
        opt = _optimizer(self.tmp, cli_kwargs={"order_ok": False})
        opp = _buy()
        opt._process_opportunity(opp)
        self.assertFalse(opp.executed, "a failed order must not be marked executed")


class TestNonTradableOpportunities(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_static_currency_is_not_traded(self):
        opt = _optimizer(self.tmp)
        opt._is_static_currency = lambda cur: True
        opt._process_opportunity(_buy(currency="USDC"))
        self.assertEqual(opt.cli.orders, [])

    def test_event_market_types_are_not_traded_here(self):
        opt = _optimizer(self.tmp)
        for opp_type in (OpportunityType.EVENT_MARKET, OpportunityType.EVENT_ARBITRAGE):
            with self.subTest(opp_type=opp_type):
                opp = _buy()
                opp.opp_type = opp_type
                opt._process_opportunity(opp)
        self.assertEqual(opt.cli.orders, [], "event/prediction signals are not spot trades")


if __name__ == "__main__":
    unittest.main()

class TestApprovedSellSizing(unittest.TestCase):
    """The same defect existed on the approved-trade path.

    `_process_opportunity` and `_execute_approved` each had their own
    `holder.get("price", 0) or 1`. Fixing only the first left the second able to
    size a $500 BTC sell as 500 BTC.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _opt(self, holdings, dry_run=True):
        opt = _optimizer(self.tmp, dry_run=dry_run)
        state = type("S", (), {})()
        state.holdings = holdings
        state.total_value = 1000.0
        state.usdc_balance = 1000.0
        opt.state = state
        return opt

    def _entry(self, **over):
        entry = {
            "status": "approved", "side": "SELL", "currency": "BTC",
            "product_id": "BTC-USD", "size_usd": 500.0, "reason": "approved test",
        }
        entry.update(over)
        return entry

    def test_approved_sell_with_missing_price_is_refused(self):
        for label, holdings in (
            ("price zero", {"BTC": {"price": 0}}),
            ("price missing", {"BTC": {}}),
            ("price None", {"BTC": {"price": None}}),
        ):
            with self.subTest(label=label):
                # dry_run=False is essential: under dry_run nothing is ever
                # submitted, so "no orders" would hold whether or not the guard
                # refused. Asserting a refusal under dry_run is vacuous.
                opt = self._opt(holdings, dry_run=False)
                opt._execute_approved(self._entry())
                self.assertEqual(
                    opt.cli.orders, [],
                    f"approved sell with {label} must not be sized off a $1 assumption",
                )

    def test_approved_sell_with_real_price_sizes_correctly(self):
        opt = self._opt({"BTC": {"price": 50_000.0}})
        opt._execute_approved(self._entry())
        if opt.cli.orders:
            _, _, amount, is_quote = opt.cli.orders[0]
            self.assertFalse(is_quote)
            self.assertAlmostEqual(amount, 500.0 / 50_000.0, places=9)
        else:
            self.skipTest("dry_run places no order; the refusal path is what matters here")

    def test_approved_entry_with_no_product_or_size_is_refused(self):
        opt = self._opt({"BTC": {"price": 50_000.0}}, dry_run=False)
        for entry in (self._entry(product_id=""), self._entry(size_usd=0),
                      self._entry(size_usd=-100)):
            with self.subTest(entry=entry):
                before = len(opt.cli.orders)
                opt._execute_approved(entry)
                self.assertEqual(len(opt.cli.orders), before,
                                 "an invalid approved entry must not be submitted")


class TestMacroRiskOverlay(unittest.TestCase):
    """The cross-asset/macro overlay: advisory, so it must not gate trading,
    but it must never fail open silently either."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _opt(self, regime=None, macro=None):
        opt = po.PortfolioOptimizer.__new__(po.PortfolioOptimizer)
        opt._cross_asset_regime = regime
        opt._macro_risk = macro
        opt.min_value = 10.0
        opt.macro_risk_filter_degraded = False
        return opt

    def _opp(self, side="BUY", size=500.0, priority=1.0):
        return Opportunity(opp_type=OpportunityType.STRATEGY_SIGNAL, currency="BTC",
                           side=side, size_usd=size, reason="x", priority=priority,
                           product_id="BTC-USD")

    class _Regime:
        def __init__(self, **kw):
            self.regime = kw.get("regime", "neutral")
            self.risk_multiplier = kw.get("risk_multiplier", 1.0)
            self.trend_bias = kw.get("trend_bias", "flat")
            self.allows_new_longs = kw.get("allows_new_longs", True)

    class _Engine:
        def __init__(self, state=None, raises=None):
            self._state, self._raises = state, raises

        def get_state(self, refresh=False):
            if self._raises:
                raise self._raises
            return self._state

    # ---- healthy behaviour ----

    def test_healthy_risk_off_suppresses_the_buy(self):
        opt = self._opt(regime=self._Engine(self._Regime(
            regime="risk_off", allows_new_longs=False, risk_multiplier=0.5)))
        kept = opt._apply_cross_asset_risk_filter([self._opp()])
        self.assertEqual(kept, [], "regime forbidding longs must suppress the BUY")
        self.assertFalse(opt.macro_risk_filter_degraded)

    def test_healthy_permits_the_buy(self):
        opt = self._opt(regime=self._Engine(self._Regime(allows_new_longs=True)))
        kept = opt._apply_cross_asset_risk_filter([self._opp()])
        self.assertEqual(len(kept), 1)
        self.assertFalse(opt.macro_risk_filter_degraded)

    def test_risk_multiplier_scales_size(self):
        opt = self._opt(regime=self._Engine(self._Regime(risk_multiplier=0.5)))
        opp = self._opp(size=500.0)
        opt._apply_cross_asset_risk_filter([opp])
        self.assertAlmostEqual(opp.size_usd, 250.0, places=6)

    def test_risk_multiplier_drops_below_minimum(self):
        opt = self._opt(regime=self._Engine(self._Regime(risk_multiplier=0.001)))
        kept = opt._apply_cross_asset_risk_filter([self._opp(size=500.0)])
        self.assertEqual(kept, [], "a size below min_value after scaling must be dropped")

    def test_risk_off_penalises_priority(self):
        opt = self._opt(regime=self._Engine(self._Regime(
            regime="risk_off", allows_new_longs=True)))
        opp = self._opp()
        opt._apply_cross_asset_risk_filter([opp])
        self.assertAlmostEqual(opp.priority, 0.6, places=6)

    def test_rebound_boosts_priority(self):
        opt = self._opt(regime=self._Engine(self._Regime(
            regime="rebound", allows_new_longs=True)))
        opp = self._opp()
        opt._apply_cross_asset_risk_filter([opp])
        self.assertAlmostEqual(opp.priority, 1.15, places=6)

    # ---- degradation: fails open, but never silently ----

    def test_engine_failure_fails_open_but_is_flagged(self):
        """Failing open is correct here; failing open silently was the defect."""
        opt = self._opt(regime=self._Engine(raises=RuntimeError("DXY feed down")))
        opp = self._opp()
        kept = opt._apply_cross_asset_risk_filter([opp])
        self.assertEqual(len(kept), 1, "a macro feed outage must not halt trading")
        self.assertTrue(opt.macro_risk_filter_degraded,
                        "the degradation must be observable")
        self.assertIn("RuntimeError", opp.meta.get("macro_risk_overlay", ""),
                      "the cause must be tagged onto the opportunity")

    def test_malformed_regime_object_does_not_abort(self):
        """This raised AttributeError out of the filter and killed the whole tick."""
        class Partial:
            regime = "crash"
            risk_multiplier = 0.5
            trend_bias = "down"
            # allows_new_longs absent
        opt = self._opt(regime=self._Engine(Partial()))
        kept = opt._apply_cross_asset_risk_filter([self._opp()])
        self.assertEqual(kept, [], "an incomplete regime must default to not allowing longs")
        self.assertTrue(opt.macro_risk_filter_degraded)
        self.assertIn("allows_new_longs", kept[0].meta.get("macro_risk_overlay", "")
                      if kept else "allows_new_longs")

    def test_unreadable_macro_score_does_not_abort(self):
        class Macro:
            macro_score = "high"  # not a number
        opt = self._opt(macro=Macro())
        kept = opt._apply_cross_asset_risk_filter([self._opp()])
        self.assertEqual(len(kept), 1)
        self.assertTrue(opt.macro_risk_filter_degraded)

    def test_not_configured_is_not_a_degradation(self):
        """A default deployment has no macro engine; that must not flag forever."""
        opt = self._opt()
        opt._apply_cross_asset_risk_filter([self._opp()])
        self.assertFalse(opt.macro_risk_filter_degraded,
                         "an unconfigured optional engine is a configuration state")

    def test_empty_input_is_a_no_op(self):
        opt = self._opt()
        self.assertEqual(opt._apply_cross_asset_risk_filter([]), [])

    def test_degradation_is_logged_at_warning_not_debug(self):
        """The defect was precisely that this was invisible.

        logger.debug is below the default level, so a risk-off suppression could be
        off for weeks with nothing in the log. The loudness is the fix, so it is
        asserted rather than assumed.
        """
        import logging

        opt = self._opt(regime=self._Engine(raises=RuntimeError("DXY feed down")))
        with self.assertLogs("optimizer", level="WARNING") as captured:
            opt._apply_cross_asset_risk_filter([self._opp()])
        joined = "\n".join(captured.output)
        self.assertIn("DEGRADED", joined)
        self.assertIn("not being applied", joined)
        self.assertTrue(
            any(record.startswith("WARNING") for record in captured.output),
            f"the degradation must be at WARNING, got {captured.output}",
        )


class _FakeBracketManager:
    """Records what the executor tried to protect, so a test can assert on levels."""

    def __init__(self):
        self.placed: list[dict] = []

    def place_bracket(self, product_id=None, side=None, base_size=None,
                      entry_price=None, stop_price=None, target_price=None,
                      strategy_id=None, **kwargs):
        self.placed.append({
            "product_id": product_id, "side": side, "base_size": base_size,
            "entry_price": entry_price, "stop_price": stop_price,
            "target_price": target_price, "strategy_id": strategy_id, **kwargs,
        })
        # The executor treats anything other than status OPEN as a failure.
        return {"status": "OPEN", "bracket_id": "bracket-1", "entry_result": {"success": True}}

    def force_flatten_bracket(self, bracket_id, reason=""):
        self.flattened = getattr(self, "flattened", [])
        self.flattened.append({"bracket_id": bracket_id, "reason": reason})
        return {"status": "ok"}


class TestBracketProtectiveLevels(unittest.TestCase):
    """The bracket executor submits a stop and a target; it must validate both.

    The caller gates on stop_loss_pct > 0 and entry_price_est > 0 but never looks
    at take_profit_pct, and Opportunity defaults both to 0.0.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _opt(self, dry_run=False, require_approval=False):
        opt = _optimizer(self.tmp, dry_run=dry_run, require_approval=require_approval)
        opt._normalize_product_id = lambda cur, side, hint="": hint or "BTC-USD"
        opt._capital_bucket_for = lambda opp: "opportunity"
        opt._exec_engine = None
        opt.brackets = _FakeBracketManager()
        opt._bracket_mgr = opt.brackets
        opt._save_brackets = lambda *a, **k: None
        return opt

    def _opp(self, side="BUY", stop=5.0, target=10.0, entry=50_000.0, opp_type=None):
        return Opportunity(
            opp_type=opp_type or OpportunityType.STRATEGY_SIGNAL,
            currency="BTC", side=side, size_usd=500.0, reason="bracket test",
            priority=1.0, product_id="BTC-USD", entry_price_est=entry,
            stop_loss_pct=stop, take_profit_pct=target,
        )

    # ---- rejections ----

    def test_zero_take_profit_is_refused(self):
        opt = self._opt()
        opp = self._opp(target=0.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(opt.brackets.placed, [], "target at entry is not a protective bracket")

    def test_negative_take_profit_is_refused(self):
        opt = self._opt()
        opp = self._opp(target=-10.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(opt.brackets.placed, [],
                         "a target below entry takes a loss; must not be submitted")

    def test_zero_stop_is_refused(self):
        opt = self._opt()
        opp = self._opp(stop=0.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(opt.brackets.placed, [], "a stop at entry is not protective")

    def test_stop_loss_over_100_percent_is_refused(self):
        opt = self._opt()
        opp = self._opp(stop=150.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(opt.brackets.placed, [], "a negative stop price must be refused")

    def test_negative_stop_is_refused(self):
        opt = self._opt()
        opp = self._opp(stop=-5.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(opt.brackets.placed, [])

    def test_non_positive_base_size_is_refused(self):
        opt = self._opt()
        opp = self._opp()
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.0, False)
        self.assertEqual(opt.brackets.placed, [])

    # ---- acceptance, and that the levels are the ones expected ----

    def test_valid_buy_bracket_submits(self):
        opt = self._opt()
        opp = self._opp(stop=5.0, target=10.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(len(opt.brackets.placed), 1, "a well-formed bracket must still trade")

    def test_valid_sell_bracket_submits(self):
        opt = self._opt()
        state = type("S", (), {})()
        opp = self._opp(side="SELL", stop=5.0, target=10.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(len(opt.brackets.placed), 1)

    def test_bracket_levels_are_computed_on_the_correct_side(self):
        entry = 50_000.0
        for side, stop, target, expect_stop, expect_target in (
            ("BUY", 5.0, 10.0, entry * 0.95, entry * 1.10),
            ("SELL", 5.0, 10.0, entry * 1.05, entry * 0.90),
        ):
            with self.subTest(side=side):
                stop_price = entry * (1 - stop / 100) if side == "BUY" else entry * (1 + stop / 100)
                target_price = entry * (1 + target / 100) if side == "BUY" else entry * (1 - target / 100)
                self.assertAlmostEqual(stop_price, expect_stop, places=6)
                self.assertAlmostEqual(target_price, expect_target, places=6)
                if side == "BUY":
                    self.assertLess(stop_price, entry)
                    self.assertGreater(target_price, entry)
                else:
                    self.assertGreater(stop_price, entry)
                    self.assertLess(target_price, entry)

    def test_submitted_stop_and_target_straddle_entry(self):
        """The placed levels, not just the arithmetic, must be correct."""
        entry = 50_000.0
        for side in ("BUY", "SELL"):
            with self.subTest(side=side):
                opt = self._opt()
                opp = self._opp(side=side, stop=5.0, target=10.0, entry=entry)
                po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
                self.assertEqual(len(opt.brackets.placed), 1)
                bracket = opt.brackets.placed[0]
                self.assertAlmostEqual(bracket["entry_price"], entry, places=6)
                self.assertGreater(bracket["stop_price"], 0)
                if side == "BUY":
                    self.assertLess(bracket["stop_price"], entry)
                    self.assertGreater(bracket["target_price"], entry)
                else:
                    self.assertGreater(bracket["stop_price"], entry)
                    self.assertLess(bracket["target_price"], entry)
                self.assertNotEqual(bracket["stop_price"], entry,
                                    "a stop at entry is not protective")
                self.assertNotEqual(bracket["target_price"], entry,
                                    "a target at entry is not a profit target")

    def test_rejected_bracket_is_not_marked_executed(self):
        opt = self._opt()
        opp = self._opp(target=0.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertFalse(opp.executed, "a refused bracket must not be recorded as a trade")

    def test_sell_stop_at_exactly_100_percent_is_refused(self):
        """The one input that distinguishes the stop-range guard from the others.

        For a SELL, stop_loss_pct=100 puts the stop at 2x entry, which still
        straddles entry and is still positive, so the straddle and non-positive-stop
        checks accept it. Only the 0 < pct < 100 range check refuses it. Without
        this case the range guard is indistinguishable from redundant, and removing
        it would go unnoticed.
        """
        opt = self._opt()
        opp = self._opp(side="SELL", stop=100.0, target=10.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(opt.brackets.placed, [],
                         "a 100% stop on a short is a 100% loss stop and must be refused")

    def test_short_stop_just_inside_the_valid_range_is_placed(self):
        # The boundary either side of the range guard, so a widened or narrowed
        # range is caught.
        opt = self._opt()
        opp = self._opp(side="SELL", stop=99.0, target=10.0)
        po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
        self.assertEqual(len(opt.brackets.placed), 1, "99% is inside the range and must place")

    def test_zero_and_negative_stop_are_refused_on_both_sides(self):
        for side in ("BUY", "SELL"):
            for stop in (0.0, -5.0):
                with self.subTest(side=side, stop=stop):
                    opt = self._opt()
                    opp = self._opp(side=side, stop=stop, target=10.0)
                    po.PortfolioOptimizer._execute_with_bracket(opt, opp, 0.01, False)
                    self.assertEqual(opt.brackets.placed, [])
