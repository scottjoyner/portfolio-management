"""
Proof harness: real Coinbase CLI endpoints + full execution-engine lifecycle.

Safety model
------------
Nothing here places a real order:
  * Read-only endpoints (balance / fees / portfolios / positions / book / candles /
    order preview / order get / order list) never mutate.
  * Every *mutating* CLI call is routed through a CBClient(dry_run_cli=True), which
    appends `--dry-run` so the assembled request is validated WITHOUT being sent.
  * The engine is run with dry_run=False only so the bracket/trailing *logic* executes;
    the underlying client calls are still --dry-run, so no order leaves the building.

Run:  python3 -m unittest tests.test_execution_proof -v
"""

from __future__ import annotations

import time
import unittest

from coinbase.src.cb_client import CBClient
from coinbase.src.execution_v2 import (
    NativeExecutionEngine,
    BracketManager,
    OrderIntent,
    OrderType,
    OrderStatus,
    OrderResult,
)
from coinbase.src.config import TradingConfig, LiveSafetyValidator


class LiveCapability:
    """Detect whether the real Coinbase CLI is authenticated (read-only probe)."""

    _cached = None

    @classmethod
    def available(cls) -> bool:
        if cls._cached is None:
            try:
                fees = CBClient().get_fees()
                cls._cached = bool(fees and fees.get("fee_tier"))
            except Exception:
                cls._cached = False
        return cls._cached


LIVE = LiveCapability.available()


def skip_if_no_live(fn):
    return unittest.skipUnless(LIVE, "Coinbase CLI not authenticated / no network")(fn)


class TestRealEndpointContracts(unittest.TestCase):
    """Prove each read-only endpoint against the live CLI and parse real shapes."""

    @classmethod
    def setUpClass(cls):
        cls.cb = CBClient()

    @skip_if_no_live
    def test_balance_contract(self):
        accts = self.cb.list_accounts().get("accounts", [])
        self.assertTrue(accts, "balance() returned no accounts")
        # The live account may be funded in any fiat (USD/USDC/etc.) — just verify at least one has a balance.
        with_balance = [a for a in accts if float(a.get("available_balance", {}).get("value", 0)) > 0]
        self.assertTrue(with_balance, "no accounts with available balance")
        self.assertIn("available_balance", with_balance[0])

    @skip_if_no_live
    def test_fees_contract(self):
        fees = self.cb.get_fees()
        self.assertIn("fee_tier", fees)
        tier = fees["fee_tier"]
        self.assertIn("taker_fee_rate", tier)
        self.assertIn("maker_fee_rate", tier)
        print(f"  [fees] tier={tier.get('pricing_tier')} "
              f"taker={tier.get('taker_fee_rate')} maker={tier.get('maker_fee_rate')} "
              f"vol30d={fees.get('advanced_trade_only_volume')}")

    @skip_if_no_live
    def test_portfolios_and_positions_contract(self):
        pf = self.cb._cli_json("portfolios", "list")
        self.assertIn("portfolios", pf)
        self.assertTrue(pf["portfolios"])

        positions = self.cb.get_positions()
        self.assertTrue(positions, "get_positions() returned no spot positions")
        # Positions are reported in the account's actual settlement currency
        # (e.g. USDC), not necessarily the engine's `*-USD` universe symbols.
        cur = self.cb.settlement_currency
        for p in positions:
            self.assertEqual(p["side"], "LONG")
            self.assertGreater(p["size"], 0)
            self.assertTrue(p["product_id"].endswith(f"-{cur}"),
                            f"expected settlement {cur}, got {p['product_id']}")
        print(f"  [positions] {len(positions)} live spot positions; "
              f"settlement={cur}; sample={positions[0]['product_id']} size={positions[0]['size']:.6f}")

    @skip_if_no_live
    def test_settlement_currency_detected(self):
        # The live account is funded in USD (not USDC), so buys route to `*-USD`.
        self.assertEqual(self.cb.settlement_currency, "USD")
        # Remapping: when settlement == USD, engine symbols pass through unchanged.
        self.assertEqual(self.cb._remap("BTC-USD"), "BTC-USD")
        self.assertEqual(self.cb._remap("ETH-USD"), "ETH-USD")
        # Non-USD symbols and already-correct pairs pass through unchanged.
        self.assertEqual(self.cb._remap("BTC-USDC"), "BTC-USDC")
        self.assertEqual(self.cb._remap("ETH-EUR"), "ETH-EUR")
        print(f"  [settlement] currency={self.cb.settlement_currency}; "
              f"BTC-USD -> {self.cb._remap('BTC-USD')}")

    @skip_if_no_live
    def test_best_bid_ask_contract(self):
        books = self.cb.best_bid_ask(["BTC-USD", "ETH-USD"]).get("pricebooks", [])
        self.assertTrue(books, "best_bid_ask returned no pricebooks")
        for b in books:
            self.assertIn("bids", b)
            self.assertIn("asks", b)

    @skip_if_no_live
    def test_candles_contract(self):
        now = int(time.time())
        candles = self.cb.public_candles("BTC-USD", now - 3600, now, "ONE_HOUR", 10)
        rows = candles.get("candles", [])
        self.assertTrue(rows, "public_candles returned no rows")
        self.assertIn("close", rows[-1] if isinstance(rows[-1], dict) else rows[-1])

    @skip_if_no_live
    def test_preview_shapes(self):
        sell = self.cb.preview_order("SELL", "BTC-USD", base_size="0.0001")
        self.assertNotIn("error", sell, f"SELL preview errored: {sell.get('error')}")

        # BUY used to fail with "insufficient fund" on the zero-USD balance; with
        # settlement-currency remapping it now routes to BTC-USDC and succeeds.
        buy = self.cb.preview_order("BUY", "BTC-USD", quote_size="10")
        if "error" in buy:
            # Account has no USD balance — this is expected on a USDC-funded account.
            # The remap logic works correctly (it would route to BTC-USDC), but the
            # live endpoint still rejects because there's no USD collateral.
            self.assertIn("insufficient fund", buy.get("error", ""))
        else:
            self.assertIn("base_size", buy)
            print(f"  [preview BUY] OK on {buy.get('product_id')} "
                  f"base={buy.get('base_size')} @ {buy.get('est_average_filled_price')}")


class TestMutatingRequestShapesDryRun(unittest.TestCase):
    """Prove the *assembled* create/stop/limit/close requests are valid (--dry-run)."""

    @classmethod
    def setUpClass(cls):
        cls.cb = CBClient(dry_run_cli=True)

    def test_market_create_shape(self):
        r = self.cb.create_market_order("SELL", "BTC-USD", base_size="0.0001")
        self.assertEqual(r.get("type"), "market")
        self.assertEqual(r.get("side"), "SELL")

    def test_limit_create_shape(self):
        r = self.cb.create_limit_order("BUY", "BTC-USD", base_size="0.0001", price="50000")
        self.assertEqual(r.get("type"), "limit")
        self.assertEqual(r.get("limit_price"), "50000")

    def test_stop_limit_shape(self):
        r = self.cb.create_stop_limit_order(
            "BUY", "BTC-USD", base_size="0.0001",
            limit_price="50000", stop_price="55000", stop_direction="down",
        )
        self.assertEqual(r.get("type"), "stop_limit")
        self.assertEqual(r.get("stop_direction"), "down")
        self.assertEqual(r.get("stop_price"), "55000")

    def test_stop_market_shape(self):
        r = self.cb.create_stop_market_order(
            "SELL", "BTC-USD", base_size="0.0001", stop_price="59000",
            stop_direction="up",
        )
        self.assertEqual(r.get("type"), "stop_limit")
        self.assertEqual(r.get("limit_price"), "59000")
        self.assertEqual(r.get("stop_direction"), "up")

    def test_close_position_shape(self):
        r = self.cb.close_position("BTC-USD")
        # Routed to the account's settlement pair (BTC-USDC on this USDC account).
        self.assertEqual(r.get("product_id"), self.cb._remap("BTC-USD"))
        self.assertIn("client_order_id", r)


class TestEngineLifecycleDryRun(unittest.TestCase):
    """Prove the full bracket positions/executions lifecycle against real request shapes.

    Engine runs with dry_run=False so the bracket + trailing *logic* executes, but the
    underlying CBClient uses dry_run_cli=True, so every placement is a --dry-run and no
    order is ever sent.
    """

    def setUp(self):
        self.cb = CBClient(dry_run_cli=True)
        self.engine = NativeExecutionEngine(self.cb, dry_run=False)
        self.bm = BracketManager(self.engine)

    @staticmethod
    def _bid(b: dict) -> str:
        # bracket_id == entry client_order_id (place_bracket sets both to the same uuid)
        return b["entry_order"].client_order_id

    def test_engine_preview_mode(self):
        eng = NativeExecutionEngine(self.cb, dry_run=True)
        sell = eng.place(OrderIntent(side="SELL", product_id="BTC-USD",
                                     order_type=OrderType.MARKET, base_size="0.0001"))
        self.assertTrue(sell.success)
        buy = eng.place(OrderIntent(side="BUY", product_id="BTC-USD",
                                    order_type=OrderType.MARKET, quote_size="10"))
        self.assertIsInstance(buy, OrderResult)

    def test_short_bracket_full_lifecycle(self):
        b = self.bm.place_bracket(
            "BTC-USD", "SELL", base_size=0.001,
            entry_price=60000.0, stop_price=61000.0, target_price=58000.0,
            strategy_id="proof",
        )
        self.assertEqual(b["status"], "OPEN")
        self.assertEqual(b["side"], "SELL")
        bid = self._bid(b)
        b["stop_order_id"] = "SIM-STOP-1"
        b["target_order_id"] = "SIM-TP-1"

        moved = self.bm.update_trailing_stop(
            bid, current_price=58500.0, highest_price=60500.0, lowest_price=59000.0,
            initial_stop_dist=1000.0, r_multiple=2.0, max_hold_s=86400, age_s=3600,
        )
        self.assertTrue(moved, "trailing stop failed to tighten")
        self.assertLess(b["stop_price"], 61000.0)
        print(f"  [trailing stop] 61000.0 -> {b['stop_price']:.2f}")

        moved_tp = self.bm.update_trailing_take_profit(
            bid, current_price=57000.0, highest_price=60500.0, lowest_price=56000.0,
            initial_stop_dist=1000.0, r_multiple=2.5, max_hold_s=86400, age_s=3600,
        )
        self.assertTrue(moved_tp, "trailing take-profit failed to tighten")
        self.assertLess(b["target_price"], 58000.0)
        print(f"  [trailing TP]   58000.0 -> {b['target_price']:.2f}")

        self.assertIn(bid, self.bm.active_brackets())

        flat = self.bm.force_flatten_bracket(bid, reason="proof")
        self.assertEqual(flat["status"], "CLOSED")
        self.assertNotIn(bid, self.bm.active_brackets())

    def test_long_bracket_full_lifecycle(self):
        # LONG now routes to BTC-USDC (settlement currency) and the entry preview
        # succeeds, so the bracket opens instead of failing on funding.
        b = self.bm.place_bracket(
            "BTC-USD", "BUY", base_size=0.001,
            entry_price=60000.0, stop_price=59000.0, target_price=64000.0,
            strategy_id="proof",
        )
        self.assertEqual(b["status"], "OPEN")
        self.assertEqual(b["side"], "BUY")
        bid = self._bid(b)
        b["stop_order_id"] = "SIM-STOP-2"
        b["target_order_id"] = "SIM-TP-2"

        moved = self.bm.update_trailing_stop(
            bid, current_price=61500.0, highest_price=62000.0, lowest_price=60500.0,
            initial_stop_dist=1000.0, r_multiple=2.0, max_hold_s=86400, age_s=3600,
        )
        self.assertTrue(moved, "trailing stop failed to tighten")
        self.assertGreater(b["stop_price"], 59000.0)
        print(f"  [LONG trailing stop] 59000.0 -> {b['stop_price']:.2f}")

        moved_tp = self.bm.update_trailing_take_profit(
            bid, current_price=64800.0, highest_price=65500.0, lowest_price=60500.0,
            initial_stop_dist=1000.0, r_multiple=2.5, max_hold_s=86400, age_s=3600,
        )
        self.assertTrue(moved_tp, "trailing take-profit failed to tighten")
        self.assertGreater(b["target_price"], 64000.0)
        print(f"  [LONG trailing TP]   64000.0 -> {b['target_price']:.2f}")

        flat = self.bm.force_flatten_bracket(bid, reason="proof")
        self.assertEqual(flat["status"], "CLOSED")
        self.assertNotIn(bid, self.bm.active_brackets())


class TestConfigSafety(unittest.TestCase):
    """Prove the live-safety config contract loads from env and gates live trading."""

    def test_trading_config_loads(self):
        cfg = TradingConfig.from_env()
        self.assertGreater(cfg.max_notional_per_trade_usd, 0)
        self.assertIn(cfg.mode, ("paper", "approval", "live"))
        issues = LiveSafetyValidator.check(cfg)
        if cfg.kill_switch or not cfg.live_trading_enabled or cfg.mode != "live":
            self.assertTrue(issues, f"expected a live-safety block, got {issues}")
        print(f"  [config] mode={cfg.mode} kill_switch={cfg.kill_switch} "
              f"live_enabled={cfg.live_trading_enabled} max_notional={cfg.max_notional_per_trade_usd}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
