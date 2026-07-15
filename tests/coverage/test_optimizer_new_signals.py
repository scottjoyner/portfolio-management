"""Integration smoke tests for the NEW optimizer detection steps.

These steps are wrapped in per-step try/except inside PortfolioOptimizer, so any
wiring bug would be silently swallowed in production (no signal, no error). This
test exercises the real method bodies with injected fakes to prove the wiring
actually calls the strategies and turns their output into Opportunity objects.

We bypass PortfolioOptimizer.__init__ (lock file / StateStore / network / CoinbaseCLI)
via __new__ and set only the attributes the new steps touch.
"""

import json
import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest import mock

import portfolio_optimizer as po
from portfolio_optimizer import Opportunity, OpportunityType, PortfolioOptimizer
from strategy_engine import (
    OrderFlowCVD,
    WickPressureFlow,
    ExchangeNetflowSignal,
    StablecoinFlowSignal,
    Signal,
)


def _make_opt():
    opt = PortfolioOptimizer.__new__(PortfolioOptimizer)
    opt.min_value = 10.0
    opt.last_execution = defaultdict(float)
    # Skip unrelated pre-existing blocks.
    opt._order_flow_engine = None
    opt._smart_money_flow = None
    opt._funding_contrarian = None
    opt._onchain_flow = None

    opt.state = SimpleNamespace(holdings={
        "BTC": {"currency": "BTC", "product_id": "BTC-USD", "value": 5000.0, "price": 64000.0,
                "volume_24h": 1e9, "spread": 0.001},
        "ETH": {"currency": "ETH", "product_id": "ETH-USD", "value": 3000.0, "price": 1800.0,
                "volume_24h": 5e8, "spread": 0.002},
        "USDC": {"currency": "USDC", "product_id": "USDC-USD", "value": 1000.0, "price": 1.0},
    })

    def _candles(pid):
        # Big upper wicks -> wick-pressure SELL; >=40 bars required.
        return [
            {"start": i, "open": 100.0, "high": 102.0, "low": 99.9,
             "close": 100.0, "volume": 1000.0}
            for i in range(50)
        ]

    class FakeFeed:
        def get_candles_batch(self, pids, granularity=3600, limit=60):
            return {pid: _candles(pid) for pid in pids}

    opt._feed_mgr = FakeFeed()

    # Deterministic helper methods.
    opt._buy_capacity = lambda *a, **k: 1_000_000.0
    opt._current_price_for_symbol = lambda *a, **k: 64000.0
    opt._risk_reward_size = lambda **kw: max(
        kw.get("max_notional", 1500.0) * 0.3, kw.get("min_notional", 10.0))
    opt._compute_exit_plan = lambda *a, **k: {
        "stop_loss_pct": 0.05, "take_profit_pct": 0.1,
        "holding_period_hours": 24, "expected_return_pct": 0.08, "risk_pct": 0.05,
    }
    opt._latency_adjusted_priority = lambda *a, **k: 0.5

    # Real candle strategies; CVD is faked (divergence is hard to craft), wick is real.
    opt._order_flow_cvd = OrderFlowCVD(lookback=30, divergence_bars=6, min_conf=0.35)
    opt._order_flow_cvd.on_bar = lambda *a, **k: Signal(
        "BUY", 64000.0, 0.7, "cvd:fake", "order_flow_cvd")
    opt._wick_pressure = WickPressureFlow(lookback=20, threshold=0.12, min_conf=0.35)

    # Real on-chain strategies with injected offline fetches.
    def _netflow_fetch(cg_id):
        n = 60
        vols = [[i, 100.0 + i] for i in range(n)]          # rising volume
        prices = [[i, 100.0 - i * 0.1] for i in range(n)]  # falling price -> BUY
        return {"total_volumes": vols, "prices": prices}

    def _stablecoin_fetch(cg_id):
        n = 70
        caps = [[i, 1000.0 + i * 10.0] for i in range(n)]  # rising supply -> BUY
        return {"market_caps": caps}

    opt._exchange_netflow = ExchangeNetflowSignal(cache_ttl=600.0, trend_window=24)
    opt._exchange_netflow._fetch_fn = _netflow_fetch
    opt._stablecoin_flow = StablecoinFlowSignal(cache_ttl=900.0, trend_window=30)
    opt._stablecoin_flow._fetch_fn = _stablecoin_fetch
    return opt


class TestOptimizerNewSignals(unittest.TestCase):
    def test_order_flow_candle_block_wires_real_strategies(self):
        opt = _make_opt()
        ops = opt._detect_order_flow_signals()
        self.assertTrue(ops, "candle order-flow block produced no opportunities")
        for op in ops:
            self.assertEqual(op.meta["source"], "order_flow_candle")
            self.assertIn(op.side, ("BUY", "SELL"))
            self.assertIsNotNone(op.product_id)
        strategies = {op.meta["strategy"] for op in ops}
        self.assertIn("wick_pressure", strategies)   # real strategy ran on candles
        self.assertIn("order_flow_cvd", strategies)

    def test_funding_onchain_block_wires_netflow_and_stablecoin(self):
        opt = _make_opt()
        ops = opt._detect_funding_and_onchain_signals()
        sources = {op.meta["source"] for op in ops}
        # exchange_netflow must now fire (was silently dead due to currency=cur bug).
        self.assertIn("onchain_netflow", sources)
        self.assertIn("stablecoin_flow", sources)
        for op in ops:
            self.assertIn(op.side, ("BUY", "SELL"))
            self.assertGreaterEqual(op.size_usd, opt.min_value)


class TestSignalCacheLabeling(unittest.TestCase):
    """The dashboard reads data/.unified_signal_cache.json and shows strategy_name.
    New signals must not collapse to the generic 'STRATEGY_SIGNAL' label."""

    def test_new_signal_sources_labeled_in_cache(self):
        opt = PortfolioOptimizer.__new__(PortfolioOptimizer)
        specs = [
            ("order_flow_cvd", "order_flow_candle", "BTC"),
            ("wick_pressure", "order_flow_candle", "ETH"),
            ("exchange_netflow", "onchain_netflow", "BTC"),
            ("stablecoin_flow", "stablecoin_flow", "BTC"),
        ]
        opps = [
            Opportunity(
                opp_type=OpportunityType.STRATEGY_SIGNAL,
                currency=cur, side="BUY", size_usd=500.0, reason=f"{strat}: test",
                priority=0.6, product_id=f"{cur}-USD",
                meta={"strategy": strat, "source": src},
            )
            for strat, src, cur in specs
        ]
        captured = {}
        with mock.patch("portfolio_optimizer.os.replace") as repl:
            opt._write_signal_cache(opps)
            tmp_path = repl.call_args[0][0]
            with open(tmp_path) as f:
                captured["payload"] = json.load(f)
            try:
                import os as _os
                _os.remove(tmp_path)
            except OSError:
                pass

        items = {it["strategy_name"]: it for it in captured["payload"]["signals"]}
        expected_src = {s[0]: s[1] for s in specs}
        for strat, src in expected_src.items():
            self.assertIn(strat, items, f"{strat} missing from cache")
            self.assertEqual(items[strat].get("meta", {}).get("source"), src)


if __name__ == "__main__":
    unittest.main()
