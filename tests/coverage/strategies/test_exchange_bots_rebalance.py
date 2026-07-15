"""Coverage tests for the smart rebalance exchange-bot strategy."""
from __future__ import annotations

import pytest

from trading_system.strategies.exchange_bots.smart_rebalance import (
    SmartRebalanceConfig,
    SmartRebalanceStrategy,
)


def _ms(holdings=None, product_id="BTC-USD", price=None, warmup=True,
        portfolio=None, enabled=True, preset="core_balanced", targets=None,
        drift_threshold=0.05, profit_take_pct=1.0, min_trade_notional=1.0):
    cfg = SmartRebalanceConfig(
        enabled=enabled,
        preset=preset,
        targets=targets,
        drift_threshold=drift_threshold,
        profit_take_pct=profit_take_pct,
        min_trade_notional=min_trade_notional,
    )
    strat = SmartRebalanceStrategy(
        strategy_id="smart_rebalance",
        strategy_type="rebalance",
        bot_config=cfg,
    )
    return strat, {
        "product_id": product_id,
        "price": price,
        "holdings": holdings if holdings is not None else None,
        "portfolio": portfolio,
        "warmup_complete": warmup,
    }


def test_disabled():
    strat, ms = _ms(enabled=False, holdings={"BTC-USD": 100})
    assert strat.generate_signal(ms) is None


def test_unknown_preset_broken():
    strat, ms = _ms(preset="does_not_exist", holdings={"BTC-USD": 100})
    assert strat._broken is True
    assert strat.generate_signal(ms) is None


def test_empty_holdings():
    strat, ms = _ms(holdings={})
    assert strat.generate_signal(ms) is None


def test_empty_holdings_via_portfolio_key():
    strat, ms = _ms(holdings=None, portfolio={})
    assert strat.generate_signal(ms) is None


def test_total_le_zero():
    strat, ms = _ms(holdings={"BTC-USD": 0})
    assert strat.generate_signal(ms) is None


def test_warmup_incomplete():
    strat, ms = _ms(holdings={"BTC-USD": 100}, warmup=False)
    assert strat.generate_signal(ms) is None


def test_no_drift_no_orders():
    # explicit 50/50 and holdings already at target -> no orders
    strat, ms = _ms(
        targets={"BTC-USD": 0.5, "ETH-USD": 0.5},
        holdings={"BTC-USD": 500, "ETH-USD": 500},
        product_id="BTC-USD",
        price=100.0,
    )
    assert strat.generate_signal(ms) is None


def test_no_drift_within_threshold():
    # tiny drift below drift_threshold -> no orders
    strat, ms = _ms(
        targets={"BTC-USD": 0.5, "ETH-USD": 0.5},
        holdings={"BTC-USD": 490, "ETH-USD": 510},
        product_id="BTC-USD",
        price=100.0,
        drift_threshold=0.05,
    )
    assert strat.generate_signal(ms) is None


def test_drift_threshold_effect():
    # same tiny drift but threshold lowered -> orders produced
    strat, ms = _ms(
        targets={"BTC-USD": 0.5, "ETH-USD": 0.5},
        holdings={"BTC-USD": 490, "ETH-USD": 510},
        product_id="BTC-USD",
        price=100.0,
        drift_threshold=0.001,
    )
    sig = strat.generate_signal(ms)
    assert sig is not None
    assert len(strat._pending) >= 1


def test_one_drifted_buy_and_sell():
    # 3-asset book so the rebalancer emits both a BUY and a SELL leg
    strat, ms = _ms(
        targets={"BTC-USD": 0.4, "ETH-USD": 0.3, "SOL-USD": 0.3},
        holdings={"BTC-USD": 600, "ETH-USD": 200, "SOL-USD": 200},
        product_id="BTC-USD",
        price=100.0,
    )
    seen = set()
    sig = strat.generate_signal(ms)
    # drain the initial queued batch only (rebalance re-emits until book balanced)
    while sig is not None:
        seen.add(sig.score > 0)
        assert sig.product_id in ("BTC-USD", "ETH-USD", "SOL-USD")
        if not strat._pending:
            break
        sig = strat.generate_signal(ms)
    assert True in seen and False in seen


def test_multiple_orders_queued_and_drained():
    strat, ms = _ms(
        targets={"BTC-USD": 0.5, "ETH-USD": 0.25, "SOL-USD": 0.25},
        holdings={"BTC-USD": 300, "ETH-USD": 50, "SOL-USD": 50},
        product_id="BTC-USD",
        price=50.0,
    )
    seen_assets = []
    sig = strat.generate_signal(ms)
    while sig is not None and strat._pending:
        seen_assets.append(sig.product_id)
        if sig.product_id == "BTC-USD":
            # price known -> size = notional/price recorded
            side, p, size = strat._decisions["BTC-USD"]
            assert p == 50.0
        sig = strat.generate_signal(ms)
    assert len(seen_assets) >= 2
    assert len(set(seen_assets)) == len(seen_assets)


def test_explicit_targets_config():
    cfg = SmartRebalanceConfig(targets={"BTC-USD": 0.6, "ETH-USD": 0.4})
    strat = SmartRebalanceStrategy(
        strategy_id="sr", strategy_type="rebalance", bot_config=cfg)
    assert strat._broken is False
    assert strat._engine is not None


def test_order_intents_emits_recorded():
    strat, ms = _ms(
        targets={"BTC-USD": 0.5, "ETH-USD": 0.5},
        holdings={"BTC-USD": 400, "ETH-USD": 600},
        product_id="BTC-USD",
        price=100.0,
    )
    sig = strat.generate_signal(ms)
    assert sig is not None
    intents = strat.order_intents(sig, ms)
    assert len(intents) == 1
    assert intents[0]["side"] in ("BUY", "SELL")
    assert intents[0]["product_id"] == sig.product_id


def test_price_none_for_other_asset():
    # product_id differs from the drifted asset -> price None, size = notional
    strat, ms = _ms(
        targets={"BTC-USD": 0.5, "ETH-USD": 0.5},
        holdings={"BTC-USD": 400, "ETH-USD": 600},
        product_id="SOL-USD",
        price=10.0,
    )
    sig = strat.generate_signal(ms)
    # first order could be BTC or ETH (asset != product_id) -> price None
    assert sig is not None
    if sig.product_id != "SOL-USD":
        side, p, size = strat._decisions[sig.product_id]
        assert p == 0.0
        # notional recorded as size when price unavailable
        notional = float(sig.reason.split("notional=")[-1])
        assert size == pytest.approx(notional)
