"""Coverage tests for trading_system.strategies.catalog.advanced."""
from __future__ import annotations

from strategies.base.interfaces import StrategySignal

from trading_system.strategies.catalog.advanced import (
    CATALOG_100,
    GenericSpecStrategy,
    StrategySpec,
    _family,
    _risk_tier,
    _status,
    advanced_specs,
)


SPECS = advanced_specs()


def test_catalog_length():
    assert len(CATALOG_100) == 100
    assert len(SPECS) == 106


def test_exchange_bot_specs_present():
    bot_ids = {s.strategy_id for s in SPECS if s.mapped_implementation in (
        "StairStepTakeProfitStrategy",
        "SpotGridStrategy",
        "DcaStrategy",
        "SpotMartingaleStrategy",
        "SmartRebalanceStrategy",
        "TwapStrategy",
    )}
    assert len(bot_ids) == 6
    for spec in SPECS:
        if spec.strategy_id in bot_ids:
            assert spec.implementation_status == "implemented"
            assert spec.live_safe is True
            assert spec.family == "research"


def test_advanced_specs_unique_ids():
    ids = [s.strategy_id for s in SPECS]
    assert len(ids) == len(set(ids))


def test_strategyspec_dataclass():
    spec = SPECS[0]
    assert isinstance(spec, StrategySpec)
    assert spec.strategy_id.startswith("S")


def test_family_boundaries():
    # index 1 -> first label, index 100 -> last label
    assert _family(1) == "trend_momentum"
    assert _family(15) == "trend_momentum"
    assert _family(16) == "mean_reversion"
    assert _family(100) == "portfolio_treasury"
    # anything above 100 -> research
    assert _family(101) == "research"


def test_status_branches():
    assert _status("Z-score mean reversion")[0] == "implemented"
    assert _status("Trend pullback continuation")[0] == "partial"
    assert _status("Some unknown name")[0] == "research_only"


def test_risk_tier_branches():
    # research_only status short-circuits to TIER_5 regardless of family
    assert _risk_tier("portfolio_treasury", "research_only") == "TIER_5_RESEARCH_ONLY"
    assert _risk_tier("execution", "research_only") == "TIER_5_RESEARCH_ONLY"
    assert _risk_tier("relative_value", "research_only") == "TIER_5_RESEARCH_ONLY"
    assert _risk_tier("weird_family", "research_only") == "TIER_5_RESEARCH_ONLY"
    # non-research_only branches
    assert _risk_tier("portfolio_treasury", "implemented") == "TIER_1_LOW_RISK"
    assert _risk_tier("execution", "implemented") == "TIER_1_LOW_RISK"
    assert _risk_tier("trend_momentum", "implemented") == "TIER_2_MODERATE_RISK"
    assert _risk_tier("mean_reversion", "partial") == "TIER_2_MODERATE_RISK"
    assert _risk_tier("relative_value", "implemented") == "TIER_2_MODERATE_RISK"
    assert _risk_tier("market_making", "implemented") == "TIER_3_HIGH_RISK"
    assert _risk_tier("microstructure", "partial") == "TIER_4_EXPERT_HIGH_RISK"


def _ms(score=0.5, **overrides):
    state = {
        "product_id": "BTC-USD",
        "score": score,
        "threshold": 0.1,
        "estimated_edge_bps": 100.0,
        "warmup_complete": True,
    }
    state.update(overrides)
    return state


def test_metadata_treasury_vs_active():
    treasury_spec = next(s for s in SPECS if s.family == "portfolio_treasury")
    active_spec = next(s for s in SPECS if s.family != "portfolio_treasury")
    assert GenericSpecStrategy(treasury_spec).metadata()["capital_bucket"] == "TREASURY"
    assert GenericSpecStrategy(active_spec).metadata()["capital_bucket"] == "ACTIVE_TRADING"


def test_generate_signal_valid():
    strat = GenericSpecStrategy(SPECS[0])
    sig = strat.generate_signal(_ms(score=0.5))
    assert isinstance(sig, StrategySignal)
    assert sig.strategy_id == SPECS[0].strategy_id


def test_generate_signal_disabled_risk_halt():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.generate_signal(_ms(risk_halt=True)) is None


def test_generate_signal_stale_data():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.generate_signal(_ms(stale_data=True)) is None


def test_generate_signal_latency_breach():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.generate_signal(_ms(latency_ms=SPECS[0].required_latency_budget_ms + 1)) is None


def test_generate_signal_drawdown_breach():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.generate_signal(_ms(drawdown=0.3)) is None


def test_generate_signal_warmup_incomplete():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.generate_signal(_ms(warmup_complete=False)) is None


def test_generate_signal_score_below_threshold():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.generate_signal(_ms(score=0.0, threshold=0.1)) is None


def test_generate_signal_edge_below_min():
    spec = SPECS[0]
    strat = GenericSpecStrategy(spec)
    # low abs(score) -> estimated edge default abs(score)*10 < min_net_edge_bps
    assert strat.generate_signal(_ms(score=0.0001, estimated_edge_bps=abs(0.0001) * 10)) is None


def test_generate_signal_default_edge_calc():
    strat = GenericSpecStrategy(SPECS[0])
    sig = strat.generate_signal({"score": 0.5, "warmup_complete": True})
    assert sig is not None


def test_explain_trade():
    strat = GenericSpecStrategy(SPECS[0])
    sig = strat.generate_signal(_ms(score=0.5))
    out = strat.explain_trade(sig)
    assert "score=" in out


def test_sizing_hints():
    strat = GenericSpecStrategy(SPECS[0])
    hints = strat.sizing_hints(_ms())
    assert hints["model"] == SPECS[0].default_sizing_method


def test_order_intents_buy_and_sell():
    strat = GenericSpecStrategy(SPECS[0])
    buy_sig = StrategySignal(strategy_id="S001", product_id="BTC-USD", score=0.5, reason="r")
    sell_sig = StrategySignal(strategy_id="S001", product_id="BTC-USD", score=-0.5, reason="r")
    buy = strat.order_intents(buy_sig, _ms(score=0.5))
    sell = strat.order_intents(sell_sig, _ms(score=-0.5))
    assert buy[0]["side"] == "buy"
    assert sell[0]["side"] == "sell"


def test_risk_hints():
    strat = GenericSpecStrategy(SPECS[0])
    hints = strat.risk_hints(_ms())
    assert "risk_tier" in hints


def test_approvals_required_method():
    strat = GenericSpecStrategy(SPECS[0])
    assert strat.approvals_required() == SPECS[0].approvals_required


def test_is_disabled_explicit():
    strat = GenericSpecStrategy(SPECS[0])
    disabled, reason = strat.is_disabled(_ms(risk_halt=True))
    assert disabled and reason == "risk engine halt"
    disabled, reason = strat.is_disabled(_ms(stale_data=True))
    assert disabled and reason == "stale data"
    disabled, reason = strat.is_disabled(_ms(latency_ms=9999))
    assert disabled and reason == "latency breach"
    disabled, reason = strat.is_disabled(_ms(drawdown=0.5))
    assert disabled and reason == "drawdown breach"
    disabled, reason = strat.is_disabled(_ms())
    assert not disabled and reason == "enabled"


def test_replay_hooks_and_analytics_tags():
    strat = GenericSpecStrategy(SPECS[0])
    hooks = strat.replay_hooks()
    assert "scenario" in hooks
    tags = strat.analytics_tags()
    assert tags["strategy"] == SPECS[0].strategy_id
