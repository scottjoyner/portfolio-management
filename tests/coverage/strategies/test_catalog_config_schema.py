"""Coverage tests for trading_system.strategies.catalog.config_schema."""
from __future__ import annotations

import pytest

from trading_system.strategies.catalog.config_schema import (
    ALLOWED_RISK_TIERS,
    ALLOWED_SIZING_MODELS,
    StrategyConfig,
    StrategyRuntimeFlags,
)


def _base_kwargs(tier="TIER_2_MODERATE_RISK", **overrides):
    kw = dict(
        strategy_id="S001_test",
        risk_tier=tier,
        max_capital_fraction=0.10,
        sizing_model="volatility_targeting",
        min_size=0.001,
        max_size=2.0,
        entry_threshold=0.1,
        exit_threshold=0.05,
        stop_loss_bps=10.0,
        take_profit_bps=20.0,
        trailing_take_profit_bps=0.0,
        cooldown_bars=10,
        warmup_bars=200,
    )
    kw.update(overrides)
    return kw


def test_runtime_flags_defaults():
    f = StrategyRuntimeFlags()
    assert f.backtest_enabled and f.paper_enabled and not f.live_enabled


@pytest.mark.parametrize("tier", list(ALLOWED_RISK_TIERS))
def test_valid_config_per_tier(tier):
    # use a capital fraction safely under the tier ceiling
    ceiling = {
        "TIER_0_CAPITAL_PRESERVATION": 0.40,
        "TIER_1_LOW_RISK": 0.30,
        "TIER_2_MODERATE_RISK": 0.20,
        "TIER_3_HIGH_RISK": 0.10,
        "TIER_4_EXPERT_HIGH_RISK": 0.03,
        "TIER_5_RESEARCH_ONLY": 0.01,
    }[tier]
    kw = _base_kwargs(tier=tier, max_capital_fraction=ceiling * 0.5)
    if tier in {"TIER_4_EXPERT_HIGH_RISK", "TIER_5_RESEARCH_ONLY"}:
        # these tiers cannot be live_enabled; enable paper only
        kw["runtime_flags"] = StrategyRuntimeFlags(live_enabled=False)
    cfg = StrategyConfig(**kw)
    assert cfg.risk_tier == tier


def test_unsupported_risk_tier():
    with pytest.raises(ValueError):
        StrategyConfig(**_base_kwargs(risk_tier="NOT_A_TIER"))


def test_unsupported_sizing_model():
    with pytest.raises(ValueError):
        StrategyConfig(**_base_kwargs(sizing_model="nonsense"))


def test_enabled_requires_a_runtime_mode():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(
                enabled=True,
                runtime_flags=StrategyRuntimeFlags(
                    backtest_enabled=False, paper_enabled=False, live_enabled=False
                ),
            )
        )


def test_max_size_less_than_min_size():
    with pytest.raises(ValueError):
        StrategyConfig(**_base_kwargs(min_size=2.0, max_size=1.0))


def test_exit_threshold_gt_entry_threshold():
    with pytest.raises(ValueError):
        StrategyConfig(**_base_kwargs(entry_threshold=0.05, exit_threshold=0.1))


def test_empty_supported_products():
    with pytest.raises(ValueError):
        StrategyConfig(**_base_kwargs(supported_products=[]))


def test_capital_fraction_exceeds_tier_ceiling():
    with pytest.raises(ValueError):
        # TIER_2 ceiling is 0.20
        StrategyConfig(**_base_kwargs(max_capital_fraction=0.30))


def test_live_enabled_requires_enabled():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(
                enabled=False,
                runtime_flags=StrategyRuntimeFlags(live_enabled=True),
            )
        )


def test_live_enabled_requires_approvals():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(
                enabled=True,
                approvals_required=False,
                runtime_flags=StrategyRuntimeFlags(live_enabled=True),
            )
        )


def test_live_enabled_expert_or_research_tier():
    for tier in ("TIER_4_EXPERT_HIGH_RISK", "TIER_5_RESEARCH_ONLY"):
        with pytest.raises(ValueError):
            StrategyConfig(
                **_base_kwargs(
                    tier=tier,
                    max_capital_fraction=0.01,
                    enabled=True,
                    approvals_required=True,
                    runtime_flags=StrategyRuntimeFlags(live_enabled=True),
                )
            )


def test_correlated_group_exposure_lt_asset():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(max_exposure_by_asset=0.5, max_exposure_by_correlated_group=0.2)
        )


def test_warmup_lt_cooldown():
    with pytest.raises(ValueError):
        StrategyConfig(**_base_kwargs(warmup_bars=5, cooldown_bars=10))


def test_live_turnover_too_high():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(
                enabled=True,
                approvals_required=True,
                max_turnover=20.0,
                live_requires_risk_gates=True,
                runtime_flags=StrategyRuntimeFlags(live_enabled=True),
            )
        )


def test_live_requires_positive_stop_loss():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(
                enabled=True,
                approvals_required=True,
                stop_loss_bps=0.0,
                live_requires_risk_gates=True,
                runtime_flags=StrategyRuntimeFlags(live_enabled=True),
            )
        )


def test_trailing_tp_without_take_profit():
    with pytest.raises(ValueError):
        StrategyConfig(
            **_base_kwargs(take_profit_bps=0.0, trailing_take_profit_bps=5.0)
        )


def test_live_disabled_turnover_ok():
    # live_enabled False -> turnover cap not enforced
    cfg = StrategyConfig(**_base_kwargs(max_turnover=20.0))
    assert cfg.max_turnover == 20.0


def test_live_disabled_stop_loss_zero_ok():
    cfg = StrategyConfig(**_base_kwargs(stop_loss_bps=0.0))
    assert cfg.stop_loss_bps == 0.0


def test_trailing_tp_with_take_profit_ok():
    cfg = StrategyConfig(**_base_kwargs(take_profit_bps=10.0, trailing_take_profit_bps=5.0))
    assert cfg.trailing_take_profit_bps == 5.0
