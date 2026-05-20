from strategies.catalog.advanced import CATALOG_100, IMPLEMENTATION_MAP, advanced_specs
from strategies.catalog.config_schema import StrategyConfig, StrategyRuntimeFlags
from strategies.registry.registry import load_strategies


def test_catalog_has_exactly_100_named_strategies():
    assert len(CATALOG_100) == 100
    assert len(set(CATALOG_100)) == 100


def test_advanced_specs_has_complete_contract():
    specs = advanced_specs()
    assert len(specs) == 100
    for spec in specs:
        assert spec.strategy_id
        assert spec.risk_tier.startswith("TIER_")
        assert 0 <= spec.max_capital_fraction <= 1
        assert spec.max_size >= spec.min_size


def test_implemented_specs_are_backed_by_concrete_registry_strategies():
    concrete_ids = {s.strategy_id for s in load_strategies() if not hasattr(s, "spec")}
    specs = advanced_specs()
    for spec in specs:
        if spec.implementation_status == "implemented":
            assert spec.mapped_implementation is not None
            assert spec.mapped_implementation in concrete_ids
    assert len(IMPLEMENTATION_MAP) >= 10


def test_strategy_config_validation_rejects_unsafe_live_enablement():
    try:
        StrategyConfig(
            strategy_id="S001",
            enabled=True,
            risk_tier="TIER_3_HIGH_RISK",
            max_capital_fraction=0.1,
            sizing_model="fixed_fraction",
            min_size=0.01,
            max_size=1.0,
            entry_threshold=1.5,
            exit_threshold=1.0,
            stop_loss_bps=20,
            take_profit_bps=30,
            trailing_take_profit_bps=0,
            cooldown_bars=5,
            warmup_bars=100,
            approvals_required=False,
            runtime_flags=StrategyRuntimeFlags(live_enabled=True),
        )
        raised = False
    except ValueError:
        raised = True
    assert raised


def test_generic_spec_strategy_exposes_contract_hooks():
    strategy = next(s for s in load_strategies() if hasattr(s, "spec"))
    metadata = strategy.metadata()
    assert metadata["strategy_type"] == strategy.spec.family
    assert "data_requirements" in metadata
    hint = strategy.sizing_hints({})
    assert hint["max_capital_fraction"] == strategy.spec.max_capital_fraction
    signal = strategy.generate_signal({"product_id": "BTC-USD", "score": 0.9, "threshold": 0.1})
    assert signal is not None
    intents = strategy.order_intents(signal, {})
    assert intents and intents[0]["edge_floor_bps"] == strategy.spec.min_net_edge_bps


def test_strategy_config_rejects_live_enablement_for_research_and_expert_tiers():
    for tier in ("TIER_4_EXPERT_HIGH_RISK", "TIER_5_RESEARCH_ONLY"):
        try:
            StrategyConfig(
                strategy_id="S999",
                enabled=True,
                risk_tier=tier,
                max_capital_fraction=0.01,
                sizing_model="volatility_targeting",
                min_size=0.01,
                max_size=0.05,
                entry_threshold=1.0,
                exit_threshold=0.5,
                stop_loss_bps=20,
                take_profit_bps=25,
                trailing_take_profit_bps=10,
                cooldown_bars=2,
                warmup_bars=20,
                approvals_required=True,
                runtime_flags=StrategyRuntimeFlags(live_enabled=True),
            )
            raised = False
        except ValueError:
            raised = True
        assert raised


def test_generic_spec_strategy_disables_for_risk_halts_and_stale_inputs():
    strategy = next(s for s in load_strategies() if hasattr(s, "spec"))
    assert strategy.generate_signal({"score": 0.9, "threshold": 0.1, "risk_halt": True}) is None
    assert strategy.generate_signal({"score": 0.9, "threshold": 0.1, "stale_data": True}) is None
    assert strategy.generate_signal({"score": 0.9, "threshold": 0.1, "latency_ms": 500}) is None


def test_strategy_config_rejects_unsupported_tiers_models_and_cap_ceilings():
    common = dict(
        strategy_id="S100",
        enabled=True,
        max_capital_fraction=0.05,
        min_size=0.01,
        max_size=0.05,
        entry_threshold=1.0,
        exit_threshold=0.5,
        stop_loss_bps=20,
        take_profit_bps=25,
        trailing_take_profit_bps=0,
        cooldown_bars=2,
        warmup_bars=20,
        approvals_required=False,
    )
    for kwargs in [
        dict(risk_tier="TIER_UNKNOWN", sizing_model="fixed_fraction"),
        dict(risk_tier="TIER_2_MODERATE_RISK", sizing_model="unknown_model"),
        dict(risk_tier="TIER_5_RESEARCH_ONLY", sizing_model="fixed_fraction", max_capital_fraction=0.5),
    ]:
        payload = {**common, **kwargs}
        try:
            StrategyConfig(**payload)
            raised = False
        except ValueError:
            raised = True
        assert raised


def test_strategy_config_live_requires_stop_loss_and_mode():
    try:
        StrategyConfig(
            strategy_id="S555",
            enabled=True,
            risk_tier="TIER_2_MODERATE_RISK",
            max_capital_fraction=0.1,
            sizing_model="fixed_fraction",
            min_size=0.01,
            max_size=0.05,
            entry_threshold=1.0,
            exit_threshold=0.5,
            stop_loss_bps=0,
            take_profit_bps=25,
            trailing_take_profit_bps=0,
            cooldown_bars=2,
            warmup_bars=20,
            approvals_required=True,
            runtime_flags=StrategyRuntimeFlags(live_enabled=True),
        )
        raised = False
    except ValueError:
        raised = True
    assert raised
