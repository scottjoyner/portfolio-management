from strategies.registry.registry import load_strategies


def test_has_50_plus_strategies():
    strategies = load_strategies()
    assert len(strategies) >= 50


def test_strategy_metadata_contract_fields_present():
    strategies = load_strategies()
    required = {
        "purpose",
        "regime_suitability",
        "required_data",
        "required_latency_budget_ms",
        "sizing_model",
        "risk_ceilings",
        "expected_holding_horizon",
        "execution_style",
        "failure_modes",
        "disable_criteria",
        "cooldown_logic",
        "explainability_output",
        "backtest_caveats",
        "live_deployment_prerequisites",
    }
    advanced = [s for s in strategies if hasattr(s, "spec")]
    assert len(advanced) >= 20
    for strat in advanced:
        assert required.issubset(set(strat.metadata().keys()))
