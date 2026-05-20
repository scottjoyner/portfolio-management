from analytics.metrics.live_transfer import BacktestRealismScorer, SimulationAssumptions


def test_realism_penalty_increases_for_optimistic_assumptions():
    optimistic = SimulationAssumptions(
        latency_ms=5,
        queue_fill_probability=0.9,
        stale_quote_decay=0.05,
        maker_ratio=0.8,
        cancel_ratio=0.9,
        rejection_rate=0.0,
        outage_rate=0.0,
    )
    conservative = SimulationAssumptions(
        latency_ms=60,
        queue_fill_probability=0.45,
        stale_quote_decay=0.6,
        maker_ratio=0.5,
        cancel_ratio=0.4,
        rejection_rate=0.03,
        outage_rate=0.02,
    )
    p1 = BacktestRealismScorer.realism_penalty(optimistic).total
    p2 = BacktestRealismScorer.realism_penalty(conservative).total
    assert p1 > p2


def test_assessment_outputs_confidence_and_fragility_bounds():
    a = BacktestRealismScorer.assess_strategy(
        strategy_id="QueueReactiveFillProbabilityMM",
        simulated_return=0.1,
        sharpe=1.2,
        assumptions=SimulationAssumptions(20, 0.7, 0.2, 0.7, 0.75, 0.02, 0.01),
        holding_horizon_hint="sub-second",
    )
    assert 0 <= a.fragility_score <= 1
    assert 0 <= a.live_transfer_confidence <= 1
    assert a.expected_live_return <= 0.1
