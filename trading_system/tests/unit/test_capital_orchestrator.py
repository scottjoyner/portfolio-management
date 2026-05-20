from portfolio.allocator.capital_orchestrator import StrategyAllocationInput, allocate_capital


def test_allocation_respects_deployable_budget_and_positive_reserves():
    inputs = [
        StrategyAllocationInput("a", "trend", "TIER_1_LOW_RISK", 0.3, 0.8, 0.05, True, True),
        StrategyAllocationInput("b", "micro", "TIER_4_EXPERT_HIGH_RISK", 0.1, 0.9, 0.02, False, True),
    ]
    decisions = allocate_capital(
        inputs,
        locked_reserve_fraction=0.2,
        cash_buffer_fraction=0.1,
        hedge_reserve_fraction=0.1,
    )
    total = sum(d.approved_fraction for d in decisions)
    assert total <= 0.6 + 1e-9
    assert decisions[1].approved_fraction <= 0.02


def test_allocation_rejects_nonpositive_deployable_budget():
    inputs = [
        StrategyAllocationInput("a", "trend", "TIER_1_LOW_RISK", 0.3, 0.8, 0.05, True, True),
    ]
    try:
        allocate_capital(
            inputs,
            locked_reserve_fraction=0.5,
            cash_buffer_fraction=0.4,
            hedge_reserve_fraction=0.1,
        )
        raised = False
    except ValueError:
        raised = True
    assert raised
