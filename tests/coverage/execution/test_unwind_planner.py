from trading_system.execution.hybrid.unwind_planner import (
    plan_unwind_sequence,
    rollback_after_failed_hedge,
)


def test_plan_cex_first():
    seq = plan_unwind_sequence(True)
    assert seq == ["close_cex_hedge", "remove_onchain_liquidity"]


def test_plan_onchain_first():
    seq = plan_unwind_sequence(False)
    assert seq == ["remove_onchain_liquidity", "close_cex_hedge"]


def test_rollback():
    seq = rollback_after_failed_hedge()
    assert seq == ["pause_strategy", "reduce_onchain_range", "raise_operator_alert"]
