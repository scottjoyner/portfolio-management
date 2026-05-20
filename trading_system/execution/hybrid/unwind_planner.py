from __future__ import annotations


def plan_unwind_sequence(prefer_cex_first: bool) -> list[str]:
    return ["close_cex_hedge", "remove_onchain_liquidity"] if prefer_cex_first else ["remove_onchain_liquidity", "close_cex_hedge"]


def rollback_after_failed_hedge() -> list[str]:
    return ["pause_strategy", "reduce_onchain_range", "raise_operator_alert"]
