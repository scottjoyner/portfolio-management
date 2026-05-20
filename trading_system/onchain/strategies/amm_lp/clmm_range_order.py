from __future__ import annotations


def range_order_action(in_range: bool) -> str:
    return "wait_fill" if in_range else "withdraw_and_settle"
