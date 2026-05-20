from __future__ import annotations


def classify_revert(error_text: str) -> str:
    e = error_text.lower()
    if "insufficient" in e:
        return "INSUFFICIENT_FUNDS"
    if "slippage" in e:
        return "SLIPPAGE"
    if "deadline" in e:
        return "DEADLINE"
    return "UNKNOWN"
