def risk_reward(entry: float, stop: float, target: float) -> float:
    if entry == stop:
        return 0.0
    return float((target - entry) / (entry - stop))
