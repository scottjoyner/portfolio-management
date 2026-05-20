import numpy as np


def basic_metrics(returns: list[float]) -> dict:
    arr = np.array(returns, dtype=float)
    if arr.size == 0:
        return {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}
    nav = (1 + arr).cumprod()
    dd = (nav / np.maximum.accumulate(nav)) - 1
    return {
        "return": float(nav[-1] - 1),
        "sharpe": float(arr.mean() / (arr.std() + 1e-9) * np.sqrt(252)),
        "max_drawdown": float(dd.min()),
    }
