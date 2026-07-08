from __future__ import annotations
class RiskState:
    def __init__(self):
        self.daily_pnl = 0.0
        self.peak_equity = None

def apply_risk_checks(intents: list[dict], equity_usd: float, risk_per_trade: float = 0.01, max_drawdown: float = 0.15, state: RiskState | None = None) -> list[dict]:
    if state is None:
        state = RiskState()
    if state.peak_equity is None or equity_usd > state.peak_equity:
        state.peak_equity = equity_usd
    if state.peak_equity and equity_usd < (1.0 - max_drawdown) * state.peak_equity:
        return []
    cap = risk_per_trade * equity_usd
    trimmed = []
    for it in intents:
        if it["quote_size"] > cap:
            scale = cap / it["quote_size"]
            it = it.copy()
            it["quote_size"] = cap
            it["base_size"] *= scale
        trimmed.append(it)
    return trimmed
