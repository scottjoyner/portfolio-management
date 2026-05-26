from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SizingResult:
    suggested_size: Decimal
    max_size: Decimal
    reason: str = ""


def fixed_fractional(capital: Decimal, risk_per_trade_pct: Decimal, stop_loss_pct: Decimal) -> SizingResult:
    if stop_loss_pct <= Decimal("0"):
        return SizingResult(suggested_size=Decimal("0"), max_size=Decimal("0"), reason="stop_loss_pct must be > 0")
    risk_amount = capital * risk_per_trade_pct / Decimal("100")
    size = risk_amount / stop_loss_pct
    return SizingResult(suggested_size=size, max_size=size)


def kelly_criterion(win_rate: Decimal, avg_win: Decimal, avg_loss: Decimal) -> Decimal:
    if avg_loss == Decimal("0"):
        return Decimal("0")
    b = avg_win / avg_loss
    p = win_rate
    q = Decimal("1") - p
    kelly = (b * p - q) / b
    return max(Decimal("0"), min(kelly, Decimal("0.25")))


def fixed_risk_usd(capital: Decimal, risk_per_trade_pct: Decimal, price: Decimal) -> SizingResult:
    risk_amount = capital * risk_per_trade_pct / Decimal("100")
    size = risk_amount / price if price > Decimal("0") else Decimal("0")
    return SizingResult(suggested_size=size, max_size=size)
