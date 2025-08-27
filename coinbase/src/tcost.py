from __future__ import annotations
import math
def _bps(x: float) -> float: return x/10000.0
def estimate_spread_bps(bid: float, ask: float) -> float:
    if bid<=0 or ask<=0: return 0.0
    return 20000.0*(ask-bid)/(ask+bid)
def impact_bps(notional_usd: float, impact_coeff: float) -> float:
    if notional_usd<=0: return 0.0
    return impact_coeff*math.sqrt(max(1e-9, notional_usd/10000.0))
def effective_fill_price(side: str, mid: float, bid: float, ask: float, notional_usd: float, taker_fee_bps: float=8.0, slippage_bps: float=0.0, impact_coeff: float=1.5) -> float:
    spr = estimate_spread_bps(bid, ask); imp = impact_bps(notional_usd, impact_coeff)
    total_bps = spr/2.0 + slippage_bps + imp + taker_fee_bps
    if mid<=0: return 0.0
    return mid*(1.0 + _bps(total_bps)) if side.lower()=="buy" else mid*(1.0 - _bps(total_bps))
