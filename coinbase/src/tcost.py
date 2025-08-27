from __future__ import annotations
import math
from typing import Tuple

def _bps(x: float) -> float:
    return x / 10000.0

def estimate_spread_bps(bid: float, ask: float) -> float:
    if bid <= 0 or ask <= 0:
        return 0.0
    mid = (bid + ask) / 2.0
    return 20000.0 * (ask - bid) / (ask + bid)  # ~2*(ask-bid)/mid in bps

def impact_bps(notional_usd: float, impact_coeff: float) -> float:
    # Simple square-root impact model (bps per sqrt($notional/10k))
    if notional_usd <= 0: return 0.0
    scale = max(1e-9, notional_usd / 10000.0)
    return impact_coeff * math.sqrt(scale)

def effective_fill_price(side: str, mid: float, bid: float, ask: float, notional_usd: float,
                         taker_fee_bps: float = 8.0, slippage_bps: float = 0.0, impact_coeff: float = 1.5) -> float:
    """
    Expected market fill price with fees + spread + optional extra slippage/impact.
    side: 'buy' → price marked up; 'sell' → price marked down.
    """
    spr = estimate_spread_bps(bid, ask)
    imp = impact_bps(notional_usd, impact_coeff)
    # Half-spread expected + explicit slippage + impact + taker fee (on notional)
    total_bps = spr/2.0 + slippage_bps + imp + taker_fee_bps
    if mid <= 0: return 0.0
    if side.lower() == "buy":
        return mid * (1.0 + _bps(total_bps))
    else:
        # For sells, effective received price accounts for costs → lower than mid
        return mid * (1.0 - _bps(total_bps))

def expected_rr_long(entry: float, stop: float, target: float, mid: float, bid: float, ask: float, notional_usd: float,
                     taker_fee_bps: float, slippage_bps: float, impact_coeff: float) -> float:
    e_entry = effective_fill_price("buy", mid, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
    e_stop  = effective_fill_price("sell", mid if stop<=0 else stop, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
    e_tgt   = effective_fill_price("sell", mid if target<=0 else target, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
    risk  = max(1e-9, e_entry - e_stop)
    reward= max(0.0, e_tgt - e_entry)
    return reward / risk if risk>0 else 0.0

def expected_rr_short(entry: float, stop: float, target: float, mid: float, bid: float, ask: float, notional_usd: float,
                      taker_fee_bps: float, slippage_bps: float, impact_coeff: float) -> float:
    e_entry = effective_fill_price("sell", mid, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
    e_stop  = effective_fill_price("buy", mid if stop<=0 else stop, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
    e_tgt   = effective_fill_price("buy", mid if target<=0 else target, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
    risk  = max(1e-9, e_stop - e_entry)
    reward= max(0.0, e_entry - e_tgt)
    return reward / risk if risk>0 else 0.0
