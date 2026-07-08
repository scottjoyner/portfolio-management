from __future__ import annotations
import math

# Try Rust-native tcost backend
try:
    from rust_core import (
        tcost_estimate_spread_bps_py as _rust_estimate_spread_bps,
        tcost_impact_bps_py as _rust_impact_bps,
        tcost_effective_fill_price_py as _rust_effective_fill_price,
    )
    _HAS_RUST_TCOST = True
except ImportError:
    _HAS_RUST_TCOST = False


def _bps(x: float) -> float:
    return x / 10000.0


def estimate_spread_bps(bid: float, ask: float) -> float:
    if _HAS_RUST_TCOST:
        return _rust_estimate_spread_bps(bid, ask)
    if bid <= 0 or ask <= 0:
        raise ValueError(f"Invalid bid/ask prices: bid={bid}, ask={ask}")
    if ask < bid:
        raise ValueError(f"Ask ({ask}) must be >= bid ({bid})")
    return 20000.0 * (ask - bid) / (ask + bid)


def impact_bps(notional_usd: float, impact_coeff: float) -> float:
    if _HAS_RUST_TCOST:
        return _rust_impact_bps(notional_usd, impact_coeff)
    if notional_usd <= 0:
        return 0.0
    return impact_coeff * math.sqrt(max(1e-9, notional_usd / 10000.0))


def effective_fill_price(
    side: str,
    mid: float,
    bid: float,
    ask: float,
    notional_usd: float,
    taker_fee_bps: float = 8.0,
    slippage_bps: float = 0.0,
    impact_coeff: float = 1.5,
) -> float:
    if _HAS_RUST_TCOST:
        return _rust_effective_fill_price(
            side, mid, bid, ask, notional_usd,
            taker_fee_bps, slippage_bps, impact_coeff,
        )
    if mid <= 0:
        raise ValueError(f"Invalid mid price: {mid}")
    spr = estimate_spread_bps(bid, ask)
    imp = impact_bps(notional_usd, impact_coeff)
    total_bps = spr / 2.0 + slippage_bps + imp + taker_fee_bps
    if side.lower() == "buy":
        return mid * (1.0 + _bps(total_bps))
    return mid * (1.0 - _bps(total_bps))
