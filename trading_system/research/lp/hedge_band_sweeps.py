from __future__ import annotations

from decimal import Decimal

from onchain.dex.clmm.hedge_model import compute_hedge_band


def sweep_bands(vols: list[Decimal], base_band: Decimal) -> dict[str, Decimal]:
    return {str(v): compute_hedge_band(v, base_band, Decimal("0.2")) for v in vols}
