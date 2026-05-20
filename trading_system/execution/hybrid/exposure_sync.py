from __future__ import annotations

from decimal import Decimal

from onchain.dex.clmm.schemas import HybridExposureSnapshot


def validate_hybrid_exposure(onchain_delta: Decimal, cex_delta: Decimal) -> HybridExposureSnapshot:
    net = onchain_delta + cex_delta
    coverage = Decimal("0") if onchain_delta == 0 else min(Decimal("2"), abs(cex_delta / onchain_delta))
    return HybridExposureSnapshot(
        onchain_delta_usd=onchain_delta,
        cex_delta_usd=cex_delta,
        net_delta_usd=net,
        hedge_coverage_ratio=coverage,
    )
