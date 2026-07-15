from decimal import Decimal

from trading_system.execution.hybrid.exposure_sync import validate_hybrid_exposure


def test_onchain_zero_coverage_is_zero():
    snap = validate_hybrid_exposure(Decimal("0"), Decimal("500"))
    assert snap.onchain_delta_usd == Decimal("0")
    assert snap.cex_delta_usd == Decimal("500")
    assert snap.net_delta_usd == Decimal("500")
    assert snap.hedge_coverage_ratio == Decimal("0")


def test_onchain_nonzero_small_ratio():
    snap = validate_hybrid_exposure(Decimal("1000"), Decimal("200"))
    assert snap.net_delta_usd == Decimal("1200")
    assert snap.hedge_coverage_ratio == Decimal("0.2")


def test_onchain_nonzero_large_ratio_capped():
    snap = validate_hybrid_exposure(Decimal("100"), Decimal("500"))
    # 500/100 = 5 -> capped at 2
    assert snap.hedge_coverage_ratio == Decimal("2")
