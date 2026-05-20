from decimal import Decimal

from onchain.dex.clmm.hedge_model import compute_hedge_band, should_hedge


def test_hedge_band_trigger():
    band = compute_hedge_band(Decimal("0.2"), Decimal("1000"), Decimal("0.2"))
    assert should_hedge(Decimal("2000"), band)
