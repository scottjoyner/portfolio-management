from decimal import Decimal

from onchain.dex.clmm.fee_accounting import fee_capture_efficiency
from onchain.dex.clmm.il_model import gas_adjusted_net_pnl, il_vs_hold_benchmark, lp_pnl_decomposition


def test_il_exists():
    il = il_vs_hold_benchmark(Decimal("2000"), Decimal("2300"), Decimal("1"), Decimal("2000"))
    assert il < 0


def test_fee_efficiency():
    eff = fee_capture_efficiency(Decimal("90"), Decimal("100000"), Decimal("10"))
    assert eff == Decimal("0.9")


def test_pnl_decomp():
    out = lp_pnl_decomposition(Decimal("1000"), Decimal("1100"), Decimal("50"), Decimal("10"), Decimal("-5"), Decimal("8"), Decimal("4"), Decimal("3"), Decimal("100"))
    assert "net_pnl_usd" in out
    assert gas_adjusted_net_pnl(out["net_pnl_usd"], Decimal("8")) == out["net_pnl_usd"] - Decimal("8")
