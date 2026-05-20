from decimal import Decimal

from onchain.dex.clmm.schemas import StressScenarioInput
from research.lp.stress_paths import run_stress_case


def test_stress_case_runs():
    out = run_stress_case(StressScenarioInput(name="shock", spot_shock_pct=Decimal("15"), vol_multiplier=Decimal("2"), gas_multiplier=Decimal("1.5"), liquidity_haircut_pct=Decimal("20")))
    assert out.name == "shock"
