from datetime import datetime, timedelta
from decimal import Decimal

from onchain.dex.clmm.schemas import ActionSimulationRequest
from onchain.simulation.path_simulator_mm import simulate_action
from onchain.simulation.route_profitability import evaluate_route_profitability


def test_route_profitability_rejects_negative():
    req = ActionSimulationRequest(action_type="deploy_and_hedge", wallet="0xw", amount_usd=Decimal("100000"), slippage_bps_limit=Decimal("50"), deadline_ts=datetime.utcnow() + timedelta(minutes=5))
    sim = simulate_action(req)
    rep = evaluate_route_profitability(sim, Decimal("999999"))
    assert not rep.is_profitable
