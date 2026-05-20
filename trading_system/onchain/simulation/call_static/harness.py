from __future__ import annotations

import hashlib

from onchain.models import ExecutionRoute, SimulationResult


class CallStaticHarness:
    def simulate(self, route: ExecutionRoute, amount_in: float, min_out: float, modeled_out: float) -> SimulationResult:
        success = modeled_out >= min_out and amount_in > 0
        digest = hashlib.sha256(f"{route.chain}:{route.protocol}:{amount_in}:{min_out}:{modeled_out}".encode()).hexdigest()
        return SimulationResult(
            success=success,
            simulation_hash=digest,
            gas_used=max(21_000, 65_000 + len(route.contracts_touched) * 25_000),
            min_out_respected=modeled_out >= min_out,
            revert_reason=None if success else "MIN_OUT_VIOLATION",
        )
