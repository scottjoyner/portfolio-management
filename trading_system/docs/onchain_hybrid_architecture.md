# Hybrid CEX + Onchain Execution Architecture (Base-first)

## Core Control Flow
1. Opportunity detector emits candidate route with expected gross edge.
2. `PathAnalyzer` computes full execution plan: route graph, economics, diagnostics, risk, simulation.
3. Mandatory safety gates enforce allowlist, selector checks, simulation success, trust threshold, and positive net edge.
4. `ApprovalPacketBuilder` generates concise voice summary + detailed operator payload.
5. If approved, execution proceeds and `HybridHedgeLinker` stages Coinbase hedge plan.
6. `ProfitCaptureEngine` enforces net-edge floor and realized gain sweep policy.

## Implemented Modules
- Contract registry + emergency denylist: `onchain/contracts/registry/service.py`
- Token/contract safety scoring: `onchain/security/*/engine.py`
- Pre-trade path analysis + simulation: `onchain/simulation/path_simulator/analyzer.py`
- Approval packet schema + voice-ready summaries: `onchain/security/approval_gates/approval_packet.py`
- Opportunity ranking and decay-aware prioritization: `onchain/strategies/execution/opportunity_ranker.py`
- Hybrid Coinbase hedge linkage and atomicity classification: `onchain/strategies/hedging/hybrid_hedge.py`
- Profit capture and reserve sweep engine: `onchain/strategies/treasury/profit_sweep.py`
- Control-plane API endpoints: `apps/api/main.py`

## Safety Guarantees in This Iteration
- Fail-closed if route has unknown/disallowed contract.
- Fail-closed when net edge is below threshold after modeled gas/slippage/hedge/bridge costs.
- Fail-closed when trust score or revert-risk violates policy.
- Simulation hash captured in every approval payload.
- Approval payload includes rollback plan and explicit operator actions.

## Backtesting/Replay Note
This iteration adds deterministic simulation primitives and schemas for CEX+DEX replay integration. Full chain-event replay and mainnet-fork execution adapters are staged for later phases.
