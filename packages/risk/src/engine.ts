import type { RiskDecision } from '@pkg/core/types.js';

export interface RiskInput {
  killSwitch: boolean;
  unresolvedRecon: boolean;
  pairApproved: boolean;
  complianceApproved: boolean;
  orderBookAgeMs: number;
  maxOrderbookStalenessMs: number;
  edgeBps: number;
  minEdgeBps: number;
  balanceSufficient: boolean;
  notionalMicros: number;
  maxNotionalMicros: number;
  live: boolean;
  runtimeConfirmed?: boolean;
  credentialsPresent?: boolean;
  venueModeExplicit?: boolean;
  liveTrading?: boolean;
  paperTrading?: boolean;
}

export function evaluateRisk(input: RiskInput): RiskDecision {
  const reasons: string[] = [];
  if (input.killSwitch) reasons.push('kill_switch_on');
  if (input.unresolvedRecon) reasons.push('unresolved_reconciliation_discrepancy');
  if (!input.pairApproved) reasons.push('pair_not_approved');
  if (!input.complianceApproved) reasons.push('compliance_rejected');
  if (input.orderBookAgeMs > input.maxOrderbookStalenessMs) reasons.push('orderbook_stale');
  if (input.edgeBps < input.minEdgeBps) reasons.push('edge_below_min');
  if (!input.balanceSufficient) reasons.push('insufficient_balance');
  if (input.notionalMicros > input.maxNotionalMicros) reasons.push('notional_limit_exceeded');
  if (input.live) {
    if (!input.runtimeConfirmed) reasons.push('runtime_confirmation_missing');
    if (!input.credentialsPresent) reasons.push('missing_credentials');
    if (!input.venueModeExplicit) reasons.push('venue_mode_not_explicit');
    if (!(input.liveTrading && !input.paperTrading)) reasons.push('live_mode_flags_invalid');
  }
  return { approved: reasons.length === 0, reasons };
}
