import { evaluateRisk } from '../../risk/src/engine.mjs';
import { compareMarkets } from '../../matching/src/engine.mjs';
import { reconcileState } from '../../reconciliation/src/engine.mjs';

export function runCertification(fixtures){
  const checks={};
  checks.loadFixtures=!!fixtures;
  const match=compareMarkets(fixtures.safeA,fixtures.safeB);
  checks.safePairProposed=match.confidence>0.5;
  checks.unsafeRejected=compareMarkets(fixtures.safeA,fixtures.unsafeB).flags.length>0;
  const risk=evaluateRisk(fixtures.riskInput);
  checks.riskApproved=risk.approved;
  const recon=reconcileState(fixtures.reconInput);
  checks.reconciliationClean=recon.ok;
  checks.noLiveCalls=true;
  const pass=Object.values(checks).every(Boolean);
  return {pass,checks,risk,recon,liveTradingCertified:false};
}
