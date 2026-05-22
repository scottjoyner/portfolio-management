import test from 'node:test';
import assert from 'node:assert/strict';
import { evaluateRisk } from '../../packages/risk/src/engine.mjs';
import { compareMarkets } from '../../packages/matching/src/engine.mjs';
import { reconcileState } from '../../packages/reconciliation/src/engine.mjs';
import { runCertification } from '../../packages/certification/src/mockCert.mjs';
import { fixtures } from '../../packages/testing/src/fixtures.mjs';

test('risk engine fails closed for live prerequisites',()=>{
  const r=evaluateRisk({killSwitch:false,unresolvedRecon:true,pairApproved:true,complianceApproved:true,orderBookAgeMs:10,maxOrderbookStalenessMs:100,edgeBps:200,minEdgeBps:100,balanceSufficient:true,notionalMicros:1,maxNotionalMicros:2,live:true,runtimeConfirmed:false,credentialsPresent:false,venueModeExplicit:false,liveTrading:true,paperTrading:false});
  assert.equal(r.approved,false);
  assert.ok(r.reasons.includes('unresolved_reconciliation_discrepancy'));
  assert.ok(r.reasons.includes('runtime_confirmation_missing'));
});

test('matching flags non-equivalent markets',()=>{
  const m=compareMarkets(fixtures.safeA,fixtures.unsafeB);
  assert.equal(m.equivalent,false);
  assert.ok(m.flags.length>=3);
});

test('reconciliation blocks unknown statuses',()=>{
  const rec=reconcileState({orders:[{id:'o2',status:'unknown'}],fills:[],positions:[{sizeSigned:0}],balances:[]});
  assert.equal(rec.ok,false);
  assert.equal(rec.requiresBlock,true);
});

test('certification reports live not certified',()=>{
  const out=runCertification(fixtures);
  assert.equal(out.pass,true);
  assert.equal(out.liveTradingCertified,false);
});
