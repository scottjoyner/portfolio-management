import test from 'node:test';
import assert from 'node:assert/strict';
import { validateSafetyGates, handleAmbiguousWrite } from '../packages/execution/src/engine.mjs';

test('live trading disabled by default context',()=>{
 const d=validateSafetyGates({killSwitch:false,pairApproved:true,complianceApproved:true,edgeBps:200,minEdgeBps:100,stale:false,depthOk:true,live:true,credentialsPresent:false,runtimeConfirmed:true});
 assert.equal(d.approved,false); assert.ok(d.reasons.includes('missing_credentials'));
});

test('kill switch blocks execution',()=>{const d=validateSafetyGates({killSwitch:true,pairApproved:true,complianceApproved:true,edgeBps:200,minEdgeBps:100,stale:false,depthOk:true,live:false,credentialsPresent:true,runtimeConfirmed:true});assert.equal(d.approved,false);});
test('unapproved pair blocks',()=>{const d=validateSafetyGates({killSwitch:false,pairApproved:false,complianceApproved:true,edgeBps:200,minEdgeBps:100,stale:false,depthOk:true,live:false,credentialsPresent:true,runtimeConfirmed:true});assert.ok(d.reasons.includes('pair_not_approved'));});
test('compliance fail blocks',()=>{const d=validateSafetyGates({killSwitch:false,pairApproved:true,complianceApproved:false,edgeBps:200,minEdgeBps:100,stale:false,depthOk:true,live:false,credentialsPresent:true,runtimeConfirmed:true});assert.ok(d.reasons.includes('compliance_failed'));});
test('insufficient edge/stale/depth block',()=>{const d=validateSafetyGates({killSwitch:false,pairApproved:true,complianceApproved:true,edgeBps:10,minEdgeBps:100,stale:true,depthOk:false,live:false,credentialsPresent:true,runtimeConfirmed:true});assert.ok(d.reasons.includes('insufficient_edge'));assert.ok(d.reasons.includes('stale_orderbook'));assert.ok(d.reasons.includes('insufficient_depth'));});
test('ambiguous write reconciles before retry',()=>{const r=handleAmbiguousWrite();assert.equal(r.retried,false);assert.equal(r.action,'reconcile_before_retry');});
