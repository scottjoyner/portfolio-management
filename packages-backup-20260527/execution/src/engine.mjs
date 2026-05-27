export function canLive(cfg){
  return cfg.LIVE_TRADING===true && cfg.PAPER_TRADING===false && cfg.REQUIRE_MANUAL_APPROVAL===true;
}
export function validateSafetyGates(ctx){
  const r=[];
  if(ctx.killSwitch) r.push('kill_switch');
  if(!ctx.pairApproved) r.push('pair_not_approved');
  if(!ctx.complianceApproved) r.push('compliance_failed');
  if(ctx.edgeBps<ctx.minEdgeBps) r.push('insufficient_edge');
  if(ctx.stale) r.push('stale_orderbook');
  if(!ctx.depthOk) r.push('insufficient_depth');
  if(ctx.live && !ctx.credentialsPresent) r.push('missing_credentials');
  if(ctx.live && !ctx.runtimeConfirmed) r.push('runtime_confirmation_missing');
  return {approved:r.length===0,reasons:r};
}
export function handleAmbiguousWrite(){return {action:'reconcile_before_retry',retried:false};}
