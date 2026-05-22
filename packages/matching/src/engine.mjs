const n=s=>String(s||'').toLowerCase().replace(/[^a-z0-9.%:-]/g,' ').replace(/\s+/g,' ').trim();
export function compareMarkets(a,b){
  const flags=[];
  if(n(a.resolutionSource)!==n(b.resolutionSource)) flags.push('different_resolution_source');
  if(a.closeTime!==b.closeTime) flags.push('different_close_time');
  if(a.resolveTime!==b.resolveTime) flags.push('different_resolution_time');
  if(String(a.threshold??'')!==String(b.threshold??'')) flags.push('different_threshold');
  if(n(a.timezone)!==n(b.timezone)) flags.push('different_timezone');
  if(n(a.marketType)!==n(b.marketType)) flags.push('different_market_type');
  if(Boolean(a.cancellable)!==Boolean(b.cancellable)) flags.push('different_cancellation_risk');
  const sameTitle=n(a.title)===n(b.title);
  const confidence=Math.max(0, (sameTitle?0.9:0.5)-flags.length*0.1);
  const equivalent=confidence>=0.8 && flags.length===0;
  return {equivalent,confidence,flags,status:equivalent?'proposed':'proposed'};
}
