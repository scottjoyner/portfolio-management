const norm=s=>String(s||'').toLowerCase().replace(/[^a-z0-9 %:.-]/g,' ').replace(/\s+/g,' ').trim();
export function matchMarkets(a,b){
  const flags=[];
  if(norm(a.resolutionSource)!==norm(b.resolutionSource)) flags.push('different_resolution_source');
  if(a.closeTime!==b.closeTime) flags.push('different_close_time');
  if(a.threshold!==b.threshold) flags.push('different_threshold');
  if(a.timezone!==b.timezone) flags.push('different_timezone');
  if(a.marketType!==b.marketType) flags.push('market_type_mismatch');
  const titleA=norm(a.title), titleB=norm(b.title);
  const exact=titleA===titleB;
  const conf=exact?0.95:(titleA.includes(titleB)||titleB.includes(titleA)?0.75:0.35);
  const status=flags.length? 'proposed':'proposed';
  return {confidence:Math.max(0,conf-flags.length*0.1),flags,status,requiresManualApproval:true};
}
