const MODES=new Set(['mock','paper','kalshi-demo','polymarket-readonly','staging','live']);
const req=(v)=>v==='true';
export function validateModeConfig(env){
  const mode=env.MODE||'paper';
  if(!MODES.has(mode)) return {ok:false,reasons:['invalid_mode']};
  const reasons=[];
  if(mode==='mock' || mode==='paper'){
    if(req(env.LIVE_TRADING)) reasons.push('live_must_be_false');
  }
  if(mode==='polymarket-readonly' && req(env.ALLOW_POLYMARKET_ORDER_SUBMISSION)) reasons.push('readonly_cannot_submit_orders');
  if(mode==='live'){
    if(!req(env.LIVE_TRADING)) reasons.push('live_trading_required');
    if(req(env.PAPER_TRADING)) reasons.push('paper_must_be_false');
    if(!req(env.REQUIRE_RUNTIME_CONFIRMATION)) reasons.push('runtime_confirmation_required');
    if(!req(env.REQUIRE_MANUAL_APPROVAL)) reasons.push('manual_approval_required');
    if(!req(env.REQUIRE_APPROVED_MARKET_PAIR)) reasons.push('approved_pair_required');
    if(!req(env.ALLOW_LIVE_SETTLEMENT_REDEMPTION)) reasons.push('live_settlement_flag_required');
  }
  return {ok:reasons.length===0,reasons,mode};
}
