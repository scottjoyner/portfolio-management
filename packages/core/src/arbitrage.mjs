export const USD=1_000_000;
export function calcEffectiveCost(level, feeBps, slippageBps, extraBps=0){
  const base=level.priceMicros;
  const totalBps=feeBps+slippageBps+extraBps;
  return base + Math.floor(base*totalBps/10_000);
}
export function computeCrossVenueArb({yesA,noB,yesB,noA,feeBps,slippageBps,settlementRiskBps,staleMs,maxAgeMs}){
  if(staleMs>maxAgeMs) return {ok:false,reason:'stale_orderbook'};
  const a = calcEffectiveCost(yesA,feeBps,slippageBps,settlementRiskBps)+calcEffectiveCost(noB,feeBps,slippageBps,settlementRiskBps);
  const b = calcEffectiveCost(noA,feeBps,slippageBps,settlementRiskBps)+calcEffectiveCost(yesB,feeBps,slippageBps,settlementRiskBps);
  const pick = a<=b?['YES_A_NO_B',a,Math.min(yesA.size,noB.size)]:['NO_A_YES_B',b,Math.min(noA.size,yesB.size)];
  const edge=USD-pick[1];
  return edge>0?{ok:true,direction:pick[0],edgeMicros:edge,edgeBps:Math.floor(edge*10_000/USD),size:pick[2]}:{ok:false,reason:'no_true_arb'};
}
