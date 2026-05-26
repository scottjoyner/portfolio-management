export function reconcileState({orders,fills,positions,balances}){
  const issues=[];
  const filledByOrder=new Map();
  for(const f of fills){filledByOrder.set(f.orderId,(filledByOrder.get(f.orderId)||0)+f.size);}
  for(const o of orders){
    if(o.status==='filled' && !filledByOrder.has(o.id)) issues.push({type:'missing_fill_for_filled_order',orderId:o.id});
    if(o.status==='unknown') issues.push({type:'unknown_order_status',orderId:o.id});
  }
  const netPos=positions.reduce((a,p)=>a+p.sizeSigned,0);
  if(!Number.isFinite(netPos)) issues.push({type:'invalid_position'});
  return {ok:issues.length===0,issues,requiresBlock:issues.some(i=>i.type.includes('unknown')||i.type.includes('missing_fill'))};
}
