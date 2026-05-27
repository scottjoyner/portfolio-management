export type Venue = "kalshi" | "polymarket";
export type Side = "yes" | "no";
export interface Market { id:string; venue:Venue; title:string; closeTime:string; resolvesAt:string; outcomes:[string,string]; rulesHash?:string; }
export interface PriceLevel { priceMicros:number; size:number; }
export interface OrderBook { marketId:string; venue:Venue; yesAsks:PriceLevel[]; noAsks:PriceLevel[]; ts:number; }
export interface ArbitrageOpportunity { pairId:string; direction:string; totalCostMicros:number; edgeBps:number; size:number; }
export interface RiskDecision { approved:boolean; reasons:string[]; }
