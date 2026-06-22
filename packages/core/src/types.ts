export type Venue = 'kalshi' | 'polymarket' | 'coinbase' | 'paper' | 'snaptrade';
export type Side = 'yes' | 'no' | 'buy' | 'sell';
export type AssetClass = 'crypto' | 'equity' | 'prediction_market' | 'commodity' | 'forex';

export interface Market {
  id: string;
  venue: Venue;
  title: string;
  symbol?: string;
  assetClass: AssetClass;
  closeTime: string;
  resolvesAt?: string;
  outcomes?: [string, string];
  rulesHash?: string;
  status: 'active' | 'settled' | 'closed';
  tickSize?: number;
  minSize?: number;
  pricePrecision?: number;
}

export interface PriceLevel {
  priceMicros: number;
  size: number;
}

export interface OrderBook {
  marketId: string;
  venue: Venue;
  yesAsks: PriceLevel[];
  noAsks: PriceLevel[];
  bids: PriceLevel[];
  asks: PriceLevel[];
  ts: number;
}

export interface ArbitrageOpportunity {
  pairId: string;
  direction: string;
  totalCostMicros: number;
  edgeBps: number;
  size: number;
}

export interface RiskDecision {
  approved: boolean;
  reasons: string[];
}

export type OrderStatus = 'draft' | 'approved' | 'rejected' | 'submitted' | 'partially_filled' | 'filled' | 'cancelled' | 'expired' | 'failed';
export type ExecutionMode = 'paper' | 'live' | 'demo' | 'readonly';
export type TimeInForce = 'GTC' | 'IOC' | 'FOK' | 'DAY';

export interface OrderIntent {
  id: string;
  strategyId: string;
  opportunityId?: string;
  marketId: string;
  symbol?: string;
  venue: Venue;
  side: Side;
  quantity: number;
  price?: number;
  stopPrice?: number;
  targetPrice?: number;
  timeInForce: TimeInForce;
  executionMode: ExecutionMode;
  orderType: 'market' | 'limit' | 'stop' | 'stop_limit' | 'bracket';
  notional?: number;
  feeBps?: number;
  slippageBps?: number;
  confidenceScore?: number;
  convictionWeight?: number;
  tags?: Record<string, string>;
  parentOrderId?: string;
  createdAt: string;
  updatedAt: string;
}

export interface OrderFill {
  id: string;
  orderId: string;
  marketId: string;
  venue: Venue;
  side: Side;
  quantity: number;
  price: number;
  fee: number;
  feeCurrency: string;
  liquidity: 'taker' | 'maker';
  filledAt: string;
  settlementStatus: 'pending' | 'settled' | 'failed';
}

export interface ExecutionState {
  id: string;
  strategyId: string;
  opportunityId?: string;
  accountId: string;
  mode: ExecutionMode;
  status: OrderStatus;
  orders: OrderIntent[];
  fills: OrderFill[];
  positionId?: string;
  confidenceScore: number;
  convictionWeight: number;
  riskDecision: RiskDecision;
  startedAt: string;
  completedAt?: string;
  lastHeartbeatAt: string;
  error?: string;
  metadata?: Record<string, unknown>;
}

export interface ConfidenceScore {
  overall: number;
  strategyConviction: number;
  marketCondition: number;
  riskAssessment: number;
  historicalPerformance: number;
  dataFreshness: number;
  explanation: string;
  components: Record<string, number>;
}

export interface BrokerAccount {
  id: string;
  name: string;
  provider: Venue;
  status: 'connected' | 'disconnected' | 'error' | 'mock';
  currency: string;
  cash: number;
  nav: number;
  availableBalance: number;
  buyingPower: number;
  marginUsed: number;
  updatedAt: string;
  capabilities: ('spot' | 'margin' | 'futures' | 'options' | 'prediction')[];
  mode: ExecutionMode;
}
