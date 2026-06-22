import type { OrderIntent, OrderFill, OrderStatus, ExecutionMode, Venue, BrokerAccount, Market, OrderBook, Side, TimeInForce } from '@pkg/core/types.js';

export interface AdapterCapabilities {
  venue: Venue;
  executionModes: ExecutionMode[];
  orderTypes: string[];
  timeInForce: TimeInForce[];
  features: string[];
  maxOrdersPerSecond: number;
  maxNotional: number;
}

export interface PreviewResult {
  ok: boolean;
  preview?: {
    estimatedPrice: number;
    estimatedFee: number;
    estimatedSlippage: number;
    estimatedTotal: number;
    liquidity: 'taker' | 'maker';
    slippageBps: number;
    validUntil: string;
  };
  errors?: string[];
  warnings?: string[];
}

export interface SubmitResult {
  ok: boolean;
  brokerOrderId?: string;
  status: OrderStatus;
  fills?: OrderFill[];
  errors?: string[];
  raw?: unknown;
}

export interface OrderStatusResult {
  brokerOrderId: string;
  status: OrderStatus;
  fills: OrderFill[];
  remainingQuantity: number;
  averageFillPrice?: number;
}

export interface PositionInfo {
  symbol: string;
  venue: Venue;
  quantity: number;
  averagePrice: number;
  markPrice: number;
  unrealizedPnl: number;
  realizedPnl: number;
  currency: string;
  status: 'open' | 'closed';
  openedAt: string;
  closedAt?: string;
}

export interface BalanceInfo {
  currency: string;
  total: number;
  available: number;
  held: number;
}

export interface VenueHealth {
  ok: boolean;
  venue: Venue;
  latencyMs: number;
  mode: ExecutionMode;
  authenticated: boolean;
  errors?: string[];
}

export interface IBrokerAdapter {
  readonly name: string;
  readonly venue: Venue;
  readonly capabilities: AdapterCapabilities;
  readonly mode: ExecutionMode;

  connect(): Promise<boolean>;
  disconnect(): Promise<void>;
  health(): Promise<VenueHealth>;

  getAccounts(): Promise<BrokerAccount[]>;
  getBalances(accountId: string): Promise<BalanceInfo[]>;
  getPositions(accountId: string): Promise<PositionInfo[]>;

  discoverMarkets(): Promise<Market[]>;
  getOrderBook(marketId: string): Promise<OrderBook>;
  getQuote(marketId: string): Promise<{ bid: number; ask: number; mid: number; spreadBps: number }>;

  previewOrder(order: OrderIntent): Promise<PreviewResult>;
  submitOrder(order: OrderIntent): Promise<SubmitResult>;
  cancelOrder(brokerOrderId: string): Promise<boolean>;
  getOrderStatus(brokerOrderId: string): Promise<OrderStatusResult>;

  getHistoricalRates(marketId: string, start: string, end: string, granularity: string): Promise<{ time: number; open: number; high: number; low: number; close: number; volume: number }[]>;
}
