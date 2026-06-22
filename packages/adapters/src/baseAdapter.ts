import type { IBrokerAdapter, AdapterCapabilities, PreviewResult, SubmitResult, OrderStatusResult, PositionInfo, BalanceInfo, VenueHealth } from './types.js';
import type { OrderIntent, OrderFill, OrderStatus, BrokerAccount, Market, OrderBook, Venue, ExecutionMode } from '@pkg/core/types.js';

export abstract class BaseBrokerAdapter implements IBrokerAdapter {
  abstract readonly name: string;
  abstract readonly venue: Venue;
  abstract readonly capabilities: AdapterCapabilities;
  abstract readonly mode: ExecutionMode;

  protected connected = false;

  async connect(): Promise<boolean> {
    this.connected = true;
    return true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
  }

  async health(): Promise<VenueHealth> {
    return {
      ok: this.connected,
      venue: this.venue,
      latencyMs: 0,
      mode: this.mode,
      authenticated: this.connected,
      errors: this.connected ? undefined : ['not_connected'],
    };
  }

  abstract getAccounts(): Promise<BrokerAccount[]>;
  abstract getBalances(accountId: string): Promise<BalanceInfo[]>;
  abstract getPositions(accountId: string): Promise<PositionInfo[]>;

  abstract discoverMarkets(): Promise<Market[]>;
  abstract getOrderBook(marketId: string): Promise<OrderBook>;
  abstract getQuote(marketId: string): Promise<{ bid: number; ask: number; mid: number; spreadBps: number }>;

  abstract previewOrder(order: OrderIntent): Promise<PreviewResult>;
  abstract submitOrder(order: OrderIntent): Promise<SubmitResult>;
  abstract cancelOrder(brokerOrderId: string): Promise<boolean>;
  abstract getOrderStatus(brokerOrderId: string): Promise<OrderStatusResult>;

  async getHistoricalRates(
    _marketId: string,
    _start: string,
    _end: string,
    _granularity: string,
  ): Promise<{ time: number; open: number; high: number; low: number; close: number; volume: number }[]> {
    return [];
  }
}
