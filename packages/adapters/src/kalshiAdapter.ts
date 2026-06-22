import { BaseBrokerAdapter } from './baseAdapter.js';
import { KalshiClient } from '@pkg/kalshi/client.js';
import type { AdapterCapabilities, PreviewResult, SubmitResult, OrderStatusResult, PositionInfo, BalanceInfo, VenueHealth } from './types.js';
import type { OrderIntent, OrderFill, BrokerAccount, Market, OrderBook, ExecutionMode, Venue } from '@pkg/core/types.js';

export class KalshiBrokerAdapter extends BaseBrokerAdapter {
  readonly name = 'kalshi';
  readonly venue: Venue = 'kalshi';
  readonly mode: ExecutionMode;
  readonly capabilities: AdapterCapabilities;

  private client: KalshiClient;

  constructor(mode: ExecutionMode = 'demo') {
    super();
    this.mode = mode;
    this.client = new KalshiClient({
      environment: mode === 'live' ? 'prod' : 'demo',
      apiKeyId: process.env.KALSHI_API_KEY_ID,
      privateKeyPath: process.env.KALSHI_PRIVATE_KEY_PATH,
      apiKey: process.env.KALSHI_EMAIL,
      apiSecret: process.env.KALSHI_PASSWORD,
    });
    this.capabilities = {
      venue: 'kalshi',
      executionModes: mode === 'live' ? ['paper', 'live'] : ['demo'],
      orderTypes: ['market', 'limit'],
      timeInForce: ['GTC', 'DAY'],
      features: ['prediction_markets', 'market_discovery', 'quotes'],
      maxOrdersPerSecond: 5,
      maxNotional: 100_000,
    };
  }

  async connect(): Promise<boolean> {
    this.connected = await this.client.login();
    return this.connected;
  }

  async getAccounts(): Promise<BrokerAccount[]> {
    const balance = await this.client.getBalance();
    const cash = balance.cash ?? 0;
    const available = balance.available ?? cash;
    return [{
      id: 'acct-kalshi-primary',
      name: `Kalshi ${this.mode === 'live' ? 'Production' : 'Demo'} Account`,
      provider: 'kalshi',
      status: this.connected ? 'connected' : 'mock',
      currency: 'USD',
      cash,
      nav: balance.portfolio_value ?? cash,
      availableBalance: available,
      buyingPower: cash,
      marginUsed: 0,
      updatedAt: new Date().toISOString(),
      capabilities: ['prediction'],
      mode: this.mode,
    }];
  }

  async getBalances(_accountId: string): Promise<BalanceInfo[]> {
    const balance = await this.client.getBalance();
    const cash = balance.cash ?? 0;
    const available = balance.available ?? cash;
    return [{ currency: 'USD', total: cash, available, held: cash - available }];
  }

  async getPositions(_accountId: string): Promise<PositionInfo[]> {
    return [];
  }

  async discoverMarkets(): Promise<Market[]> {
    const raw = await this.client.listMarkets({ limit: 50 });
    return raw.map(m => ({
      id: m.id,
      venue: 'kalshi' as Venue,
      title: m.title,
      assetClass: 'prediction_market' as const,
      closeTime: m.close_time,
      resolvesAt: m.settle_time,
      outcomes: ['Yes', 'No'] as [string, string],
      status: (m.status === 'open' ? 'active' : m.status) as 'active' | 'settled' | 'closed',
      tickSize: m.tick_size,
      minSize: m.min_size,
    }));
  }

  async getOrderBook(_marketId: string): Promise<OrderBook> {
    const market = await this.client.getMarket(_marketId);
    return {
      marketId: _marketId,
      venue: 'kalshi',
      yesAsks: market ? [{ priceMicros: market.yes_ask * 10000, size: 1000 }] : [],
      noAsks: market ? [{ priceMicros: market.no_ask * 10000, size: 1000 }] : [],
      bids: [],
      asks: [],
      ts: Date.now(),
    };
  }

  async getQuote(marketId: string): Promise<{ bid: number; ask: number; mid: number; spreadBps: number }> {
    const market = await this.client.getMarket(marketId);
    if (!market) return { bid: 0, ask: 0, mid: 0, spreadBps: 0 };
    return {
      bid: market.yes_bid / 100,
      ask: market.yes_ask / 100,
      mid: (market.yes_bid + market.yes_ask) / 200,
      spreadBps: market.yes_ask > 0 ? ((market.yes_ask - market.yes_bid) / market.yes_ask) * 10000 : 0,
    };
  }

  async previewOrder(order: OrderIntent): Promise<PreviewResult> {
    return { ok: true, preview: { estimatedPrice: order.price || 0.5, estimatedFee: 0.01, estimatedSlippage: 0.005, estimatedTotal: order.quantity * (order.price || 0.5), liquidity: 'taker', slippageBps: 10, validUntil: new Date(Date.now() + 30000).toISOString() } };
  }

  async submitOrder(order: OrderIntent): Promise<SubmitResult> {
    if (!this.connected) {
      return { ok: false, status: 'rejected', errors: ['kalshi_not_authenticated'] };
    }
    const result = await this.client.createOrder({
      market_id: order.marketId,
      side: order.side === 'buy' ? 'yes' : 'no',
      type: order.orderType === 'limit' ? 'limit' : 'market',
      count: order.quantity,
      price: order.price,
    });
    if (!result) return { ok: false, status: 'failed', errors: ['order_creation_failed'] };
    return {
      ok: true,
      brokerOrderId: result.id,
      status: result.status === 'filled' ? 'filled' : 'submitted',
      fills: result.status === 'filled' ? [{
        id: `fill-${result.id}`,
        orderId: result.id,
        marketId: order.marketId,
        venue: 'kalshi',
        side: order.side,
        quantity: result.count,
        price: result.price / 100,
        fee: 0,
        feeCurrency: 'USD',
        liquidity: 'taker',
        filledAt: result.created_at,
        settlementStatus: 'pending',
      }] : [],
    };
  }

  async cancelOrder(brokerOrderId: string): Promise<boolean> {
    return this.client.cancelOrder(brokerOrderId);
  }

  async getOrderStatus(brokerOrderId: string): Promise<OrderStatusResult> {
    return { brokerOrderId, status: 'filled', fills: [], remainingQuantity: 0 };
  }
}
