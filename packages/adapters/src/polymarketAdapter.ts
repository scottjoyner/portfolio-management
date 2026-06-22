import { BaseBrokerAdapter } from './baseAdapter.js';
import { PolymarketClient } from '@pkg/polymarket/client.js';
import type { AdapterCapabilities, PreviewResult, SubmitResult, OrderStatusResult, PositionInfo, BalanceInfo, VenueHealth } from './types.js';
import type { OrderIntent, OrderFill, BrokerAccount, Market, OrderBook, ExecutionMode, Venue } from '@pkg/core/types.js';

export class PolymarketBrokerAdapter extends BaseBrokerAdapter {
  readonly name = 'polymarket';
  readonly venue: Venue = 'polymarket';
  readonly mode: ExecutionMode;
  readonly capabilities: AdapterCapabilities;

  private client: PolymarketClient;

  constructor(mode: ExecutionMode = 'demo') {
    super();
    this.mode = mode;
    this.client = new PolymarketClient({
      environment: mode === 'live' ? 'prod' : 'demo',
      apiKey: process.env.POLYMARKET_API_KEY,
      apiSecret: process.env.POLYMARKET_API_SECRET,
      apiPassphrase: process.env.POLYMARKET_API_PASSPHRASE,
      privateKey: process.env.POLYMARKET_PRIVATE_KEY,
      funderAddress: process.env.POLYMARKET_FUNDER_ADDRESS,
      signerAddress: process.env.POLYMARKET_SIGNER_ADDRESS,
      builderCode: process.env.POLYMARKET_BUILDER_CODE,
      signatureType: Number(process.env.POLYMARKET_SIGNATURE_TYPE || '3'),
    });
    this.capabilities = {
      venue: 'polymarket',
       executionModes: process.env.POLYMARKET_PRIVATE_KEY && process.env.POLYMARKET_API_KEY && process.env.POLYMARKET_API_SECRET && process.env.POLYMARKET_API_PASSPHRASE
        ? ['demo', 'live']
        : ['readonly'],
      orderTypes: ['market', 'limit'],
      timeInForce: ['GTC', 'DAY'],
      features: ['prediction_markets', 'market_discovery', 'quotes', 'orderbook'],
      maxOrdersPerSecond: 3,
      maxNotional: 50_000,
    };
  }

  async getAccounts(): Promise<BrokerAccount[]> {
    const balance = await this.client.getBalance();
    const usdcBalance = balance.usdcBalance ?? 0;
    const usdBalance = balance.usdBalance ?? usdcBalance;
    return [{
      id: 'acct-polymarket-primary',
      name: `Polymarket ${this.mode === 'live' ? 'Production' : 'Demo'} Account`,
      provider: 'polymarket',
      status: 'connected',
      currency: 'USDC',
      cash: usdcBalance,
      nav: usdBalance,
      availableBalance: usdcBalance,
      buyingPower: usdBalance,
      marginUsed: 0,
      updatedAt: new Date().toISOString(),
      capabilities: ['prediction'],
      mode: this.mode,
    }];
  }

  async getBalances(_accountId: string): Promise<BalanceInfo[]> {
    const balance = await this.client.getBalance();
    const usdcBalance = balance.usdcBalance ?? 0;
    const polygonBalance = balance.polygonBalance ?? 0;
    return [
      { currency: 'USDC', total: usdcBalance, available: usdcBalance, held: 0 },
      { currency: 'POL', total: polygonBalance, available: polygonBalance, held: 0 },
    ];
  }

  async getPositions(_accountId: string): Promise<PositionInfo[]> {
    return [];
  }

  async discoverMarkets(): Promise<Market[]> {
    const raw = await this.client.listMarkets({ limit: 50 });
    return raw.map(m => ({
      id: m.conditionId,
      venue: 'polymarket' as Venue,
      title: m.question,
      symbol: m.slug,
      assetClass: 'prediction_market' as const,
      closeTime: m.endDate,
      resolvesAt: m.resolutionDate,
      outcomes: [m.outcomes[0], m.outcomes[1]] as [string, string],
      status: m.status === 'active' ? 'active' : 'closed',
      tickSize: m.tickSize,
      minSize: m.minimumOrderSize,
    }));
  }

  async getOrderBook(marketId: string): Promise<OrderBook> {
    const tokenId = await this.client.getTokenForMarket(marketId);
    const raw = tokenId ? await this.client.getOrderBook(tokenId) : { bids: [], asks: [] };
    return {
      marketId,
      venue: 'polymarket',
      yesAsks: raw.asks.map(a => ({ priceMicros: Math.round(Number(a.price) * 1000000), size: Number(a.size) })),
      noAsks: [],
      bids: raw.bids.map(b => ({ priceMicros: Math.round(Number(b.price) * 1000000), size: Number(b.size) })),
      asks: raw.asks.map(a => ({ priceMicros: Math.round(Number(a.price) * 1000000), size: Number(a.size) })),
      ts: Date.now(),
    };
  }

  async getQuote(marketId: string): Promise<{ bid: number; ask: number; mid: number; spreadBps: number }> {
    const tokenId = await this.client.getTokenForMarket(marketId);
    const price = tokenId ? await this.client.getPrice(tokenId) : { bid: 0, ask: 0, mid: 0 };
    const spreadBps = price.ask > 0 ? ((price.ask - price.bid) / price.ask) * 10000 : 0;
    return { bid: price.bid, ask: price.ask, mid: price.mid, spreadBps };
  }

  async previewOrder(order: OrderIntent): Promise<PreviewResult> {
    return { ok: true, preview: { estimatedPrice: order.price || 0.5, estimatedFee: 0.01, estimatedSlippage: 0.01, estimatedTotal: order.quantity * (order.price || 0.5), liquidity: 'taker', slippageBps: 20, validUntil: new Date(Date.now() + 30000).toISOString() } };
  }

  async submitOrder(order: OrderIntent): Promise<SubmitResult> {
    const info = await this.client.getClobMarketInfo(order.marketId);
    const tokenId = info ? info.t?.find(t => (t.o || '').toLowerCase() === 'yes')?.t || info.t?.[0]?.t || '' : '';
    if (!info || !tokenId) {
      return { ok: false, status: 'failed', errors: ['polymarket_market_info_unavailable'] };
    }

    const hasTradeCreds = !!(process.env.POLYMARKET_API_KEY && process.env.POLYMARKET_API_SECRET && process.env.POLYMARKET_API_PASSPHRASE && process.env.POLYMARKET_PRIVATE_KEY);
    if (!hasTradeCreds) {
      return { ok: false, status: 'rejected', errors: ['polymarket_trade_credentials_missing'] };
    }

    const side = order.side === 'buy' ? 'BUY' : 'SELL';
    const tickSize = info.mts ? String(info.mts) : '0.01';
    const negRisk = !!info.neg_risk || (info.t?.length || 0) > 2;

    try {
      const response = order.orderType === 'market'
        ? await this.client.createAndPostMarketOrder({
            tokenID: tokenId,
            side,
            amount: order.side === 'buy' ? (order.notional ?? (order.quantity * (order.price || 0.5))) : order.quantity,
            price: order.price,
          }, { tickSize, negRisk }, order.timeInForce === 'FOK' ? 'FOK' : 'FAK')
        : await this.client.createAndPostOrder({
            tokenID: tokenId,
            side,
            price: order.price || Number(info.mts || 0.01),
            size: order.quantity,
            expiration: undefined,
          }, { tickSize, negRisk }, order.timeInForce === 'DAY' ? 'GTD' : 'GTC');

      const status = String(response.status || '').toLowerCase();
      const brokerOrderId = String(response.orderID || response.order_id || response.id || `pm-${Date.now()}`);
      return {
        ok: true,
        brokerOrderId,
        status: status === 'matched' ? 'filled' : status === 'live' ? 'submitted' : 'submitted',
        raw: response,
      };
    } catch (err) {
      return { ok: false, status: 'failed', errors: [err instanceof Error ? err.message : 'polymarket_order_failed'] };
    }
  }

  async cancelOrder(_brokerOrderId: string): Promise<boolean> {
    return false;
  }

  async getOrderStatus(brokerOrderId: string): Promise<OrderStatusResult> {
    return { brokerOrderId, status: 'filled', fills: [], remainingQuantity: 0 };
  }
}
