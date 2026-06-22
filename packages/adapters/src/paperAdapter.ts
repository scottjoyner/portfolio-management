import { BaseBrokerAdapter } from './baseAdapter.js';
import type { AdapterCapabilities, PreviewResult, SubmitResult, OrderStatusResult, PositionInfo, BalanceInfo, VenueHealth } from './types.js';
import type { OrderIntent, OrderFill, OrderStatus, BrokerAccount, Market, OrderBook, ExecutionMode, Venue } from '@pkg/core/types.js';

interface PaperState {
  cash: number;
  nav: number;
  positions: Map<string, { quantity: number; averagePrice: number; openedAt: string }>;
  orders: Map<string, { intent: OrderIntent; fills: OrderFill[]; status: string }>;
}

export class PaperBrokerAdapter extends BaseBrokerAdapter {
  readonly name = 'paper';
  readonly venue: Venue = 'paper';
  readonly mode: ExecutionMode = 'paper';
  readonly capabilities: AdapterCapabilities = {
    venue: 'paper',
    executionModes: ['paper'],
    orderTypes: ['market', 'limit', 'stop', 'bracket'],
    timeInForce: ['GTC', 'IOC', 'FOK', 'DAY'],
    features: ['preview', 'fill_simulation', 'position_tracking', 'reconciliation'],
    maxOrdersPerSecond: 100,
    maxNotional: 1_000_000_000,
  };

  private state: PaperState;

  constructor(initialCash = 100_000) {
    super();
    this.state = {
      cash: initialCash,
      nav: initialCash,
      positions: new Map(),
      orders: new Map(),
    };
  }

  async getAccounts(): Promise<BrokerAccount[]> {
    return [{
      id: 'acct-paper-primary',
      name: 'Paper Trading Account',
      provider: 'paper',
      status: 'connected',
      currency: 'USD',
      cash: this.state.cash,
      nav: this.state.nav,
      availableBalance: this.state.cash,
      buyingPower: this.state.cash,
      marginUsed: 0,
      updatedAt: new Date().toISOString(),
      capabilities: ['spot'],
      mode: 'paper',
    }];
  }

  async getBalances(_accountId: string): Promise<BalanceInfo[]> {
    return [{ currency: 'USD', total: this.state.cash, available: this.state.cash, held: 0 }];
  }

  async getPositions(_accountId: string): Promise<PositionInfo[]> {
    return Array.from(this.state.positions.entries()).map(([symbol, pos]) => ({
      symbol,
      venue: 'paper' as Venue,
      quantity: pos.quantity,
      averagePrice: pos.averagePrice,
      markPrice: pos.averagePrice,
      unrealizedPnl: 0,
      realizedPnl: 0,
      currency: 'USD',
      status: pos.quantity !== 0 ? 'open' : 'closed',
      openedAt: pos.openedAt,
    }));
  }

  async discoverMarkets(): Promise<Market[]> {
    return [
      { id: 'BTC-USD', venue: 'paper', title: 'Bitcoin / USD', symbol: 'BTC-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.0001, pricePrecision: 2 },
      { id: 'ETH-USD', venue: 'paper', title: 'Ethereum / USD', symbol: 'ETH-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.001, pricePrecision: 2 },
      { id: 'SOL-USD', venue: 'paper', title: 'Solana / USD', symbol: 'SOL-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.01, pricePrecision: 2 },
      { id: 'AAPL', venue: 'paper', title: 'Apple Inc.', symbol: 'AAPL', assetClass: 'equity', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 1, pricePrecision: 2 },
    ];
  }

  async getOrderBook(_marketId: string): Promise<OrderBook> {
    return { marketId: _marketId, venue: 'paper', yesAsks: [], noAsks: [], bids: [], asks: [], ts: Date.now() };
  }

  async getQuote(_marketId: string): Promise<{ bid: number; ask: number; mid: number; spreadBps: number }> {
    const mid = _marketId.includes('BTC') ? 68250 : _marketId.includes('ETH') ? 3712 : 100;
    return { bid: mid * 0.999, ask: mid * 1.001, mid, spreadBps: 10 };
  }

  async previewOrder(order: OrderIntent): Promise<PreviewResult> {
    const price = order.price || 100;
    const quantity = order.quantity;
    const fee = quantity * price * (order.feeBps || 5) / 10000;
    const slippage = quantity * price * (order.slippageBps || 10) / 10000;
    const total = quantity * price + fee + slippage;

    if (order.side === 'buy' && total > this.state.cash) {
      return { ok: false, errors: ['insufficient_paper_cash'], warnings: [] };
    }

    return {
      ok: true,
      preview: {
        estimatedPrice: price,
        estimatedFee: fee,
        estimatedSlippage: slippage,
        estimatedTotal: total,
        liquidity: 'taker',
        slippageBps: order.slippageBps || 10,
        validUntil: new Date(Date.now() + 30000).toISOString(),
      },
    };
  }

  async submitOrder(order: OrderIntent): Promise<SubmitResult> {
    const preview = await this.previewOrder(order);
    if (!preview.ok) return { ok: false, status: 'rejected', errors: preview.errors };

    const brokerOrderId = `paper-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const price = preview.preview!.estimatedPrice;
    const fill: OrderFill = {
      id: `fill-${brokerOrderId}`,
      orderId: brokerOrderId,
      marketId: order.marketId,
      venue: 'paper',
      side: order.side,
      quantity: order.quantity,
      price,
      fee: preview.preview!.estimatedFee,
      feeCurrency: 'USD',
      liquidity: 'taker',
      filledAt: new Date().toISOString(),
      settlementStatus: 'settled',
    };

    const signedQuantity = order.side === 'buy' ? order.quantity : -order.quantity;
    const cashImpact = -(fill.price * fill.quantity + fill.fee);
    this.state.cash += cashImpact;
    this.state.nav += cashImpact;

    const key = order.symbol || order.marketId;
    const existing = this.state.positions.get(key);
    if (existing) {
      const newQty = existing.quantity + signedQuantity;
      if (newQty === 0) {
        this.state.positions.delete(key);
      } else {
        existing.averagePrice = existing.quantity === 0 || Math.sign(existing.quantity) === Math.sign(signedQuantity)
          ? ((Math.abs(existing.quantity) * existing.averagePrice + Math.abs(signedQuantity) * price) / Math.abs(newQty))
          : existing.averagePrice;
        existing.quantity = newQty;
      }
    } else {
      this.state.positions.set(key, { quantity: signedQuantity, averagePrice: price, openedAt: new Date().toISOString() });
    }

    this.state.orders.set(brokerOrderId, { intent: order, fills: [fill], status: 'filled' });

    return { ok: true, brokerOrderId, status: 'filled', fills: [fill] };
  }

  async cancelOrder(_brokerOrderId: string): Promise<boolean> {
    return true;
  }

  async getOrderStatus(brokerOrderId: string): Promise<OrderStatusResult & { fills: OrderFill[] }> {
    const order = this.state.orders.get(brokerOrderId);
    if (!order) return { brokerOrderId, status: 'cancelled', fills: [], remainingQuantity: 0 };
    const filledQty = order.fills.reduce((s, f) => s + f.quantity, 0);
    return {
      brokerOrderId,
      status: 'filled' as OrderStatus,
      fills: order.fills,
      remainingQuantity: order.intent.quantity - filledQty,
      averageFillPrice: order.fills.length > 0 ? order.fills.reduce((s, f) => s + f.price * f.quantity, 0) / filledQty : undefined,
    };
  }

  reset(initialCash = 100_000) {
    this.state = {
      cash: initialCash,
      nav: initialCash,
      positions: new Map(),
      orders: new Map(),
    };
  }
}
