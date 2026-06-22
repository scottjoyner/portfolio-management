import { BaseBrokerAdapter } from './baseAdapter.js';
import type { AdapterCapabilities, PreviewResult, SubmitResult, OrderStatusResult, PositionInfo, BalanceInfo, VenueHealth } from './types.js';
import type { OrderIntent, OrderFill, BrokerAccount, Market, OrderBook, ExecutionMode, Venue } from '@pkg/core/types.js';
import { execSync, exec } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

interface CoinbaseBridgeConfig {
  pythonPath: string;
  bridgeScript: string;
  cliBridgeScript: string;
  dryRun: boolean;
  timeoutMs: number;
}

const CLI_BRIDGE = resolve(process.env.COINBASE_CLI_BRIDGE || 'coinbase/src/bridge_cli.mjs');

const DEFAULT_CONFIG: CoinbaseBridgeConfig = {
  pythonPath: process.env.COINBASE_PYTHON_PATH || 'python3',
  bridgeScript: resolve(process.env.COINBASE_BRIDGE_SCRIPT || 'coinbase/src/bridge_execution.py'),
  cliBridgeScript: existsSync(CLI_BRIDGE) ? CLI_BRIDGE : '',
  dryRun: process.env.COINBASE_DRY_RUN !== 'false',
  timeoutMs: Number(process.env.COINBASE_BRIDGE_TIMEOUT_MS || '30000'),
};

interface BridgeCommand {
  action: 'list_accounts' | 'best_bid_ask' | 'preview_order' | 'submit_order' | 'get_candles' | 'list_orders' | 'get_order' | 'list_fills' | 'get_product' | 'get_products' | 'health';
  payload?: Record<string, unknown>;
}

interface BridgeResponse {
  ok: boolean;
  data?: unknown;
  error?: string;
}

export class CoinbaseBrokerAdapter extends BaseBrokerAdapter {
  readonly name = 'coinbase';
  readonly venue: Venue = 'coinbase';
  readonly mode: ExecutionMode;
  readonly capabilities: AdapterCapabilities;

  private config: CoinbaseBridgeConfig;

  constructor(mode: ExecutionMode = 'paper', config: Partial<CoinbaseBridgeConfig> = {}) {
    super();
    this.mode = mode;
    this.config = { ...DEFAULT_CONFIG, ...config };

    const liveEnabled = mode === 'live' && !this.config.dryRun;
    this.capabilities = {
      venue: 'coinbase',
      executionModes: liveEnabled ? ['paper', 'live'] : ['paper'],
      orderTypes: ['market', 'limit', 'stop', 'bracket'],
      timeInForce: ['GTC', 'IOC', 'FOK'],
      features: ['preview', 'spot_trading', 'candles', 'accounts', 'balances'],
      maxOrdersPerSecond: 10,
      maxNotional: 500_000,
    };
  }

  private async bridge(cmd: BridgeCommand): Promise<BridgeResponse> {
    const inputJson = JSON.stringify(cmd);
    // Try CLI bridge first (coinbase CLI tool), then fall back to Python bridge
    if (this.config.cliBridgeScript) {
      try {
        const escaped = inputJson.replace(/'/g, "'\\''");
        const stdout = execSync(
          `node ${this.config.cliBridgeScript} '${escaped}'`,
          { timeout: this.config.timeoutMs, encoding: 'utf-8' },
        );
        return JSON.parse(stdout.trim()) as BridgeResponse;
      } catch { /* CLI bridge failed, try Python bridge */ }
    }
    try {
      const escaped = inputJson.replace(/'/g, "'\\''");
      const stdout = execSync(
        `${this.config.pythonPath} ${this.config.bridgeScript} '${escaped}'`,
        { timeout: this.config.timeoutMs, encoding: 'utf-8' },
      );
      return JSON.parse(stdout.trim()) as BridgeResponse;
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : String(error);
      return { ok: false, error: `bridge_error: ${msg}` };
    }
  }

  private async bridgeAsync(cmd: BridgeCommand): Promise<BridgeResponse> {
    const inputJson = JSON.stringify(cmd);
    const escaped = inputJson.replace(/'/g, "'\\''");
    return new Promise((resolvePromise) => {
      const doExec = (bin: string, script: string) => {
        exec(`${bin} ${script} '${escaped}'`, { timeout: this.config.timeoutMs }, (error: any, stdout: string) => {
          if (error) { resolvePromise({ ok: false, error: `bridge_error: ${error.message}` }); return; }
          try { resolvePromise(JSON.parse(stdout.trim()) as BridgeResponse); } catch { resolvePromise({ ok: false, error: 'bridge_response_parse_error' }); }
        });
      };
      if (this.config.cliBridgeScript) {
        try { doExec('node', this.config.cliBridgeScript); return; } catch { /* fall through to Python */ }
      }
      doExec(this.config.pythonPath, this.config.bridgeScript);
    });
  }

  async health(): Promise<VenueHealth> {
    try {
      const res = await this.bridge({ action: 'health' });
      return {
        ok: res.ok,
        venue: 'coinbase',
        latencyMs: 0,
        mode: this.mode,
        authenticated: res.ok,
        errors: res.ok ? undefined : [res.error || 'bridge_unreachable'],
      };
    } catch {
      return { ok: false, venue: 'coinbase', latencyMs: 0, mode: this.mode, authenticated: false, errors: ['bridge_exception'] };
    }
  }

  async getAccounts(): Promise<BrokerAccount[]> {
    const res = await this.bridge({ action: 'list_accounts' });
    if (!res.ok || !res.data) return [];
    const accounts = res.data as Array<{ currency: string; balance: number; available: number; hold: number }>;
    const totalUsd = accounts.find(a => a.currency === 'USD')?.balance || 0;
    return [{
      id: 'acct-coinbase-primary',
      name: 'Coinbase USD Account',
      provider: 'coinbase',
      status: 'connected',
      currency: 'USD',
      cash: totalUsd,
      nav: totalUsd,
      availableBalance: accounts.find(a => a.currency === 'USD')?.available || 0,
      buyingPower: totalUsd,
      marginUsed: 0,
      updatedAt: new Date().toISOString(),
      capabilities: ['spot'],
      mode: this.mode,
    }];
  }

  async getBalances(_accountId: string): Promise<BalanceInfo[]> {
    const res = await this.bridge({ action: 'list_accounts' });
    if (!res.ok || !res.data) return [];
    return (res.data as Array<{ currency: string; balance: number; available: number; hold: number }>).map(a => ({
      currency: a.currency,
      total: a.balance,
      available: a.available,
      held: a.hold,
    }));
  }

  async getPositions(_accountId: string): Promise<PositionInfo[]> {
    const res = await this.bridge({ action: 'list_accounts' });
    if (!res.ok || !res.data) return [];
    const accounts = res.data as Array<{ currency: string; balance: number; available: number }>;
    return accounts
      .filter(a => a.currency !== 'USD' && a.balance > 0)
      .map(a => ({
        symbol: `${a.currency}-USD`,
        venue: 'coinbase' as Venue,
        quantity: a.balance,
        averagePrice: 0,
        markPrice: 0,
        unrealizedPnl: 0,
        realizedPnl: 0,
        currency: a.currency,
        status: 'open' as const,
        openedAt: new Date().toISOString(),
      }));
  }

  async discoverMarkets(): Promise<Market[]> {
    return [
      { id: 'BTC-USD', venue: 'coinbase', title: 'Bitcoin / USD', symbol: 'BTC-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.0001, pricePrecision: 2 },
      { id: 'ETH-USD', venue: 'coinbase', title: 'Ethereum / USD', symbol: 'ETH-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.001, pricePrecision: 2 },
      { id: 'SOL-USD', venue: 'coinbase', title: 'Solana / USD', symbol: 'SOL-USD', assetClass: 'crypto', status: 'active', closeTime: '2099-12-31T23:59:59Z', tickSize: 0.01, minSize: 0.01, pricePrecision: 2 },
    ];
  }

  async getOrderBook(_marketId: string): Promise<OrderBook> {
    return { marketId: _marketId, venue: 'coinbase', yesAsks: [], noAsks: [], bids: [], asks: [], ts: Date.now() };
  }

  async getQuote(marketId: string): Promise<{ bid: number; ask: number; mid: number; spreadBps: number }> {
    const res = await this.bridge({ action: 'best_bid_ask', payload: { product_ids: [marketId] } });
    if (!res.ok || !res.data) return { bid: 0, ask: 0, mid: 0, spreadBps: 0 };
    const data = res.data as { pricebooks?: Array<{ product_id: string; bids: Array<{ price: string }>; asks: Array<{ price: string }> }> };
    const book = data.pricebooks?.[0];
    if (!book) return { bid: 0, ask: 0, mid: 0, spreadBps: 0 };
    const bid = Number(book.bids?.[0]?.price || 0);
    const ask = Number(book.asks?.[0]?.price || 0);
    const mid = (bid + ask) / 2;
    const spreadBps = ask > 0 ? ((ask - bid) / ask) * 10000 : 0;
    return { bid, ask, mid, spreadBps };
  }

  async previewOrder(order: OrderIntent): Promise<PreviewResult> {
    const res = await this.bridge({
      action: 'preview_order',
      payload: {
        side: order.side,
        product_id: order.symbol || order.marketId,
        base_size: order.side === 'sell' ? String(order.quantity) : undefined,
        quote_size: order.side === 'buy' ? String(order.notional || order.quantity * (order.price || 0)) : undefined,
      },
    });
    if (!res.ok) return { ok: false, errors: [res.error || 'preview_failed'] };
    return {
      ok: true,
      preview: {
        estimatedPrice: order.price || 0,
        estimatedFee: 0,
        estimatedSlippage: 0,
        estimatedTotal: order.quantity * (order.price || 0),
        liquidity: 'taker',
        slippageBps: order.slippageBps || 10,
        validUntil: new Date(Date.now() + 15000).toISOString(),
      },
    };
  }

  async submitOrder(order: OrderIntent): Promise<SubmitResult> {
    if (this.mode === 'paper' || this.config.dryRun) {
      return {
        ok: true,
        brokerOrderId: `cb-dry-${Date.now()}`,
        status: 'filled',
        fills: [{
          id: `fill-cb-dry-${Date.now()}`,
          orderId: '',
          marketId: order.marketId,
          venue: 'coinbase',
          side: order.side,
          quantity: order.quantity,
          price: order.price || 100,
          fee: 0,
          feeCurrency: 'USD',
          liquidity: 'taker',
          filledAt: new Date().toISOString(),
          settlementStatus: 'settled',
        }],
      };
    }

    const res = await this.bridge({
      action: 'submit_order',
      payload: {
        side: order.side,
        product_id: order.symbol || order.marketId,
        base_size: String(order.quantity),
        quote_size: order.notional ? String(order.notional) : undefined,
      },
    });

    if (!res.ok) return { ok: false, status: 'failed', errors: [res.error || 'order_failed'] };
    return { ok: true, brokerOrderId: `cb-${Date.now()}`, status: 'filled' };
  }

  async cancelOrder(_brokerOrderId: string): Promise<boolean> {
    return true;
  }

  async getOrderStatus(brokerOrderId: string): Promise<OrderStatusResult> {
    return { brokerOrderId, status: 'filled', fills: [], remainingQuantity: 0 };
  }

  async listOpenOrders(productId?: string): Promise<Array<{ order_id: string; product_id: string; side: string; status: string; size: string; filled_size: string; price: string; created_time: string }>> {
    const res = await this.bridge({ action: 'list_orders', payload: { product_id: productId, order_status: 'OPEN', limit: 100 } });
    if (!res.ok || !res.data) return [];
    return res.data as Array<{ order_id: string; product_id: string; side: string; status: string; size: string; filled_size: string; price: string; created_time: string }>;
  }

  async getOrderDetails(orderId: string): Promise<Record<string, unknown> | null> {
    const res = await this.bridge({ action: 'get_order', payload: { order_id: orderId } });
    if (!res.ok || !res.data) return null;
    return res.data as Record<string, unknown>;
  }

  async listRecentFills(productId?: string, orderId?: string): Promise<Array<{ fill_id: string; order_id: string; product_id: string; side: string; size: string; price: string; fee: string; created_at: string }>> {
    const res = await this.bridge({ action: 'list_fills', payload: { product_id: productId, order_id: orderId, limit: 100 } });
    if (!res.ok || !res.data) return [];
    return res.data as Array<{ fill_id: string; order_id: string; product_id: string; side: string; size: string; price: string; fee: string; created_at: string }>;
  }

  async getProductInfo(productId: string): Promise<Record<string, unknown> | null> {
    const res = await this.bridge({ action: 'get_product', payload: { product_id: productId } });
    if (!res.ok || !res.data) return null;
    return res.data as Record<string, unknown>;
  }

  async listAllProducts(): Promise<Array<{ product_id: string; price: string; price_percentage_change_24h: string; volume_24h: string; status: string }>> {
    const res = await this.bridge({ action: 'get_products' });
    if (!res.ok || !res.data) return [];
    return res.data as Array<{ product_id: string; price: string; price_percentage_change_24h: string; volume_24h: string; status: string }>;
  }

  /** Sync coinbase account data into the operator store format */
  async syncOperatorState(): Promise<{ accounts: BrokerAccount[]; positions: PositionInfo[]; markets: Market[]; quotes: Array<{ symbol: string; bid: number; ask: number; spreadBps: number }> }> {
    const [accounts, positions, markets] = await Promise.all([
      this.getAccounts(),
      this.getPositions(''),
      this.discoverMarkets(),
    ]);

    const quotes: Array<{ symbol: string; bid: number; ask: number; spreadBps: number }> = [];
    for (const m of markets.slice(0, 10)) {
      try {
        const symbol = m.symbol || m.id;
        const q = await this.getQuote(symbol);
        if (q.bid > 0) quotes.push({ symbol, ...q });
      } catch { /* skip */ }
    }

    return { accounts, positions, markets, quotes };
  }

  async getHistoricalRates(marketId: string, start: string, end: string, granularity: string): Promise<{ time: number; open: number; high: number; low: number; close: number; volume: number }[]> {
    const res = await this.bridge({
      action: 'get_candles',
      payload: { product_id: marketId, start_unix: Math.floor(new Date(start).getTime() / 1000), end_unix: Math.floor(new Date(end).getTime() / 1000), granularity },
    });
    if (!res.ok || !res.data) return [];
    return (res.data as Array<{ start: number; open: string; high: string; low: string; close: string; volume: string }>).map(c => ({
      time: c.start,
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close),
      volume: Number(c.volume),
    }));
  }
}
