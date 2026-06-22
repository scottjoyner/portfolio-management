import { readFileSync } from 'node:fs';
import { createPrivateKey, sign, constants } from 'node:crypto';

export interface KalshiConfig {
  apiKey?: string;
  apiSecret?: string;
  apiKeyId?: string;
  privateKeyPath?: string;
  environment: 'demo' | 'prod';
  baseUrl?: string;
}

interface KalshiMarket {
  id: string;
  title: string;
  subtitle?: string;
  close_time: string;
  settle_time?: string;
  status: 'open' | 'closed' | 'settled';
  yes_bid: number;
  yes_ask: number;
  no_bid: number;
  no_ask: number;
  last_price: number;
  volume: number;
  liquidity: number;
  tick_size: number;
  min_size: number;
}

interface KalshiOrder {
  id: string;
  market_id: string;
  side: 'yes' | 'no';
  type: 'market' | 'limit';
  status: 'open' | 'filled' | 'cancelled';
  count: number;
  price: number;
  remaining_count: number;
  created_at: string;
}

interface KalshiEvent {
  event_ticker: string;
  title: string;
  sub_title?: string;
  category?: string;
  series_ticker?: string;
  mutually_exclusive?: boolean;
  status?: string;
}

interface KalshiBalance {
  cash?: number;
  portfolio_value?: number;
  available?: number;
  error?: string;
}

export class KalshiClient {
  private config: KalshiConfig;
  private baseUrl: string;
  private sessionToken?: string;
  private privateKeyPem: string | null = null;

  constructor(config?: Partial<KalshiConfig>) {
    const env = (process.env.KALSHI_ENV || 'prod') as 'demo' | 'prod';
    this.config = {
      apiKey: process.env.KALSHI_API_KEY,
      apiSecret: process.env.KALSHI_API_SECRET,
      apiKeyId: process.env.KALSHI_API_KEY_ID,
      privateKeyPath: process.env.KALSHI_PRIVATE_KEY_PATH,
      environment: env,
      baseUrl: env === 'prod' ? 'https://api.elections.kalshi.com/trade-api/v2' : 'https://demo-api.kalshi.co/trade-api/v2',
      ...config,
    };
    this.baseUrl = this.config.baseUrl!;
  }

  private _hasApiKeyAuth(): boolean {
    return !!(this.config.apiKeyId && this.config.privateKeyPath);
  }

  private _loadPrivateKey(): string | null {
    if (this.privateKeyPem !== null) return this.privateKeyPem;
    if (!this.config.privateKeyPath) {
      this.privateKeyPem = null;
      return null;
    }
    try {
      this.privateKeyPem = readFileSync(this.config.privateKeyPath, 'utf8');
      return this.privateKeyPem;
    } catch {
      this.privateKeyPem = null;
      return null;
    }
  }

  private _authHeaders(method: string, path: string): Record<string, string> {
    if (!this._hasApiKeyAuth()) return {};
    const pem = this._loadPrivateKey();
    if (!pem) return {};
    const timestamp = String(Date.now());
    const message = `${timestamp}${method.toUpperCase()}${path}`;
    const key = createPrivateKey(pem);
    const signature = sign('sha256', Buffer.from(message), {
      key,
      padding: constants.RSA_PKCS1_PSS_PADDING,
      saltLength: constants.RSA_PSS_SALTLEN_DIGEST,
    }).toString('base64');
    return {
      'KALSHI-ACCESS-KEY': this.config.apiKeyId!,
      'KALSHI-ACCESS-TIMESTAMP': timestamp,
      'KALSHI-ACCESS-SIGNATURE': signature,
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    };
  }

  private async request(method: string, path: string, body?: Record<string, unknown>): Promise<unknown> {
    const url = `${this.baseUrl}${path}`;
    const headers: Record<string, string> = this._hasApiKeyAuth() ? this._authHeaders(method, path) : { 'Content-Type': 'application/json' };
    if (this.sessionToken) {
      headers['Authorization'] = `Bearer ${this.sessionToken}`;
    }

    const res = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Kalshi API error ${res.status}: ${text}`);
    }

    return res.json();
  }

  async login(): Promise<boolean> {
    if (this._hasApiKeyAuth()) {
      this.sessionToken = 'api_key_auth';
      return true;
    }
    if (!this.config.apiKey || !this.config.apiSecret) {
      console.warn('[Kalshi] No API credentials configured — running in readonly mode');
      return false;
    }
    try {
      const res = await this.request('POST', '/login', {
        email: this.config.apiKey,
        password: this.config.apiSecret,
      }) as { token?: string };
      if (res.token) {
        this.sessionToken = res.token;
        return true;
      }
      return false;
    } catch {
      console.warn('[Kalshi] Login failed — running in readonly mode');
      return false;
    }
  }

  isAuthenticated(): boolean {
    return !!this.sessionToken;
  }

  async listMarkets(params?: { limit?: number; status?: string; cursor?: string }): Promise<KalshiMarket[]> {
    try {
      const res = await this.listMarketsPage(params);
      return res.markets;
    } catch {
      return [];
    }
  }

  async listMarketsPage(params?: { limit?: number; status?: string; cursor?: string }): Promise<{ markets: KalshiMarket[]; nextCursor: string | null }> {
    const searchParams = new URLSearchParams();
    if (params?.limit) searchParams.set('limit', String(params.limit));
    if (params?.status) searchParams.set('status', params.status);
    if (params?.cursor) searchParams.set('cursor', params.cursor);

    const res = await this.request('GET', `/markets?${searchParams.toString()}`) as { markets?: KalshiMarket[]; next_cursor?: string; nextCursor?: string; cursor?: string };
    return {
      markets: res.markets || [],
      nextCursor: res.next_cursor || res.nextCursor || null,
    };
  }

  async listAllMarkets(params?: { limit?: number; status?: string; maxPages?: number }): Promise<KalshiMarket[]> {
    const pageSize = Math.max(1, Math.min(params?.limit || 100, 200));
    const maxPages = Math.max(1, params?.maxPages || 20);
    const collected: KalshiMarket[] = [];
    let cursor: string | undefined;
    for (let i = 0; i < maxPages; i += 1) {
      const page = await this.listMarketsPage({ limit: pageSize, status: params?.status, cursor });
      collected.push(...page.markets);
      if (!page.nextCursor || page.nextCursor === cursor || page.markets.length < pageSize) break;
      cursor = page.nextCursor;
    }
    return collected;
  }

  async getMarket(marketId: string): Promise<KalshiMarket | null> {
    try {
      const res = await this.request('GET', `/markets/${marketId}`) as { market?: KalshiMarket };
      return res.market || null;
    } catch {
      return null;
    }
  }

  async listEvents(params?: { limit?: number; cursor?: string }): Promise<KalshiEvent[]> {
    try {
      const searchParams = new URLSearchParams();
      if (params?.limit) searchParams.set('limit', String(params.limit));
      if (params?.cursor) searchParams.set('cursor', params.cursor);
      const res = await this.request('GET', `/events?${searchParams.toString()}`) as { events?: KalshiEvent[] };
      return res.events || [];
    } catch {
      return [];
    }
  }

  async getBalance(): Promise<KalshiBalance> {
    try {
      if (!this.isAuthenticated()) return { error: 'not_authenticated' };
      const res = await this.request('GET', '/portfolio/balance') as KalshiBalance;
      return res;
    } catch {
      return { error: 'balance_unavailable' };
    }
  }

  async createOrder(params: {
    market_id: string;
    side: 'yes' | 'no';
    type: 'market' | 'limit';
    count: number;
    price?: number;
  }): Promise<KalshiOrder | null> {
    if (!this.isAuthenticated()) {
      console.warn('[Kalshi] Not authenticated — cannot create order');
      return null;
    }
    try {
      const side = params.side === 'yes' ? 'bid' : 'ask';
      const body = {
        ticker: params.market_id,
        side,
        count: params.count.toFixed(2),
        price: (params.price ?? 0.5).toFixed(4),
        time_in_force: params.type === 'market' ? 'immediate_or_cancel' : 'good_till_canceled',
        self_trade_prevention_type: 'taker_at_cross',
        post_only: false,
        cancel_order_on_pause: false,
        reduce_only: false,
        exchange_index: 0,
      };
      const res = await this.request('POST', '/portfolio/events/orders', body) as {
        order_id?: string;
        client_order_id?: string;
        fill_count?: string;
        remaining_count?: string;
        average_fill_price?: string;
        average_fee_paid?: string;
        ts_ms?: number;
      };
      return {
        id: res.order_id || res.client_order_id || `kls-${Date.now()}`,
        market_id: params.market_id,
        side: params.side,
        type: params.type,
        status: Number(res.fill_count || 0) > 0 ? 'filled' : 'open',
        count: params.count,
        price: params.price ?? 0,
        remaining_count: Number(res.remaining_count || params.count),
        created_at: new Date((res.ts_ms || Date.now())).toISOString(),
      };
    } catch {
      return null;
    }
  }

  async cancelOrder(orderId: string): Promise<boolean> {
    if (!this.isAuthenticated()) return false;
    try {
      await this.request('DELETE', `/orders/${orderId}`);
      return true;
    } catch {
      return false;
    }
  }


}
