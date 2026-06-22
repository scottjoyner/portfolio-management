import { createHmac } from 'node:crypto';
import { privateKeyToAccount } from 'viem/accounts';

export interface PolymarketConfig {
  apiKey?: string;
  apiSecret?: string;
  apiPassphrase?: string;
  privateKey?: string;
  funderAddress?: string;
  signerAddress?: string;
  builderCode?: string;
  signatureType?: number;
  environment: 'demo' | 'prod';
  baseUrl?: string;
  chainId?: number;
}

interface PolymarketMarket {
  id: string;
  question: string;
  description?: string;
  conditionId: string;
  slug: string;
  endDate: string;
  resolutionDate?: string;
  status: 'active' | 'closed' | 'resolved';
  outcomes: string[];
  outcomePrices: string[];
  volume: number;
  liquidity: number;
  fee: number;
  minimumOrderSize: number;
  tickSize: number;
}

interface PolymarketOrder {
  id: string;
  market: string;
  side: 'BUY' | 'SELL';
  price: string;
  size: string;
  status: 'OPEN' | 'MATCHED' | 'CANCELLED';
  matchedSize: string;
  createdAt: string;
}

interface PolymarketBalance {
  address?: string;
  usdBalance?: number;
  polygonBalance?: number;
  usdcBalance?: number;
  error?: string;
}

interface ClobMarketInfo {
  mts?: number;
  mos?: number;
  rfqe?: boolean;
  itode?: boolean;
  neg_risk?: boolean;
  t?: Array<{ t?: string; o?: string }>;
}

interface SignedOrder {
  maker: string;
  signer: string;
  tokenId: string;
  makerAmount: string;
  takerAmount: string;
  side: 'BUY' | 'SELL';
  expiration: string;
  timestamp: string;
  metadata: string;
  builder: string;
  signature: string;
  salt: string;
  signatureType: number;
}

const DEFAULT_BUILDER = '0x0000000000000000000000000000000000000000000000000000000000000000';
const DEFAULT_TOKEN_BYTES = '0x0000000000000000000000000000000000000000000000000000000000000000';
const PROD_HOST = 'https://clob.polymarket.com';
const STAGING_HOST = 'https://clob-staging.polymarket.com';
const PROD_CHAIN = 137;
const STAGING_CHAIN = 80001;
const EXCHANGE_DOMAIN = {
  name: 'Polymarket CTF Exchange',
  version: '2',
};

export class PolymarketClient {
  private config: PolymarketConfig;
  private baseUrl: string;
  private account: ReturnType<typeof privateKeyToAccount> | null = null;

  constructor(config?: Partial<PolymarketConfig>) {
    const env = (process.env.POLYMARKET_ENV || 'prod') as 'demo' | 'prod';
    this.config = {
      apiKey: process.env.POLYMARKET_API_KEY,
      apiSecret: process.env.POLYMARKET_API_SECRET,
      apiPassphrase: process.env.POLYMARKET_API_PASSPHRASE,
      privateKey: process.env.POLYMARKET_PRIVATE_KEY,
      funderAddress: process.env.POLYMARKET_FUNDER_ADDRESS,
      signerAddress: process.env.POLYMARKET_SIGNER_ADDRESS,
      builderCode: process.env.POLYMARKET_BUILDER_CODE,
      signatureType: Number(process.env.POLYMARKET_SIGNATURE_TYPE || '3'),
      environment: env,
      baseUrl: env === 'prod' ? PROD_HOST : STAGING_HOST,
      chainId: env === 'prod' ? PROD_CHAIN : STAGING_CHAIN,
      ...config,
    };
    this.baseUrl = this.config.baseUrl!;
  }

  private _ensureAccount(): ReturnType<typeof privateKeyToAccount> | null {
    if (this.account) return this.account;
    if (!this.config.privateKey) return null;
    this.account = privateKeyToAccount(this.config.privateKey as `0x${string}`);
    return this.account;
  }

  private _signerAddress(): string | null {
    if (this.config.signerAddress) return this.config.signerAddress;
    return this._ensureAccount()?.address || null;
  }

  private _funderAddress(): string | null {
    return this.config.funderAddress || this._signerAddress();
  }

  private _authHeaders(method: string, path: string, body?: string): Record<string, string> {
    if (!this.config.apiKey || !this.config.apiSecret || !this.config.apiPassphrase) {
      return {};
    }
    const timestamp = String(Math.floor(Date.now() / 1000));
    const secretBytes = Buffer.from(this.config.apiSecret.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(this.config.apiSecret.length / 4) * 4, '='), 'base64');
    const message = `${timestamp}${method.toUpperCase()}${path}${body || ''}`;
    const signature = createHmac('sha256', secretBytes).update(message).digest('base64').replace(/\+/g, '-').replace(/\//g, '_');
    const address = this._signerAddress() || '';
    return {
      'Content-Type': 'application/json',
      'POLY_ADDRESS': address,
      'POLY_SIGNATURE': signature,
      'POLY_TIMESTAMP': timestamp,
      'POLY_API_KEY': this.config.apiKey,
      'POLY_PASSPHRASE': this.config.apiPassphrase,
    };
  }

  private async request(method: string, path: string, body?: Record<string, unknown>, headers?: Record<string, string>): Promise<unknown> {
    const url = `${this.baseUrl}${path}`;
    const bodyJson = body ? JSON.stringify(body) : undefined;
    const defaultHeaders: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(headers || {}),
    };
    const authHeaders = this._authHeaders(method, path, bodyJson);
    const res = await fetch(url, {
      method,
      headers: { ...defaultHeaders, ...authHeaders },
      body: bodyJson,
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Polymarket API error ${res.status}: ${text}`);
    }

    return res.json();
  }

  async listMarkets(params?: { limit?: number; cursor?: string; closed?: boolean; tag?: string }): Promise<PolymarketMarket[]> {
    try {
      const res = await this.listMarketsPage(params);
      return res.markets;
    } catch {
      return [];
    }
  }

  async listMarketsPage(params?: { limit?: number; cursor?: string; closed?: boolean; tag?: string }): Promise<{ markets: PolymarketMarket[]; nextCursor: string | null }> {
    const searchParams = new URLSearchParams();
    if (params?.limit) searchParams.set('limit', String(params.limit));
    if (params?.cursor) searchParams.set('cursor', params.cursor);
    if (params?.tag) searchParams.set('tag', params.tag);
    if (params?.closed !== undefined) searchParams.set('closed', String(params.closed));

    const res = await this.request('GET', `/markets?${searchParams.toString()}`) as { data?: PolymarketMarket[]; next_cursor?: string; nextCursor?: string; cursor?: string };
    return {
      markets: res.data || [],
      nextCursor: res.next_cursor || res.nextCursor || null,
    };
  }

  async listAllMarkets(params?: { limit?: number; cursor?: string; closed?: boolean; tag?: string; maxPages?: number }): Promise<PolymarketMarket[]> {
    const pageSize = Math.max(1, Math.min(params?.limit || 100, 200));
    const maxPages = Math.max(1, params?.maxPages || 20);
    const collected: PolymarketMarket[] = [];
    let cursor: string | undefined = params?.cursor;
    for (let i = 0; i < maxPages; i += 1) {
      const page = await this.listMarketsPage({ limit: pageSize, cursor, closed: params?.closed, tag: params?.tag });
      collected.push(...page.markets);
      if (!page.nextCursor || page.nextCursor === cursor || page.markets.length < pageSize) break;
      cursor = page.nextCursor;
    }
    return collected;
  }

  async getMarket(marketId: string): Promise<PolymarketMarket | null> {
    try {
      const res = await this.request('GET', `/markets/${marketId}`) as PolymarketMarket;
      return res;
    } catch {
      return null;
    }
  }

  async getClobMarketInfo(conditionId: string): Promise<ClobMarketInfo | null> {
    try {
      const res = await this.request('GET', `/clob-markets/${conditionId}`) as ClobMarketInfo;
      return res;
    } catch {
      return null;
    }
  }

  async getOrderBook(tokenId: string): Promise<{ bids: Array<{ price: string; size: string }>; asks: Array<{ price: string; size: string }> }> {
    try {
      const res = await this.request('GET', `/book?token_id=${encodeURIComponent(tokenId)}`) as { bids?: Array<{ price: string; size: string }>; asks?: Array<{ price: string; size: string }> };
      return { bids: res.bids || [], asks: res.asks || [] };
    } catch {
      return { bids: [], asks: [] };
    }
  }

  async getBalance(): Promise<PolymarketBalance> {
    try {
      return await this.request('GET', '/balance') as PolymarketBalance;
    } catch {
      return { error: 'balance_unavailable' };
    }
  }

  private _zeroBytes32(): string {
    return DEFAULT_TOKEN_BYTES;
  }

  private _normalizeAmount(value: number): string {
    return Math.max(0, Math.round(value * 1_000_000)).toString();
  }

  private _selectTokenId(info: ClobMarketInfo, outcome: string = 'Yes'): string {
    const tokens = info.t || [];
    const match = tokens.find((t) => (t.o || '').toLowerCase() === outcome.toLowerCase());
    return match?.t || tokens[0]?.t || '';
  }

  private async _buildSignedOrder(params: {
    tokenId: string;
    side: 'BUY' | 'SELL';
    price: number;
    size: number;
    expiration?: number;
    negRisk?: boolean;
  }): Promise<SignedOrder> {
    const account = this._ensureAccount();
    const funder = this._funderAddress();
    if (!account || !funder) {
      throw new Error('Polymarket signer/private key and funder address are required');
    }

    const sideValue = params.side === 'BUY' ? 0 : 1;
    const makerAmount = params.side === 'BUY' ? params.price * params.size : params.size;
    const takerAmount = params.side === 'BUY' ? params.size : params.price * params.size;
    const salt = BigInt(Math.floor(Math.random() * Number.MAX_SAFE_INTEGER));
    const timestamp = BigInt(Date.now());
    const expiration = String(params.expiration || 0);
    const metadata = this._zeroBytes32();
    const builder = (this.config.builderCode && this.config.builderCode.startsWith('0x') && this.config.builderCode.length === 66)
      ? this.config.builderCode
      : this._zeroBytes32();
    const signatureType = Number.isFinite(this.config.signatureType) ? Number(this.config.signatureType) : 3;
    const domain = {
      name: EXCHANGE_DOMAIN.name,
      version: EXCHANGE_DOMAIN.version,
      chainId: this.config.chainId || PROD_CHAIN,
      verifyingContract: params.negRisk
        ? '0xe2222d279d744050d28e00520010520000310F59'
        : '0xE111180000d2663C0091e4f400237545B87B996B',
    } as const;
    const types = {
      Order: [
        { name: 'salt', type: 'uint256' },
        { name: 'maker', type: 'address' },
        { name: 'signer', type: 'address' },
        { name: 'tokenId', type: 'uint256' },
        { name: 'makerAmount', type: 'uint256' },
        { name: 'takerAmount', type: 'uint256' },
        { name: 'side', type: 'uint8' },
        { name: 'signatureType', type: 'uint8' },
        { name: 'timestamp', type: 'uint256' },
        { name: 'metadata', type: 'bytes32' },
        { name: 'builder', type: 'bytes32' },
      ],
    } as const;
    const message: any = {
      salt,
      maker: funder,
      signer: account.address,
      tokenId: BigInt(params.tokenId),
      makerAmount: BigInt(this._normalizeAmount(makerAmount)),
      takerAmount: BigInt(this._normalizeAmount(takerAmount)),
      side: sideValue,
      signatureType,
      timestamp,
      metadata,
      builder,
    };
    const signature = await account.signTypedData({ domain, types, primaryType: 'Order', message });
    return {
      maker: funder,
      signer: account.address,
      tokenId: params.tokenId,
      makerAmount: this._normalizeAmount(makerAmount),
      takerAmount: this._normalizeAmount(takerAmount),
      side: params.side,
      expiration,
      timestamp: timestamp.toString(),
      metadata,
      builder,
      signature,
      salt: salt.toString(),
      signatureType,
    };
  }

  async createOrder(params: {
    tokenID: string;
    price: number;
    size: number;
    side: 'BUY' | 'SELL';
    expiration?: number;
  }, options?: { negRisk?: boolean }): Promise<SignedOrder> {
    return this._buildSignedOrder({
      tokenId: params.tokenID,
      side: params.side,
      price: params.price,
      size: params.size,
      expiration: params.expiration,
      negRisk: options?.negRisk,
    });
  }

  async createMarketOrder(params: {
    tokenID: string;
    side: 'BUY' | 'SELL';
    amount: number;
    price?: number;
  }, options?: { negRisk?: boolean }): Promise<SignedOrder> {
    const price = params.price || 0.5;
    const size = params.side === 'BUY' ? params.amount / Math.max(price, 1e-9) : params.amount;
    return this._buildSignedOrder({
      tokenId: params.tokenID,
      side: params.side,
      price,
      size,
      expiration: 0,
      negRisk: options?.negRisk,
    });
  }

  async postOrder(order: SignedOrder, orderType: 'GTC' | 'GTD' | 'FOK' | 'FAK' = 'GTC', postOnly = false): Promise<Record<string, unknown>> {
    const body = {
      order,
      owner: this.config.apiKey,
      orderType,
      deferExec: false,
      postOnly,
    };
    return await this.request('POST', '/order', body) as Record<string, unknown>;
  }

  async createAndPostOrder(params: {
    tokenID: string;
    price: number;
    size: number;
    side: 'BUY' | 'SELL';
    expiration?: number;
  }, options?: { tickSize?: string; negRisk?: boolean }, orderType: 'GTC' | 'GTD' = 'GTC'): Promise<Record<string, unknown>> {
    const order = await this.createOrder(params, { negRisk: options?.negRisk });
    return await this.postOrder(order, orderType, false);
  }

  async createAndPostMarketOrder(params: {
    tokenID: string;
    side: 'BUY' | 'SELL';
    amount: number;
    price?: number;
  }, options?: { tickSize?: string; negRisk?: boolean }, orderType: 'FOK' | 'FAK' = 'FOK'): Promise<Record<string, unknown>> {
    const order = await this.createMarketOrder(params, { negRisk: options?.negRisk });
    return await this.postOrder(order, orderType, false);
  }

  async cancelOrder(orderId: string): Promise<boolean> {
    try {
      await this.request('DELETE', `/order/${orderId}`);
      return true;
    } catch {
      return false;
    }
  }

  async getPrice(marketId: string): Promise<{ bid: number; ask: number; mid: number }> {
    try {
      const book = await this.getOrderBook(marketId);
      const bid = book.bids.length > 0 ? Number(book.bids[0].price) : 0;
      const ask = book.asks.length > 0 ? Number(book.asks[0].price) : 0;
      return { bid, ask, mid: (bid + ask) / 2 };
    } catch {
      return { bid: 0, ask: 0, mid: 0 };
    }
  }

  async getTokenForMarket(conditionId: string): Promise<string> {
    const info = await this.getClobMarketInfo(conditionId);
    if (!info) return '';
    return this._selectTokenId(info);
  }

}
