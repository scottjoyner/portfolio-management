import type { IBrokerAdapter } from './types.js';
import type { Venue, ExecutionMode } from '@pkg/core/types.js';
import { PaperBrokerAdapter } from './paperAdapter.js';
import { CoinbaseBrokerAdapter } from './coinbaseAdapter.js';
import { KalshiBrokerAdapter } from './kalshiAdapter.js';
import { PolymarketBrokerAdapter } from './polymarketAdapter.js';

export type AdapterFactory = () => IBrokerAdapter;

export class AdapterRegistry {
  private adapters: Map<string, IBrokerAdapter> = new Map();
  private factories: Map<string, AdapterFactory> = new Map();

  constructor() {
    this.registerDefaultFactories();
  }

  private registerDefaultFactories() {
    this.factories.set('paper', () => new PaperBrokerAdapter());
    this.factories.set('coinbase-paper', () => new CoinbaseBrokerAdapter('paper'));
    this.factories.set('coinbase-live', () => new CoinbaseBrokerAdapter('live'));
    this.factories.set('kalshi-demo', () => new KalshiBrokerAdapter('demo'));
    this.factories.set('kalshi-live', () => new KalshiBrokerAdapter('live'));
    this.factories.set('polymarket', () => new PolymarketBrokerAdapter('demo'));
  }

  registerAdapter(name: string, adapter: IBrokerAdapter): void {
    this.adapters.set(name, adapter);
  }

  registerFactory(name: string, factory: AdapterFactory): void {
    this.factories.set(name, factory);
  }

  getAdapter(name: string): IBrokerAdapter | undefined {
    if (this.adapters.has(name)) return this.adapters.get(name);
    const factory = this.factories.get(name);
    if (factory) {
      const adapter = factory();
      this.adapters.set(name, adapter);
      return adapter;
    }
    return undefined;
  }

  getOrCreate(name: string, mode?: ExecutionMode): IBrokerAdapter {
    const existing = this.getAdapter(name);
    if (existing) return existing;

    if (name === 'coinbase') {
      const adapter = new CoinbaseBrokerAdapter(mode || 'paper');
      this.adapters.set(name, adapter);
      return adapter;
    }
    if (name === 'kalshi') {
      const adapter = new KalshiBrokerAdapter(mode || 'demo');
      this.adapters.set(name, adapter);
      return adapter;
    }
    if (name === 'polymarket') {
      const adapter = new PolymarketBrokerAdapter('demo');
      this.adapters.set(name, adapter);
      return adapter;
    }

    const adapter = new PaperBrokerAdapter();
    this.adapters.set(name, adapter);
    return adapter;
  }

  getAdapterForVenue(venue: Venue, mode: ExecutionMode = 'paper'): IBrokerAdapter {
    const key = venue === 'coinbase' ? `coinbase-${mode}` : venue === 'kalshi' ? `kalshi-${mode === 'live' ? 'live' : 'demo'}` : venue === 'polymarket' ? 'polymarket' : 'paper';
    return this.getOrCreate(key, mode);
  }

  listAdapters(): { name: string; venue: Venue; mode: ExecutionMode; connected: boolean }[] {
    const names = new Set([...this.adapters.keys(), ...this.factories.keys()]);
    return Array.from(names).map(name => {
      const adapter = this.getAdapter(name);
      return {
        name,
        venue: adapter?.venue || 'paper',
        mode: adapter?.mode || 'paper',
        connected: adapter ? this.adapters.has(name) : false,
      };
    });
  }

  async connectAll(): Promise<{ name: string; ok: boolean; error?: string }[]> {
    const results: { name: string; ok: boolean; error?: string }[] = [];
    for (const [name, adapter] of this.adapters) {
      try {
        await adapter.connect();
        results.push({ name, ok: true });
      } catch (error) {
        results.push({ name, ok: false, error: String(error) });
      }
    }
    return results;
  }

  removeAdapter(name: string): void {
    this.adapters.delete(name);
  }

  clear(): void {
    this.adapters.clear();
  }
}

let defaultRegistry: AdapterRegistry | null = null;

export function getDefaultRegistry(): AdapterRegistry {
  if (!defaultRegistry) {
    defaultRegistry = new AdapterRegistry();
  }
  return defaultRegistry;
}

export function resetDefaultRegistry(): void {
  defaultRegistry = null;
}
