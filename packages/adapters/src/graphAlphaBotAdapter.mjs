// Adapter that bridges the graph-alpha-bot's signal generation into the unified execution system.
// graph-alpha-bot produces signals via Neo4j queries (news centrality, insider cluster drift, etc.)
// This adapter consumes those signals and turns them into executable OrderIntents.

import { execSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const SIGNAL_CACHE_PATH = resolve(process.env.HOME || '/tmp', '.hermes', 'signals', 'graph-alpha-signals.json');

export class GraphAlphaBotAdapter {
  name = 'graph-alpha-bot';
  venue = 'paper';
  mode = 'paper';
  connected = false;

  constructor(mode = 'paper') {
    this.mode = mode;
  }

  async connect() {
    this.connected = true;
    return true;
  }

  async disconnect() {
    this.connected = false;
  }

  async health() {
    return { ok: this.connected, venue: 'graph-alpha-bot', latencyMs: 0, mode: this.mode, authenticated: this.connected };
  }

  // Fetch signals from the graph-alpha-bot's Neo4j pipeline.
  // Tries cache first, then falls back to running the Python signal fetcher.
  async fetchSignals(limit = 20) {
    // Try cache
    if (existsSync(SIGNAL_CACHE_PATH)) {
      try {
        const raw = readFileSync(SIGNAL_CACHE_PATH, 'utf-8');
        const cached = JSON.parse(raw);
        if (Array.isArray(cached)) return cached.slice(0, limit);
      } catch { /* ignore corrupt cache */ }
    }

    // Try running the graph-alpha-bot signal fetcher
    try {
      const script = resolve(process.cwd(), 'graph-alpha-bot', 'app', 'tools', 'live_paper_trading.py');
      if (existsSync(script)) {
        const stdout = execSync(`python3 ${script} --limit ${limit} --json 2>/dev/null`, { timeout: 30000, encoding: 'utf-8' });
        const signals = JSON.parse(stdout.trim());
        if (Array.isArray(signals)) return signals;
      }
    } catch { /* pipeline not available */ }

    return [];
  }

  // Convert graph-alpha-bot signals to OrderIntents compatible with the execution engine
  async signalsToOrders(cash = 100000) {
    const signals = await this.fetchSignals(20);
    const totalScore = signals.reduce((s, sig) => s + Math.max(0, sig.score), 0);
    if (totalScore <= 0) return [];

    return signals.map((sig) => {
      const weight = Math.max(0, sig.score) / totalScore;
      const alloc = cash * weight;
      const price = 0;
      const quantity = 0;
      const now = new Date().toISOString();

      return {
        id: `gab-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
        strategyId: `graph-alpha-${sig.source}`,
        opportunityId: null,
        marketId: `${sig.symbol}-${sig.direction === 'long' ? 'buy' : 'sell'}`,
        symbol: sig.symbol,
        venue: 'paper',
        side: sig.direction === 'long' ? 'buy' : 'sell',
        quantity: Math.max(0.001, quantity),
        price,
        orderType: 'market',
        timeInForce: 'GTC',
        executionMode: this.mode,
        confidenceScore: Math.min(0.95, Math.max(0.1, sig.score)),
        convictionWeight: weight,
        tags: { source: sig.source, strategyName: sig.strategyName, conviction: sig.conviction },
        createdAt: now,
        updatedAt: now,
      };
    });
  }
}

export function createGraphAlphaAdapter(mode = 'paper') {
  return new GraphAlphaBotAdapter(mode);
}
