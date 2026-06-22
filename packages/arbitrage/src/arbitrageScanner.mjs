import { matchMarkets } from './marketMatcher.mjs';

async function importKalshiClient() {
  try {
    const { KalshiClient } = await import('../../../packages/kalshi/src/client.ts');
    return KalshiClient;
  } catch {
    return null;
  }
}

async function importPolymarketClient() {
  try {
    const { PolymarketClient } = await import('../../../packages/polymarket/src/client.ts');
    return PolymarketClient;
  } catch {
    return null;
  }
}



async function fetchKalshiMarkets(config = {}) {
  const Kalshi = await importKalshiClient();
  if (!Kalshi) return [];
  try {
    const client = new Kalshi(config.kalshiEmail, config.kalshiPassword);
    const raw = typeof client.listAllMarkets === 'function'
      ? await client.listAllMarkets({ limit: config.marketPageSize || 100, status: config.kalshiStatus || 'open', maxPages: config.maxPages || 20 })
      : await client.listMarkets({ limit: config.marketPageSize || 100, status: config.kalshiStatus || 'open' });
    return raw.map(m => ({ ...m, venue: 'kalshi' }));
  } catch { return []; }
}

async function fetchPolymarketMarkets(_config = {}) {
  const Polymarket = await importPolymarketClient();
  if (!Polymarket) return [];
  try {
    const client = new PolymarketClient();
    const markets = typeof client.listAllMarkets === 'function'
      ? await client.listAllMarkets({ limit: _config.marketPageSize || 100, closed: false, maxPages: _config.maxPages || 20 })
      : await client.listMarkets({ limit: _config.marketPageSize || 100, closed: false });
    return markets.map(m => ({ ...m, venue: 'polymarket' }));
  } catch { return []; }
}

function getPrice(market, side, venue) {
  if (venue === 'kalshi') {
    if (side === 'yes') return { bid: market.yes_bid || 0, ask: market.yes_ask || 0 };
    if (side === 'no') return { bid: market.no_bid || 0, ask: market.no_ask || 0 };
  }
  if (venue === 'polymarket') {
    const prices = (market.outcomePrices || []).map(p => Number(p));
    const yesPrice = prices[0] || 0;
    const noPrice = prices[1] || (1 - yesPrice);
    if (side === 'yes') return { bid: yesPrice * 0.998, ask: yesPrice * 1.002, mid: yesPrice };
    if (side === 'no') return { bid: noPrice * 0.998, ask: noPrice * 1.002, mid: noPrice };
  }
  return { bid: 0, ask: 0, mid: 0 };
}

export function computeArbitrageForPair(kalshiMarket, polymarketMarket) {
  const kYes = getPrice(kalshiMarket, 'yes', 'kalshi');
  const kNo = getPrice(kalshiMarket, 'no', 'kalshi');
  const pYes = getPrice(polymarketMarket, 'yes', 'polymarket');
  const pNo = getPrice(polymarketMarket, 'no', 'polymarket');

  const results = [];

  const strategies = [
    {
      id: 'buy_yes_kalshi_buy_no_polymarket',
      label: 'Buy Yes on Kalshi, Buy No on Polymarket',
      cost: kYes.ask + pNo.ask,
      payout: 1.0,
      size: Math.min(Number(kalshiMarketSize(kalshiMarket)), Number(polymarketMarketSize(polymarketMarket))),
      legs: [
        { venue: 'kalshi', side: 'yes', action: 'buy', price: kYes.ask },
        { venue: 'polymarket', side: 'no', action: 'buy', price: pNo.ask },
      ],
    },
    {
      id: 'buy_no_kalshi_buy_yes_polymarket',
      label: 'Buy No on Kalshi, Buy Yes on Polymarket',
      cost: kNo.ask + pYes.ask,
      payout: 1.0,
      size: Math.min(Number(kalshiMarketSize(kalshiMarket)), Number(polymarketMarketSize(polymarketMarket))),
      legs: [
        { venue: 'kalshi', side: 'no', action: 'buy', price: kNo.ask },
        { venue: 'polymarket', side: 'yes', action: 'buy', price: pYes.ask },
      ],
    },
  ];

  for (const s of strategies) {
    if (s.cost <= 0 || s.cost >= 0.999) continue;
    const profit = s.payout - s.cost;
    const edgeBps = Math.round((profit / s.cost) * 10000);
    if (edgeBps > 0) {
      results.push({
        strategyId: s.id,
        label: s.label,
        totalCost: Number(s.cost.toFixed(6)),
        payout: Number(s.payout.toFixed(2)),
        profitPerShare: Number(profit.toFixed(6)),
        edgeBps,
        returnPct: Number((profit / s.cost * 100).toFixed(2)),
        expectedProfitUsd: Number((profit * (s.size || 1)).toFixed(2)),
        legs: s.legs,
        kalshiPrice: { yes: { bid: kYes.bid, ask: kYes.ask }, no: { bid: kNo.bid, ask: kNo.ask } },
        polymarketPrice: { yes: { bid: pYes.bid, ask: pYes.ask }, no: { bid: pNo.bid, ask: pNo.ask } },
      });
    }
  }

  results.sort((a, b) => (b.expectedProfitUsd - a.expectedProfitUsd) || (b.edgeBps - a.edgeBps));
  return results;
}

function kalshiMarketSize(market) {
  const vol = Number(market.volume || market.volume_fp || 0);
  const liquidity = Number(market.liquidity || 0);
  return Math.max(10, Math.min(1000, Math.round(Math.max(vol / 1000, liquidity))));
}

function polymarketMarketSize(market) {
  const vol = Number(market.volume || 0);
  const liquidity = Number(market.liquidity || 0);
  return Math.max(10, Math.min(1000, Math.round(Math.max(vol / 1000, liquidity))));
}

export async function scanForArbitrage(config = {}) {
  const [kalshiMarkets, polymarketMarkets] = await Promise.all([
    fetchKalshiMarkets(config),
    fetchPolymarketMarkets(config),
  ]);

  const matches = matchMarkets(kalshiMarkets, polymarketMarkets, config.matchOptions || {});

  const opportunities = [];

  for (const match of matches) {
    const arbs = computeArbitrageForPair(match.kalshiMarket, match.polymarketMarket);
    if (arbs.length > 0) {
      const best = arbs[0];
      const title = `${match.kalshiMarket.title} ↔ ${match.polymarketMarket.question || match.polymarketMarket.title}`;
      const perShare = best.profitPerShare;

      opportunities.push({
        pairId: `${match.kalshiMarket.id}|${match.polymarketMarket.conditionId || match.polymarketMarket.id}`,
        kalshiMarket: match.kalshiMarket,
        polymarketMarket: match.polymarketMarket,
        similarity: match.similarity,
        title,
        bestStrategy: best,
        allStrategies: arbs,
        matchType: match.forced ? 'manual' : 'auto',
        totalCostPerShare: best.totalCost,
        edgeBps: best.edgeBps,
        returnPct: best.returnPct,
        size: 1000,
        confidenceScore: Math.min(1, Math.max(0.1, best.edgeBps / 500)),
        liquidityScore: Math.min(100, Math.round(Math.min(
          Number(match.kalshiMarket.volume || 0) / 50000,
          Number(match.polymarketMarket.volume || 0) / 50000,
        ) * 50)),
        createdAt: new Date().toISOString(),
      });
    }
  }

  return opportunities.sort((a, b) => (b.expectedProfitUsd || 0) - (a.expectedProfitUsd || 0) || b.edgeBps - a.edgeBps);
}
