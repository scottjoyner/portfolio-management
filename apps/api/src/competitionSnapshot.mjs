import { readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const DATA_ROOT = '/app/data';
const SNAPSHOT_FILE = 'competition_state.json';
const STALE_AFTER_SECONDS = 180;

function finite(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function round(value, decimals = 2) {
  return Number(Number(value).toFixed(decimals));
}

function readObject(path) {
  try {
    const value = JSON.parse(readFileSync(path, 'utf8'));
    return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
  } catch {
    return null;
  }
}

function ageSeconds(path, now) {
  try {
    return Math.max(0, now - statSync(path).mtimeMs / 1000);
  } catch {
    return null;
  }
}

function utcDay(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString().slice(0, 10);
}

function currentAgentCost(state, now) {
  const rows = Array.isArray(state?.agentCostLedger) ? state.agentCostLedger : [];
  const today = new Date(now * 1000).toISOString().slice(0, 10);
  const relevant = rows.filter(row => {
    const agentId = String(row?.agentId || '').toLowerCase();
    const day = utcDay(row?.createdAt);
    return day === today && (!agentId || ['agent', 'hermes', 'trader', 'openrouter'].some(token => agentId.includes(token)));
  });
  const cost = relevant.reduce(
    (sum, row) => sum + Number(row?.remoteApiCost || 0) + Number(row?.localComputeCost || 0),
    0,
  );
  return {
    value: round(cost, 6),
    rows: relevant.length,
    source: relevant.length ? 'operator_agent_cost_ledger_today' : 'competition_snapshot',
  };
}

function normalizeCompetitor(raw, side) {
  const start = finite(raw?.starting_capital_usd);
  const gross = finite(raw?.gross_equity_usd);
  const cost = finite(raw?.operating_cost_usd) ?? 0;
  const net = finite(raw?.net_equity_usd) ?? (gross === null ? null : gross - cost);
  return {
    side,
    label: String(raw?.label || (side === 'agent' ? 'OpenRouter Agent' : 'EventTraderV4 Bot')),
    status: String(raw?.status || 'unknown'),
    source: raw?.source || 'competition_snapshot',
    age_seconds: finite(raw?.age_seconds),
    starting_capital_usd: start,
    gross_equity_usd: gross,
    operating_cost_usd: cost,
    net_equity_usd: net,
    gross_pnl_usd: finite(raw?.gross_pnl_usd) ?? (gross !== null && start !== null ? gross - start : null),
    net_pnl_usd: finite(raw?.net_pnl_usd) ?? (net !== null && start !== null ? net - start : null),
    gross_return_pct: finite(raw?.gross_return_pct),
    net_return_pct: finite(raw?.net_return_pct),
    realized_pnl_usd: finite(raw?.realized_pnl_usd),
    unrealized_pnl_usd: finite(raw?.unrealized_pnl_usd),
    max_drawdown_pct: finite(raw?.max_drawdown_pct),
    open_positions: finite(raw?.open_positions),
    round_trips: finite(raw?.round_trips),
    trade_events: finite(raw?.trade_events),
    wins: finite(raw?.wins),
    losses: finite(raw?.losses),
    win_rate: finite(raw?.win_rate),
    profit_factor: finite(raw?.profit_factor),
  };
}

function recompute(agent, bot, sourceValid, warnings) {
  const sameStartingCapital = agent.starting_capital_usd !== null
    && bot.starting_capital_usd !== null
    && Math.abs(agent.starting_capital_usd - bot.starting_capital_usd) <= 0.01;
  if (!sameStartingCapital) warnings.push('starting_capital_mismatch');
  const valid = Boolean(
    sourceValid
    && sameStartingCapital
    && agent.status === 'ok'
    && bot.status === 'ok'
    && agent.net_equity_usd !== null
    && bot.net_equity_usd !== null,
  );
  const delta = valid ? agent.net_equity_usd - bot.net_equity_usd : null;
  const agentCost = agent.operating_cost_usd || 0;
  const coverage = agentCost > 0 && agent.gross_pnl_usd !== null ? agent.gross_pnl_usd / agentCost : null;
  return {
    valid_for_ranking: valid,
    leader: !valid ? 'unknown' : delta > 0 ? 'agent' : delta < 0 ? 'bot' : 'tie',
    edge_usd: delta === null ? null : round(Math.abs(delta), 6),
    agent_minus_bot_usd: delta === null ? null : round(delta, 6),
    agent_cost_coverage_ratio: coverage === null ? null : round(coverage, 6),
    agent_break_even_gap_usd: agent.gross_pnl_usd === null ? null : round(Math.max(0, agentCost - agent.gross_pnl_usd), 6),
    agent_alpha_after_cost_pct_points: valid && agent.net_return_pct !== null && bot.net_return_pct !== null
      ? round(agent.net_return_pct - bot.net_return_pct, 6)
      : null,
    ranking_basis: 'net_equity_after_agent_operating_costs',
  };
}

export function buildCompetitionSnapshot({ state = {}, dataDir = DATA_ROOT, now = Date.now() / 1000 } = {}) {
  const path = join(dataDir, SNAPSHOT_FILE);
  const age = ageSeconds(path, now);
  const payload = readObject(path);
  const warnings = Array.isArray(payload?.warnings) ? [...payload.warnings] : [];
  if (!payload) {
    return {
      generated_at: new Date(now * 1000).toISOString(),
      status: 'unknown',
      source: { file: SNAPSHOT_FILE, freshness: 'unknown', age_seconds: age },
      competitors: {
        agent: normalizeCompetitor(null, 'agent'),
        bot: normalizeCompetitor(null, 'bot'),
      },
      standings: {
        valid_for_ranking: false,
        leader: 'unknown',
        edge_usd: null,
        agent_minus_bot_usd: null,
        agent_cost_coverage_ratio: null,
        agent_break_even_gap_usd: null,
        agent_alpha_after_cost_pct_points: null,
        ranking_basis: 'net_equity_after_agent_operating_costs',
      },
      warnings: ['competition_snapshot_missing'],
    };
  }

  const freshness = age !== null && age <= STALE_AFTER_SECONDS ? 'fresh' : 'stale';
  if (freshness !== 'fresh') warnings.push('competition_snapshot_stale');
  const agent = normalizeCompetitor(payload?.competitors?.agent, 'agent');
  const bot = normalizeCompetitor(payload?.competitors?.bot, 'bot');

  const liveCost = currentAgentCost(state, now);
  if (liveCost.rows > 0) {
    agent.operating_cost_usd = liveCost.value;
    agent.net_equity_usd = agent.gross_equity_usd === null ? null : round(agent.gross_equity_usd - liveCost.value, 6);
    agent.net_pnl_usd = agent.net_equity_usd === null || agent.starting_capital_usd === null
      ? null
      : round(agent.net_equity_usd - agent.starting_capital_usd, 6);
    agent.net_return_pct = agent.net_pnl_usd === null || !agent.starting_capital_usd
      ? null
      : round(agent.net_pnl_usd / agent.starting_capital_usd * 100, 6);
    agent.cost_source = liveCost.source;
  }

  const standings = recompute(agent, bot, freshness === 'fresh' && payload.status !== 'unknown', warnings);
  return {
    schema_version: Number(payload.schema_version || 2),
    generated_at: new Date(now * 1000).toISOString(),
    snapshot_generated_at: payload.generated_at || null,
    status: standings.valid_for_ranking ? 'ok' : 'degraded',
    source: {
      file: SNAPSHOT_FILE,
      freshness,
      age_seconds: age === null ? null : round(age, 3),
    },
    competitors: { agent, bot },
    standings,
    contracts: payload.contracts || {
      bot_equity: 'paper_cash + marked_unrealized_pnl',
      agent_score: 'gross_equity - attributable_model_and_compute_cost',
      leader: 'higher_net_equity_after_costs',
    },
    warnings: [...new Set(warnings)].sort(),
  };
}
