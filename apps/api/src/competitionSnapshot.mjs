import { readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';

const DATA_ROOT = '/app/data';
const SNAPSHOT_FILE = 'competition_state.json';
const STALE_AFTER_SECONDS = 180;
const REQUIRED_AGENT_ACCOUNTING_VERSION = 2;

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
    accounting_version: finite(raw?.accounting_version),
    ranking_eligible: raw?.ranking_eligible !== false,
    history_valid_from: raw?.history_valid_from || null,
    epoch_id: raw?.epoch_id || null,
    starting_capital_usd: start,
    raw_lifetime_equity_usd: finite(raw?.raw_lifetime_equity_usd),
    epoch_baseline_equity_usd: finite(raw?.epoch_baseline_equity_usd),
    gross_equity_usd: gross,
    operating_cost_usd: cost,
    cost_source: raw?.cost_source || 'competition_snapshot',
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

function recompute(agent, bot, sourceValid, warnings, epoch) {
  const sameStartingCapital = agent.starting_capital_usd !== null
    && bot.starting_capital_usd !== null
    && Math.abs(agent.starting_capital_usd - bot.starting_capital_usd) <= 0.01;
  if (!sameStartingCapital) warnings.push('starting_capital_mismatch');

  const epochId = epoch?.epoch_id || null;
  const sharedEpoch = Boolean(epochId && agent.epoch_id === epochId && bot.epoch_id === epochId);
  if (!epochId) warnings.push('competition_epoch_missing');
  else if (!sharedEpoch) warnings.push('competition_epoch_mismatch');

  const agentAccountingValid = agent.accounting_version === REQUIRED_AGENT_ACCOUNTING_VERSION
    && agent.ranking_eligible === true;
  if (agent.accounting_version !== REQUIRED_AGENT_ACCOUNTING_VERSION) warnings.push('agent_accounting_version_invalid');
  if (agent.ranking_eligible !== true) warnings.push('agent_history_not_ranking_eligible');

  const valid = Boolean(
    sourceValid
    && sameStartingCapital
    && sharedEpoch
    && agentAccountingValid
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
    ranking_basis: 'shared_epoch_net_equity_after_agent_operating_costs',
    epoch_id: epochId,
    required_agent_accounting_version: REQUIRED_AGENT_ACCOUNTING_VERSION,
  };
}

export function buildCompetitionSnapshot({ dataDir = DATA_ROOT, now = Date.now() / 1000 } = {}) {
  const path = join(dataDir, SNAPSHOT_FILE);
  const age = ageSeconds(path, now);
  const payload = readObject(path);
  const warnings = Array.isArray(payload?.warnings) ? [...payload.warnings] : [];
  if (!payload) {
    return {
      generated_at: new Date(now * 1000).toISOString(),
      status: 'unknown',
      source: { file: SNAPSHOT_FILE, freshness: 'unknown', age_seconds: age },
      epoch: null,
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
        ranking_basis: 'shared_epoch_net_equity_after_agent_operating_costs',
        epoch_id: null,
        required_agent_accounting_version: REQUIRED_AGENT_ACCOUNTING_VERSION,
      },
      warnings: ['competition_snapshot_missing'],
    };
  }

  const freshness = age !== null && age <= STALE_AFTER_SECONDS ? 'fresh' : 'stale';
  if (freshness !== 'fresh') warnings.push('competition_snapshot_stale');
  const agent = normalizeCompetitor(payload?.competitors?.agent, 'agent');
  const bot = normalizeCompetitor(payload?.competitors?.bot, 'bot');
  const epoch = payload?.epoch && typeof payload.epoch === 'object' ? payload.epoch : null;
  const standings = recompute(
    agent,
    bot,
    freshness === 'fresh' && payload.status !== 'unknown',
    warnings,
    epoch,
  );
  return {
    schema_version: Number(payload.schema_version || 3),
    generated_at: new Date(now * 1000).toISOString(),
    snapshot_generated_at: payload.generated_at || null,
    status: standings.valid_for_ranking ? 'ok' : 'degraded',
    source: {
      file: SNAPSHOT_FILE,
      freshness,
      age_seconds: age === null ? null : round(age, 3),
    },
    epoch,
    competitors: { agent, bot },
    standings,
    contracts: payload.contracts || {
      bot_raw_equity: 'paper_cash + marked_unrealized_pnl',
      epoch_normalization: 'common_start + current_raw_equity - epoch_raw_equity_baseline',
      agent_score: 'normalized_gross_equity - post_epoch_attributable_model_and_compute_cost',
      agent_accounting: 'v2_margin_notional_quantity_leverage_once',
      leader: 'higher_shared_epoch_net_equity_after_costs',
    },
    warnings: [...new Set(warnings)].sort(),
  };
}
