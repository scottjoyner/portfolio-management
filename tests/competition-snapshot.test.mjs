import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildCompetitionSnapshot } from '../apps/api/src/competitionSnapshot.mjs';

async function writeSnapshot(dataDir, agentOverrides = {}) {
  await writeFile(join(dataDir, 'competition_state.json'), JSON.stringify({
    schema_version: 2,
    generated_at: '2026-07-29T12:00:00Z',
    status: 'ok',
    competitors: {
      agent: {
        label: 'OpenRouter Agent',
        status: 'ok',
        accounting_version: 2,
        ranking_eligible: true,
        history_valid_from: '2026-07-29T12:00:00Z',
        starting_capital_usd: 10000,
        gross_equity_usd: 10120,
        operating_cost_usd: 0,
        net_equity_usd: 10120,
        net_return_pct: 1.2,
        ...agentOverrides,
      },
      bot: {
        label: 'EventTraderV4 Bot',
        status: 'ok',
        starting_capital_usd: 10000,
        gross_equity_usd: 10110,
        operating_cost_usd: 0,
        net_equity_usd: 10110,
        net_return_pct: 1.1,
      },
    },
    warnings: [],
  }));
}

test('competition snapshot deducts current-day agent API cost and recomputes the leader', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'competition-'));
  const now = Date.parse('2026-07-29T12:00:00Z') / 1000;
  await writeSnapshot(dataDir);

  const state = {
    agentCostLedger: [
      { agentId: 'openrouter-trading-agent', remoteApiCost: 25, localComputeCost: 0, createdAt: '2026-07-29T11:00:00Z' },
      { agentId: 'openrouter-trading-agent', remoteApiCost: 99, localComputeCost: 0, createdAt: '2026-07-28T11:00:00Z' },
    ],
  };
  const out = buildCompetitionSnapshot({ state, dataDir, now });

  assert.equal(out.competitors.agent.operating_cost_usd, 25);
  assert.equal(out.competitors.agent.net_equity_usd, 10095);
  assert.equal(out.standings.valid_for_ranking, true);
  assert.equal(out.standings.leader, 'bot');
  assert.equal(out.standings.agent_minus_bot_usd, -15);
  assert.equal(out.standings.agent_cost_coverage_ratio, 4.8);
  assert.equal(out.standings.required_agent_accounting_version, 2);
});

test('competition snapshot refuses legacy or invalidated agent history', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'competition-'));
  const now = Date.parse('2026-07-29T12:00:00Z') / 1000;
  await writeSnapshot(dataDir, { accounting_version: 1, ranking_eligible: false });
  const out = buildCompetitionSnapshot({ dataDir, now });
  assert.equal(out.standings.valid_for_ranking, false);
  assert.equal(out.standings.leader, 'unknown');
  assert.ok(out.warnings.includes('agent_accounting_version_invalid'));
  assert.ok(out.warnings.includes('agent_history_not_ranking_eligible'));
});

test('competition snapshot fails closed when the file is missing', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'competition-'));
  const out = buildCompetitionSnapshot({ dataDir, now: Date.parse('2026-07-29T12:00:00Z') / 1000 });
  assert.equal(out.status, 'unknown');
  assert.equal(out.standings.valid_for_ranking, false);
  assert.equal(out.standings.leader, 'unknown');
  assert.deepEqual(out.warnings, ['competition_snapshot_missing']);
});
