import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildCompetitionSnapshot } from '../apps/api/src/competitionSnapshot.mjs';

async function writeSnapshot(dataDir, agentOverrides = {}, snapshotOverrides = {}) {
  const epoch = {
    epoch_id: 'epoch-test',
    started_at: '2026-07-29T11:00:00Z',
    normalized_starting_capital_usd: 10000,
  };
  await writeFile(join(dataDir, 'competition_state.json'), JSON.stringify({
    schema_version: 3,
    generated_at: '2026-07-29T12:00:00Z',
    status: 'ok',
    epoch,
    competitors: {
      agent: {
        label: 'OpenRouter Agent',
        status: 'ok',
        accounting_version: 2,
        ranking_eligible: true,
        history_valid_from: '2026-07-29T11:00:00Z',
        epoch_id: epoch.epoch_id,
        starting_capital_usd: 10000,
        gross_equity_usd: 10120,
        operating_cost_usd: 5,
        cost_source: 'agent_cost_ledger_rows',
        net_equity_usd: 10115,
        gross_pnl_usd: 120,
        net_pnl_usd: 115,
        net_return_pct: 1.15,
        ...agentOverrides,
      },
      bot: {
        label: 'EventTraderV4 Bot',
        status: 'ok',
        epoch_id: epoch.epoch_id,
        starting_capital_usd: 10000,
        gross_equity_usd: 10110,
        operating_cost_usd: 0,
        net_equity_usd: 10110,
        gross_pnl_usd: 110,
        net_pnl_usd: 110,
        net_return_pct: 1.1,
      },
    },
    warnings: [],
    ...snapshotOverrides,
  }));
}

test('competition snapshot preserves authoritative post-epoch agent cost', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'competition-'));
  const now = Date.parse('2026-07-29T12:00:00Z') / 1000;
  await writeSnapshot(dataDir);

  // State may contain a larger today-total, but the API must not overwrite the
  // scoreboard's epoch-baselined operating cost.
  const state = {
    agentCostLedger: [
      { agentId: 'openrouter-trading-agent', remoteApiCost: 25, createdAt: '2026-07-29T11:00:00Z' },
    ],
  };
  const out = buildCompetitionSnapshot({ state, dataDir, now });

  assert.equal(out.competitors.agent.operating_cost_usd, 5);
  assert.equal(out.competitors.agent.net_equity_usd, 10115);
  assert.equal(out.standings.valid_for_ranking, true);
  assert.equal(out.standings.leader, 'agent');
  assert.equal(out.standings.agent_minus_bot_usd, 5);
  assert.equal(out.standings.agent_cost_coverage_ratio, 24);
  assert.equal(out.standings.epoch_id, 'epoch-test');
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

test('competition snapshot refuses a missing shared epoch', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'competition-'));
  const now = Date.parse('2026-07-29T12:00:00Z') / 1000;
  await writeSnapshot(dataDir, {}, { epoch: null });
  const out = buildCompetitionSnapshot({ dataDir, now });
  assert.equal(out.standings.valid_for_ranking, false);
  assert.equal(out.standings.leader, 'unknown');
  assert.ok(out.warnings.includes('competition_epoch_missing'));
});

test('competition snapshot fails closed when the file is missing', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'competition-'));
  const out = buildCompetitionSnapshot({ dataDir, now: Date.parse('2026-07-29T12:00:00Z') / 1000 });
  assert.equal(out.status, 'unknown');
  assert.equal(out.standings.valid_for_ranking, false);
  assert.equal(out.standings.leader, 'unknown');
  assert.deepEqual(out.warnings, ['competition_snapshot_missing']);
});
