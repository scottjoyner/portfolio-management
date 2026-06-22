#!/usr/bin/env node
// CLI commands for the unified execution system
// Usage: node apps/cli/src/execution.mjs <command> [options]

import { execSync } from 'node:child_process';

const API_BASE = process.env.API_BASE || 'http://localhost:3000';
const AUTH_TOKEN = process.env.API_TOKEN || 'op-token-001';

async function api(method, path, body) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      'content-type': 'application/json',
      'authorization': `Bearer ${AUTH_TOKEN}`,
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || JSON.stringify(data.errors));
  return data;
}

function fmt(v) { return JSON.stringify(v, null, 2); }

async function listExecutions() {
  const data = await api('GET', '/api/executions');
  console.log(fmt(data.executions || []));
}

async function getExecution(id) {
  const data = await api('GET', `/api/executions/${id}`);
  console.log('Execution:', fmt(data.execution));
  console.log('Events:', fmt(data.events || []));
}

async function getEvents(id) {
  const data = await api('GET', id ? `/api/executions/${id}/events` : '/api/execution/events');
  console.log(fmt(data.events || []));
}

async function execute(body) {
  const data = await api('POST', '/api/execution/execute', body);
  console.log(fmt(data));
}

async function approve(id) {
  const data = await api('POST', `/api/execution/${id}/approve`);
  console.log(fmt(data));
}

async function reject(id, reason) {
  const data = await api('POST', `/api/execution/${id}/reject`, { reason });
  console.log(fmt(data));
}

async function cancel(id) {
  const data = await api('POST', `/api/execution/${id}/cancel`);
  console.log(fmt(data));
}

async function plan(body) {
  const data = await api('POST', '/api/execution/plan', body);
  console.log(fmt(data));
}

async function listAdapters() {
  const data = await api('GET', '/api/execution/adapters');
  console.log('Registered adapters:');
  for (const a of data.adapters || []) {
    console.log(`  ${a.name.padEnd(20)} venue=${a.venue.padEnd(12)} mode=${a.mode.padEnd(8)} connected=${a.connected}`);
  }
}

async function fetchGraphSignals() {
  const { GraphAlphaBotAdapter } = await import('../../packages/adapters/src/graphAlphaBotAdapter.mjs');
  const adapter = new GraphAlphaBotAdapter();
  const signals = await adapter.fetchSignals(10);
  console.log('Graph-Alpha-Bot signals:');
  for (const s of signals) {
    console.log(`  ${s.symbol.padEnd(10)} score=${s.score.toFixed(3)}  source=${s.source.padEnd(30)} direction=${s.direction.padEnd(6)} conviction=${s.conviction}`);
  }

  console.log('\nGenerated orders:');
  const orders = await adapter.signalsToOrders(50000);
  console.log(fmt(orders));
}

async function ingestGraphSignals() {
  const data = await api('POST', '/api/execution/graph-signals/ingest');
  console.log(`Ingested ${data.signals.length} graph-alpha-bot signals → ${data.opportunities.length} opportunities created`);
  if (data.errors?.length) {
    console.log('Errors:', fmt(data.errors));
  }
  if (data.opportunities?.length) {
    console.log('\nOpportunities:');
    for (const opp of data.opportunities) {
      console.log(`  ${opp.id.padEnd(24)} ${opp.symbol.padEnd(10)} score=${(opp.confidenceScore * 100).toFixed(0)}%  status=${opp.status}`);
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!command || command === '--help' || command === '-h') {
    console.log(`
Usage: node apps/cli/src/execution.mjs <command> [args]

Commands:
  list                          List all executions
  get <id>                      Get execution details and events
  events [id]                   Get events (all or for an execution)
  execute '<json>'              Submit an execution request
  approve <id>                  Approve a draft execution
  reject <id> <reason>          Reject a draft execution
  cancel <id>                   Cancel a draft execution
  plan '<json>'                 Plan an execution without committing
  adapters                      List registered broker adapters
  graph-signals                 Fetch and display graph-alpha-bot signals
  graph-ingest                  Ingest graph-alpha-bot signals as opportunities
    `);
    return;
  }

  switch (command) {
    case 'list':
      await listExecutions();
      break;
    case 'get':
      await getExecution(args[1]);
      break;
    case 'events':
      await getEvents(args[1]);
      break;
    case 'execute':
      await execute(JSON.parse(args[1]));
      break;
    case 'approve':
      await approve(args[1]);
      break;
    case 'reject':
      await reject(args[1], args[2] || 'cli_reject');
      break;
    case 'cancel':
      await cancel(args[1]);
      break;
    case 'plan':
      await plan(JSON.parse(args[1]));
      break;
    case 'adapters':
      await listAdapters();
      break;
    case 'graph-signals':
      await fetchGraphSignals();
      break;
    case 'graph-ingest':
      await ingestGraphSignals();
      break;
    default:
      console.error(`Unknown command: ${command}`);
      process.exit(1);
  }
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
