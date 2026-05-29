import { createBacktest, createStrategyFromTemplate, cloneStrategyVersion, updateStrategyStatus, requestApproval, decideApproval, startPaperExecution, stopPaperExecution } from './operatorFlows.mjs';
import { nextId } from '../../../packages/storage/src/operatorStore.mjs';
import { executePaperSignal } from '../../../packages/execution/src/paperEngine.mjs';

export function routeMatch(pathname, pattern) {
  const pathParts = pathname.split('/').filter(Boolean);
  const patternParts = pattern.split('/').filter(Boolean);
  if (pathParts.length !== patternParts.length) return null;
  const params = {};
  for (let i = 0; i < patternParts.length; i += 1) {
    const patternPart = patternParts[i];
    if (patternPart.startsWith(':')) params[patternPart.slice(1)] = decodeURIComponent(pathParts[i]);
    else if (patternPart !== pathParts[i]) return null;
  }
  return params;
}

async function mutate(store, fn) {
  const result = await store.mutate(async state => fn(state));
  if (result?.errors?.length) {
    const missing = result.errors.some(error => error.endsWith('_not_found'));
    return { status: missing ? 404 : 400, body: { ok: false, errors: result.errors } };
  }
  return { status: 200, body: { ok: true, ...result } };
}

export async function handleOperatorRoute({ method, pathname, state, store, readJsonBody }) {
  if (method === 'GET' && pathname === '/api/accounts') return { status: 200, body: { accounts: state.accounts } };
  if (method === 'GET' && pathname === '/api/instruments') return { status: 200, body: { instruments: state.instruments } };
  if (method === 'GET' && pathname === '/api/strategy-templates') return { status: 200, body: { templates: state.strategyTemplates } };

  const clone = routeMatch(pathname, '/api/strategies/:id/clone');
  if (method === 'POST' && clone) {
    const body = await readJsonBody();
    const result = await mutate(store, current => cloneStrategyVersion(current, clone.id, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  const strategyStatus = routeMatch(pathname, '/api/strategies/:id/status');
  if (method === 'POST' && strategyStatus) {
    const body = await readJsonBody();
    return mutate(store, current => updateStrategyStatus(current, strategyStatus.id, body.status));
  }

  const backtestReport = routeMatch(pathname, '/api/backtests/:id/report');
  if (method === 'GET' && backtestReport) {
    const backtest = state.backtests.find(run => run.id === backtestReport.id);
    if (!backtest) return { status: 404, body: { ok: false, errors: ['backtest_not_found'] } };
    return { status: 200, body: { report: backtest.report || {}, metrics: backtest.metrics, assumptions: backtest.assumptions, trades: backtest.trades, equityCurve: backtest.equityCurve } };
  }

  const approvalDecision = routeMatch(pathname, '/api/approvals/:id/decision');
  if (method === 'POST' && approvalDecision) {
    const body = await readJsonBody();
    return mutate(store, current => decideApproval(current, approvalDecision.id, body));
  }

  if (method === 'GET' && pathname === '/api/paper-executions') return { status: 200, body: { executions: state.paperExecutions } };

  if (method === 'POST' && pathname === '/api/paper-executions') {
    const body = await readJsonBody();
    const result = await mutate(store, current => startPaperExecution(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  const stopPaper = routeMatch(pathname, '/api/paper-executions/:id/stop');
  if (method === 'POST' && stopPaper) {
    const body = await readJsonBody();
    return mutate(store, current => stopPaperExecution(current, stopPaper.id, body));
  }

  const signalPaper = routeMatch(pathname, '/api/paper-executions/:id/signal');
  if (method === 'POST' && signalPaper) {
    const body = await readJsonBody();
    return mutate(store, current => {
      const result = executePaperSignal(current, signalPaper.id, body.signal || body, body.quote || {});
      if (!result.ok) return { errors: result.errors };
      current.audit.push({ id: nextId('audit', current.audit), action: 'paper_signal_filled', actor: 'operator', at: new Date().toISOString(), details: result.fill.id, payload: { executionId: signalPaper.id, symbol: result.fill.symbol } });
      return result;
    });
  }

  if (method === 'POST' && pathname === '/api/strategies/from-template') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createStrategyFromTemplate(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/backtests/run') {
    const body = await readJsonBody();
    const result = await mutate(store, current => createBacktest(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/approvals/request') {
    const body = await readJsonBody();
    const result = await mutate(store, current => requestApproval(current, body));
    return { ...result, status: result.status === 200 ? 201 : result.status };
  }

  if (method === 'POST' && pathname === '/api/kill-switch/stop-paper') {
    const result = await mutate(store, current => {
      const now = new Date().toISOString();
      for (const execution of current.paperExecutions) {
        if (execution.status === 'running') {
          execution.status = 'stopped';
          execution.stoppedAt = now;
          execution.stopReason = 'operator_stop_all';
        }
      }
      current.audit.push({ id: nextId('audit', current.audit), action: 'paper_execution_stop_all', actor: 'operator', at: now, details: 'operator_stop_all' });
      return { executions: current.paperExecutions };
    });
    return result;
  }

  return null;
}
