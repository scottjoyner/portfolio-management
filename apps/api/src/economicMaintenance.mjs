import * as legacy from './economicMaintenanceLegacy.mjs';
import {
  matureCertifiedShadowTrials,
  summarizeCertifiedShadowAttribution,
} from '../../../packages/economics/src/certifiedShadowAttribution.mjs';

export * from './economicMaintenanceLegacy.mjs';

function positive(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : fallback;
}

export async function runEconomicMaintenance(state, options = {}) {
  // The legacy routine intentionally replaces economicMaintenance with a
  // compact status document at completion. Preserve the shadow evidence ledger
  // across that replacement before maturing canonical shadow exits.
  const shadowLedger = state.economicMaintenance?.certifiedShadowAttribution || null;
  const report = await legacy.runEconomicMaintenance(state, options);
  state.economicMaintenance ||= {};
  if (shadowLedger) state.economicMaintenance.certifiedShadowAttribution = shadowLedger;

  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const shadow = matureCertifiedShadowTrials(state, now);
  const summary = summarizeCertifiedShadowAttribution(state);

  report.certifiedShadowOutcomesCreated = shadow.shadowOutcomes.length;
  report.certifiedShadowTrialsPending = summary.openTrials;
  report.details ||= {};
  report.details.certifiedShadowOutcomes = shadow.shadowOutcomes.map(row => row.id);
  report.details.pendingCertifiedShadowTrials = shadow.pendingShadowTrials;
  report.details.certifiedShadowSummary = summary;

  state.economicMaintenance.counters ||= {};
  state.economicMaintenance.counters.certifiedShadowOutcomesCreated = shadow.shadowOutcomes.length;
  state.economicMaintenance.counters.certifiedShadowTrialsPending = summary.openTrials;
  state.economicMaintenance.counters.certifiedShadowTrialsClosed = summary.closedTrials;
  state.economicMaintenance.counters.certifiedShadowSignalObservations = summary.signalObservations;

  return report;
}

export function startEconomicMaintenance({ store, env = process.env, fetchImpl = globalThis.fetch, quoteFetcher } = {}) {
  if (!store) throw new Error('economic_maintenance_store_required');
  const enabled = env.ECONOMIC_RUNTIME_ENABLED === 'true';
  if (!enabled) return null;
  const intervalMs = Math.max(10000, positive(env.ECONOMIC_MAINTENANCE_INTERVAL_MS, 60000));
  let running = false;
  const run = async () => {
    if (running) return { ok: false, skipped: true, reason: 'economic_maintenance_already_running' };
    running = true;
    try {
      return await store.mutate(state => runEconomicMaintenance(state, { env, fetchImpl, quoteFetcher }));
    } finally {
      running = false;
    }
  };
  const timer = setInterval(() => { run().catch(() => {}); }, intervalMs);
  timer.unref?.();
  const initialTimer = setTimeout(() => { run().catch(() => {}); }, Math.min(1000, intervalMs));
  initialTimer.unref?.();
  return {
    intervalMs,
    run,
    stop() {
      clearInterval(timer);
      clearTimeout(initialTimer);
    },
  };
}
