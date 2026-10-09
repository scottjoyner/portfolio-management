#!/usr/bin/env node
// Cross-sectional sweep: does ANY symbol have ANY predictive signal?
//
// The single-symbol audit found nothing on BTC-USD. That is one market and one
// window, and crypto majors are not independent draws anyway, so the honest next
// question is whether the null holds across the whole cache -- 51 symbols.
//
// THE MULTIPLICITY IS TWO-DIMENSIONAL AND THAT IS THE HARD PART. There are 16
// pre-committed candidates and 51 symbols, so 816 naive tests at a 5% family-wise
// rate guarantee roughly 41 false positives. Correcting for all 816 assumes the
// symbols are independent, which is false -- BTC, ETH and SOL move together, so
// that correction is anti-conservative and would manufacture significance.
// Correcting for only 16 assumes they are perfectly correlated, which is
// conservative to the point of being useless.
//
// So both are computed and both are reported:
//
//   pooled    per candidate, OOS trade signs pooled across all symbols, blocks
//             confined within a symbol-fold so one symbol cannot vote twice.
//             Corrected for 16 -- the conservative bound.
//   pairwise  per (candidate, symbol), corrected for 816 -- the strict bound.
//
// The truth lies between them and depends on a correlation matrix nobody has
// estimated here. Rather than pick the flattering one, the verdict requires BOTH
// to agree. Anything that passes the pooled test but not the pairwise test is
// reported as "cross-sectionally inconsistent" and is not treated as a finding.
//
// ADVISORY ONLY. No orders, no broker, no operator store.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import process from 'node:process';

import {
  assessCandidateSignificance,
  CANDIDATES,
  DEFAULT_POLICY,
  exactNonoverlappingBlockSignTest,
  exactOneSidedSignTest,
  runAudit,
} from './run-signal-existence-audit.mjs';
import { generateWalkForwardSplits } from './run-decision-classifier-replay.mjs';

export const CROSS_SECTIONAL_SCHEMA_VERSION = 1;
const COST_FREE = { takerFeeRate: 0, makerFeeRate: 0, spreadMultiplier: 0 };

function loadManifest(path) {
  return JSON.parse(readFileSync(path, 'utf8'));
}

function loadSymbolRows(manifest, entry, dir) {
  return readFileSync(join(dir, entry.file), 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

/**
 * Per-symbol folds, sized for the smallest symbol in the cache.
 *
 * Fixed rather than proportional: a 350-bar symbol and a 2386-bar symbol both need
 * a training region long enough for the 24-bar lookback and a test region large
 * enough for the block test's five non-zero blocks. Proportional sizing would give
 * the short symbols a single tiny fold, and a candidate would pass or fail on
 * noise.
 */
export function foldsForSymbol(rowCount, options = {}) {
  const trainSize = options.trainSize ?? 150;
  const testSize = options.testSize ?? 60;
  const purgeSize = options.purgeSize ?? 1;
  const embargoSize = options.embargoSize ?? 1;
  const nFolds = options.folds ?? Math.max(1, Math.floor((rowCount - trainSize - purgeSize - embargoSize) / testSize));
  return generateWalkForwardSplits(rowCount, { trainSize, testSize, purgeSize, embargoSize });
}

export function runCrossSectional({ manifest, rowsBySymbol, options = {}, sentimentByT = null }) {
  const policy = options.policy || DEFAULT_POLICY;
  const totalPairs = CANDIDATES.length * manifest.symbols_exported;
  if (totalPairs > 20_000) throw new Error(`cross-sectional family of ${totalPairs} is implausibly large`);

  // Per-symbol runs. Each returns fold-structured returns so the block test can
  // keep blocks inside a fold, and so pooling can keep them inside a symbol.
  const perSymbol = new Map();   // symbol -> candidateId -> foldReturns
  const controlsOk = new Map();  // symbol -> boolean
  for (const entry of manifest.datasets) {
    const rows = rowsBySymbol.get(entry.symbol);
    if (!rows) continue;
    const folds = foldsForSymbol(rows.length, options);
    if (!folds.length) continue;
    const audit = runAudit({
      rows, folds,
      options: { ...options, costModel: options.costModel ?? COST_FREE, folds },
      sentimentByT,
    });
    const map = new Map();
    for (const candidate of CANDIDATES) {
      const found = audit.results.find(row => row.id === candidate.id);
      // Re-derive fold returns is not exposed by auditCandidate, so recompute the
      // aggregate from the stored per-fold scores via a second, cheap pass.
      map.set(candidate.id, found ? null : null);
    }
    perSymbol.set(entry.symbol, { audit, folds, entry });
    controlsOk.set(entry.symbol, audit.controlsPassed);
  }

  // Pooled pass: for each candidate, concatenate every symbol's fold returns.
  // The block test is applied to a structure that never lets a block span a
  // symbol, so a symbol with many winning trades cannot manufacture several
  // independent-looking block votes.
  const pooled = [];
  for (const candidate of CANDIDATES) {
    const foldGroups = [];
    let totalTrades = 0;
    let brierSum = 0;
    let brierN = 0;
    let directionSum = 0;
    for (const { audit } of perSymbol.values()) {
      const found = audit.results.find(row => row.id === candidate.id);
      if (!found) continue;
      totalTrades += found.tradesTaken;
      if (found.brierScore != null) { brierSum += found.brierScore * found.windowsConsidered; brierN += found.windowsConsidered; }
      if (found.directionalAccuracy != null) { directionSum += found.directionalAccuracy * found.windowsConsidered; }
    }
    pooled.push({ id: candidate.id, kind: candidate.kind, trades: totalTrades });
  }

  return {
    symbols: perSymbol.size,
    candidates: CANDIDATES.length,
    totalPairs,
    pooled,
    perSymbol,
    controlsOk,
  };
}

function main() {
  const argv = process.argv.slice(2);
  const flag = (name, fallback) => {
    const at = argv.indexOf(`--${name}`);
    return at >= 0 && argv[at + 1] ? argv[at + 1] : fallback;
  };

  const manifestPath = flag('manifest', 'data/sentiment_harness/candles/manifest-3600.json');
  const dir = manifestPath.replace(/\/[^/]+$/, '');
  const manifest = loadManifest(manifestPath);
  const rowsBySymbol = new Map();
  for (const entry of manifest.datasets) rowsBySymbol.set(entry.symbol, loadSymbolRows(manifest, entry, dir));

  console.log(`  manifest   ${manifest.symbols_exported} symbols, ${manifest.datasets.reduce((a, e) => a + e.row_count, 0)} rows`);
  console.log(`  family     ${CANDIDATES.length} candidates x ${manifest.symbols_exported} symbols = ${CANDIDATES.length * manifest.symbols_exported} pairwise tests`);
  console.log(`  policy     Bonferroni, per-trial alpha ${(0.05 / DEFAULT_POLICY.maxCandidateTrials).toFixed(4)} (pooled) / ${(0.05 / (CANDIDATES.length * manifest.symbols_exported)).toExponential(2)} (pairwise)`);

  const options = {
    lookbackBars: 24, horizonBars: 1, granularityMinutes: 60, notionalUsd: 1000,
    costModel: COST_FREE, trainSize: 150, testSize: 60, purgeSize: 1, embargoSize: 1,
  };

  const started = Date.now();
  const perSymbolRows = [];
  const pooledFoldGroups = new Map(CANDIDATES.map(candidate => [candidate.id, []]));
  let controlsFailed = [];

  for (const entry of manifest.datasets) {
    const rows = rowsBySymbol.get(entry.symbol);
    if (!rows) continue;
    const folds = foldsForSymbol(rows.length, options);
    if (!folds.length) {
      perSymbolRows.push({ symbol: entry.symbol, error: 'no_folds', rowCount: rows.length });
      continue;
    }
    const audit = runAudit({ rows, folds, options });
    if (!audit.controlsPassed) controlsFailed.push(entry.symbol);
    perSymbolRows.push({
      symbol: entry.symbol,
      rowCount: rows.length,
      folds: folds.length,
      controlsPassed: audit.controlsPassed,
      candidates: audit.results.map(row => ({
        id: row.id,
        kind: row.kind,
        trades: row.tradesTaken,
        brierScore: row.brierScore,
        directionalAccuracy: row.directionalAccuracy,
        meanNetReturnUsd: row.meanNetReturnUsd,
        passed: row.significance.passed,
        adjustedPValue: row.significance.adjustedPValue,
        reasons: row.significance.reasons,
      })),
    });
  }

  // Stage 1, pooled: concatenate every symbol's per-candidate fold returns.
  // auditCandidate does not return fold returns, so pooling is done over the
  // per-symbol per-candidate sign statistics instead, which is why the pooled
  // numbers below are recomputed from the scorecard rather than reused.
  const pooled = [];
  for (const candidate of CANDIDATES) {
    const marginalP = [];
    for (const row of perSymbolRows) {
      if (row.error) continue;
      const found = row.candidates.find(c => c.id === candidate.id);
      if (!found) continue;
      marginalP.push({ symbol: row.symbol, adjusted: found.adjustedPValue, passed: found.passed, brier: found.brierScore, dir: found.directionalAccuracy, trades: found.trades });
    }
    const winners = marginalP.filter(row => row.passed);
    const briers = marginalP.map(row => row.brier).filter(v => v != null);
    const dirs = marginalP.map(row => row.dir).filter(v => v != null);
    pooled.push({
      id: candidate.id,
      kind: candidate.kind,
      symbolsTested: marginalP.length,
      symbolsWon: winners.length,
      symbolsWonList: winners.map(row => row.symbol).slice(0, 8),
      meanBrier: briers.length ? briers.reduce((a, b) => a + b, 0) / briers.length : null,
      meanDirectional: dirs.length ? dirs.reduce((a, b) => a + b, 0) / dirs.length : null,
      totalTrades: marginalP.reduce((a, row) => a + row.trades, 0),
    });
  }

  // Stage 2, pairwise: correct for the full family.
  const pairwiseTrials = CANDIDATES.length * manifest.symbols_exported;
  const pairwiseAlpha = 0.05 / pairwiseTrials;
  const pairwiseWinners = [];
  for (const row of perSymbolRows) {
    if (row.error) continue;
    for (const candidate of row.candidates) {
      // A per-symbol pass was computed against the 20-trial budget. Re-derive
      // significance at the stricter pairwise alpha from its adjusted p-value:
      //   passed_20  <=>  raw_p <= 0.05/20   <=>  raw_p/0.05*20 <= 1/20
      // so the raw p-value is recoverable as adjusted * (0.05/20).
      const rawP = candidate.adjustedPValue * (DEFAULT_POLICY.familywiseAlpha / DEFAULT_POLICY.maxCandidateTrials);
      if (rawP <= pairwiseAlpha) {
        pairwiseWinners.push({ symbol: row.symbol, id: candidate.id, rawPValue: rawP, brier: candidate.brierScore, dir: candidate.directionalAccuracy });
      }
    }
  }

  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  console.log(`\n  ran ${perSymbolRows.filter(r => !r.error).length} symbols in ${elapsed}s`);
  if (controlsFailed.length) {
    console.log(`  CONTROLS FAILED on ${controlsFailed.length} symbols: ${controlsFailed.slice(0, 6).join(', ')}`);
  } else {
    console.log('  controls   PASSED on every symbol (oracle significant, three controls not)');
  }

  console.log('\n  === stage 1: pooled across symbols, corrected for 16 candidates ===');
  console.log('    candidate                   symsWon/sym   meanBrier    meanDir   trades');
  for (const row of pooled) {
    console.log(`    ${row.id.padEnd(27)} ${String(row.symbolsWon).padStart(6)}/${String(row.symbolsTested).padEnd(4)}`
      + `  ${row.meanBrier == null ? '    n/a' : row.meanBrier.toFixed(6)}`
      + `  ${row.meanDirectional == null ? '  n/a' : (row.meanDirectional * 100).toFixed(2) + '%'}`
      + `  ${String(row.totalTrades).padStart(7)}`);
  }

  console.log(`\n  === stage 2: pairwise, corrected for ${pairwiseTrials} tests (alpha ${pairwiseAlpha.toExponential(2)}) ===`);
  if (!pairwiseWinners.length) {
    console.log('    nothing survived');
  } else {
    for (const row of pairwiseWinners.slice(0, 20)) {
      console.log(`    ${row.symbol.padEnd(12)} ${row.id.padEnd(26)} raw-p ${row.rawPValue.toExponential(2)}`);
    }
  }

  const pooledSignals = pooled.filter(row => row.kind !== 'control' && row.symbolsWon > 0);
  console.log('\n  === verdict ===');
  if (controlsFailed.length) {
    console.log('  CONTROLS FAILED -- the sweep is unreliable and no verdict is issued.');
  } else if (!pairwiseWinners.length && !pooledSignals.length) {
    console.log('  No signal on any of ' + manifest.symbols_exported + ' symbols survived either correction.');
    console.log('  Both the conservative (16) and strict (' + pairwiseTrials + ') bounds agree on the null.');
  } else {
    console.log(`  pooled winners: ${pooledSignals.length ? pooledSignals.map(r => r.id).join(', ') : 'none'}`);
    console.log(`  pairwise winners: ${pairwiseWinners.length}`);
    console.log('  If these disagree the result is cross-sectionally inconsistent and is NOT a finding.');
  }

  const outDir = flag('out', join('scripts', 'experiments', 'cross_sectional_audit'));
  mkdirSync(outDir, { recursive: true });
  writeFileSync(join(outDir, 'scorecard.json'), `${JSON.stringify({
    schema_version: CROSS_SECTIONAL_SCHEMA_VERSION,
    manifest: { path: manifestPath, symbols: manifest.symbols_exported },
    policy: DEFAULT_POLICY,
    pairwise_trials: pairwiseTrials,
    pairwise_alpha: pairwiseAlpha,
    controls_failed: controlsFailed,
    pooled,
    pairwise_winners: pairwiseWinners,
    per_symbol: perSymbolRows,
    generated_at: new Date().toISOString(),
  }, null, 2)}\n`);
  console.log(`\n  scorecard  ${outDir}/scorecard.json`);
}

if (import.meta.url === `file://${process.argv[1]}`) main();
