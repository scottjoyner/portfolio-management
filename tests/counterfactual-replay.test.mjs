import test from 'node:test';
import assert from 'node:assert/strict';
import {
  REPLAY_OUTCOME,
  attributeAgentDecision,
  buildReplayEnvelope,
  normalizeReplayBar,
  replayBotOverEnvelope,
  runCounterfactualReplay
} from '../packages/backtesting/src/counterfactualReplay.mjs';

function risingBars(count = 40, start = 100, step = 1) {
  return Array.from({ length: count }, (_, index) => ({
    t: `2026-09-30T${String(Math.floor(index / 60) % 24).padStart(2, '0')}:${String(index % 60).padStart(2, '0')}:00Z`,
    open: start + index * step,
    high: start + index * step + 0.5,
    low: start + index * step - 0.5,
    close: start + index * step,
    volume: 1000 + index
  }));
}

function envelopeInput(overrides = {}) {
  return {
    opportunityId: 'opp-1',
    instrument: 'BTC-USD',
    bars: risingBars(),
    decisionAt: '2026-09-30T12:00:00.000Z',
    capitalUsd: 10000,
    feeBps: 5,
    slippageBps: 10,
    universe: ['BTC-USD', 'ETH-USD'],
    ...overrides
  };
}

test('replay envelope is built and hashed', () => {
  const built = buildReplayEnvelope(envelopeInput());
  assert.equal(built.ok, true);
  assert.equal(built.outcome, REPLAY_OUTCOME.RESOLVED);
  assert.match(built.envelopeHash, /^[0-9a-f]{64}$/);
  assert.equal(built.envelope.economics.feeBps, 5);
  assert.deepEqual(built.envelope.universe, ['BTC-USD', 'ETH-USD']);
});

test('THE SAME INPUTS ALWAYS PRODUCE THE SAME HASH', () => {
  const a = runCounterfactualReplay(envelopeInput());
  const b = runCounterfactualReplay(envelopeInput());
  assert.equal(a.envelopeHash, b.envelopeHash);
  assert.equal(a.counterfactualPnlUsd, b.counterfactualPnlUsd);
  assert.equal(a.botAction, b.botAction);
});

test('the hash is order-insensitive on the universe but not on the window', () => {
  const a = buildReplayEnvelope(envelopeInput({ universe: ['ETH-USD', 'BTC-USD'] }));
  const b = buildReplayEnvelope(envelopeInput({ universe: ['BTC-USD', 'ETH-USD'] }));
  assert.equal(a.envelopeHash, b.envelopeHash, 'universe ordering is normalized');
  const shorter = buildReplayEnvelope(envelopeInput({ bars: risingBars(30) }));
  assert.notEqual(a.envelopeHash, shorter.envelopeHash, 'a different window is a different experiment');
});

test('bar normalization makes float formatting irrelevant to the hash', () => {
  const a = normalizeReplayBar({ t: 'x', close: 100.123456789, high: 101, low: 99, volume: 5 });
  const b = normalizeReplayBar({ t: 'x', close: 100.1234567891, high: 101, low: 99, volume: 5 });
  assert.equal(a.close, b.close);
});

test('a changed fee or slippage assumption changes the hash', () => {
  // Otherwise the counterfactual could be re-run with friendlier costs and
  // still be presented as the same experiment.
  const base = buildReplayEnvelope(envelopeInput());
  const cheaper = buildReplayEnvelope(envelopeInput({ feeBps: 0 }));
  const slipperier = buildReplayEnvelope(envelopeInput({ slippageBps: 50 }));
  assert.notEqual(base.envelopeHash, cheaper.envelopeHash);
  assert.notEqual(base.envelopeHash, slipperier.envelopeHash);
});

test('a changed capital base or decision time changes the hash', () => {
  const base = buildReplayEnvelope(envelopeInput());
  assert.notEqual(base.envelopeHash, buildReplayEnvelope(envelopeInput({ capitalUsd: 20000 })).envelopeHash);
  assert.notEqual(base.envelopeHash, buildReplayEnvelope(envelopeInput({ decisionAt: '2026-09-30T13:00:00.000Z' })).envelopeHash);
});

test('an incomplete envelope is refused rather than approximated', () => {
  for (const [override, reason] of [
    [{ capitalUsd: 0 }, 'replay_envelope_capital_invalid'],
    [{ feeBps: null }, 'replay_envelope_fee_bps_required'],
    [{ slippageBps: undefined }, 'replay_envelope_slippage_bps_required'],
    [{ instrument: '' }, 'replay_envelope_instrument_required'],
    [{ decisionAt: null }, 'replay_envelope_decision_at_required'],
    [{ bars: [] }, 'replay_bars_insufficient']
  ]) {
    const built = buildReplayEnvelope(envelopeInput(override));
    assert.equal(built.ok, false, `expected refusal for ${reason}`);
    assert.equal(built.outcome, REPLAY_OUTCOME.PENDING);
    assert.ok(built.reasons.includes(reason), `expected ${reason} in ${built.reasons}`);
    assert.equal(built.counterfactualPnlUsd, null);
  }
});

/**
 * Build bars whose shape is controlled: `shape` returns the close for index i.
 * A crossover strategy only trades on a *fresh* cross, so the tests below
 * construct genuine crossings rather than assuming a trend implies a signal.
 */
function shapedBars(count, shape) {
  return Array.from({ length: count }, (_, index) => {
    const close = shape(index);
    return {
      t: `2026-09-30T${String(Math.floor(index / 60) % 24).padStart(2, '0')}:${String(index % 60).padStart(2, '0')}:00Z`,
      open: close,
      high: close + 0.5,
      low: close - 0.5,
      close,
      volume: 1000 + index
    };
  });
}

/** Falls then recovers, leaving the fast average above the slow one. */
function recoveryBars() {
  return shapedBars(60, index => (index < 40 ? 200 - index * 2 : 120 + (index - 40) * 12));
}

/** Rises then breaks down, leaving the fast average below the slow one. */
function breakdownBars() {
  return shapedBars(60, index => (index < 40 ? 100 + index * 3 : 220 - (index - 40) * 14));
}

test('an established uptrend makes the bot long and nets of costs', () => {
  const result = runCounterfactualReplay(envelopeInput({ bars: risingBars(60) }));
  assert.equal(result.outcome, REPLAY_OUTCOME.RESOLVED);
  assert.equal(result.botAction, 'buy');
  assert.ok(result.counterfactualPnlUsd > 0);
  const trade = result.trades[0];
  assert.ok(trade.feesUsd > 0);
  assert.ok(trade.slippageUsd > 0);
  assert.equal(trade.grossPnlUsd - trade.feesUsd - trade.slippageUsd, trade.netPnlUsd);
});

test('an established downtrend makes the bot short and nets of costs', () => {
  const result = runCounterfactualReplay(envelopeInput({ bars: shapedBars(60, i => 200 - i * 2) }));
  assert.equal(result.botAction, 'sell');
  assert.ok(result.counterfactualPnlUsd > 0, 'a short into a decline profits');
});

test('a recovery leaves the bot long and a breakdown leaves it short', () => {
  assert.equal(runCounterfactualReplay(envelopeInput({ bars: recoveryBars() })).botAction, 'buy');
  assert.equal(runCounterfactualReplay(envelopeInput({ bars: breakdownBars() })).botAction, 'sell');
});

test('a flat window holds and produces no P&L', () => {
  const result = runCounterfactualReplay(envelopeInput({ bars: shapedBars(60, () => 150) }));
  assert.equal(result.botAction, 'hold');
  assert.equal(result.counterfactualPnlUsd, 0);
  assert.deepEqual(result.trades, []);
});

test('a window too short to compute both averages stays pending', () => {
  const result = runCounterfactualReplay(envelopeInput({ bars: shapedBars(10, i => 100 + i) }));
  assert.equal(result.outcome, REPLAY_OUTCOME.PENDING);
  assert.equal(result.counterfactualPnlUsd, null);
  assert.ok(result.reasons.includes('replay_bars_insufficient_for_crossover'));
});

test('the counterfactual respects the position cap in the envelope', () => {
  const bars = risingBars(60);
  const uncapped = runCounterfactualReplay(envelopeInput({ bars, capitalUsd: 100000 }));
  const capped = runCounterfactualReplay(envelopeInput({ bars, capitalUsd: 100000, riskLimits: { maxPositionNotionalUsd: 1000 } }));
  assert.equal(uncapped.botAction, 'buy');
  assert.equal(capped.botAction, 'buy');
  assert.ok(capped.counterfactualPnlUsd < uncapped.counterfactualPnlUsd);
  assert.equal(capped.trades[0].notionalUsd, 1000);
});

test('a replay with no usable bars stays pending and yields no P&L', () => {
  const replay = replayBotOverEnvelope({ bars: [] });
  assert.equal(replay.outcome, REPLAY_OUTCOME.PENDING);
  assert.equal(replay.counterfactualPnlUsd, null);
});

test('cost-adjusted attribution nets the agent cost out of the incremental value', () => {
  const counterfactual = runCounterfactualReplay(envelopeInput());
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: 'hold',
    realizedPnlUsd: counterfactual.counterfactualPnlUsd + 500,
    agentCostUsd: 50,
    counterfactual
  });
  assert.equal(result.outcome, REPLAY_OUTCOME.RESOLVED);
  const record = result.record;
  assert.equal(record.incrementalValueUsd, 500);
  assert.equal(record.netValueUsd, 450);
  assert.equal(record.costJustified, true);
  assert.equal(record.netPositive, true);
  assert.equal(record.envelopeHash, counterfactual.envelopeHash);
});

test('a profitable override that did not cover its cost is not counted as a win', () => {
  const counterfactual = runCounterfactualReplay(envelopeInput());
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: 'hold',
    realizedPnlUsd: counterfactual.counterfactualPnlUsd + 5,
    agentCostUsd: 50,
    counterfactual
  });
  assert.equal(result.record.incrementalValueUsd, 5);
  assert.equal(result.record.netValueUsd, -45);
  assert.equal(result.record.costJustified, false);
  assert.equal(result.record.netPositive, false);
  assert.equal(result.record.harmfulOverride, true, 'changing the decision for a net loss is harmful');
});

test('a pending counterfactual produces no P&L at all', () => {
  // The failure mode this prevents: filling counterfactualPnlUsd with the
  // agent's own realized number, which would make every agent look perfect.
  const pending = runCounterfactualReplay(envelopeInput({ feeBps: null }));
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: 'buy',
    realizedPnlUsd: 1000,
    agentCostUsd: 25,
    counterfactual: pending
  });
  assert.equal(result.ok, false);
  assert.equal(result.outcome, REPLAY_OUTCOME.PENDING);
  assert.equal(result.record, null);
  assert.ok(result.reasons.includes('attribution_counterfactual_pending'));
});

test('a pending realized P&L also stays pending', () => {
  const counterfactual = runCounterfactualReplay(envelopeInput());
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: 'buy',
    realizedPnlUsd: null,
    agentCostUsd: 25,
    counterfactual
  });
  assert.equal(result.outcome, REPLAY_OUTCOME.PENDING);
  assert.equal(result.record, null);
});

test('provider cost is included alongside agent cost', () => {
  const counterfactual = runCounterfactualReplay(envelopeInput());
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: 'hold',
    realizedPnlUsd: counterfactual.counterfactualPnlUsd + 100,
    agentCostUsd: 10,
    providerCostUsd: 5,
    counterfactual
  });
  assert.equal(result.record.totalCostUsd, 15);
  assert.equal(result.record.netValueUsd, 85);
});

test('an unchanged decision is not scored as an override', () => {
  const counterfactual = runCounterfactualReplay(envelopeInput());
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: counterfactual.botAction,
    realizedPnlUsd: counterfactual.counterfactualPnlUsd,
    agentCostUsd: 5,
    counterfactual
  });
  assert.equal(result.record.changedDecision, false);
  assert.equal(result.record.harmfulOverride, false);
  assert.equal(result.record.profitableOverride, false);
});

test('every null or malformed attribution input stays pending', () => {
  // Number(null) is 0. Each of these would otherwise be scored as a real number
  // and produce a confident-looking attribution from missing evidence.
  const counterfactual = runCounterfactualReplay(envelopeInput({ bars: risingBars(60) }));
  assert.equal(counterfactual.outcome, REPLAY_OUTCOME.RESOLVED);
  const base = { opportunityId: 'opp-1', agentAction: 'hold', realizedPnlUsd: 1, agentCostUsd: 1, counterfactual };
  for (const patch of [
    { realizedPnlUsd: null },
    { realizedPnlUsd: undefined },
    { agentCostUsd: null },
    { opportunityId: null },
    { agentAction: null },
    { counterfactual: null },
    { counterfactual: { ...counterfactual, counterfactualPnlUsd: null } },
    { counterfactual: { ...counterfactual, counterfactualPnlUsd: 'not-a-number' } },
    { counterfactual: { ...counterfactual, outcome: REPLAY_OUTCOME.PENDING } }
  ]) {
    const result = attributeAgentDecision({ ...base, ...patch });
    assert.equal(result.outcome, REPLAY_OUTCOME.PENDING, `expected pending for ${JSON.stringify(patch)}`);
    assert.equal(result.record, null, 'a pending attribution must carry no record');
  }
});

test('a genuinely zero agent cost is still a valid attribution', () => {
  // A local model with no provider really can cost nothing, so zero must not be
  // conflated with "not recorded".
  const counterfactual = runCounterfactualReplay(envelopeInput({ bars: risingBars(60) }));
  const result = attributeAgentDecision({
    opportunityId: 'opp-1',
    agentAction: 'hold',
    realizedPnlUsd: counterfactual.counterfactualPnlUsd + 10,
    agentCostUsd: 0,
    counterfactual
  });
  assert.equal(result.outcome, REPLAY_OUTCOME.RESOLVED);
  assert.equal(result.record.totalCostUsd, 0);
  assert.equal(result.record.netValueUsd, 10);
});
