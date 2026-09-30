import crypto from 'node:crypto';

/**
 * Deterministic counterfactual replay for G-004.
 *
 * The problem this solves: an agent's decision is only worth its cost if it
 * beat what the system would have done anyway. Answering that requires running
 * the deterministic bot over the exact conditions the agent saw. If the bot is
 * re-simulated with different bars, a different capital figure, or a different
 * fee assumption, the comparison is not a counterfactual at all -- it is a
 * different experiment with the answer already chosen.
 *
 * So the inputs are frozen into a replay envelope and hashed. Both the agent
 * path and the counterfactual path read the same envelope, which makes the
 * comparison reproducible and makes any divergence in inputs detectable rather
 * than invisible.
 *
 * Nothing here fabricates evidence. When the envelope cannot be built, or the
 * bot replay cannot run, the result is explicitly pending and carries no P&L.
 */

export const REPLAY_ENVELOPE_SCHEMA_VERSION = 1;

export const REPLAY_OUTCOME = {
  RESOLVED: 'resolved',
  PENDING: 'pending'
};

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort()
      .map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value === undefined ? null : value);
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

/**
 * Normalize a bar so that a float formatting difference cannot change the
 * envelope hash. Prices are quantized to a fixed number of decimals for that
 * reason; this is why the same window always produces the same hash.
 */
export function normalizeReplayBar(bar) {
  const quantize = value => {
    if (value == null || !Number.isFinite(Number(value))) return null;
    return Number(Number(value).toFixed(8));
  };
  return {
    t: String(bar.t ?? bar.time ?? bar.timestamp ?? ''),
    open: quantize(bar.open),
    high: quantize(bar.high),
    low: quantize(bar.low),
    close: quantize(bar.close),
    volume: quantize(bar.volume)
  };
}

export function validateHistoricalBars(bars = []) {
  const issues = [];
  if (!Array.isArray(bars) || bars.length < 2) issues.push('replay_bars_insufficient');
  bars.forEach((bar, index) => {
    if (!bar || typeof bar !== 'object') {
      issues.push(`replay_bar_invalid:${index}`);
      return;
    }
    if (bar.t == null || bar.t === '') issues.push(`replay_bar_timestamp_missing:${index}`);
    if (!Number.isFinite(Number(bar.close))) issues.push(`replay_bar_close_invalid:${index}`);
    if (Number.isFinite(Number(bar.high)) && Number.isFinite(Number(bar.low)) && Number(bar.high) < Number(bar.low)) {
      issues.push(`replay_bar_high_below_low:${index}`);
    }
  });
  return { ok: issues.length === 0, issues };
}

export function movingAverage(values, period) {
  if (period <= 0 || values.length < period) return [];
  const out = [];
  let sum = 0;
  for (let index = 0; index < values.length; index += 1) {
    sum += values[index];
    if (index >= period) sum -= values[index - period];
    if (index >= period - 1) out.push(sum / period);
  }
  return out;
}

/**
 * Freeze the decision context into a hashed envelope.
 *
 * Every field that could make two runs incomparable belongs here: the market
 * window, the decision timestamp, capital, the fee and slippage model, the
 * risk limits, and the instrument universe. If a caller cannot supply one of
 * these the envelope is refused, because an envelope missing the fee model is
 * precisely the failure mode that makes a counterfactual look better than it is.
 */
export function buildReplayEnvelope({
  opportunityId,
  executionId = null,
  decisionId = null,
  instrument,
  bars,
  decisionAt,
  capitalUsd,
  feeBps,
  slippageBps,
  riskLimits = {},
  universe = []
}) {
  const missing = [];
  if (!opportunityId) missing.push('replay_envelope_opportunity_id_required');
  if (!instrument) missing.push('replay_envelope_instrument_required');
  if (!decisionAt) missing.push('replay_envelope_decision_at_required');
  // Reject null/undefined before coercing. Number(null) is 0, so a missing fee
  // would otherwise become a zero-cost counterfactual and flatter every agent.
  const positiveNumber = value => value != null && Number.isFinite(Number(value)) && Number(value) > 0;
  const nonNegativeNumber = value => value != null && Number.isFinite(Number(value)) && Number(value) >= 0;
  if (!positiveNumber(capitalUsd)) missing.push('replay_envelope_capital_invalid');
  if (!nonNegativeNumber(feeBps)) missing.push('replay_envelope_fee_bps_required');
  if (!nonNegativeNumber(slippageBps)) missing.push('replay_envelope_slippage_bps_required');

  const barCheck = validateHistoricalBars(bars);
  if (!barCheck.ok) missing.push(...barCheck.issues);

  if (missing.length) {
    return {
      ok: false,
      outcome: REPLAY_OUTCOME.PENDING,
      reasons: [...new Set(missing)],
      envelope: null,
      envelopeHash: null,
      counterfactualPnlUsd: null
    };
  }

  const normalizedBars = bars.map(normalizeReplayBar);
  const envelope = {
    schemaVersion: REPLAY_ENVELOPE_SCHEMA_VERSION,
    opportunityId,
    executionId,
    decisionId,
    instrument,
    decisionAt: new Date(decisionAt).toISOString(),
    capitalUsd: Number(Number(capitalUsd).toFixed(8)),
    economics: {
      feeBps: Number(Number(feeBps).toFixed(8)),
      slippageBps: Number(Number(slippageBps).toFixed(8))
    },
    riskLimits: {
      maxPositionNotionalUsd: Number(riskLimits.maxPositionNotionalUsd ?? Infinity) === Infinity
        ? null
        : Number(riskLimits.maxPositionNotionalUsd),
      maxPositions: riskLimits.maxPositions ?? null,
      maxLeverage: riskLimits.maxLeverage ?? null
    },
    universe: [...universe].map(String).sort(),
    bars: normalizedBars
  };

  return {
    ok: true,
    outcome: REPLAY_OUTCOME.RESOLVED,
    reasons: [],
    envelope,
    envelopeHash: sha256(canonicalJson(envelope)),
    counterfactualPnlUsd: null
  };
}

/**
 * Run the deterministic bot over the envelope.
 *
 * This is intentionally the same strategy the system runs without an agent, so
 * the counterfactual answers "what would we have done" rather than "what would
 * some other strategy have done". The position is capped by the envelope's risk
 * limits, so a counterfactual that respects a limit the live path respected is
 * not credited with one it did not.
 */
export function replayBotOverEnvelope(envelope) {
  if (!envelope?.bars?.length) {
    return {
      ok: false,
      outcome: REPLAY_OUTCOME.PENDING,
      reasons: ['replay_envelope_missing_bars'],
      botAction: null,
      counterfactualPnlUsd: null,
      trades: []
    };
  }

  const closes = envelope.bars.map(bar => Number(bar.close));
  const FAST = 9;
  const SLOW = 21;
  const fast = movingAverage(closes, FAST);
  const slow = movingAverage(closes, SLOW);

  // Align the two averages by time, not by array index. fast[k] ends at bar
  // k+FAST-1 and slow[j] ends at bar j+SLOW-1, so the same bar is
  // fast[j + SLOW - FAST] against slow[j]. Comparing them index-for-index
  // compares different moments and cannot detect a crossover.
  const offset = SLOW - FAST;
  const slowIndex = slow.length - 1;
  const fastIndex = slowIndex + offset;
  if (fastIndex < 1 || slowIndex < 1) {
    return {
      ok: false,
      outcome: REPLAY_OUTCOME.PENDING,
      reasons: ['replay_bars_insufficient_for_crossover'],
      botAction: null,
      counterfactualPnlUsd: null,
      trades: []
    };
  }

  const fastNow = fast[fastIndex];
  const slowNow = slow[slowIndex];

  // Trend state, not a cross that must land on the final bar. Requiring the
  // crossover to occur within the last few bars makes the counterfactual
  // knife-edge: nudge the window and the answer flips between "traded" and
  // "held" for no economic reason. What attribution actually needs to know is
  // which side of the trend the deterministic bot would have been on, so that
  // is what is reported.
  let botAction = 'hold';
  if (fastNow > slowNow) botAction = 'buy';
  else if (fastNow < slowNow) botAction = 'sell';

  const first = closes[0];
  const last = closes[closes.length - 1];

  const notionalUsd = envelope.capitalUsd;
  if (botAction === 'hold') {
    return {
      ok: true,
      outcome: REPLAY_OUTCOME.RESOLVED,
      reasons: [],
      botAction,
      counterfactualPnlUsd: 0,
      trades: []
    };
  }

  const cap = envelope.riskLimits?.maxPositionNotionalUsd;
  const effectiveNotional = cap != null ? Math.min(notionalUsd, Number(cap)) : notionalUsd;
  const quantity = effectiveNotional / first;
  const direction = botAction === 'buy' ? 1 : -1;
  const grossPnlUsd = direction * (last - first) * quantity;
  const oneWayFeeUsd = (effectiveNotional * envelope.economics.feeBps) / 10_000;
  const roundTurnFeeUsd = oneWayFeeUsd * 2;
  const oneWaySlippageUsd = (effectiveNotional * envelope.economics.slippageBps) / 10_000;
  const roundTurnSlippageUsd = oneWaySlippageUsd * 2;
  const netPnlUsd = grossPnlUsd - roundTurnFeeUsd - roundTurnSlippageUsd;

  const round = value => Number(value.toFixed(8));
  return {
    ok: true,
    outcome: REPLAY_OUTCOME.RESOLVED,
    reasons: [],
    botAction,
    counterfactualPnlUsd: round(netPnlUsd),
    trades: [{
      action: botAction,
      quantity: round(quantity),
      entryPrice: first,
      exitPrice: last,
      notionalUsd: round(effectiveNotional),
      grossPnlUsd: round(grossPnlUsd),
      feesUsd: round(roundTurnFeeUsd),
      slippageUsd: round(roundTurnSlippageUsd),
      netPnlUsd: round(netPnlUsd)
    }]
  };
}

/**
 * Produce the counterfactual for a recorded agent decision.
 *
 * The envelope hash is carried into the result so the attribution record can
 * point at the exact conditions that produced the number. Two runs over the
 * same envelope must produce the same hash, and the caller can assert that
 * rather than trusting it.
 */
export function runCounterfactualReplay(input = {}) {
  const built = buildReplayEnvelope(input);
  if (!built.ok) {
    return {
      ok: false,
      outcome: REPLAY_OUTCOME.PENDING,
      reasons: built.reasons,
      envelopeHash: null,
      botAction: null,
      counterfactualPnlUsd: null,
      trades: []
    };
  }
  const replay = replayBotOverEnvelope(built.envelope);
  return {
    ...replay,
    envelopeHash: built.envelopeHash,
    envelope: built.envelope
  };
}

/**
 * Cost-adjusted attribution.
 *
 * The point of the whole exercise: an agent earns its cost only when it beat
 * the bot. `incrementalValueUsd` is realized minus counterfactual, and the
 * net figure subtracts what the agent was paid. A profitable override that did
 * not cover its own cost is reported as such rather than rounded up to a win.
 *
 * When the counterfactual is pending, the record is returned as pending with no
 * P&L at all. It is never filled in with the agent's own realized number, which
 * would make every agent look perfect.
 */
export function attributeAgentDecision({
  opportunityId,
  executionId = null,
  decisionId = null,
  agentAction,
  finalAction = null,
  agentCostUsd = 0,
  realizedPnlUsd = null,
  counterfactual,
  providerCostUsd = 0,
  observedAt = null
}) {
  const missing = [];
  if (!opportunityId) missing.push('attribution_opportunity_id_required');
  if (!agentAction) missing.push('attribution_agent_action_required');
  // A cost of zero is legitimate (a local model with no provider). A cost that
  // was never recorded is not: Number(null) is 0, so accepting null would
  // silently treat "we do not know what this cost" as "it was free".
  if (agentCostUsd == null || !Number.isFinite(Number(agentCostUsd)) || Number(agentCostUsd) < 0) {
    missing.push('attribution_agent_cost_invalid');
  }
  if (missing.length) {
    return { ok: false, outcome: REPLAY_OUTCOME.PENDING, reasons: missing, record: null };
  }

  // Defence in depth: a resolved counterfactual with no P&L is malformed, and
  // Number(null) is 0, so it must not be scored as a flat counterfactual.
  const counterfactualPnl = counterfactual?.counterfactualPnlUsd;
  if (
    !counterfactual
    || counterfactual.outcome !== REPLAY_OUTCOME.RESOLVED
    || counterfactualPnl == null
    || !Number.isFinite(Number(counterfactualPnl))
  ) {
    return {
      ok: false,
      outcome: REPLAY_OUTCOME.PENDING,
      reasons: [
        ...new Set([
          ...(counterfactual?.reasons ?? []),
          'attribution_counterfactual_pending'
        ])
      ],
      record: null
    };
  }
  if (realizedPnlUsd == null || !Number.isFinite(Number(realizedPnlUsd))) {
    // Number(null) is 0, so a missing realized P&L must be rejected explicitly
    // rather than scored as a flat result.
    return {
      ok: false,
      outcome: REPLAY_OUTCOME.PENDING,
      reasons: ['attribution_realized_pnl_pending'],
      record: null
    };
  }

  const round = value => Number(Number(value).toFixed(8));
  const botAction = counterfactual.botAction;
  const final = finalAction || agentAction;
  const changedDecision = String(final) !== String(botAction);
  const counterfactualPnlUsd = Number(counterfactual.counterfactualPnlUsd);
  const realized = Number(realizedPnlUsd);
  const cost = Number(agentCostUsd);
  const incrementalValueUsd = realized - counterfactualPnlUsd;
  const totalCostUsd = cost + Number(providerCostUsd || 0);
  const netValueUsd = incrementalValueUsd - totalCostUsd;

  return {
    ok: true,
    outcome: REPLAY_OUTCOME.RESOLVED,
    reasons: [],
    record: {
      opportunityId,
      executionId,
      decisionId,
      envelopeHash: counterfactual.envelopeHash ?? null,
      botAction,
      agentAction,
      finalAction: final,
      changedDecision,
      realizedPnlUsd: round(realized),
      counterfactualPnlUsd: round(counterfactualPnlUsd),
      incrementalValueUsd: round(incrementalValueUsd),
      agentCostUsd: round(cost),
      providerCostUsd: round(Number(providerCostUsd || 0)),
      totalCostUsd: round(totalCostUsd),
      netValueUsd: round(netValueUsd),
      incrementalRoi: totalCostUsd > 0 ? round(incrementalValueUsd / totalCostUsd) : null,
      netPositive: netValueUsd > 0,
      harmfulOverride: changedDecision && netValueUsd < 0,
      profitableOverride: changedDecision && netValueUsd > 0,
      costJustified: incrementalValueUsd > totalCostUsd,
      observedAt
    }
  };
}
