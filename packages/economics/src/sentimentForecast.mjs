// Sentiment-augmented price forecasts, scored in shadow alongside the baseline.
//
// WHAT THIS IS FOR. docs/TRADING_SYSTEM_VALUE_REVIEW.md:118 calls the fixed
// ensemble weights "a forecasting hypothesis rather than established edge", and
// :129 says forecast weights and probability calibration should only be tuned once
// enough shadow outcomes exist. Zero forecast outcomes exist. So this module does
// not retune anything -- it produces a SECOND forecast, tagged with its own
// modelVersion, so the existing recordForecastOutcome/summarizeForecastCalibration
// machinery can score the two against each other on identical inputs.
//
// THREE RULES THE BLEND MUST NOT BREAK.
//
//  1. Sentiment cannot dominate. The tilt is a fraction of the baseline's own
//     horizon sigma, not an absolute return. A confident, maximally negative read
//     moves the expected return by at most that fraction of one sigma, so a model
//     hallucinating conviction cannot manufacture a tradeable edge out of noise.
//
//  2. Confidence attenuates, and a confident zero is a no-op. A score near zero is
//     the model saying "no directional view", which must leave the baseline
//     exactly as it was. Multiplying by confidence gets this for free; adding a
//     constant offset would not.
//
//  3. Nothing here grants execution. This module returns forecasts. Authorisation
//     lives in evaluateEconomicDecision, keyed on the forecast's blockers and on
//     executionAllowed. A sentiment-augmented forecast is scored like any other,
//     and a model with a perfect track record still cannot flip a blocked decision.

const SENTIMENT_AUGMENTED_VERSION = 'sentiment-augmented-price-ensemble-v1';

const DEFAULT_MAX_TILT_SIGMA = 0.35;
const MAX_PROBABILITY_SPAN = 0.2; // never move probabilityUp more than this in total

function finite(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value, low, high) {
  return Math.min(high, Math.max(low, value));
}

function round(value, digits = 8) {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

// Abramowitz & Stegun 7.1.26 via erf. The engine keeps its own copy private in
// economicDecisionEngineLegacy.mjs; duplicating five lines of well-known maths is
// preferable to exporting an internal helper purely for this.
//
// The zero-tilt no-op test is what keeps this honest: if this CDF disagrees with
// the engine's by even a little, a forecast with no sentiment at all stops
// reproducing its own baseline and every comparison the harness reports is noise.
function erf(x) {
  const sign = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const t = 1 / (1 + 0.3275911 * ax);
  const poly = t * (0.254829592
    + t * (-0.284496736
      + t * (1.421413741
        + t * (-1.453152027 + t * 1.061405429))));
  return sign * (1 - poly * Math.exp(-ax * ax));
}

function normalCdf(x) {
  return 0.5 * (1 + erf(x / Math.sqrt(2)));
}

function classifyRegime(volatilityBps, expectedReturnBps) {
  if (volatilityBps >= 250) return 'extreme_volatility';
  if (volatilityBps >= 120) return Math.abs(expectedReturnBps) >= 40 ? 'high_volatility_trend' : 'high_volatility_range';
  return Math.abs(expectedReturnBps) >= 25 ? 'moderate_trend' : 'low_volatility_range';
}

/**
 * Blend a normalized sentiment reading into a baseline forecast.
 *
 * Returns a NEW forecast record and never mutates the baseline, because the
 * baseline is the comparison arm: overwriting it would leave the harness scoring
 * the augmented model against itself and reporting a large, entirely fictional
 * improvement.
 *
 * The baseline record is not copied wholesale. A spread forecast that only
 * redefined probabilityUp would keep the baseline's p10/p90, and the coverage
 * metric would then describe the wrong distribution while looking perfectly
 * healthy. The interval moves with the mean.
 */
export function blendSentimentForecast(baseline, sentiment, options = {}) {
  if (!baseline || baseline.status !== 'valid') return { errors: ['baseline_forecast_required'] };
  if (!sentiment || typeof sentiment !== 'object') return { errors: ['sentiment_reading_required'] };

  const score = finite(sentiment.score);
  const confidence = finite(sentiment.confidence);
  if (score == null || score < -1 || score > 1) return { errors: ['sentiment_score_out_of_range'] };
  if (confidence == null || confidence < 0 || confidence > 1) return { errors: ['sentiment_confidence_out_of_range'] };

  const currentPrice = finite(baseline.currentPrice);
  const expectedPrice = finite(baseline.expectedPrice);
  if (currentPrice == null || currentPrice <= 0 || expectedPrice == null) return { errors: ['baseline_forecast_prices_invalid'] };

  const horizonVolatility = finite(baseline.expectedVolatilityBps, 0) / 10_000;
  if (horizonVolatility <= 0) return { errors: ['baseline_forecast_volatility_invalid'] };

  const maxTiltSigma = clamp(finite(options.maxTiltSigma, DEFAULT_MAX_TILT_SIGMA), 0, 3);
  // Rule 1: bounded by the baseline's own sigma.
  // Rule 2: scaled by the model's own confidence, so a confident zero is a no-op.
  const tiltLogReturn = score * confidence * maxTiltSigma * horizonVolatility;

  const baselineLogReturn = Math.log(expectedPrice / currentPrice);
  const blendedLogReturn = clamp(
    baselineLogReturn + tiltLogReturn,
    baselineLogReturn - MAX_PROBABILITY_SPAN,
    baselineLogReturn + MAX_PROBABILITY_SPAN,
  );

  // A zero tilt must reproduce the baseline exactly, not merely closely. The
  // engine rounds probabilityUp to 6dp and expectedPrice to 8dp, so recomputing
  // the normal CDF from those rounded values drifts by ~1e-6 -- which is nothing
  // next to a real effect but is exactly the size of the differences the harness
  // is trying to measure. Taking the baseline's own numbers keeps the comparison
  // arm exact.
  const isNoOp = tiltLogReturn === 0;
  const z80 = 1.281551565545;
  const blendedExpectedPrice = isNoOp ? expectedPrice : currentPrice * Math.exp(blendedLogReturn);
  const p10 = isNoOp ? finite(baseline.p10Price) : currentPrice * Math.exp(blendedLogReturn - z80 * horizonVolatility);
  const p90 = isNoOp ? finite(baseline.p90Price) : currentPrice * Math.exp(blendedLogReturn + z80 * horizonVolatility);
  const probabilityUp = isNoOp ? finite(baseline.probabilityUp) : clamp(normalCdf(blendedLogReturn / horizonVolatility), 0.001, 0.999);
  const expectedReturnBps = isNoOp ? finite(baseline.expectedReturnBps) : (blendedExpectedPrice / currentPrice - 1) * 10_000;

  const forecast = {
    ...baseline,
    id: `${baseline.id}:sentiment`,
    expectedPrice: round(blendedExpectedPrice, 8),
    p10Price: round(p10, 8),
    p50Price: round(blendedExpectedPrice, 8),
    p90Price: round(p90, 8),
    expectedReturnBps: round(expectedReturnBps, 4),
    probabilityUp: round(probabilityUp, 6),
    regime: isNoOp ? baseline.regime : classifyRegime(finite(baseline.expectedVolatilityBps, 0), round(expectedReturnBps, 4)),
    modelVersion: options.modelVersion || SENTIMENT_AUGMENTED_VERSION,
    baselineForecastId: baseline.id,
    baselineModelVersion: baseline.modelVersion,
    baselineProbabilityUp: finite(baseline.probabilityUp),
    sentimentContributionBps: round(tiltLogReturn * 10_000, 6),
    sentiment: {
      score,
      confidence,
      horizonMinutes: finite(sentiment.horizonMinutes),
      maxTiltSigma,
      model: sentiment.model || null,
      rationale: sentiment.rationale || null,
      drivers: sentiment.drivers || [],
    },
    outcomeRecordedAt: null,
  };
  return { priceForecast: forecast, baselineForecast: baseline };
}

/**
 * Record the augmented arm on the state so the standard outcome machinery can
 * score it.
 *
 * The baseline must already have been recorded by the caller via the standard
 * buildPriceForecast, so both arms travel the identical code path -- building the
 * baseline here instead would make the two arms differ by more than sentiment and
 * quietly invalidate every comparison the harness reports.
 */
export function recordSentimentAugmentedForecast(state, baseline, sentiment, options = {}) {
  if (!state || !Array.isArray(state.priceForecasts)) return { errors: ['economic_state_required'] };
  const blended = blendSentimentForecast(baseline, sentiment, options);
  if (blended.errors) return blended;
  state.priceForecasts.push(blended.priceForecast);
  return blended;
}

// Exported for the CDF calibration test. The zero-tilt fast path deliberately
// skips normalCdf entirely, so the invariant that a neutral reading reproduces the
// baseline exactly cannot detect a broken CDF -- only a direct check against known
// standard-normal quantiles can.
export { SENTIMENT_AUGMENTED_VERSION, DEFAULT_MAX_TILT_SIGMA, normalCdf };