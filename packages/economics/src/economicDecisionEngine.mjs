import * as legacy from './economicDecisionEngineLegacy.mjs';

export * from './economicDecisionEngineLegacy.mjs';

const DEFAULT_WEIGHTS = {
  naive: 0.15,
  momentum: 0.40,
  meanReversion: 0.25,
  microstructure: 0.20,
};

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function nonNegative(value, fallback = 0) {
  const number = finite(value, fallback);
  return Number.isFinite(number) && number >= 0 ? number : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function round(value, digits = 6) {
  if (!Number.isFinite(value)) return null;
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const average = mean(values);
  return Math.sqrt(values.reduce((sum, value) => sum + ((value - average) ** 2), 0) / (values.length - 1));
}

function normalCdf(value) {
  const sign = value < 0 ? -1 : 1;
  const x = Math.abs(value) / Math.sqrt(2);
  const t = 1 / (1 + 0.3275911 * x);
  const polynomial = (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
  const erf = sign * (1 - polynomial * Math.exp(-(x ** 2)));
  return 0.5 * (1 + erf);
}

function timestamp(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function observationsFrom(body = {}) {
  const source = Array.isArray(body.observations) ? body.observations : Array.isArray(body.prices) ? body.prices : [];
  return source.map((row, index) => {
    if (typeof row === 'number') return { price: row, timestamp: null, index };
    return {
      price: finite(row?.price ?? row?.close ?? row?.mid, null),
      timestamp: row?.timestamp || row?.time || row?.at || row?.start || null,
      index,
    };
  }).filter(row => row.price != null && row.price > 0);
}

function correctedForecastSignals(body, forecast) {
  const observations = observationsFrom(body);
  if (observations.length < 5) return null;
  const prices = observations.map(row => row.price);
  const returns = [];
  for (let index = 1; index < prices.length; index += 1) returns.push(Math.log(prices[index] / prices[index - 1]));
  const recentReturns = returns.slice(-Math.min(12, returns.length));
  const recentPrices = prices.slice(-Math.min(20, prices.length));
  const currentPrice = prices.at(-1);
  const momentumReturn = mean(recentReturns.slice(-Math.min(5, recentReturns.length)));
  const rollingMean = mean(recentPrices);
  const meanReversionIntervals = Math.max(1, recentPrices.length - 1);
  const meanReversionReturn = rollingMean > 0
    ? clamp(((rollingMean - currentPrice) / currentPrice) / meanReversionIntervals, -0.03, 0.03)
    : 0;
  const orderBookImbalance = clamp(finite(body.orderBookImbalance, 0), -1, 1);
  const spreadBps = nonNegative(body.spreadBps, 0);
  const microstructureReturn = orderBookImbalance * Math.max(0.0001, spreadBps / 10000) * 0.5;
  const weights = { ...DEFAULT_WEIGHTS, ...(body.weights || {}) };
  const weightTotal = Object.values(weights).reduce((sum, value) => sum + Math.max(0, finite(value, 0)), 0) || 1;
  const normalizedWeights = Object.fromEntries(Object.entries(weights).map(([key, value]) => [key, Math.max(0, finite(value, 0)) / weightTotal]));
  const intervalMinutes = finite(body.observationIntervalMinutes, null)
    || (() => {
      const first = timestamp(observations.at(-2)?.timestamp);
      const last = timestamp(observations.at(-1)?.timestamp);
      return first && last ? Math.max(1 / 60, (last.getTime() - first.getTime()) / 60000) : 1;
    })();
  const horizonMinutes = Math.max(1, nonNegative(body.horizonMinutes, forecast.horizonMinutes || 60));
  const horizonScale = Math.sqrt(horizonMinutes / Math.max(intervalMinutes, 1 / 60));
  const perIntervalExpected =
    (normalizedWeights.naive * 0)
    + (normalizedWeights.momentum * momentumReturn)
    + (normalizedWeights.meanReversion * meanReversionReturn)
    + (normalizedWeights.microstructure * microstructureReturn);
  const volatility = Math.max(standardDeviation(recentReturns), 0.000001);
  const horizonVolatility = volatility * horizonScale;
  const expectedLogReturn = clamp(perIntervalExpected * horizonScale, -3 * horizonVolatility, 3 * horizonVolatility);
  const z80 = 1.281551565545;
  const expectedPrice = currentPrice * Math.exp(expectedLogReturn);
  const p10 = currentPrice * Math.exp(expectedLogReturn - z80 * horizonVolatility);
  const p90 = currentPrice * Math.exp(expectedLogReturn + z80 * horizonVolatility);
  const probabilityUp = clamp(normalCdf(expectedLogReturn / horizonVolatility), 0.001, 0.999);
  const expectedReturnBps = (expectedPrice / currentPrice - 1) * 10000;
  const volatilityBps = horizonVolatility * 10000;
  const regime = volatilityBps >= 250
    ? 'extreme_volatility'
    : volatilityBps >= 120
      ? Math.abs(expectedReturnBps) >= 40 ? 'high_volatility_trend' : 'high_volatility_range'
      : Math.abs(expectedReturnBps) >= 25 ? 'moderate_trend' : 'low_volatility_range';
  return {
    currentPrice,
    expectedPrice,
    p10,
    p90,
    probabilityUp,
    expectedReturnBps,
    volatilityBps,
    regime,
    horizonScale,
    momentumReturn,
    meanReversionReturn,
    meanReversionIntervals,
    microstructureReturn,
    normalizedWeights,
  };
}

export function buildPriceForecast(state, body = {}, now = new Date().toISOString()) {
  const result = legacy.buildPriceForecast(state, body, now);
  const forecast = result?.priceForecast;
  if (!forecast) return result;
  const corrected = correctedForecastSignals(body, forecast);
  if (!corrected) return result;

  forecast.currentPrice = round(corrected.currentPrice, 8);
  forecast.expectedPrice = round(corrected.expectedPrice, 8);
  forecast.p10Price = round(corrected.p10, 8);
  forecast.p50Price = forecast.expectedPrice;
  forecast.p90Price = round(corrected.p90, 8);
  forecast.expectedReturnBps = round(corrected.expectedReturnBps, 4);
  forecast.probabilityUp = round(corrected.probabilityUp, 6);
  forecast.expectedVolatilityBps = round(corrected.volatilityBps, 4);
  forecast.regime = corrected.regime;
  forecast.components = {
    naiveReturn: 0,
    momentumReturn: round(corrected.momentumReturn * corrected.horizonScale, 8),
    meanReversionReturn: round(corrected.meanReversionReturn * corrected.horizonScale, 8),
    meanReversionLookbackIntervals: corrected.meanReversionIntervals,
    microstructureReturn: round(corrected.microstructureReturn * corrected.horizonScale, 8),
    weights: corrected.normalizedWeights,
  };
  forecast.calculationRevision = 'per_interval_mean_reversion_v2';
  return result;
}
