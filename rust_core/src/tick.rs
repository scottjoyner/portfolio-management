//! Per-tick strategy-signal + backtest-verdict gate.
//!
//! `tick_signals_py` ingests raw candles into the persistent candle store,
//! evaluates all strategies per product inside Rust, and filters the emitted
//! signals by membership in a pre-computed set of `(strategy/currency)` keys
//! that are allowed to pass the backtest-verdict gate. This moves the
//! products×strategies bt-cache loop out of Python for the hot path.

use std::collections::HashMap;
use std::collections::HashSet;

use pyo3::prelude::*;

use crate::candle_store::candle_store_eval_py;
use crate::candle_store::candle_store_ingest_py;

/// Wilder's smoothing of an already-computed per-step series.
fn wilder_smooth(values: &[f64], period: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    if values.len() < period {
        return values.iter().sum::<f64>() / values.len() as f64;
    }
    let mut result = values[..period].iter().sum::<f64>() / period as f64;
    for v in &values[period..] {
        result = result + (*v - result) / period as f64;
    }
    result
}

/// Port of `portfolio_optimizer._compute_adx` (Wilder smoothing, simplified
/// to smoothed-DX). Mirrors the Python helper exactly.
fn compute_adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> f64 {
    let n = highs.len();
    if n < period + 1 || lows.len() < period + 1 || closes.len() < period + 1 {
        return 20.0; // default to neutral
    }
    let mut plus_dm = Vec::with_capacity(n);
    let mut minus_dm = Vec::with_capacity(n);
    let mut tr_list = Vec::with_capacity(n);
    for i in 1..n {
        let high_diff = highs[i] - highs[i - 1];
        let low_diff = lows[i - 1] - lows[i];
        plus_dm.push(if high_diff > low_diff && high_diff > 0.0 {
            high_diff
        } else {
            0.0
        });
        minus_dm.push(if low_diff > high_diff && low_diff > 0.0 {
            low_diff
        } else {
            0.0
        });
        let tr = (highs[i] - lows[i])
            .max((highs[i] - closes[i - 1]).abs())
            .max((lows[i] - closes[i - 1]).abs());
        tr_list.push(tr);
    }
    let tr_smooth = wilder_smooth(&tr_list, period);
    let plus_di = if tr_smooth > 0.0 {
        100.0 * wilder_smooth(&plus_dm, period) / tr_smooth
    } else {
        0.0
    };
    let minus_di = if tr_smooth > 0.0 {
        100.0 * wilder_smooth(&minus_dm, period) / tr_smooth
    } else {
        0.0
    };
    let dx = if (plus_di + minus_di) > 0.0 {
        100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    } else {
        0.0
    };
    dx
}

/// Port of `portfolio_optimizer._detect_market_regime`.
fn detect_market_regime(highs: &[f64], lows: &[f64], closes: &[f64]) -> String {
    if closes.len() < 30 {
        return "neutral".to_string();
    }
    let adx = compute_adx(highs, lows, closes, 14);
    let recent: Vec<f64> = closes.iter().rev().take(20).rev().cloned().collect();
    let returns: Vec<f64> = recent
        .windows(2)
        .map(|w| (w[1] - w[0]) / w[0])
        .collect();
    let volatility = if returns.is_empty() {
        0.0
    } else {
        (returns.iter().map(|r| r * r).sum::<f64>() / returns.len() as f64).sqrt()
    };
    if adx > 25.0 {
        "trending".to_string()
    } else if adx < 20.0 {
        if volatility > 0.03 {
            "volatile".to_string()
        } else {
            "ranging".to_string()
        }
    } else {
        "neutral".to_string()
    }
}

/// Strategy-group constants (mirror Python TREND_STRATEGIES / MEAN_REVERSION /
/// VOLATILITY sets in portfolio_optimizer.py).
fn trend_strategies() -> HashSet<&'static str> {
    [
        "ema_cross", "macd", "adx", "trix", "psar", "hma", "aroon", "ichimoku",
        "dmi_cross", "supertrend", "vortex", "coppock", "kama", "fisher",
    ]
    .iter()
    .cloned()
    .collect()
}
fn mean_reversion_strategies() -> HashSet<&'static str> {
    [
        "rsi_revert", "boll_break", "zscore_revert", "vwap_revert", "williams_r",
        "cmo", "stoch", "rsi_fail", "mean_reversion", "gap_revert", "de_marker",
        "ultimate_osc",
    ]
    .iter()
    .cloned()
    .collect()
}
fn volatility_strategies() -> HashSet<&'static str> {
    [
        "keltner", "donchian", "bb_squeeze", "atr_channel", "std_channel",
        "vol_prof", "liq_vac", "vcp", "choppiness", "mass_idx", "range_exp_idx",
    ]
    .iter()
    .cloned()
    .collect()
}

/// Per-tick fast path: ingest candles, evaluate all strategies per product, and
/// emit only the `(pid, name, action, confidence)` tuples whose
/// `f"{name}/{currency}"` key is in `pass_cache_keys`.
///
/// `pass_cache_keys` should contain every `strategy/currency` combo that is
/// allowed to pass the backtest-verdict gate: currently cached PASS verdicts
/// AND not-yet-cached combos (the latter are backtested downstream in Phase 2
/// exactly as the legacy per-product path does). Only cached-FAIL combos are
/// excluded, which is behaviour-preserving relative to the legacy bt-cache loop.
///
/// `opens_map` is accepted for signature compatibility but currently unused on
/// the optimizer hot path (the legacy `_batch_signals_cached` call also passes
/// `None`); the persistent candle store ingests OHLCV from the raw candles.
#[pyfunction]
#[pyo3(signature = (products, currencies, candles_map, pass_cache_keys, opens_map=None))]
pub fn tick_signals_py(
    products: Vec<String>,
    currencies: Vec<String>,
    mut candles_map: HashMap<String, Vec<PyObject>>,
    pass_cache_keys: Vec<String>,
    #[allow(unused_variables)]
    opens_map: Option<HashMap<String, Vec<f64>>>,
) -> Vec<(String, String, String, f64)> {
    // Ingest each product's raw candles into the persistent buffer (moved out
    // of the map by value; PyObject is not Clone without the GIL, so we avoid
    // cloning). Mirrors batch_signals_cached().
    for pid in &products {
        if let Some(candles) = candles_map.remove(pid) {
            candle_store_ingest_py(pid.clone(), candles);
        }
    }

    // Evaluate all strategies for every product in one rayon-backed call.
    let eval = candle_store_eval_py(products.clone());

    let allowed: HashSet<String> = pass_cache_keys.into_iter().collect();

    products
        .into_iter()
        .enumerate()
        .filter_map(|(i, pid)| {
            let currency = currencies.get(i).cloned().unwrap_or_default();
            let sigs = eval.get(&pid)?;
            Some(
                sigs
                    .iter()
                    .filter_map(|(name, action, conf, _reason)| {
                        let key = format!("{}/{}", name, currency);
                        if allowed.contains(&key) {
                            Some((pid.clone(), name.clone(), action.clone(), *conf))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<(String, String, String, f64)>>(),
            )
        })
        .flatten()
        .collect()
}

/// Per-tick fast path that produces the FULL candidate shape
/// `(pid, currency, closes, volumes, highs, lows, [(name, action), ...])`
/// directly inside Rust, applying the same bt-cache gate as `tick_signals_py`
/// plus the market-regime strategy-group filter used by the legacy Python
/// fast path in `portfolio_optimizer._detect_strategy_signals`.
///
/// For each product we:
///   1. Ingest raw candles into the persistent store (for rayon eval).
///   2. Parse OHLCV from the raw candles under the GIL (dict or tuple form,
///      oldest-first, capped at 100) — also yielding opens if available.
///   3. Evaluate all strategies and keep only signals whose
///      `f"{name}/{currency}"` key is in `pass_cache_keys`, dropping HOLD.
///   4. Apply the 3-group regime filter (ranging→TREND, trending→
///      MEAN_REVERSION, quiet→VOLATILITY drop; other regimes drop nothing).
///   5. Emit the tuple only if at least one signal survives.
#[pyfunction]
#[pyo3(signature = (products, currencies, candles_map, pass_cache_keys, opens_map=None))]
pub fn tick_candidates_py(
    products: Vec<String>,
    currencies: Vec<String>,
    mut candles_map: HashMap<String, Vec<PyObject>>,
    pass_cache_keys: Vec<String>,
    opens_map: Option<HashMap<String, Vec<f64>>>,
) -> Vec<(
    String,
    String,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<f64>,
    Vec<(String, String)>,
)> {
    // Parse OHLCV from the raw candles FIRST (under the GIL), because the
    // ingest step below consumes the map by value. We keep a clone of each
    // product's candles for ingestion afterwards.
    let pid_ohlcv: Vec<(String, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>, Option<Vec<f64>>)> =
        Python::with_gil(|py| {
            products
                .iter()
                .map(|pid| {
                    let candles = candles_map.get(pid);
                    let mut closes: Vec<f64> = Vec::new();
                    let mut volumes: Vec<f64> = Vec::new();
                    let mut highs: Vec<f64> = Vec::new();
                    let mut lows: Vec<f64> = Vec::new();
                    let mut opens: Vec<f64> = Vec::new();
                    if let Some(candles) = candles {
                        for candle in candles {
                            let bound = candle.bind(py);
                            if bound.get_item("close").is_ok() {
                                let get = |key: &str| -> f64 {
                                    bound
                                        .get_item(key)
                                        .and_then(|v| v.extract::<f64>())
                                        .unwrap_or(0.0)
                                };
                                opens.push(get("open"));
                                highs.push(get("high"));
                                lows.push(get("low"));
                                closes.push(get("close"));
                                volumes.push(get("volume"));
                            } else {
                                let get_idx = |idx: usize| -> f64 {
                                    bound
                                        .get_item(idx)
                                        .and_then(|v| v.extract::<f64>())
                                        .unwrap_or(0.0)
                                };
                                opens.push(get_idx(3));
                                highs.push(get_idx(2));
                                lows.push(get_idx(1));
                                closes.push(get_idx(4));
                                volumes.push(get_idx(5));
                            }
                        }
                    }
                    // Cap to the most-recent 100 bars (oldest-first retained).
                    let cap = |v: &mut Vec<f64>| {
                        if v.len() > 100 {
                            let drop = v.len() - 100;
                            v.drain(0..drop);
                        }
                    };
                    cap(&mut closes);
                    cap(&mut volumes);
                    cap(&mut highs);
                    cap(&mut lows);
                    cap(&mut opens);
                    // Reverse to oldest-first to match the optimizer's parsed order.
                    closes.reverse();
                    volumes.reverse();
                    highs.reverse();
                    lows.reverse();
                    opens.reverse();
                    (pid.clone(), closes, volumes, highs, lows, Some(opens))
                })
                .collect()
        });

    // Ingest raw candles into the persistent buffer (cloned out of the map,
    // mirroring tick_signals_py / batch_signals_cached).
    for pid in &products {
        if let Some(candles) = candles_map.remove(pid) {
            candle_store_ingest_py(pid.clone(), candles);
        }
    }

    // Evaluate all strategies for every product in one rayon-backed call.
    let eval = candle_store_eval_py(products.clone());

    let allowed: HashSet<String> = pass_cache_keys.into_iter().collect();
    let opens_by_pid = opens_map.unwrap_or_default();

    let trend = trend_strategies();
    let mean_rev = mean_reversion_strategies();
    let vol = volatility_strategies();

    products
        .into_iter()
        .enumerate()
        .filter_map(|(i, pid)| {
            let currency = currencies.get(i).cloned().unwrap_or_default();
            let sigs = eval.get(&pid)?;
            let (closes, volumes, highs, lows, _opens) = {
                let o = pid_ohlcv.iter().find(|(p, ..)| p == &pid)?;
                (o.1.clone(), o.2.clone(), o.3.clone(), o.4.clone(), o.5.clone())
            };

            // Apply bt-cache gate + drop HOLD, then group filter.
            let mut kept: Vec<(String, String)> = Vec::new();
            for (name, action, _conf, _reason) in sigs {
                let key = format!("{}/{}", name, currency);
                if !allowed.contains(&key) {
                    continue;
                }
                if action == "HOLD" {
                    continue;
                }
                kept.push((name.clone(), action.clone()));
            }

            // Regime filter (only if we have highs+lows and >=30 closes).
            if !kept.is_empty() && !highs.is_empty() && !lows.is_empty() && closes.len() >= 30 {
                let regime = detect_market_regime(&highs, &lows, &closes);
                let _ = &opens_by_pid;
                kept.retain(|(name, _action)| {
                    if regime == "ranging" && trend.contains(name.as_str()) {
                        return false;
                    }
                    if regime == "trending" && mean_rev.contains(name.as_str()) {
                        return false;
                    }
                    if regime == "quiet" && vol.contains(name.as_str()) {
                        return false;
                    }
                    true
                });
            }

            if kept.is_empty() {
                None
            } else {
                Some((pid, currency, closes, volumes, highs, lows, kept))
            }
        })
        .collect()
}
