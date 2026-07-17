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
