//! Persistent, mutable candle store living inside the Rust extension.
//!
//! Each tick the optimizer APPENDS only the new bars (or re-ingests the tail)
//! and reuses cached `Vec<f64>` OHLCV per product, eliminating the per-tick
//! Python→Rust candle re-extraction that dominated `batch_signals_from_candles_py`.

use std::collections::HashMap;
use std::sync::Mutex;
use pyo3::prelude::*;

use crate::strategies;

#[derive(Default)]
struct ProductCandles {
    pub closes: Vec<f64>,
    pub volumes: Vec<f64>,
    pub highs: Vec<f64>,
    pub lows: Vec<f64>,
    pub opens: Vec<f64>,
}

#[derive(Default)]
struct Store {
    products: HashMap<String, ProductCandles>,
    max_len: usize,
}

static STORE: Mutex<Option<Store>> = Mutex::new(None);

/// Lazily-initialize and lock the module-level singleton store.
fn store_get() -> std::sync::MutexGuard<'static, Option<Store>> {
    let mut guard = STORE.lock().unwrap();
    if guard.is_none() {
        *guard = Some(Store { products: HashMap::new(), max_len: 100 });
    }
    guard
}

/// Extract (open, high, low, close, volume) from a single raw candle object.
/// Handles BOTH the dict form `{open,high,low,close,volume}` AND the tuple/list
/// form `[ts, low, high, open, close, volume]` (idx 1=low,2=high,3=open,4=close,5=volume).
/// Returns Err on malformed candles so the caller can skip them.
fn extract_ohlcv(candle: &Bound<'_, PyAny>) -> PyResult<(f64, f64, f64, f64, f64)> {
    // Try dict form first (presence of "close" key => dict-like).
    if let Ok(close_item) = candle.get_item("close") {
        let c: f64 = close_item.extract()?;
        let get = |key: &str| -> f64 {
            candle
                .get_item(key)
                .and_then(|v| v.extract::<f64>())
                .unwrap_or(0.0)
        };
        Ok((get("open"), get("high"), get("low"), c, get("volume")))
    } else {
        // tuple/list form: [ts, low, high, open, close, volume]
        let low: f64 = candle.get_item(1)?.extract()?;
        let high: f64 = candle.get_item(2)?.extract()?;
        let open: f64 = candle.get_item(3)?.extract()?;
        let close: f64 = candle.get_item(4)?.extract()?;
        let volume: f64 = candle.get_item(5)?.extract()?;
        Ok((open, high, low, close, volume))
    }
}

/// Ingest raw candles for a product into the persistent buffer.
///
/// Extracts OHLCV under the GIL, appends to the product's cached buffers, trims
/// to `max_len` (keeping the most-recent `max_len` bars). Returns the new length.
#[pyfunction]
pub fn candle_store_ingest_py(product: String, candles: Vec<PyObject>) -> usize {
    let extracted: Vec<(f64, f64, f64, f64, f64)> = Python::with_gil(|py| {
        let mut out = Vec::with_capacity(candles.len());
        for candle in &candles {
            let bound = candle.bind(py);
            match extract_ohlcv(bound) {
                Ok(t) => out.push(t),
                Err(_) => continue, // skip malformed candle
            }
        }
        out
    });

    let mut guard = store_get();
    let store = guard.as_mut().unwrap();
    let max_len = store.max_len;
    let pc = store.products.entry(product).or_default();
    // Re-ingest semantics: the optimizer feeds the tail window each tick. Replace
    // the buffers with the freshly-extracted window (trimmed to max_len). This
    // keeps the cached Vecs stable in shape while reflecting the latest bars.
    let n = extracted.len();
    let start = n.saturating_sub(max_len);
    pc.opens.clear();
    pc.highs.clear();
    pc.lows.clear();
    pc.closes.clear();
    pc.volumes.clear();
    for &(o, h, l, c, v) in &extracted[start..] {
        pc.opens.push(o);
        pc.highs.push(h);
        pc.lows.push(l);
        pc.closes.push(c);
        pc.volumes.push(v);
    }
    pc.closes.len()
}

/// Clear all products from the persistent buffer.
#[pyfunction]
pub fn candle_store_clear_py() {
    let mut guard = store_get();
    if let Some(store) = guard.as_mut() {
        store.products.clear();
    }
}

/// Return `(closes, volumes, highs, lows)` for a product (empty vecs if absent).
#[pyfunction]
pub fn candle_store_get_py(product: String) -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
    let guard = store_get();
    let store = guard.as_ref().unwrap();
    match store.products.get(&product) {
        Some(pc) => (
            pc.closes.clone(),
            pc.volumes.clone(),
            pc.highs.clone(),
            pc.lows.clone(),
        ),
        None => (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
    }
}

/// Evaluate ALL strategies for each product using the cached OHLCV, in parallel
/// via rayon. Same output shape as `batch_signals_from_candles_py`.
#[pyfunction]
pub fn candle_store_eval_py(
    products: Vec<String>,
) -> HashMap<String, Vec<(String, String, f64, String)>> {
    use rayon::prelude::*;

    // Snapshot the cached OHLCV under the lock (cheap clones of Vec<f64>), then
    // release the lock before the rayon compute so workers never contend on it.
    let snapshot: Vec<(String, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)> = {
        let guard = store_get();
        let store = guard.as_ref().unwrap();
        products
            .iter()
            .map(|pid| match store.products.get(pid) {
                Some(pc) => (
                    pid.clone(),
                    pc.closes.clone(),
                    pc.volumes.clone(),
                    pc.highs.clone(),
                    pc.lows.clone(),
                ),
                None => (pid.clone(), Vec::new(), Vec::new(), Vec::new(), Vec::new()),
            })
            .collect()
    };

    snapshot
        .into_par_iter()
        .map(|(pid, closes, volumes, highs, lows)| {
            let results = strategies::evaluate_all(&closes, &volumes, &highs, &lows);
            let mapped: Vec<(String, String, f64, String)> = results
                .into_iter()
                .map(|(n, s)| (n, s.action, s.confidence, s.reason))
                .collect();
            (pid, mapped)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_ingest_get_eval_clear() {
        candle_store_clear_py();
        // Seed a product's buffers directly (bypassing PyObject extraction).
        {
            let mut guard = store_get();
            let store = guard.as_mut().unwrap();
            let pc = store.products.entry("BTC-USD".to_string()).or_default();
            for i in 0..120 {
                let c = 100.0 + i as f64 * 0.5;
                pc.opens.push(c);
                pc.highs.push(c + 2.0);
                pc.lows.push(c - 2.0);
                pc.closes.push(c);
                pc.volumes.push(1000.0 + (i % 7) as f64 * 50.0);
            }
        }
        let (cl, vo, hi, lo) = candle_store_get_py("BTC-USD".to_string());
        assert_eq!(cl.len(), 120);
        assert_eq!(vo.len(), 120);
        assert_eq!(hi.len(), 120);
        assert_eq!(lo.len(), 120);

        let out = candle_store_eval_py(vec!["BTC-USD".to_string()]);
        assert!(out.contains_key("BTC-USD"));
        let sigs = &out["BTC-USD"];
        assert!(!sigs.is_empty());
        for (_, _, conf, _) in sigs {
            assert!(*conf >= 0.0 && *conf <= 1.0);
        }

        // Absent product returns empty vecs.
        let (cl2, _, _, _) = candle_store_get_py("NOPE".to_string());
        assert!(cl2.is_empty());

        candle_store_clear_py();
        let (cl3, _, _, _) = candle_store_get_py("BTC-USD".to_string());
        assert!(cl3.is_empty());
    }
}
