pub mod confidence;
pub mod indicators;
pub mod strategies;
pub mod backtest;
pub mod streaming;
pub mod regime;
pub mod tcost;
pub mod fee;

use pyo3::prelude::*;
use std::collections::HashMap;

/// Python wrapper: compute EMA on a list of floats.
#[pyfunction]
fn ema_py(values: Vec<f64>, period: usize) -> f64 {
    indicators::ema(&values, period)
}

/// Python wrapper: compute EMA slice (full series).
#[pyfunction]
fn ema_slice_py(values: Vec<f64>, period: usize) -> Vec<f64> {
    indicators::ema_slice(&values, period)
}

/// Python wrapper: compute RSI.
#[pyfunction]
fn rsi_py(values: Vec<f64>, period: usize) -> f64 {
    indicators::rsi(&values, period)
}

/// Python wrapper: compute Bollinger Bands. Returns (lower, middle, upper, bandwidth).
#[pyfunction]
fn bollinger_py(values: Vec<f64>, period: usize, std_mult: f64) -> (f64, f64, f64, f64) {
    indicators::bollinger(&values, period, std_mult)
}

/// Python wrapper: compute Z-Score. Returns (zscore, mean, std).
#[pyfunction]
fn zscore_py(values: Vec<f64>, period: usize) -> (f64, f64, f64) {
    indicators::zscore(&values, period)
}

/// Python wrapper: compute SMA.
#[pyfunction]
fn sma_py(values: Vec<f64>, period: usize) -> f64 {
    indicators::sma(&values, period)
}

/// Python wrapper: compute WMA.
#[pyfunction]
fn wma_py(values: Vec<f64>) -> f64 {
    indicators::wma(&values)
}

/// Python wrapper: compute TRIX series. Returns Vec<f64>.
#[pyfunction]
fn trix_series_py(values: Vec<f64>, period: usize) -> Vec<f64> {
    indicators::trix_series(&values, period)
}

/// Python wrapper: compute MACD. Returns (macd_line, signal_line, histogram).
#[pyfunction]
fn macd_py(closes: Vec<f64>, fast: usize, slow: usize, signal: usize) -> (f64, f64, f64) {
    indicators::macd(&closes, fast, slow, signal)
}

// ── Regime module PyO3 bindings ───────────────────────────────────

/// Python wrapper: detect regime from OHLCV data.
/// Returns (regime_str, adx, trend_strength, volatility, price_position).
#[pyfunction]
#[pyo3(signature = (closes, highs=None, lows=None, volumes=None, adx_period=None, lookback=None))]
fn detect_regime_py(
    closes: Vec<f64>,
    highs: Option<Vec<f64>>,
    lows: Option<Vec<f64>>,
    volumes: Option<Vec<f64>>,
    adx_period: Option<usize>,
    lookback: Option<usize>,
) -> (String, f64, f64, f64, f64) {
    let detector = regime::RegimeDetector::new(
        adx_period.unwrap_or(14),
        20,
        lookback.unwrap_or(50),
    );
    let (regime, features) = detector.detect(
        &closes,
        highs.as_deref(),
        lows.as_deref(),
        volumes.as_deref(),
    );
    (
        regime.as_str().to_string(),
        features.adx,
        features.trend_strength,
        features.volatility,
        features.price_position,
    )
}

/// Python wrapper: get recommended strategies for a regime.
#[pyfunction]
fn regime_recommended_strategies_py(regime_str: String) -> Vec<String> {
    let regime = match regime_str.as_str() {
        "strong_uptrend" => regime::Regime::StrongUptrend,
        "weak_uptrend" => regime::Regime::WeakUptrend,
        "ranging" => regime::Regime::Ranging,
        "weak_downtrend" => regime::Regime::WeakDowntrend,
        "strong_downtrend" => regime::Regime::StrongDowntrend,
        "high_volatility" => regime::Regime::HighVolatility,
        "low_volatility" => regime::Regime::LowVolatility,
        _ => regime::Regime::Unknown,
    };
    regime.recommended_strategies().iter().map(|s| s.to_string()).collect()
}

/// Python wrapper: compute regime features (full details).
/// Returns a dict-like tuple of all features.
#[pyfunction]
#[pyo3(signature = (closes, highs=None, lows=None, volumes=None))]
fn regime_features_py(
    closes: Vec<f64>,
    highs: Option<Vec<f64>>,
    lows: Option<Vec<f64>>,
    volumes: Option<Vec<f64>>,
) -> Vec<f64> {
    let detector = regime::RegimeDetector::new(14, 20, 50);
    let (_, features) = detector.detect(
        &closes,
        highs.as_deref(),
        lows.as_deref(),
        volumes.as_deref(),
    );
    vec![
        features.adx,
        features.trend_strength,
        features.volatility,
        features.volume_trend,
        features.price_position,
        features.hurst_exponent,
        features.serial_correlation,
        features.skewness,
        features.kurtosis,
    ]
}

/// Python wrapper: compute ATR (Average True Range).
#[pyfunction]
fn atr_py(highs: Vec<f64>, lows: Vec<f64>, closes: Vec<f64>, period: usize) -> f64 {
    indicators::atr(&highs, &lows, &closes, period)
}

// ── Transaction cost (tcost) PyO3 bindings ────────────────────────

#[pyfunction]
fn tcost_estimate_spread_bps_py(bid: f64, ask: f64) -> f64 {
    tcost::estimate_spread_bps(bid, ask)
}

#[pyfunction]
fn tcost_impact_bps_py(notional_usd: f64, impact_coeff: f64) -> f64 {
    tcost::impact_bps(notional_usd, impact_coeff)
}

#[pyfunction]
#[pyo3(signature = (side, mid, bid, ask, notional_usd, taker_fee_bps=8.0, slippage_bps=0.0, impact_coeff=1.5))]
fn tcost_effective_fill_price_py(
    side: String,
    mid: f64,
    bid: f64,
    ask: f64,
    notional_usd: f64,
    taker_fee_bps: f64,
    slippage_bps: f64,
    impact_coeff: f64,
) -> f64 {
    tcost::effective_fill_price(&side, mid, bid, ask, notional_usd, taker_fee_bps, slippage_bps, impact_coeff)
}

// ── Fee optimizer PyO3 bindings ───────────────────────────────────

#[pyclass]
struct PyFeeTracker {
    inner: fee::FeeTracker,
}

#[pymethods]
impl PyFeeTracker {
    #[new]
    #[pyo3(signature = (initial_volume_30d=0.0))]
    fn new(initial_volume_30d: f64) -> Self {
        Self { inner: fee::FeeTracker::new(initial_volume_30d) }
    }

    fn rolling_30d_volume(&self) -> f64 {
        self.inner.rolling_30d_volume()
    }

    fn current_tier_min_volume(&self) -> f64 {
        self.inner.current_tier().min_volume
    }

    fn current_tier_maker_rate(&self) -> f64 {
        self.inner.current_tier().maker_rate
    }

    fn current_tier_taker_rate(&self) -> f64 {
        self.inner.current_tier().taker_rate
    }

    fn next_tier_min_volume(&self) -> Option<f64> {
        self.inner.next_tier().map(|t| t.min_volume)
    }

    fn volume_to_next_tier(&self) -> f64 {
        self.inner.volume_to_next_tier()
    }

    #[pyo3(signature = (volume_usd, timestamp=None))]
    fn record_trade(&mut self, volume_usd: f64, timestamp: Option<f64>) {
        self.inner.record_trade(volume_usd, timestamp);
    }

    fn fee_cost(&self, trade_volume: f64, is_maker: bool) -> f64 {
        self.inner.fee_cost(trade_volume, is_maker)
    }

    fn maker_rate(&self) -> f64 {
        self.inner.maker_rate()
    }

    fn taker_rate(&self) -> f64 {
        self.inner.taker_rate()
    }

    fn savings_to_next_tier(&self, projected_monthly_volume: f64) -> f64 {
        self.inner.savings_to_next_tier(projected_monthly_volume)
    }

    fn to_state(&self) -> (f64, Vec<(f64, f64)>) {
        self.inner.to_state()
    }

    #[staticmethod]
    fn from_state(initial_volume: f64, trades: Vec<(f64, f64)>) -> Self {
        Self { inner: fee::FeeTracker::from_state((initial_volume, trades)) }
    }
}

/// ── Streaming module PyO3 bindings ────────────────────────────────

/// Python wrapper: RingBuffer (incremental, fixed-size circular buffer)
#[pyclass]
#[derive(Clone)]
#[allow(clippy::derivable_impls)]
struct PyRingBuffer {
    inner: streaming::RingBuffer,
}

#[pymethods]
impl PyRingBuffer {
    #[new]
    fn new(maxlen: usize) -> Self {
        Self { inner: streaming::RingBuffer::new(maxlen) }
    }

    fn append(&mut self, value: f64) {
        self.inner.append(value);
    }

    fn __getitem__(&self, index: isize) -> PyResult<f64> {
        let adjusted = if index < 0 {
            let sz = self.inner.len() as isize;
            if sz == 0 { return Err(pyo3::exceptions::PyIndexError::new_err("empty buffer")); }
            (sz + index) as isize
        } else {
            index
        };
        self.inner.get(adjusted as usize)
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err("index out of range"))
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn to_list(&self) -> Vec<f64> {
        self.inner.to_vec()
    }

    fn last(&self) -> Option<f64> {
        self.inner.last()
    }

    #[getter]
    fn size(&self) -> usize {
        self.inner.len()
    }
}

/// Python wrapper: StreamingIndicators (incremental indicators for one product)
#[pyclass]
struct PyStreamingIndicators {
    inner: streaming::StreamingIndicators,
}

#[pymethods]
impl PyStreamingIndicators {
    #[new]
    fn new(product_id: String, maxlen: usize) -> Self {
        Self { inner: streaming::StreamingIndicators::new(&product_id, maxlen) }
    }

    #[getter]
    fn product_id(&self) -> String {
        self.inner.product_id.clone()
    }

    #[getter]
    fn closes(&self) -> PyRingBuffer {
        PyRingBuffer { inner: self.inner.closes.clone() }
    }

    #[getter]
    fn volumes(&self) -> PyRingBuffer {
        PyRingBuffer { inner: self.inner.volumes.clone() }
    }

    fn update(&mut self, close: f64, volume: f64) {
        self.inner.update(close, volume);
    }

    fn seed_ema(&mut self, period: usize, value: f64) {
        self.inner.seed_ema(period, value);
    }

    fn ema(&self, period: usize) -> Option<f64> {
        self.inner.ema(period)
    }

    fn seed_sma(&mut self, period: usize, sma: f64, sq_sum: f64) {
        self.inner.seed_sma(period, sma, sq_sum);
    }

    fn sma(&self, period: usize) -> Option<f64> {
        self.inner.sma(period)
    }

    fn bollinger(&self, period: usize) -> Option<(f64, f64, f64)> {
        self.inner.bollinger(period)
    }

    fn seed_rsi(&mut self, closes: Vec<f64>, period: usize) -> f64 {
        self.inner.seed_rsi(&closes, period)
    }

    fn rsi(&self) -> f64 {
        self.inner.rsi()
    }

    fn seed_macd(&mut self, ema_fast: f64, ema_slow: f64) {
        self.inner.seed_macd(ema_fast, ema_slow);
    }

    fn macd(&self) -> Option<(f64, f64, f64)> {
        self.inner.macd()
    }
}

/// Python wrapper: StreamingEngine (manages multiple products)
/// Uses Arc<Mutex<>> to allow client references to be mutated via the engine.
#[pyclass]
struct PyStreamingEngine {
    inner: std::sync::Arc<std::sync::Mutex<streaming::StreamingEngine>>,
}

#[pymethods]
impl PyStreamingEngine {
    #[new]
    fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(std::sync::Mutex::new(streaming::StreamingEngine::new())),
        }
    }

    fn get_or_create(&mut self, product_id: String, maxlen: usize) -> PyStreamingIndicatorsHandle {
        let pid = product_id.clone();
        self.inner.lock().unwrap().get_or_create(&pid, maxlen);
        PyStreamingIndicatorsHandle {
            engine: self.inner.clone(),
            product_id,
        }
    }

    /// Return a handle if the product exists, None otherwise (like _products.get())
    fn try_get(&self, product_id: String) -> Option<PyStreamingIndicatorsHandle> {
        let eng = self.inner.lock().unwrap();
        if eng.products.contains_key(&product_id) {
            drop(eng);
            Some(PyStreamingIndicatorsHandle {
                engine: self.inner.clone(),
                product_id,
            })
        } else {
            None
        }
    }

    fn update(&mut self, product_id: String, close: f64, volume: f64) {
        self.inner.lock().unwrap().update(&product_id, close, volume);
    }

    fn ema(&self, product_id: String, period: usize) -> Option<f64> {
        self.inner.lock().unwrap().ema(&product_id, period)
    }

    fn rsi(&self, product_id: String) -> Option<f64> {
        self.inner.lock().unwrap().rsi(&product_id)
    }

    fn macd(&self, product_id: String) -> Option<(f64, f64, f64)> {
        self.inner.lock().unwrap().macd(&product_id)
    }
}

/// Handle to a specific product's streaming indicators within an engine.
/// All mutations go through the parent engine.
#[pyclass]
struct PyStreamingIndicatorsHandle {
    engine: std::sync::Arc<std::sync::Mutex<streaming::StreamingEngine>>,
    product_id: String,
}

#[pymethods]
impl PyStreamingIndicatorsHandle {
    #[getter]
    fn product_id(&self) -> String {
        self.product_id.clone()
    }

    #[getter]
    fn closes(&self) -> PyRingBuffer {
        self.with_indicators(|ind| PyRingBuffer { inner: ind.closes.clone() })
    }

    #[getter]
    fn volumes(&self) -> PyRingBuffer {
        self.with_indicators(|ind| PyRingBuffer { inner: ind.volumes.clone() })
    }

    fn update(&self, close: f64, volume: f64) {
        self.engine.lock().unwrap().update(&self.product_id, close, volume);
    }

    fn seed_ema(&self, period: usize, value: f64) {
        let pid = self.product_id.clone();
        let mut eng = self.engine.lock().unwrap();
        if let Some(ind) = eng.products.get_mut(&pid) {
            ind.seed_ema(period, value);
        }
    }

    fn ema(&self, period: usize) -> Option<f64> {
        self.with_indicators(|ind| ind.ema(period))
    }

    fn seed_sma(&self, period: usize, sma: f64, sq_sum: f64) {
        let pid = self.product_id.clone();
        let mut eng = self.engine.lock().unwrap();
        if let Some(ind) = eng.products.get_mut(&pid) {
            ind.seed_sma(period, sma, sq_sum);
        }
    }

    fn sma(&self, period: usize) -> Option<f64> {
        self.with_indicators(|ind| ind.sma(period))
    }

    fn bollinger(&self, period: usize) -> Option<(f64, f64, f64)> {
        self.with_indicators(|ind| ind.bollinger(period))
    }

    fn seed_rsi(&self, closes: Vec<f64>, period: usize) -> f64 {
        let pid = self.product_id.clone();
        let mut eng = self.engine.lock().unwrap();
        if let Some(ind) = eng.products.get_mut(&pid) {
            ind.seed_rsi(&closes, period)
        } else {
            50.0
        }
    }

    fn rsi(&self) -> f64 {
        self.with_indicators(|ind| ind.rsi())
    }

    fn seed_macd(&self, ema_fast: f64, ema_slow: f64) {
        let pid = self.product_id.clone();
        let mut eng = self.engine.lock().unwrap();
        if let Some(ind) = eng.products.get_mut(&pid) {
            ind.seed_macd(ema_fast, ema_slow);
        }
    }

    fn macd(&self) -> Option<(f64, f64, f64)> {
        self.with_indicators(|ind| ind.macd())
    }
}

impl PyStreamingIndicatorsHandle {
    fn with_indicators<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&streaming::StreamingIndicators) -> R,
    {
        let pid = &self.product_id;
        let eng = self.engine.lock().unwrap();
        if let Some(ind) = eng.products.get(pid) {
            f(ind)
        } else {
            panic!("PyStreamingIndicatorsHandle: product {} not found in engine", pid);
        }
    }
}

/// Python wrapper: run a single strategy and return (action, confidence, reason) or None.
#[pyfunction]
fn run_strategy_py(strategy_name: &str, closes: Vec<f64>, volumes: Vec<f64>,
                    highs: Vec<f64>, lows: Vec<f64>) -> Option<(String, f64, String)> {
    let sig = strategies::evaluate(strategy_name, &closes, &volumes, &highs, &lows);
    sig.map(|s| (s.action, s.confidence, s.reason))
}

/// Python wrapper: run ALL strategies and return Vec of (name, action, confidence, reason).
#[pyfunction]
fn evaluate_all_py(closes: Vec<f64>, volumes: Vec<f64>,
                    highs: Vec<f64>, lows: Vec<f64>) -> Vec<(String, String, f64, String)> {
    let results = strategies::evaluate_all(&closes, &volumes, &highs, &lows);
    results.into_iter().map(|(n, s)| (n, s.action, s.confidence, s.reason)).collect()
}

/// Python wrapper: run ALL 35 strategies with opens data.
#[pyfunction]
fn evaluate_all_opens_py(closes: Vec<f64>, opens: Vec<f64>, volumes: Vec<f64>,
                          highs: Vec<f64>, lows: Vec<f64>) -> Vec<(String, String, f64, String)> {
    let results = strategies::evaluate_all_opens(&closes, &opens, &volumes, &highs, &lows);
    results.into_iter().map(|(n, s)| (n, s.action, s.confidence, s.reason)).collect()
}

/// Python wrapper: run a single strategy with opens data.
#[pyfunction]
fn run_strategy_opens_py(strategy_name: &str, closes: Vec<f64>, opens: Vec<f64>,
                          volumes: Vec<f64>, highs: Vec<f64>, lows: Vec<f64>) -> Option<(String, f64, String)> {
    let sig = strategies::evaluate_opens(strategy_name, &closes, &opens, &volumes, &highs, &lows);
    sig.map(|s| (s.action, s.confidence, s.reason))
}

/// Python wrapper: backtest a strategy. Returns a dict-like tuple.
#[pyfunction]
#[pyo3(signature = (strategy_name, closes, volumes, warmup=20, highs=None, lows=None))]
fn backtest_strategy_py(
    strategy_name: &str,
    closes: Vec<f64>,
    volumes: Vec<f64>,
    warmup: usize,
    highs: Option<Vec<f64>>,
    lows: Option<Vec<f64>>,
) -> PyResult<Vec<f64>> {
    let verdict = backtest::backtest_strategy(
        strategy_name, &closes, &volumes, highs.as_deref(), lows.as_deref(), warmup,
    );
    Ok(vec![
        verdict.total_trades as f64,
        verdict.winning_trades as f64,
        verdict.losing_trades as f64,
        verdict.win_rate,
        verdict.total_return_pct,
        verdict.sharpe_ratio,
        verdict.profit_factor,
        verdict.max_drawdown_pct,
        if verdict.passed { 1.0 } else { 0.0 },
    ])
}

/// Python wrapper: aggregate signals through the confidence matrix.
/// signals: list of (strategy: str, action: str, confidence: float, reason: str)
/// bt_weights: dict of strategy_name -> precomputed_weight (from backtest cache)
/// Returns: list of (asset, direction, confidence, raw_confidence, agreeing_groups,
///                   total_groups, strategy_count, strategies: Vec<String>, best_reason, asset_class)
#[pyfunction]
fn confidence_aggregate_py(
    signals: Vec<(String, String, f64, String)>,
    asset_class: String,
    currency: String,
    bt_weights: HashMap<String, f64>,
) -> Vec<(String, String, f64, f64, usize, usize, usize, Vec<String>, String, String)> {
    let raw_signals: Vec<confidence::RawSignal> = signals
        .into_iter()
        .map(|(strategy, action, confidence, reason)| confidence::RawSignal {
            strategy,
            action,
            confidence,
            reason,
        })
        .collect();

    let results = confidence::aggregate(&raw_signals, &asset_class, &currency, &bt_weights);

    results
        .into_iter()
        .map(|a| {
            (
                a.asset,
                a.direction,
                a.confidence,
                a.raw_confidence,
                a.agreeing_groups,
                a.total_groups,
                a.strategy_count,
                a.strategies,
                a.best_reason,
                a.asset_class,
            )
        })
        .collect()
}

/// Python wrapper: compute backtest-derived weight for a strategy.
#[pyfunction]
fn confidence_weight_from_bt_py(win_rate: f64, sharpe: f64) -> f64 {
    confidence::compute_weight_from_bt(win_rate, sharpe)
}

/// Python wrapper: get default weight for a strategy.
#[pyfunction]
fn confidence_default_weight_py(strategy: String) -> f64 {
    confidence::default_weight(&strategy)
}

/// Python wrapper: get class boost for a strategy + asset class.
#[pyfunction]
fn confidence_class_boost_py(strategy: String, asset_class: String) -> f64 {
    confidence::class_boost(&strategy, &asset_class)
}

/// Python wrapper: backtest MULTIPLE strategies in parallel using rayon.
/// Returns a list of (strategy_name, [total_trades, win_rate, ...]) tuples.
#[pyfunction]
#[pyo3(signature = (strategy_names, closes, volumes, warmup=20, highs=None, lows=None))]
fn backtest_multi_py(
    strategy_names: Vec<String>,
    closes: Vec<f64>,
    volumes: Vec<f64>,
    warmup: usize,
    highs: Option<Vec<f64>>,
    lows: Option<Vec<f64>>,
) -> Vec<(String, Vec<f64>)> {
    use rayon::prelude::*;
    strategy_names
        .par_iter()
        .map(|name| {
            let verdict = backtest::backtest_strategy(
                name, &closes, &volumes, highs.as_deref(), lows.as_deref(), warmup,
            );
            (
                name.clone(),
                vec![
                    verdict.total_trades as f64,
                    verdict.winning_trades as f64,
                    verdict.losing_trades as f64,
                    verdict.win_rate,
                    verdict.total_return_pct,
                    verdict.sharpe_ratio,
                    verdict.profit_factor,
                    verdict.max_drawdown_pct,
                    if verdict.passed { 1.0 } else { 0.0 },
                ],
            )
        })
        .collect()
}

/// The Python module.
#[pymodule]
fn rust_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ema_py, m)?)?;
    m.add_function(wrap_pyfunction!(ema_slice_py, m)?)?;
    m.add_function(wrap_pyfunction!(rsi_py, m)?)?;
    m.add_function(wrap_pyfunction!(bollinger_py, m)?)?;
    m.add_function(wrap_pyfunction!(zscore_py, m)?)?;
    m.add_function(wrap_pyfunction!(sma_py, m)?)?;
    m.add_function(wrap_pyfunction!(wma_py, m)?)?;
    m.add_function(wrap_pyfunction!(trix_series_py, m)?)?;
    m.add_function(wrap_pyfunction!(macd_py, m)?)?;
    m.add_function(wrap_pyfunction!(run_strategy_py, m)?)?;
    m.add_function(wrap_pyfunction!(evaluate_all_py, m)?)?;
    m.add_function(wrap_pyfunction!(evaluate_all_opens_py, m)?)?;
    m.add_function(wrap_pyfunction!(run_strategy_opens_py, m)?)?;
    m.add_function(wrap_pyfunction!(backtest_strategy_py, m)?)?;
    m.add_function(wrap_pyfunction!(backtest_multi_py, m)?)?;
    m.add_function(wrap_pyfunction!(confidence_aggregate_py, m)?)?;
    m.add_function(wrap_pyfunction!(confidence_weight_from_bt_py, m)?)?;
    m.add_function(wrap_pyfunction!(confidence_default_weight_py, m)?)?;
    m.add_function(wrap_pyfunction!(confidence_class_boost_py, m)?)?;
    m.add_function(wrap_pyfunction!(detect_regime_py, m)?)?;
    m.add_function(wrap_pyfunction!(regime_recommended_strategies_py, m)?)?;
    m.add_function(wrap_pyfunction!(regime_features_py, m)?)?;
    m.add_function(wrap_pyfunction!(atr_py, m)?)?;
    m.add_function(wrap_pyfunction!(tcost_estimate_spread_bps_py, m)?)?;
    m.add_function(wrap_pyfunction!(tcost_impact_bps_py, m)?)?;
    m.add_function(wrap_pyfunction!(tcost_effective_fill_price_py, m)?)?;
    m.add_class::<PyFeeTracker>()?;
    m.add_class::<PyRingBuffer>()?;
    m.add_class::<PyStreamingIndicators>()?;
    m.add_class::<PyStreamingEngine>()?;
    m.add_class::<PyStreamingIndicatorsHandle>()?;
    Ok(())
}
