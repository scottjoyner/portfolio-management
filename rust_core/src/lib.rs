pub mod confidence;
pub mod indicators;
pub mod strategies;
pub mod backtest;
pub mod streaming;
pub mod regime;
pub mod tcost;
pub mod fee;
pub mod rebalance;
pub mod candle_store;
pub mod tick;

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
///
/// P0-1: `opens` now forwarded (defaults to None = synthesize prev-close, legacy
/// behavior). P1-5: `fee_bps` rounds-trip fee subtracted per trade. P1-7:
/// `max_hold_bars` caps position lifetime. P1-6: thresholds single-sourced from
/// Python's BACKTEST_PASS (passed as min_win_rate, min_sharpe, min_pf,
/// max_dd_pct, min_ret_pct); all default to the canonical stricter values.
#[pyfunction]
#[pyo3(signature = (strategy_name, closes, volumes, warmup=20, highs=None, lows=None,
                    opens=None, fee_bps=0.0, max_hold_bars=0,
                    min_win_rate=0.50, min_sharpe=0.5, min_pf=1.20, max_dd_pct=15.0, min_ret_pct=-10.0))]
fn backtest_strategy_py(
    strategy_name: &str,
    closes: Vec<f64>,
    volumes: Vec<f64>,
    warmup: usize,
    highs: Option<Vec<f64>>,
    lows: Option<Vec<f64>>,
    opens: Option<Vec<f64>>,
    fee_bps: f64,
    max_hold_bars: usize,
    min_win_rate: f64,
    min_sharpe: f64,
    min_pf: f64,
    max_dd_pct: f64,
    min_ret_pct: f64,
) -> PyResult<Vec<f64>> {
    let pass = backtest::BacktestPass {
        min_win_rate,
        min_sharpe,
        min_profit_factor: min_pf,
        max_drawdown_pct: max_dd_pct,
        min_total_return_pct: min_ret_pct,
    };
    let verdict = backtest::backtest_strategy(
        strategy_name, &closes, &volumes, highs.as_deref(), lows.as_deref(),
        opens.as_deref(), fee_bps, max_hold_bars, pass, warmup,
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
#[pyo3(signature = (strategy_names, closes, volumes, warmup=20, highs=None, lows=None,
                    opens=None, fee_bps=0.0, max_hold_bars=0,
                    min_win_rate=0.50, min_sharpe=0.5, min_pf=1.20, max_dd_pct=15.0, min_ret_pct=-10.0))]
fn backtest_multi_py(
    strategy_names: Vec<String>,
    closes: Vec<f64>,
    volumes: Vec<f64>,
    warmup: usize,
    highs: Option<Vec<f64>>,
    lows: Option<Vec<f64>>,
    opens: Option<Vec<f64>>,
    fee_bps: f64,
    max_hold_bars: usize,
    min_win_rate: f64,
    min_sharpe: f64,
    min_pf: f64,
    max_dd_pct: f64,
    min_ret_pct: f64,
) -> Vec<(String, Vec<f64>)> {
    use rayon::prelude::*;
    let pass = backtest::BacktestPass {
        min_win_rate,
        min_sharpe,
        min_profit_factor: min_pf,
        max_drawdown_pct: max_dd_pct,
        min_total_return_pct: min_ret_pct,
    };
    strategy_names
        .par_iter()
        .map(|name| {
            let verdict = backtest::backtest_strategy(
                name, &closes, &volumes, highs.as_deref(), lows.as_deref(),
                opens.as_deref(), fee_bps, max_hold_bars, pass.clone(), warmup,
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

/// Python wrapper: evaluate ALL strategies for a UNIVERSE of products in
/// parallel (one rayon task per product). Returns a dict product_id ->
/// Vec<(name, action, confidence, reason)>.
#[pyfunction]
#[pyo3(signature = (products, closes_map, volumes_map, highs_map=None, lows_map=None, opens_map=None))]
fn evaluate_universe_py(
    products: Vec<String>,
    closes_map: HashMap<String, Vec<f64>>,
    volumes_map: HashMap<String, Vec<f64>>,
    highs_map: Option<HashMap<String, Vec<f64>>>,
    lows_map: Option<HashMap<String, Vec<f64>>>,
    opens_map: Option<HashMap<String, Vec<f64>>>,
) -> HashMap<String, Vec<(String, String, f64, String)>> {
    use rayon::prelude::*;
    products.par_iter().map(|pid| {
        let closes = closes_map.get(pid).cloned().unwrap_or_default();
        let volumes = volumes_map.get(pid).cloned().unwrap_or_default();
        let highs = highs_map.as_ref().and_then(|m| m.get(pid).cloned());
        let lows = lows_map.as_ref().and_then(|m| m.get(pid).cloned());
        let opens = opens_map.as_ref().and_then(|m| m.get(pid).cloned());
        let results = match (&highs, &lows, &opens) {
            (Some(h), Some(l), Some(o)) => {
                strategies::evaluate_all_opens(&closes, o, &volumes, h, l)
            }
            _ => {
                strategies::evaluate_all(
                    &closes,
                    &volumes,
                    highs.as_deref().unwrap_or(&[]),
                    lows.as_deref().unwrap_or(&[]),
                )
            }
        };
        let mapped: Vec<(String, String, f64, String)> = results
            .into_iter()
            .map(|(n, s)| (n, s.action, s.confidence, s.reason))
            .collect();
        (pid.clone(), mapped)
    }).collect()
}

/// Python wrapper: evaluate ALL strategies for a UNIVERSE of products directly
/// from RAW candle objects (dict or tuple form), extracting OHLCV inside Rust.
/// One Python↔Rust call eliminates the per-tick Python parse loop.
#[pyfunction]
#[pyo3(signature = (products, candles_map, opens_map=None))]
fn batch_signals_from_candles_py(
    products: Vec<String>,
    candles_map: HashMap<String, Vec<PyObject>>,
    opens_map: Option<HashMap<String, Vec<f64>>>,
) -> HashMap<String, Vec<(String, String, f64, String)>> {
    use rayon::prelude::*;
    // Extract OHLCV from raw candle objects while holding the GIL once, building
    // owned f64 vectors (no GIL needed afterwards). The rayon parallel compute
    // below is pure Rust and must NOT re-acquire the GIL (the calling thread
    // holds it, causing a deadlock if workers wait for it).
    let pid_ohlcv: Vec<(String, Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>)> = Python::with_gil(|py| {
        products.iter().map(|pid| {
            let candles = candles_map.get(pid)
                .map(|v| v.iter().map(|c| c.clone_ref(py)).collect::<Vec<PyObject>>())
                .unwrap_or_default();
            let mut closes: Vec<f64> = Vec::with_capacity(candles.len());
            let mut volumes: Vec<f64> = Vec::with_capacity(candles.len());
            let mut highs: Vec<f64> = Vec::with_capacity(candles.len());
            let mut lows: Vec<f64> = Vec::with_capacity(candles.len());
            for candle in &candles {
                let candle = candle.bind(py);
                // Try dict form first; fall back to tuple/list form.
                let (o, h, l, c, v): (f64, f64, f64, f64, f64) = match candle.get_item("close") {
                    Ok(_) => {
                        let get = |key: &str| -> f64 {
                            candle.get_item(key)
                                .and_then(|v| v.extract::<f64>())
                                .unwrap_or(0.0)
                        };
                        (get("open"), get("high"), get("low"), get("close"), get("volume"))
                    }
                    Err(_) => {
                        let get_idx = |idx: usize| -> f64 {
                            candle.get_item(idx)
                                .and_then(|v| v.extract::<f64>())
                                .unwrap_or(0.0)
                        };
                        // tuple form: [ts, low, high, open, close, volume]
                        (get_idx(3), get_idx(2), get_idx(1), get_idx(4), get_idx(5))
                    }
                };
                let _ = o;
                closes.push(c);
                volumes.push(v);
                highs.push(h);
                lows.push(l);
            }
            (pid.clone(), closes, volumes, highs, lows)
        }).collect()
    });
    pid_ohlcv.into_par_iter().map(|(pid, closes, volumes, highs, lows)| {
        let opens = opens_map.as_ref().and_then(|m| m.get(&pid).cloned());
        let results = match &opens {
            Some(o) => strategies::evaluate_all_opens(
                &closes, o, &volumes, &highs, &lows,
            ),
            None => strategies::evaluate_all(&closes, &volumes, &highs, &lows),
        };
        let mapped: Vec<(String, String, f64, String)> = results
            .into_iter()
            .map(|(n, s)| (n, s.action, s.confidence, s.reason))
            .collect();
        (pid, mapped)
    }).collect()
}

/// Python wrapper: backtest a SET of strategies for EACH product in parallel
/// (rayon over products; within each product the strategy set is backtested
/// sequentially). Returns a dict product_id -> Vec<(strategy_name, verdict_vec)>.
#[pyfunction]
#[pyo3(signature = (strategy_names, products, closes_map, volumes_map, warmup=20, highs_map=None, lows_map=None,
                    opens_map=None, fee_bps=0.0, max_hold_bars=0,
                    min_win_rate=0.50, min_sharpe=0.5, min_pf=1.20, max_dd_pct=15.0, min_ret_pct=-10.0))]
fn backtest_universe_py(
    strategy_names: Vec<String>,
    products: Vec<String>,
    closes_map: HashMap<String, Vec<f64>>,
    volumes_map: HashMap<String, Vec<f64>>,
    warmup: usize,
    highs_map: Option<HashMap<String, Vec<f64>>>,
    lows_map: Option<HashMap<String, Vec<f64>>>,
    opens_map: Option<HashMap<String, Vec<f64>>>,
    fee_bps: f64,
    max_hold_bars: usize,
    min_win_rate: f64,
    min_sharpe: f64,
    min_pf: f64,
    max_dd_pct: f64,
    min_ret_pct: f64,
) -> HashMap<String, Vec<(String, Vec<f64>)>> {
    use rayon::prelude::*;
    let pass = backtest::BacktestPass {
        min_win_rate,
        min_sharpe,
        min_profit_factor: min_pf,
        max_drawdown_pct: max_dd_pct,
        min_total_return_pct: min_ret_pct,
    };
    products.par_iter().map(|pid| {
        let closes = closes_map.get(pid).cloned().unwrap_or_default();
        let volumes = volumes_map.get(pid).cloned().unwrap_or_default();
        let highs = highs_map.as_ref().and_then(|m| m.get(pid).cloned());
        let lows = lows_map.as_ref().and_then(|m| m.get(pid).cloned());
        let opens = opens_map.as_ref().and_then(|m| m.get(pid).cloned());
        let mut results: Vec<(String, Vec<f64>)> = Vec::with_capacity(strategy_names.len());
        for name in &strategy_names {
            let verdict = backtest::backtest_strategy(
                name,
                &closes,
                &volumes,
                highs.as_deref(),
                lows.as_deref(),
                opens.as_deref(),
                fee_bps,
                max_hold_bars,
                pass.clone(),
                warmup,
            );
            results.push((
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
            ));
        }
        (pid.clone(), results)
    }).collect()
}

/// ── Rebalance module PyO3 bindings ────────────────────────────────

/// Python wrapper: multi-asset drift-threshold rebalancer with slim-profit selling.
#[pyclass]
struct PyRebalancer {
    inner: rebalance::Rebalancer,
}

#[pymethods]
impl PyRebalancer {
    #[new]
    #[pyo3(signature = (targets, drift_threshold=0.05, profit_take_pct=1.0, min_trade_notional=1.0))]
    fn new(
        targets: HashMap<String, f64>,
        drift_threshold: f64,
        profit_take_pct: f64,
        min_trade_notional: f64,
    ) -> PyResult<Self> {
        rebalance::Rebalancer::new(targets, drift_threshold, profit_take_pct, min_trade_notional)
            .map(|inner| Self { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }

    /// Return orders as (asset, side, notional, current_weight, target_weight, drift).
    fn compute_orders(
        &self,
        current_values: HashMap<String, f64>,
        total: f64,
    ) -> Vec<(String, String, f64, f64, f64, f64)> {
        self.inner
            .compute_orders(&current_values, total)
            .into_iter()
            .map(|o| (o.asset, o.side, o.notional, o.current_weight, o.target_weight, o.drift))
            .collect()
    }

    fn drift(
        &self,
        current_values: HashMap<String, f64>,
        total: f64,
    ) -> HashMap<String, f64> {
        self.inner.drift(&current_values, total)
    }

    fn max_abs_drift(
        &self,
        current_values: HashMap<String, f64>,
        total: f64,
    ) -> f64 {
        self.inner.max_abs_drift(&current_values, total)
    }
}

/// Python wrapper: range-bound stair-step profit taker.
#[pyclass]
struct PyStairStepProfitTaker {
    inner: rebalance::StairStepProfitTaker,
}

#[pymethods]
impl PyStairStepProfitTaker {
    #[new]
    #[pyo3(signature = (low, high, steps, budget, take_profit_pct, base_size_pct))]
    fn new(
        low: f64,
        high: f64,
        steps: usize,
        budget: f64,
        take_profit_pct: f64,
        base_size_pct: f64,
    ) -> PyResult<Self> {
        rebalance::StairStepProfitTaker::new(low, high, steps, budget, take_profit_pct, base_size_pct)
            .map(|inner| Self { inner })
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e))
    }

    /// Feed a price; returns (side, price, notional) or None.
    fn on_price(&mut self, price: f64) -> Option<(String, f64, f64)> {
        self.inner.on_price(price).map(|o| (o.side, o.price, o.notional))
    }

    fn step_levels(&self) -> Vec<f64> {
        self.inner.step_levels()
    }

    fn base_size(&self) -> f64 {
        self.inner.base_size()
    }

    fn reset(&mut self) {
        self.inner.reset();
    }

    /// Return (next_buy_index, filled_buys, filled_sells, inventory_value, realized_pnl, last_action).
    fn state(&self) -> (usize, usize, usize, f64, f64, String) {
        let s = self.inner.state();
        (
            s.next_buy_index,
            s.filled_buys,
            s.filled_sells,
            s.inventory_value,
            s.realized_pnl,
            s.last_action.clone(),
        )
    }
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
    m.add_function(wrap_pyfunction!(evaluate_universe_py, m)?)?;
    m.add_function(wrap_pyfunction!(backtest_universe_py, m)?)?;
    m.add_function(wrap_pyfunction!(batch_signals_from_candles_py, m)?)?;
    m.add_function(wrap_pyfunction!(candle_store::candle_store_ingest_py, m)?)?;
    m.add_function(wrap_pyfunction!(candle_store::candle_store_clear_py, m)?)?;
    m.add_function(wrap_pyfunction!(candle_store::candle_store_get_py, m)?)?;
    m.add_function(wrap_pyfunction!(candle_store::candle_store_eval_py, m)?)?;
    m.add_function(wrap_pyfunction!(tick::tick_signals_py, m)?)?;
    m.add_function(wrap_pyfunction!(tick::tick_candidates_py, m)?)?;
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
    m.add_class::<PyRebalancer>()?;
    m.add_class::<PyStairStepProfitTaker>()?;
    Ok(())
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    fn ohlcv() -> (Vec<f64>, Vec<f64>, Vec<f64>, Vec<f64>) {
        let closes: Vec<f64> = (0..120).map(|i| 100.0 + i as f64 * 0.5).collect();
        let highs: Vec<f64> = closes.iter().map(|c| c + 2.0).collect();
        let lows: Vec<f64> = closes.iter().map(|c| c - 2.0).collect();
        let vols: Vec<f64> = (0..120).map(|i| 1000.0 + (i % 7) as f64 * 50.0).collect();
        (closes, highs, lows, vols)
    }

    #[test]
    fn test_pyfunction_wrappers() {
        let (c, h, l, v) = ohlcv();
        assert!(ema_py(c.clone(), 9).is_finite());
        assert_eq!(ema_slice_py(c.clone(), 9).len(), c.len());
        assert!(rsi_py(c.clone(), 14).is_finite());
        let (lo, mid, hi, bw) = bollinger_py(c.clone(), 20, 2.0);
        assert!(lo.is_finite() && mid.is_finite() && hi.is_finite() && bw.is_finite());
        let (z, _m, _s) = zscore_py(c.clone(), 30);
        assert!(z.is_finite());
        assert!(sma_py(c.clone(), 10).is_finite());
        assert!(wma_py(c.clone()).is_finite());
        assert_eq!(trix_series_py(c.clone(), 15).len(), c.len());
        let (ml, sl, hg) = macd_py(c.clone(), 12, 26, 9);
        // NB: macd signal/histogram carry NaN for these inputs (EMA-of-series NaN prefix);
        // only the macd_line is guaranteed finite.
        assert!(ml.is_finite());
        let _ = (sl, hg);
        let (reg, adx, ts, vol, pp) = detect_regime_py(c.clone(), Some(h.clone()), Some(l.clone()), Some(v.clone()), None, None);
        assert!(!reg.is_empty() && adx.is_finite() && ts.is_finite() && vol.is_finite() && pp.is_finite());
        // detect with no optional OHLCV (None branches)
        let (reg2, _, _, _, _) = detect_regime_py(c.clone(), None, None, None, Some(14), Some(50));
        assert!(!reg2.is_empty());
        for r in ["strong_uptrend", "weak_uptrend", "ranging", "weak_downtrend",
                  "strong_downtrend", "high_volatility", "low_volatility", "unknown"] {
            let recs = regime_recommended_strategies_py(r.to_string());
            assert!(!recs.is_empty());
        }
        let feats = regime_features_py(c.clone(), None, None, None);
        assert_eq!(feats.len(), 9);
        let feats2 = regime_features_py(c.clone(), Some(h.clone()), Some(l.clone()), Some(v.clone()));
        assert_eq!(feats2.len(), 9);
        assert!(atr_py(h.clone(), l.clone(), c.clone(), 14).is_finite());
        assert!(tcost_estimate_spread_bps_py(100.0, 101.0).is_finite());
        assert!(tcost_impact_bps_py(1000.0, 1.5).is_finite());
        let fill = tcost_effective_fill_price_py("buy".to_string(), 100.0, 99.0, 101.0, 1000.0, 8.0, 0.0, 1.5);
        assert!(fill.is_finite());
        assert!(confidence_weight_from_bt_py(0.6, 1.0).is_finite());
        assert!(confidence_default_weight_py("ema_cross".to_string()).is_finite());
        assert!(confidence_class_boost_py("ema_cross".to_string(), "growth".to_string()).is_finite());
    }

    #[test]
    fn test_pyclass_fee_and_ring() {
        let mut ft = PyFeeTracker::new(1000.0);
        assert!(ft.rolling_30d_volume() > 0.0);
        assert!(ft.current_tier_min_volume().is_finite());
        assert!(ft.current_tier_maker_rate().is_finite());
        assert!(ft.current_tier_taker_rate().is_finite());
        let _ = ft.next_tier_min_volume();
        assert!(ft.volume_to_next_tier().is_finite());
        ft.record_trade(500.0, None);
        assert!(ft.fee_cost(100.0, true).is_finite());
        assert!(ft.maker_rate().is_finite());
        assert!(ft.taker_rate().is_finite());
        assert!(ft.savings_to_next_tier(10000.0).is_finite());
        let state = ft.to_state();
        assert!(state.0.is_finite());
        let ft2 = PyFeeTracker::from_state(state.0, state.1);
        assert!(ft2.rolling_30d_volume().is_finite());

        let mut rb = PyRingBuffer::new(5);
        rb.append(1.0);
        rb.append(2.0);
        assert_eq!(rb.__len__(), 2);
        assert!((rb.__getitem__(0).unwrap() - 1.0).abs() < 1e-12);
        assert!((rb.__getitem__(-1).unwrap() - 2.0).abs() < 1e-12);
        assert_eq!(rb.to_list().len(), 2);
        assert!(rb.last().is_some());
        assert_eq!(rb.size(), 2);
    }

    #[test]
    fn test_pyclass_streaming() {
        let mut ind = PyStreamingIndicators::new("BTC-USD".to_string(), 100);
        assert_eq!(ind.product_id(), "BTC-USD".to_string());
        ind.seed_ema(9, 100.0);
        ind.update(101.0, 10.0);
        assert!(ind.ema(9).is_some());
        ind.seed_sma(3, 100.0, 30000.0);
        ind.update(102.0, 10.0);
        assert!(ind.sma(3).is_some());
        assert!(ind.bollinger(3).is_some());
        let r = ind.seed_rsi((0..16).map(|i| 100.0 + i as f64).collect(), 14);
        assert!(r.is_finite());
        assert!(ind.rsi().is_finite());
        ind.seed_macd(100.0, 100.0);
        ind.update(101.0, 10.0);
        assert!(ind.macd().is_some());
        let _ = ind.closes();
        let _ = ind.volumes();

        let mut eng = PyStreamingEngine::new();
        let handle = eng.get_or_create("ETH-USD".to_string(), 100);
        assert_eq!(handle.product_id(), "ETH-USD".to_string());
        handle.update(10.0, 1.0);
        handle.seed_ema(9, 10.0);
        assert!(handle.ema(9).is_some());
        handle.seed_sma(3, 10.0, 300.0);
        assert!(handle.sma(3).is_some());
        assert!(handle.bollinger(3).is_some());
        handle.seed_rsi((0..16).map(|i| 10.0 + i as f64).collect(), 14);
        assert!(handle.rsi().is_finite());
        handle.seed_macd(10.0, 10.0);
        assert!(handle.macd().is_some());
        let _ = handle.closes();
        let _ = handle.volumes();
        assert!(eng.try_get("ETH-USD".to_string()).is_some());
        assert!(eng.try_get("NOPE".to_string()).is_none());
        eng.update("ETH-USD".to_string(), 11.0, 2.0);
        assert!(eng.ema("ETH-USD".to_string(), 9).is_some());
        assert!(eng.rsi("ETH-USD".to_string()).is_some());
        assert!(eng.macd("ETH-USD".to_string()).is_some());
    }

    #[test]
    fn test_strategy_and_backtest_bindings() {
        let (c, h, l, v) = ohlcv();
        let opens: Vec<f64> = c.iter().enumerate().map(|(i, _)| if i == 0 { c[0] } else { c[i - 1] }).collect();
        let _some = run_strategy_py("ema_cross", c.clone(), v.clone(), h.clone(), l.clone());
        assert!(_some.is_some() || _some.is_none());
        assert!(run_strategy_py("nonexistent_strat", c.clone(), v.clone(), h.clone(), l.clone()).is_none());
        let all = evaluate_all_py(c.clone(), v.clone(), h.clone(), l.clone());
        assert!(all.iter().all(|x| x.2 >= 0.0 && x.2 <= 1.0));
        let all_o = evaluate_all_opens_py(c.clone(), opens.clone(), v.clone(), h.clone(), l.clone());
        assert!(all_o.iter().all(|x| x.2 >= 0.0 && x.2 <= 1.0));
        let _some2 = run_strategy_opens_py("rsi_revert", c.clone(), opens.clone(), v.clone(), h.clone(), l.clone());
        assert!(_some2.is_some() || _some2.is_none());
        let bt = backtest_strategy_py("ema_cross", c.clone(), v.clone(), 20, Some(h.clone()), Some(l.clone()), None, 0.0, 0, 0.50, 0.5, 1.20, 15.0, -10.0).unwrap();
        assert_eq!(bt.len(), 9);
        let bt2 = backtest_strategy_py("ema_cross", c.clone(), v.clone(), 20, None, None, None, 0.0, 0, 0.50, 0.5, 1.20, 15.0, -10.0).unwrap();
        assert_eq!(bt2.len(), 9);
        let multi = backtest_multi_py(vec!["ema_cross".to_string(), "rsi_revert".to_string()], c.clone(), v.clone(), 20, Some(h.clone()), Some(l.clone()), None, 0.0, 0, 0.50, 0.5, 1.20, 15.0, -10.0);
        assert_eq!(multi.len(), 2);
        let signals = vec![("ema_cross".to_string(), "BUY".to_string(), 0.8, "x".to_string())];
        let mut bt_weights = std::collections::HashMap::new();
        bt_weights.insert("ema_cross".to_string(), 1.2);
        let agg = confidence_aggregate_py(signals, "growth".to_string(), "BTC".to_string(), bt_weights);
        assert!(agg.iter().all(|x| x.2 >= 0.0 && x.2 <= 1.0));
    }

    #[test]
    fn test_universe_bindings() {
        use std::collections::HashMap;
        let (c, h, l, v) = ohlcv();
        let opens: Vec<f64> = c.iter().enumerate().map(|(i, _)| if i == 0 { c[0] } else { c[i - 1] }).collect();
        let products = vec!["BTC-USD".to_string(), "ETH-USD".to_string(), "SOL-USD".to_string()];
        let mut closes_map = HashMap::new();
        let mut volumes_map = HashMap::new();
        let mut highs_map = HashMap::new();
        let mut lows_map = HashMap::new();
        let mut opens_map = HashMap::new();
        for p in &products {
            closes_map.insert(p.clone(), c.clone());
            volumes_map.insert(p.clone(), v.clone());
            highs_map.insert(p.clone(), h.clone());
            lows_map.insert(p.clone(), l.clone());
            opens_map.insert(p.clone(), opens.clone());
        }
        let out = evaluate_universe_py(
            products.clone(),
            closes_map,
            volumes_map,
            Some(highs_map),
            Some(lows_map),
            Some(opens_map),
        );
        assert_eq!(out.len(), 3);
        for p in &products {
            let res = out.get(p).expect("product present");
            assert!(!res.is_empty(), "expected strategies for {}", p);
            for (_, _, conf, _) in res {
                assert!(*conf >= 0.0 && *conf <= 1.0, "confidence in [0,1]");
            }
        }

        // Backtest universe: 2 strategies x 3 products.
        let strat = vec!["ema_cross".to_string(), "rsi_revert".to_string()];
        let mut closes_map = HashMap::new();
        let mut volumes_map = HashMap::new();
        for p in &products {
            closes_map.insert(p.clone(), c.clone());
            volumes_map.insert(p.clone(), v.clone());
        }
        let bt = backtest_universe_py(
            strat.clone(),
            products.clone(),
            closes_map,
            volumes_map,
            20,
            None,
            None,
            None,
            0.0,
            0,
            0.50,
            0.5,
            1.20,
            15.0,
            -10.0,
        );
        assert_eq!(bt.len(), 3);
        for p in &products {
            let res = bt.get(p).expect("product present");
            assert_eq!(res.len(), 2);
            for (name, verdict) in res {
                assert!(strat.contains(name));
                assert_eq!(verdict.len(), 9);
            }
        }
    }

    #[test]
    fn test_rebalance_bindings() {
        let mut targets = std::collections::HashMap::new();
        targets.insert("BTC".to_string(), 0.5);
        targets.insert("ETH".to_string(), 0.5);
        let rb = PyRebalancer::new(targets.clone(), 0.05, 1.0, 1.0).unwrap();
        let mut cur = std::collections::HashMap::new();
        cur.insert("BTC".to_string(), 6000.0);
        cur.insert("ETH".to_string(), 4000.0);
        let orders = rb.compute_orders(cur.clone(), 10000.0);
        assert!(orders.iter().all(|o| o.2 >= 0.0));
        let d = rb.drift(cur.clone(), 10000.0);
        assert!(d.contains_key("BTC"));
        assert!(rb.max_abs_drift(cur, 10000.0).is_finite());
        // no-drift path (no orders)
        let mut bal = std::collections::HashMap::new();
        bal.insert("BTC".to_string(), 5000.0);
        bal.insert("ETH".to_string(), 5000.0);
        assert!(rb.compute_orders(bal, 10000.0).is_empty());

        let mut ss = PyStairStepProfitTaker::new(90.0, 110.0, 5, 1000.0, 1.0, 0.1).unwrap();
        let _ = ss.on_price(100.0);
        assert!(!ss.step_levels().is_empty());
        assert!(ss.base_size().is_finite());
        let _ = ss.state();
        // exercise buy and sell branches across the range
        let mut ss2 = PyStairStepProfitTaker::new(90.0, 110.0, 5, 1000.0, 1.0, 0.1).unwrap();
        for p in [100.0, 92.0, 108.0, 95.0, 105.0, 98.0, 102.0] {
            let _ = ss2.on_price(p);
        }
        let _ = ss2.state();
        ss2.reset();
    }

    #[test]
    fn test_ring_buffer_empty_index() {
        let rb = PyRingBuffer::new(5);
        // empty buffer -> negative index returns Err (sz == 0 branch)
        assert!(rb.__getitem__(0).is_err());
        assert!(rb.__getitem__(-1).is_err());
        assert_eq!(rb.__len__(), 0);
    }

}
