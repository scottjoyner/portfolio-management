/// Regime detection — classifies market conditions from OHLCV data.
///
/// Computes ADX, trend strength, volatility, Hurst exponent, skewness,
/// kurtosis, serial correlation, and price position, then classifies
/// into one of 8 regimes.  Mirrors coinbase/src/regime.py in Rust.

// ── Regime enum ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Regime {
    StrongUptrend,
    WeakUptrend,
    Ranging,
    WeakDowntrend,
    StrongDowntrend,
    HighVolatility,
    LowVolatility,
    Unknown,
}

impl Regime {
    pub fn as_str(&self) -> &'static str {
        match self {
            Regime::StrongUptrend => "strong_uptrend",
            Regime::WeakUptrend => "weak_uptrend",
            Regime::Ranging => "ranging",
            Regime::WeakDowntrend => "weak_downtrend",
            Regime::StrongDowntrend => "strong_downtrend",
            Regime::HighVolatility => "high_volatility",
            Regime::LowVolatility => "low_volatility",
            Regime::Unknown => "unknown",
        }
    }

    pub fn recommended_strategies(&self) -> &'static [&'static str] {
        match self {
            Regime::StrongUptrend => &[
                "ema_cross", "macd", "donchian", "adx", "hma",
                "trix", "psar", "aroon", "force_idx", "vpt",
            ],
            Regime::WeakUptrend => &[
                "ema_cross", "macd", "donchian", "adx", "vwap_revert",
                "keltner", "chaikin_mf",
            ],
            Regime::Ranging => &[
                "rsi_revert", "boll_break", "zscore_revert", "williams_r",
                "cmo", "scci", "ema_dev", "snr_idx",
            ],
            Regime::WeakDowntrend => &[
                "rsi_revert", "boll_break", "zscore_revert", "williams_r",
                "vwap_revert", "obv_div",
            ],
            Regime::StrongDowntrend => &[
                "psar", "aroon", "adx", "donchian", "range_exp_idx",
                "force_idx", "vpt",
            ],
            Regime::HighVolatility => &[
                "boll_break", "keltner", "donchian", "vol_mom",
                "range_exp_idx", "snr_idx",
            ],
            Regime::LowVolatility => &[
                "rsi_revert", "zscore_revert", "scci", "ema_dev",
                "vwap_revert",
            ],
            Regime::Unknown => &[
                "ema_cross", "rsi_revert", "boll_break", "donchian",
            ],
        }
    }
}

// ── RegimeFeatures ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RegimeFeatures {
    pub regime: Regime,
    pub adx: f64,
    pub trend_strength: f64,
    pub volatility: f64,
    pub volume_trend: f64,
    pub price_position: f64,
    pub hurst_exponent: f64,
    pub serial_correlation: f64,
    pub skewness: f64,
    pub kurtosis: f64,
}

impl RegimeFeatures {
    fn new() -> Self {
        Self {
            regime: Regime::Unknown,
            adx: 0.0,
            trend_strength: 0.0,
            volatility: 0.0,
            volume_trend: 0.0,
            price_position: 0.5,
            hurst_exponent: 0.5,
            serial_correlation: 0.0,
            skewness: 0.0,
            kurtosis: 3.0,
        }
    }

    pub fn is_trending(&self) -> bool {
        self.adx > 25.0 && self.trend_strength.abs() > 0.02
    }

    pub fn is_volatile(&self) -> bool {
        self.volatility > 0.03
    }

    pub fn is_ranging(&self) -> bool {
        self.adx < 20.0 && self.volatility < 0.02
    }
}

// ── RegimeDetector ─────────────────────────────────────────────────

pub struct RegimeDetector {
    pub adx_period: usize,
    pub volatility_period: usize,
    pub lookback: usize,
}

impl RegimeDetector {
    pub fn new(adx_period: usize, volatility_period: usize, lookback: usize) -> Self {
        Self { adx_period, volatility_period, lookback }
    }

    pub fn detect(
        &self,
        closes: &[f64],
        highs: Option<&[f64]>,
        lows: Option<&[f64]>,
        volumes: Option<&[f64]>,
    ) -> (Regime, RegimeFeatures) {
        let mut features = self.compute_features(closes, highs, lows, volumes);
        let regime = self.classify(&features);
        features.regime = regime;
        (regime, features)
    }

    fn compute_features(
        &self,
        closes: &[f64],
        highs: Option<&[f64]>,
        lows: Option<&[f64]>,
        volumes: Option<&[f64]>,
    ) -> RegimeFeatures {
        let mut features = RegimeFeatures::new();
        if closes.len() < 30 {
            return features;
        }

        let n = closes.len().min(self.lookback);
        let recent = &closes[closes.len() - n..];

        let returns = compute_returns(recent);
        features.volatility = compute_volatility(&returns);
        features.trend_strength = compute_trend_strength(recent);
        features.adx = match (highs, lows) {
            (Some(h), Some(l)) => self.compute_adx(closes, h, l),
            _ => 25.0,
        };
        features.price_position = compute_price_position(recent);
        features.skewness = compute_skewness(&returns);
        features.kurtosis = compute_kurtosis(&returns);
        features.hurst_exponent = hurst_exponent(recent);
        features.serial_correlation = serial_correlation(&returns, 1);

        if let Some(vols) = volumes {
            let vn = vols.len().min(self.lookback);
            let vol_recent = &vols[vols.len() - vn..];
            let vol_returns = compute_returns(vol_recent);
            if vol_returns.len() >= 5 {
                features.volume_trend =
                    vol_returns[vol_returns.len() - 5..].iter().sum::<f64>() / 5.0;
            }
        }

        features
    }

    fn classify(&self, f: &RegimeFeatures) -> Regime {
        if f.is_volatile() && f.is_trending() && f.trend_strength > 0.0 {
            return Regime::StrongUptrend;
        }
        if f.is_volatile() && f.is_trending() && f.trend_strength < 0.0 {
            return Regime::StrongDowntrend;
        }
        if f.is_trending() && f.trend_strength > 0.0 {
            return Regime::WeakUptrend;
        }
        if f.is_trending() && f.trend_strength < 0.0 {
            return Regime::WeakDowntrend;
        }
        if f.is_volatile() {
            if f.hurst_exponent > 0.6 && f.trend_strength.abs() > 0.01 {
                return if f.trend_strength > 0.0 {
                    Regime::StrongUptrend
                } else {
                    Regime::StrongDowntrend
                };
            }
            return Regime::HighVolatility;
        }
        if f.is_ranging() {
            return Regime::Ranging;
        }
        if f.volatility < 0.01 {
            return Regime::LowVolatility;
        }
        Regime::Unknown
    }

    fn compute_adx(&self, closes: &[f64], highs: &[f64], lows: &[f64]) -> f64 {
        let n = closes.len().min(highs.len()).min(lows.len());
        if n < 2 {
            return 25.0;
        }
        let highs = &highs[highs.len() - n..];
        let lows = &lows[lows.len() - n..];
        let closes = &closes[closes.len() - n..];

        let mut trs = Vec::with_capacity(n - 1);
        let mut plus_dm = Vec::with_capacity(n - 1);
        let mut minus_dm = Vec::with_capacity(n - 1);

        for i in 1..n {
            let up_move = highs[i] - highs[i - 1];
            let down_move = lows[i - 1] - lows[i];
            plus_dm.push(if up_move > down_move && up_move > 0.0 { up_move } else { 0.0 });
            minus_dm.push(if down_move > up_move && down_move > 0.0 { down_move } else { 0.0 });
            let tr = (highs[i] - lows[i])
                .max((highs[i] - closes[i - 1]).abs())
                .max((lows[i] - closes[i - 1]).abs());
            trs.push(tr);
        }

        if trs.is_empty() {
            return 25.0;
        }

        let period = self.adx_period.min(trs.len());
        let tr_sum: f64 = trs[trs.len() - period..].iter().sum();
        if tr_sum <= 0.0 {
            return 25.0;
        }

        let plus_di = 100.0 * plus_dm[plus_dm.len() - period..].iter().sum::<f64>() / tr_sum;
        let minus_di = 100.0 * minus_dm[minus_dm.len() - period..].iter().sum::<f64>() / tr_sum;
        let denom = (plus_di + minus_di).max(1e-9);
        let dx = 100.0 * (plus_di - minus_di).abs() / denom;
        dx.clamp(0.0, 100.0)
    }
}

// ── Stateless helper functions ─────────────────────────────────────

fn compute_returns(prices: &[f64]) -> Vec<f64> {
    if prices.len() < 2 {
        return vec![];
    }
    prices
        .windows(2)
        .map(|w| (w[1] - w[0]) / w[0].max(1e-9))
        .collect()
}

fn compute_volatility(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    variance.sqrt()
}

fn compute_trend_strength(prices: &[f64]) -> f64 {
    if prices.len() < 20 {
        return 0.0;
    }
    let n = prices.len();
    let ma50 = if n >= 50 {
        prices[n - 50..].iter().sum::<f64>() / 50.0
    } else {
        prices.iter().sum::<f64>() / n as f64
    };
    let ma20 = prices[n - 20..].iter().sum::<f64>() / 20.0;
    (ma20 - ma50) / ma50.max(1e-9)
}

fn compute_price_position(prices: &[f64]) -> f64 {
    if prices.len() < 2 {
        return 0.5;
    }
    let lo = prices.iter().cloned().fold(f64::INFINITY, f64::min);
    let hi = prices.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if (hi - lo).abs() < 1e-12 {
        return 0.5;
    }
    (prices[prices.len() - 1] - lo) / (hi - lo)
}

fn compute_skewness(returns: &[f64]) -> f64 {
    if returns.len() < 3 {
        return 0.0;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    if variance <= 0.0 {
        return 0.0;
    }
    let std = variance.sqrt();
    let skew = returns.iter().map(|r| (r - mean).powi(3)).sum::<f64>() / (n * std.powi(3));
    skew
}

fn compute_kurtosis(returns: &[f64]) -> f64 {
    if returns.len() < 4 {
        return 3.0;
    }
    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
    if variance <= 0.0 {
        return 3.0;
    }
    let std = variance.sqrt();
    let kurt = returns.iter().map(|r| (r - mean).powi(4)).sum::<f64>() / (n * std.powi(4));
    kurt
}

fn hurst_exponent(prices: &[f64]) -> f64 {
    if prices.len() < 100 {
        return 0.5;
    }
    let n = prices.len();
    let max_lag = (n / 4).min(50);
    if max_lag <= 2 {
        return 0.5;
    }

    let mut tau = Vec::new();
    for lag in 2..=max_lag {
        let sum: f64 = (lag..n)
            .map(|i| (prices[i] - prices[i - lag]).abs())
            .sum();
        let avg = sum / (n - lag) as f64;
        tau.push(avg);
    }

    if tau.is_empty() || tau[0] <= 0.0 {
        return 0.5;
    }

    let log_tau: Vec<f64> = tau.iter().map(|t| t.ln()).collect();
    let log_lag: Vec<f64> = (2..=max_lag).map(|l| (l as f64).ln()).collect();
    let m = log_tau.len() as f64;

    let mean_x = log_lag.iter().sum::<f64>() / m;
    let mean_y = log_tau.iter().sum::<f64>() / m;

    let num: f64 = log_lag.iter().zip(log_tau.iter())
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum();
    let den: f64 = log_lag.iter()
        .map(|x| (x - mean_x).powi(2))
        .sum();

    if den == 0.0 {
        return 0.5;
    }
    let h = num / den;
    h.clamp(0.0, 1.0)
}

fn serial_correlation(returns: &[f64], lag: usize) -> f64 {
    if returns.len() < lag + 2 {
        return 0.0;
    }
    let n = returns.len() - lag;
    let x: Vec<f64> = returns[..n].to_vec();
    let y: Vec<f64> = returns[lag..lag + n].to_vec();

    let mean_x = x.iter().sum::<f64>() / n as f64;
    let mean_y = y.iter().sum::<f64>() / n as f64;

    let num: f64 = x.iter().zip(y.iter())
        .map(|(xi, yi)| (xi - mean_x) * (yi - mean_y))
        .sum();
    let den_x: f64 = x.iter().map(|xi| (xi - mean_x).powi(2)).sum();
    let den_y: f64 = y.iter().map(|yi| (yi - mean_y).powi(2)).sum();
    let den = (den_x * den_y).sqrt();

    if den == 0.0 { 0.0 } else { num / den }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn test_classify_all_branches() {
        let det = RegimeDetector::new(14, 20, 50);

        // StrongUptrend: volatile + trending + ts>0
        let mut f = RegimeFeatures::new();
        f.adx = 30.0; f.volatility = 0.05; f.trend_strength = 0.1;
        assert_eq!(det.classify(&f), Regime::StrongUptrend);

        // StrongDowntrend: volatile + trending + ts<0
        f.trend_strength = -0.1;
        assert_eq!(det.classify(&f), Regime::StrongDowntrend);

        // WeakUptrend: trending (not volatile) + ts>0
        f.volatility = 0.01; f.trend_strength = 0.1;
        assert_eq!(det.classify(&f), Regime::WeakUptrend);

        // WeakDowntrend: trending (not volatile) + ts<0
        f.trend_strength = -0.1;
        assert_eq!(det.classify(&f), Regime::WeakDowntrend);

        // HighVolatility: volatile, not trending, hurst<=0.6
        f.adx = 10.0; f.volatility = 0.05; f.trend_strength = 0.1; f.hurst_exponent = 0.4;
        assert_eq!(det.classify(&f), Regime::HighVolatility);

        // HighVolatility via hurst>0.6 but |ts|<=0.01
        f.hurst_exponent = 0.7; f.trend_strength = 0.0;
        assert_eq!(det.classify(&f), Regime::HighVolatility);

        // Ranging
        f = RegimeFeatures::new();
        f.adx = 10.0; f.volatility = 0.01;
        assert_eq!(det.classify(&f), Regime::Ranging);

        // LowVolatility: not ranging, vol<0.01
        f.adx = 22.0; f.volatility = 0.005;
        assert_eq!(det.classify(&f), Regime::LowVolatility);

        // Unknown
        f.adx = 22.0; f.volatility = 0.02; f.trend_strength = 0.0;
        assert_eq!(det.classify(&f), Regime::Unknown);
    }

    #[test]
    fn test_detect_with_ohlc_sets_adx() {
        let closes: Vec<f64> = (0..60).map(|i| 100.0 + i as f64).collect();
        let highs: Vec<f64> = closes.iter().map(|c| c + 2.0).collect();
        let lows: Vec<f64> = closes.iter().map(|c| c - 2.0).collect();
        let det = RegimeDetector::new(14, 20, 50);
        let (regime, features) = det.detect(&closes, Some(&highs), Some(&lows), None);
        assert!(features.adx >= 0.0 && features.adx <= 100.0);
        assert!(regime != Regime::Unknown);
    }

    #[test]
    fn test_compute_features_short() {
        let det = RegimeDetector::new(14, 20, 50);
        let (regime, features) = det.detect(&[1.0, 2.0, 3.0], None, None, None);
        // <30 closes -> default features -> adx 0 (<20), vol 0 (<0.02) -> Ranging
        assert_eq!(regime, Regime::Ranging);
        assert_eq!(features.adx, 0.0);
    }

    #[test]
    fn test_compute_adx_edges() {
        let det = RegimeDetector::new(14, 20, 50);
        assert_eq!(det.compute_adx(&[1.0], &[1.0], &[1.0]), 25.0); // n<2
        let flat: Vec<f64> = vec![100.0; 30];
        let adx = det.compute_adx(&flat, &flat, &flat); // tr_sum 0
        assert_eq!(adx, 25.0);
    }

    #[test]
    fn test_helper_edges() {
        assert_eq!(compute_returns(&[1.0]).len(), 0);
        assert_eq!(compute_volatility(&[0.01]), 0.0);
        assert_eq!(compute_trend_strength(&[1.0; 10]), 0.0); // len<20
        // n>=50 vs else branch
        let long: Vec<f64> = (0..80).map(|i| 100.0 + i as f64 * 0.1).collect();
        assert!(compute_trend_strength(&long).is_finite());
        assert_eq!(compute_price_position(&[5.0]), 0.5);
        let flat = vec![5.0; 10];
        assert_eq!(compute_price_position(&flat), 0.5); // hi==lo
        assert_eq!(compute_skewness(&[0.01, 0.02]), 0.0); // len<3
        let c = vec![0.01; 5];
        assert_eq!(compute_skewness(&c), 0.0); // variance 0
        assert_eq!(compute_kurtosis(&[0.01, 0.02, 0.03]), 3.0); // len<4
        assert_eq!(compute_kurtosis(&c), 3.0); // variance 0
        assert_eq!(hurst_exponent(&[1.0; 50]), 0.5); // len<100
        assert_eq!(hurst_exponent(&[1.0; 5]), 0.5); // max_lag<=2
        let const_prices: Vec<f64> = vec![10.0; 100];
        assert_eq!(hurst_exponent(&const_prices), 0.5); // tau[0]<=0
        assert_eq!(serial_correlation(&[0.01], 1), 0.0); // len<lag+2
        let zero_var = vec![0.02; 6];
        assert_eq!(serial_correlation(&zero_var, 1), 0.0); // den 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regime_as_str() {
        assert_eq!(Regime::StrongUptrend.as_str(), "strong_uptrend");
        assert_eq!(Regime::Unknown.as_str(), "unknown");
    }

    #[test]
    fn test_recommended_strategies() {
        let strats = Regime::Ranging.recommended_strategies();
        assert!(strats.contains(&"rsi_revert"));
        assert!(strats.contains(&"boll_break"));
    }

    #[test]
    fn test_volatility() {
        let prices = vec![100.0, 101.0, 99.0, 102.0, 98.0, 103.0];
        let returns = compute_returns(&prices);
        let vol = compute_volatility(&returns);
        assert!(vol > 0.0);
    }

    #[test]
    fn test_trend_strength() {
        // Uptrend
        let uptrend: Vec<f64> = (0..100).map(|i| 100.0 + i as f64 * 0.5).collect();
        let ts = compute_trend_strength(&uptrend);
        assert!(ts > 0.0);

        // Downtrend
        let downtrend: Vec<f64> = (0..100).map(|i| 100.0 - i as f64 * 0.5).collect();
        let ts = compute_trend_strength(&downtrend);
        assert!(ts < 0.0);
    }

    #[test]
    fn test_price_position() {
        let prices = vec![90.0, 95.0, 100.0, 105.0, 110.0];
        let pos = compute_price_position(&prices);
        assert!((pos - 1.0).abs() < 0.01); // last = max = 110
    }

    #[test]
    fn test_skewness_kurtosis() {
        let returns = vec![0.01, -0.02, 0.015, -0.01, 0.005, -0.005];
        let skew = compute_skewness(&returns);
        assert!(skew.is_finite());
        let kurt = compute_kurtosis(&returns);
        assert!(kurt > 0.0);
    }

    #[test]
    fn test_hurst() {
        // Brownian motion should give H ~ 0.5
        let prices: Vec<f64> = (0..200).map(|i| (i as f64).sin()).collect();
        let h = hurst_exponent(&prices);
        assert!(h >= 0.0 && h <= 1.0);
    }

    #[test]
    fn test_serial_correlation() {
        let returns = vec![0.01, 0.02, 0.015, -0.01, -0.02, -0.015];
        let corr = serial_correlation(&returns, 1);
        assert!(corr.is_finite());
    }

    #[test]
    fn test_detect_ranging() {
        let prices: Vec<f64> = vec![100.0; 60];
        let detector = RegimeDetector::new(14, 20, 50);
        let (_regime, features) = detector.detect(&prices, None, None, None);
        assert_eq!(features.volatility, 0.0);
        assert_eq!(features.trend_strength, 0.0);
    }

    #[test]
    fn test_detect_uptrend() {
        let prices: Vec<f64> = (0..60).map(|i| 100.0 + i as f64).collect();
        let detector = RegimeDetector::new(14, 20, 50);
        let (_regime, features) = detector.detect(&prices, None, None, None);
        assert!(features.trend_strength > 0.0);
    }

    #[test]
    fn test_adx() {
        let closes: Vec<f64> = (0..30).map(|i| 100.0 + i as f64).collect();
        let highs: Vec<f64> = closes.iter().map(|c| c + 1.0).collect();
        let lows: Vec<f64> = closes.iter().map(|c| c - 1.0).collect();
        let detector = RegimeDetector::new(14, 20, 50);
        let adx = detector.compute_adx(&closes, &highs, &lows);
        assert!(adx >= 0.0 && adx <= 100.0);
    }

    #[test]
    fn test_features_properties() {
        let mut f = RegimeFeatures::new();
        f.adx = 30.0;
        f.trend_strength = 0.05;
        f.volatility = 0.04;
        assert!(f.is_trending());
        assert!(f.is_volatile());
        assert!(!f.is_ranging());
    }

    #[test]
    fn test_empty_data() {
        let detector = RegimeDetector::new(14, 20, 50);
        let (_regime, features) = detector.detect(&[], None, None, None);
        // All-zero features get classified as Ranging (adx<20, vol<0.02)
        assert!(!features.volatility.is_nan());
    }

    #[test]
    fn test_volume_trend() {
        let prices: Vec<f64> = (0..60).map(|i| 100.0 + (i as f64).sin()).collect();
        let volumes: Vec<f64> = (0..60).map(|i| 1000.0 + i as f64).collect();
        let detector = RegimeDetector::new(14, 20, 50);
        let (_, features) = detector.detect(&prices, None, None, Some(&volumes));
        // volume_trend may be finite
        assert!(features.volume_trend.is_finite());
    }
}
