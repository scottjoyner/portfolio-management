/// Vectorized technical indicators for trading strategies.
/// All functions operate on &[f64] slices and return Vec<f64> or f64.

/// Simple Moving Average — returns the SMA over `period` elements.
pub fn sma(values: &[f64], period: usize) -> f64 {
    if values.len() < period || period == 0 {
        return f64::NAN;
    }
    let slice = &values[values.len() - period..];
    slice.iter().sum::<f64>() / period as f64
}

/// SMA slice — returns full SMA series (first period-1 values are NAN).
pub fn sma_slice(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period || period == 0 {
        return vec![f64::NAN; n];
    }
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        if i < period - 1 {
            result.push(f64::NAN);
        } else {
            let slice = &values[i + 1 - period..=i];
            result.push(slice.iter().sum::<f64>() / period as f64);
        }
    }
    result
}

/// Exponential Moving Average — returns the EMA value at the last element.
pub fn ema(values: &[f64], period: usize) -> f64 {
    let n = values.len();
    if n < period || period == 0 {
        return f64::NAN;
    }
    let k = 2.0 / (period as f64 + 1.0);
    let mut result = values[..period].iter().sum::<f64>() / period as f64;
    for &v in &values[period..] {
        result = v * k + result * (1.0 - k);
    }
    result
}

/// Full EMA series — returns Vec<f64> with EMA values for each position.
/// First `period-1` values are NAN, index `period-1` onward are valid.
pub fn ema_slice(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period || period == 0 {
        return vec![f64::NAN; n];
    }
    let k = 2.0 / (period as f64 + 1.0);
    let mut result = Vec::with_capacity(n);
    let seed: f64 = values[..period].iter().sum::<f64>() / period as f64;
    for _ in 0..period - 1 {
        result.push(f64::NAN);
    }
    result.push(seed);
    for &v in &values[period..] {
        let prev = *result.last().unwrap();
        result.push(v * k + prev * (1.0 - k));
    }
    result
}

/// Get last two EMA values efficiently (avoids full slice allocation).
pub fn ema_last_two(values: &[f64], period: usize) -> (f64, f64) {
    let n = values.len();
    if n < period + 1 || period == 0 {
        return (f64::NAN, f64::NAN);
    }
    // Compute EMA up to n-2
    let k = 2.0 / (period as f64 + 1.0);
    let mut prev: f64 = values[..period].iter().sum::<f64>() / period as f64;
    for &v in &values[period..n - 1] {
        prev = v * k + prev * (1.0 - k);
    }
    // Compute current EMA
    let curr = values[n - 1] * k + prev * (1.0 - k);
    (prev, curr)
}

/// Triple EMA (for TRIX) — returns the full TRIX series.
pub fn trix_series(values: &[f64], period: usize) -> Vec<f64> {
    let ema1 = ema_slice(values, period);
    let ema2 = ema_slice(&ema1, period);
    let ema3 = ema_slice(&ema2, period);
    let n = ema3.len();
    if n < 2 {
        return vec![f64::NAN; n];
    }
    let mut result = Vec::with_capacity(n);
    result.push(f64::NAN);
    for i in 1..n {
        if ema3[i - 1].is_finite() && ema3[i].is_finite() && ema3[i - 1] != 0.0 {
            result.push((ema3[i] - ema3[i - 1]) / ema3[i - 1]);
        } else {
            result.push(f64::NAN);
        }
    }
    result
}

/// RSI — returns the RSI value at the last element.
pub fn rsi(values: &[f64], period: usize) -> f64 {
    let n = values.len();
    if n < period + 1 || period == 0 {
        return 50.0;
    }
    let mut gains = 0.0;
    let mut losses = 0.0;
    for i in n - period..n {
        let delta = values[i] - values[i - 1];
        if delta > 0.0 {
            gains += delta;
        } else {
            losses -= delta;
        }
    }
    let avg_gain = gains / period as f64;
    let avg_loss = losses / period as f64;
    if avg_loss == 0.0 {
        return 100.0;
    }
    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

/// Bollinger Bands — returns (lower, middle, upper, bandwidth).
pub fn bollinger(values: &[f64], period: usize, std_mult: f64) -> (f64, f64, f64, f64) {
    if values.len() < period || period == 0 {
        return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
    }
    let slice = &values[values.len() - period..];
    let mean = slice.iter().sum::<f64>() / period as f64;
    let variance = slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / period as f64;
    let std = variance.sqrt();
    let lower = mean - std_mult * std;
    let upper = mean + std_mult * std;
    let bandwidth = if mean != 0.0 { (upper - lower) / mean } else { 0.0 };
    (lower, mean, upper, bandwidth)
}

/// Z-Score — returns (zscore, mean, std).
pub fn zscore(values: &[f64], period: usize) -> (f64, f64, f64) {
    if values.len() < period || period == 0 {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    let slice = &values[values.len() - period..];
    let mean = slice.iter().sum::<f64>() / period as f64;
    let variance = slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / period as f64;
    let std = variance.sqrt();
    if std == 0.0 {
        return (0.0, mean, 0.0);
    }
    let z = (values[values.len() - 1] - mean) / std;
    (z, mean, std)
}

/// Weighted Moving Average — returns WMA over all values.
pub fn wma(values: &[f64]) -> f64 {
    let n = values.len();
    if n == 0 {
        return f64::NAN;
    }
    let weight_sum = (n * (n + 1)) / 2;
    if weight_sum == 0 {
        return 0.0;
    }
    let numerator: f64 = values.iter().enumerate().map(|(i, &v)| v * (i + 1) as f64).sum();
    numerator / weight_sum as f64
}

/// WMA slice — full series of WMA values over given period.
pub fn wma_slice(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period || period == 0 {
        return vec![f64::NAN; n];
    }
    let mut result = Vec::with_capacity(n);
    for i in 0..n {
        if i < period - 1 {
            result.push(f64::NAN);
        } else {
            let slice = &values[i + 1 - period..=i];
            let ws = (period * (period + 1)) / 2;
            let sum: f64 = slice.iter().enumerate().map(|(j, &v)| v * (j + 1) as f64).sum();
            result.push(sum / ws as f64);
        }
    }
    result
}

/// Average True Range — returns ATR over period.
pub fn atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> f64 {
    let n = closes.len();
    if n < period + 1 || period == 0 {
        return f64::NAN;
    }
    let mut tr_values = Vec::with_capacity(n - 1);
    for i in 1..n {
        let high_low = highs[i] - lows[i];
        let high_close = (highs[i] - closes[i - 1]).abs();
        let low_close = (lows[i] - closes[i - 1]).abs();
        tr_values.push(high_low.max(high_close).max(low_close));
    }
    let seed: f64 = tr_values[..period].iter().sum::<f64>() / period as f64;
    if tr_values.len() <= period {
        return seed;
    }
    let k = 1.0 / period as f64;
    let mut result = seed;
    for &tr in &tr_values[period..] {
        result = tr * k + result * (1.0 - k);
    }
    result
}

/// MACD — returns (macd_line, signal_line, histogram) values at the last element.
pub fn macd(closes: &[f64], fast: usize, slow: usize, signal: usize) -> (f64, f64, f64) {
    let n = closes.len();
    if n < slow + signal || fast == 0 || slow == 0 || signal == 0 {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    let ema_fast = ema_slice(closes, fast);
    let ema_slow = ema_slice(closes, slow);
    let macd_line = ema_fast[n - 1] - ema_slow[n - 1];
    if !macd_line.is_finite() {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    let mut macd_series = Vec::with_capacity(n);
    for i in 0..n {
        if ema_fast[i].is_finite() && ema_slow[i].is_finite() {
            macd_series.push(ema_fast[i] - ema_slow[i]);
        } else {
            macd_series.push(f64::NAN);
        }
    }
    let sig_line = ema(&macd_series, signal);
    (macd_line, sig_line, macd_line - sig_line)
}

/// Highest value over the last `period` elements.
pub fn highest(values: &[f64], period: usize) -> f64 {
    if values.len() < period || period == 0 {
        return f64::NAN;
    }
    let slice = &values[values.len() - period..];
    slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
}

/// Lowest value over the last `period` elements.
pub fn lowest(values: &[f64], period: usize) -> f64 {
    if values.len() < period || period == 0 {
        return f64::NAN;
    }
    let slice = &values[values.len() - period..];
    slice.iter().cloned().fold(f64::INFINITY, f64::min)
}

/// Returns the index of the highest value within the last `period` elements.
pub fn index_of_highest(values: &[f64], period: usize) -> usize {
    let n = values.len();
    if n < period || period == 0 {
        return 0;
    }
    let start = n - period;
    let mut max_idx = start;
    let mut max_val = values[start];
    for i in (start + 1)..n {
        if values[i] > max_val {
            max_val = values[i];
            max_idx = i;
        }
    }
    n - 1 - max_idx // days since high
}

/// Returns the index of the lowest value within the last `period` elements.
pub fn index_of_lowest(values: &[f64], period: usize) -> usize {
    let n = values.len();
    if n < period || period == 0 {
        return 0;
    }
    let start = n - period;
    let mut min_idx = start;
    let mut min_val = values[start];
    for i in (start + 1)..n {
        if values[i] < min_val {
            min_val = values[i];
            min_idx = i;
        }
    }
    n - 1 - min_idx // days since low
}

/// On-Balance Volume — full cumulative series.
pub fn obv_series(closes: &[f64], volumes: &[f64]) -> Vec<f64> {
    let n = closes.len().min(volumes.len());
    if n == 0 {
        return vec![];
    }
    let mut result = Vec::with_capacity(n);
    result.push(0.0); // first OBV is 0
    let mut obv = 0.0;
    for i in 1..n {
        if closes[i] > closes[i - 1] {
            obv += volumes[i];
        } else if closes[i] < closes[i - 1] {
            obv -= volumes[i];
        }
        result.push(obv);
    }
    result
}

/// Volume Price Trend — full cumulative series.
pub fn vpt_series(closes: &[f64], volumes: &[f64]) -> Vec<f64> {
    let n = closes.len().min(volumes.len());
    if n < 2 {
        return vec![0.0; n];
    }
    let mut result = Vec::with_capacity(n);
    result.push(0.0); // first VPT is 0
    let mut vpt = 0.0;
    for i in 1..n {
        if closes[i - 1] != 0.0 {
            vpt += volumes[i] * (closes[i] - closes[i - 1]) / closes[i - 1];
        }
        result.push(vpt);
    }
    result
}

/// Parabolic SAR — full series (stateless, computed from OHLC).
/// Returns Vec<f64> where each element is the SAR value at that bar.
pub fn sar_series(highs: &[f64], lows: &[f64], af_start: f64, af_inc: f64, af_max: f64) -> Vec<f64> {
    let n = highs.len().min(lows.len());
    if n < 2 {
        return vec![f64::NAN; n];
    }
    let mut sar = vec![f64::NAN; n];
    let mut ep: f64;
    let mut af = af_start;
    let mut uptrend: bool;

    // Determine initial trend from first 2 bars
    if highs[0] <= lows[0] {
        return sar;
    }
    if highs[1] > highs[0] {
        // First bar was a low → starting uptrend
        uptrend = true;
        sar[0] = lows[0];
        ep = highs[1];
        sar[1] = sar[0] + af * (ep - sar[0]);
    } else {
        // First bar was a high → starting downtrend
        uptrend = false;
        sar[0] = highs[0];
        ep = lows[1];
        sar[1] = sar[0] - af * (sar[0] - ep);
    }

    for i in 2..n {
        if uptrend {
            // SAR for uptrend
            let mut next_sar = sar[i - 1] + af * (ep - sar[i - 1]);
            // SAR cannot be above the prior two lows
            next_sar = next_sar.min(lows[i - 1]).min(lows[i - 2]);
            sar[i] = next_sar;

            // Check trend flip
            if next_sar >= lows[i] {
                // Flip to downtrend
                uptrend = false;
                ep = lows[i];
                af = af_start;
                sar[i] = ep;
            } else {
                // Update EP if new high
                if highs[i] > ep {
                    ep = highs[i];
                    af = (af + af_inc).min(af_max);
                }
            }
        } else {
            // SAR for downtrend
            let mut next_sar = sar[i - 1] - af * (sar[i - 1] - ep);
            // SAR cannot be below the prior two highs
            next_sar = next_sar.max(highs[i - 1]).max(highs[i - 2]);
            sar[i] = next_sar;

            // Check trend flip
            if next_sar <= highs[i] {
                // Flip to uptrend
                uptrend = true;
                ep = highs[i];
                af = af_start;
                sar[i] = ep;
            } else {
                // Update EP if new low
                if lows[i] < ep {
                    ep = lows[i];
                    af = (af + af_inc).min(af_max);
                }
            }
        }
    }
    sar
}

/// Wilder's smoothing — returns the smoothed value at the last element.
/// Formula: first `period` values averaged, then new = val*(1/p) + prev*((p-1)/p).
pub fn wilder_smooth(values: &[f64], period: usize) -> f64 {
    let n = values.len();
    if n < period || period == 0 {
        return f64::NAN;
    }
    let mut result = values[..period].iter().sum::<f64>() / period as f64;
    let k = 1.0 / period as f64;
    for &v in &values[period..] {
        result = v * k + result * (1.0 - k);
    }
    result
}

/// Full Wilder's smooth series — returns Vec<f64>.
pub fn wilder_smooth_slice(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period || period == 0 {
        return vec![f64::NAN; n];
    }
    let k = 1.0 / period as f64;
    let mut result = Vec::with_capacity(n);
    for _ in 0..period - 1 {
        result.push(f64::NAN);
    }
    let seed = values[..period].iter().sum::<f64>() / period as f64;
    result.push(seed);
    for &v in &values[period..] {
        let prev = *result.last().unwrap();
        result.push(v * k + prev * (1.0 - k));
    }
    result
}

/// Rate of Change — returns ROC at the last element.
pub fn roc(values: &[f64], period: usize) -> f64 {
    let n = values.len();
    if n < period + 1 || period == 0 || values[n - period - 1] == 0.0 {
        return f64::NAN;
    }
    (values[n - 1] - values[n - period - 1]) / values[n - period - 1]
}

/// Full ROC series — returns Vec<f64>.
pub fn roc_series(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period + 1 || period == 0 {
        return vec![f64::NAN; n];
    }
    let mut result = vec![f64::NAN; period];
    for i in period..n {
        result.push((values[i] - values[i - period]) / values[i - period]);
    }
    result
}

/// Candle body (absolute value).
pub fn candle_body(open: f64, close: f64) -> f64 {
    (close - open).abs()
}

/// Upper wick size.
pub fn candle_upper_wick(high: f64, _low: f64, open: f64, close: f64) -> f64 {
    let top = open.max(close);
    (high - top).max(0.0)
}

/// Lower wick size.
pub fn candle_lower_wick(_high: f64, low: f64, open: f64, close: f64) -> f64 {
    let bottom = open.min(close);
    (bottom - low).max(0.0)
}

/// Body-to-range ratio (0..1). Small values = doji-like.
pub fn body_to_range_ratio(open: f64, close: f64, high: f64, low: f64) -> f64 {
    let range = high - low;
    if range <= 0.0 {
        return 1.0;
    }
    (close - open).abs() / range
}

/// Detect a doji candle (body is small fraction of range).
pub fn is_doji(open: f64, close: f64, high: f64, low: f64, threshold: f64) -> bool {
    let range = high - low;
    range > 0.0 && (close - open).abs() / range < threshold
}

/// Returns indices and values of swing highs — bars where the center bar is the highest
/// among its `period` neighbors on each side.
pub fn swing_highs(highs: &[f64], period: usize) -> Vec<(usize, f64)> {
    let n = highs.len();
    if n < 2 * period + 1 {
        return vec![];
    }
    let mut swings = Vec::new();
    for i in period..n - period {
        let center = highs[i];
        let mut is_high = true;
        for j in 1..=period {
            if highs[i - j] >= center || highs[i + j] >= center {
                is_high = false;
                break;
            }
        }
        if is_high {
            swings.push((i, center));
        }
    }
    swings
}

/// Returns indices and values of swing lows — bars where the center bar is the lowest
/// among its `period` neighbors on each side.
pub fn swing_lows(lows: &[f64], period: usize) -> Vec<(usize, f64)> {
    let n = lows.len();
    if n < 2 * period + 1 {
        return vec![];
    }
    let mut swings = Vec::new();
    for i in period..n - period {
        let center = lows[i];
        let mut is_low = true;
        for j in 1..=period {
            if lows[i - j] <= center || lows[i + j] <= center {
                is_low = false;
                break;
            }
        }
        if is_low {
            swings.push((i, center));
        }
    }
    swings
}

/// Volume Profile result — point of control, value area, and bins.
#[derive(Debug, Clone)]
pub struct VolumeProfileResult {
    pub poc: f64,
    pub vah: f64,
    pub val: f64,
    pub poc_volume: f64,
    pub total_volume: f64,
    pub n_bins: usize,
    pub bin_width: f64,
    pub min_price: f64,
    pub max_price: f64,
}

/// Compute a simplified volume profile over the last `window` bars.
/// Divides the price range into `n_bins` levels and sums volume at each level.
pub fn volume_profile(closes: &[f64], volumes: &[f64], window: usize, n_bins: usize) -> Option<VolumeProfileResult> {
    let n = closes.len().min(volumes.len());
    if n < window || n_bins < 3 || window < 3 {
        return None;
    }
    let start = n - window;
    let slice = &closes[start..n];
    let min_price = slice.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_price = slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    if min_price >= max_price || min_price <= 0.0 {
        return None;
    }
    let bin_width = (max_price - min_price) / n_bins as f64;
    if bin_width <= 0.0 {
        return None;
    }

    // Bin volumes
    let mut bins = vec![0.0_f64; n_bins];
    let mut total_volume = 0.0;
    for i in start..n {
        let idx = ((closes[i] - min_price) / bin_width).floor() as usize;
        let idx = idx.min(n_bins - 1);
        bins[idx] += volumes[i];
        total_volume += volumes[i];
    }

    if total_volume <= 0.0 {
        return None;
    }

    // Find POC (max volume bin)
    let mut poc_idx = 0;
    let mut poc_volume = bins[0];
    for i in 1..n_bins {
        if bins[i] > poc_volume {
            poc_volume = bins[i];
            poc_idx = i;
        }
    }
    let poc = min_price + (poc_idx as f64 + 0.5) * bin_width;

    // Value Area: bins from POC outward until 70% volume captured
    let mut cum_vol = poc_volume;
    let target_vol = total_volume * 0.70;
    let mut left = poc_idx;
    let mut right = poc_idx;
    while cum_vol < target_vol && (left > 0 || right < n_bins - 1) {
        let left_vol = if left > 0 { bins[left - 1] } else { -1.0 };
        let right_vol = if right < n_bins - 1 { bins[right + 1] } else { -1.0 };
        if left_vol >= right_vol && left > 0 {
            left -= 1;
            cum_vol += bins[left];
        } else if right < n_bins - 1 {
            right += 1;
            cum_vol += bins[right];
        } else {
            break;
        }
    }
    let val = min_price + left as f64 * bin_width;
    let vah = min_price + (right + 1) as f64 * bin_width;

    Some(VolumeProfileResult { poc, vah, val, poc_volume, total_volume, n_bins, bin_width, min_price, max_price })
}

/// Rolling highest over the last `period` elements — returns full series.
pub fn highest_slice(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period || period == 0 {
        return vec![f64::NAN; n];
    }
    let mut result = Vec::with_capacity(n);
    for _ in 0..period - 1 {
        result.push(f64::NAN);
    }
    for i in period - 1..n {
        let slice = &values[i + 1 - period..=i];
        result.push(slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max));
    }
    result
}

/// Rolling lowest over the last `period` elements — returns full series.
pub fn lowest_slice(values: &[f64], period: usize) -> Vec<f64> {
    let n = values.len();
    if n < period || period == 0 {
        return vec![f64::NAN; n];
    }
    let mut result = Vec::with_capacity(n);
    for _ in 0..period - 1 {
        result.push(f64::NAN);
    }
    for i in period - 1..n {
        let slice = &values[i + 1 - period..=i];
        result.push(slice.iter().cloned().fold(f64::INFINITY, f64::min));
    }
    result
}

/// Money Flow Index — returns the MFI value at the last element.
/// Uses typical price = (high + low + close) / 3 and raw money flow = typical_price * volume.
/// RSI of positive/negative money flow over `period` bars.
pub fn mfi(highs: &[f64], lows: &[f64], closes: &[f64], volumes: &[f64], period: usize) -> f64 {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n < period + 1 || period == 0 {
        return 50.0;
    }
    let mut pos_flow = 0.0;
    let mut neg_flow = 0.0;
    let tp = |i: usize| (highs[i] + lows[i] + closes[i]) / 3.0;
    for i in n - period..n {
        let mf = tp(i) * volumes[i];
        if tp(i) > tp(i - 1) {
            pos_flow += mf;
        } else {
            neg_flow += mf;
        }
    }
    if neg_flow == 0.0 {
        return 100.0;
    }
    let mfr = pos_flow / neg_flow;
    100.0 - (100.0 / (1.0 + mfr))
}

/// Stochastic %K and %D — returns (%K, %D) at the last element.
/// %K = 100 * (close - lowest_n) / (highest_n - lowest_n)
/// %D = SMA(%K, d_period)
pub fn stochastic_kd(closes: &[f64], highs: &[f64], lows: &[f64], k_period: usize, d_period: usize) -> (f64, f64) {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < k_period + d_period || k_period == 0 || d_period == 0 {
        return (50.0, 50.0);
    }
    let high_slice = &highs[n - k_period..];
    let low_slice = &lows[n - k_period..];
    let highest_k = high_slice.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let lowest_k = low_slice.iter().cloned().fold(f64::INFINITY, f64::min);
    let range = highest_k - lowest_k;
    let k = if range > 0.0 {
        100.0 * (closes[n - 1] - lowest_k) / range
    } else {
        50.0
    };
    // Compute %D as SMA of last d_period %K values (rolling window)
    let mut k_values = Vec::with_capacity(d_period);
    for i in n - d_period..n {
        let start = i + 1 - k_period;
        let h = highs[start..=i].iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let l = lows[start..=i].iter().cloned().fold(f64::INFINITY, f64::min);
        let r = h - l;
        let k_i = if r > 0.0 { 100.0 * (closes[i] - l) / r } else { 50.0 };
        k_values.push(k_i);
    }
    let d = k_values.iter().sum::<f64>() / d_period as f64;
    (k, d)
}

/// Ease of Movement — returns the 1-period EMV value (un-smoothed).
/// Distance = ((high + low) / 2) - ((prev_high + prev_low) / 2)
/// Box Ratio = volume / (high - low) (scaled by 100_000 for normalization)
/// EMV = Distance / BoxRatio
pub fn emv(highs: &[f64], lows: &[f64], volumes: &[f64]) -> f64 {
    let n = highs.len().min(lows.len()).min(volumes.len());
    if n < 2 {
        return 0.0;
    }
    let mid = (highs[n - 1] + lows[n - 1]) / 2.0;
    let prev_mid = (highs[n - 2] + lows[n - 2]) / 2.0;
    let distance = mid - prev_mid;
    let range = highs[n - 1] - lows[n - 1];
    if range <= 0.0 {
        return 0.0;
    }
    let box_ratio = volumes[n - 1] / range / 100_000.0;
    if box_ratio == 0.0 {
        return 0.0;
    }
    distance / box_ratio
}

/// Accumulation/Distribution Line series — returns full Vec<f64>.
/// CLV = ((close - low) - (high - close)) / (high - low)
/// AD = cumulative sum of CLV * volume
pub fn ad_line_series(closes: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Vec<f64> {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n == 0 {
        return vec![];
    }
    let mut result = Vec::with_capacity(n);
    result.push(0.0);
    let mut ad = 0.0;
    for i in 1..n {
        let range = highs[i] - lows[i];
        let clv = if range > 0.0 {
            ((closes[i] - lows[i]) - (highs[i] - closes[i])) / range
        } else {
            0.0
        };
        ad += clv * volumes[i];
        result.push(ad);
    }
    result
}

/// True Range series — returns Vec<f64> of TR values for each bar (first is NAN).
pub fn true_range_series(highs: &[f64], lows: &[f64], closes: &[f64]) -> Vec<f64> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 2 {
        return vec![f64::NAN; n];
    }
    let mut result = Vec::with_capacity(n);
    result.push(f64::NAN);
    for i in 1..n {
        let hl = highs[i] - lows[i];
        let hc = (highs[i] - closes[i - 1]).abs();
        let lc = (lows[i] - closes[i - 1]).abs();
        result.push(hl.max(hc).max(lc));
    }
    result
}

/// Kaufman's Adaptive Moving Average — returns the KAMA value at the last element.
/// Fast SC = 2/(fast+1), Slow SC = 2/(slow+1). Default fast=2, slow=30.
pub fn kama(values: &[f64], period: usize, fast: usize, slow: usize) -> f64 {
    let n = values.len();
    if n < period + 1 || period < 2 || fast == 0 || slow == 0 {
        return f64::NAN;
    }
    let fast_sc = 2.0 / (fast as f64 + 1.0);
    let slow_sc = 2.0 / (slow as f64 + 1.0);
    // First value is SMA
    let mut kama_val = sma(&values[..period], period);
    for i in period..n {
        let change = (values[i] - values[i - period]).abs();
        let mut volatility = 0.0;
        for j in i - period + 1..=i {
            volatility += (values[j] - values[j - 1]).abs();
        }
        let er = if volatility > 0.0 { change / volatility } else { 0.0 };
        let sc = er * (fast_sc - slow_sc) + slow_sc;
        let sc_sq = sc * sc;
        kama_val = kama_val + sc_sq * (values[i] - kama_val);
    }
    kama_val
}

/// DI+ (Directional Indicator Plus) for DMI — returns the +DI value at the last element.
pub fn plus_di(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> f64 {
    let n = highs.len().min(lows.len()).min(closes.len());
    if n < period + 2 || period == 0 {
        return f64::NAN;
    }
    let tr = true_range_series(highs, lows, closes);
    let mut plus_dm_sum = 0.0;
    let mut tr_sum = 0.0;
    let start = n - period;
    for i in start..n {
        let up_move = highs[i] - highs[i - 1];
        let down_move = lows[i - 1] - lows[i];
        let pdm = if up_move > down_move && up_move > 0.0 { up_move } else { 0.0 };
        plus_dm_sum += pdm;
        tr_sum += tr[i];
    }
    if tr_sum == 0.0 { return 0.0; }
    100.0 * plus_dm_sum / tr_sum
}

/// DI- (Directional Indicator Minus) for DMI — returns the -DI value at the last element.
pub fn minus_di(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> f64 {
    let n = highs.len().min(lows.len()).min(closes.len());
    if n < period + 2 || period == 0 {
        return f64::NAN;
    }
    let tr = true_range_series(highs, lows, closes);
    let mut minus_dm_sum = 0.0;
    let mut tr_sum = 0.0;
    let start = n - period;
    for i in start..n {
        let up_move = highs[i] - highs[i - 1];
        let down_move = lows[i - 1] - lows[i];
        let ndm = if down_move > up_move && down_move > 0.0 { down_move } else { 0.0 };
        minus_dm_sum += ndm;
        tr_sum += tr[i];
    }
    if tr_sum == 0.0 { return 0.0; }
    100.0 * minus_dm_sum / tr_sum
}

/// Vortex Indicator — returns (VI_plus, VI_minus) at the last element.
pub fn vortex(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> (f64, f64) {
    let n = highs.len().min(lows.len()).min(closes.len());
    if n < period + 2 || period == 0 {
        return (f64::NAN, f64::NAN);
    }
    let tr = true_range_series(highs, lows, closes);
    let mut vm_plus_sum = 0.0;
    let mut vm_minus_sum = 0.0;
    let mut tr_sum = 0.0;
    let start = n - period;
    for i in start..n {
        vm_plus_sum += (highs[i] - lows[i - 1]).abs();
        vm_minus_sum += (lows[i] - highs[i - 1]).abs();
        tr_sum += tr[i];
    }
    if tr_sum == 0.0 { return (0.0, 0.0); }
    (vm_plus_sum / tr_sum, vm_minus_sum / tr_sum)
}

/// Relative Volatility Index — RVI(period). Like RSI but measures std dev direction.
pub fn rvi(values: &[f64], period: usize) -> f64 {
    let n = values.len();
    if n < period + 2 || period == 0 {
        return 50.0;
    }
    let mut up_std = 0.0_f64;
    let mut down_std = 0.0_f64;
    for i in n - period..n {
        let slice = &values[i + 1 - period..=i];
        let mean = slice.iter().sum::<f64>() / period as f64;
        let variance = slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / period as f64;
        let std = variance.sqrt();
        if values[i] > values[i - 1] {
            up_std += std;
        } else {
            down_std += std;
        }
    }
    if down_std == 0.0 { return 100.0; }
    100.0 - (100.0 / (1.0 + up_std / down_std))
}

/// Coppock Curve — long-term indicator: WMA period of ROC(14) + ROC(11).
pub fn coppock(values: &[f64], wma_period: usize) -> f64 {
    let n = values.len();
    if n < 15 + wma_period || wma_period == 0 {
        return f64::NAN;
    }
    let mut roc_series = Vec::with_capacity(n - 14);
    for i in 14..n {
        let roc14 = if values[i - 14] != 0.0 { (values[i] - values[i - 14]) / values[i - 14] } else { 0.0 };
        let roc11 = if i >= 11 && values[i - 11] != 0.0 { (values[i] - values[i - 11]) / values[i - 11] } else { 0.0 };
        roc_series.push((roc14 + roc11) * 100.0);
    }
    let m = roc_series.len();
    if m < wma_period {
        return f64::NAN;
    }
    let slice = &roc_series[m - wma_period..];
    let ws = (wma_period * (wma_period + 1)) / 2;
    if ws == 0 { return 0.0; }
    let sum: f64 = slice.iter().enumerate().map(|(j, &v)| v * (j + 1) as f64).sum();
    sum / ws as f64
}

/// Linear Regression Channel — returns (lower, middle, upper) where middle is the
/// regression value at the current bar, and bands are ±std_mult * standard error.
pub fn linreg_channel(values: &[f64], period: usize, std_mult: f64) -> (f64, f64, f64) {
    let n = values.len();
    if n < period || period < 3 {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    let slice = &values[n - period..];
    let sum_x = (period * (period - 1)) as f64 / 2.0;
    let sum_y: f64 = slice.iter().sum();
    let sum_xy: f64 = slice.iter().enumerate().map(|(i, &v)| i as f64 * v).sum();
    let sum_x2 = (period * (period - 1) * (2 * period - 1)) as f64 / 6.0;
    let denom = (period as f64) * sum_x2 - sum_x * sum_x;
    let (slope, intercept) = if denom != 0.0 {
        let s = ((period as f64) * sum_xy - sum_x * sum_y) / denom;
        let i_ = (sum_y - s * sum_x) / period as f64;
        (s, i_)
    } else {
        (0.0, sum_y / period as f64)
    };
    // Line value at the last bar (x = period - 1)
    let middle = intercept + slope * (period - 1) as f64;
    // Standard error
    let se = (slice.iter().enumerate().map(|(j, &v)| {
        let pred = intercept + slope * j as f64;
        (v - pred).powi(2)
    }).sum::<f64>() / period as f64).sqrt();
    (middle - std_mult * se, middle, middle + std_mult * se)
}

/// Volatility Ratio — current bar range / average range over `period` bars.
pub fn volatility_ratio(highs: &[f64], lows: &[f64], period: usize) -> f64 {
    let n = highs.len().min(lows.len());
    if n < period + 1 || period == 0 {
        return f64::NAN;
    }
    let current_range = highs[n - 1] - lows[n - 1];
    let mut sum_range = 0.0;
    for i in n - period..n - 1 {
        sum_range += highs[i] - lows[i];
    }
    let avg_range = sum_range / (period - 1) as f64;
    if avg_range == 0.0 { return 0.0; }
    current_range / avg_range
}

/// DeMarker oscillator — returns DeM(period) at the last element.
/// DeM = SMA(de_max) / (SMA(de_max) + SMA(de_min)), where
///   de_max = high - prev_high if > 0 else 0
///   de_min = prev_low - low if > 0 else 0
pub fn demarker(highs: &[f64], lows: &[f64], period: usize) -> f64 {
    let n = highs.len().min(lows.len());
    if n < period + 2 || period == 0 {
        return 0.5;
    }
    let mut dem_max_sum = 0.0;
    let mut dem_min_sum = 0.0;
    let start = n - period;
    for i in start..n {
        dem_max_sum += (highs[i] - highs[i - 1]).max(0.0);
        dem_min_sum += (lows[i - 1] - lows[i]).max(0.0);
    }
    let total = dem_max_sum + dem_min_sum;
    if total == 0.0 { return 0.5; }
    dem_max_sum / total
}

/// Simple linear regression slope over `period` values.
/// Returns slope (f64). Positive = upward trend, negative = downward.
pub fn linreg_slope(values: &[f64], period: usize) -> f64 {
    let n = values.len();
    if n < period || period < 2 {
        return f64::NAN;
    }
    let slice = &values[n - period..];
    let sum_x = (period * (period - 1)) as f64 / 2.0;
    let sum_y: f64 = slice.iter().sum();
    let sum_xy: f64 = slice.iter().enumerate().map(|(i, &v)| i as f64 * v).sum();
    let sum_x2 = (period * (period - 1) * (2 * period - 1)) as f64 / 6.0;
    let denom = (period as f64) * sum_x2 - sum_x * sum_x;
    if denom == 0.0 {
        return 0.0;
    }
    ((period as f64) * sum_xy - sum_x * sum_y) / denom
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sma() {
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert!((sma(&v, 3) - 4.0).abs() < 1e-10);
    }

    #[test]
    fn test_ema() {
        let v = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let e = ema(&v, 3);
        assert!(e.is_finite());
    }

    #[test]
    fn test_rsi() {
        let v: Vec<f64> = (0..30).map(|i| 100.0 + (i as f64 * 0.5)).collect();
        let r = rsi(&v, 14);
        assert!(r > 50.0);
    }

    #[test]
    fn test_highest_lowest() {
        let v = vec![3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0];
        assert!((highest(&v, 3) - 9.0).abs() < 1e-10);
        assert!((lowest(&v, 3) - 2.0).abs() < 1e-10);
    }

    #[test]
    fn test_obv() {
        let c = vec![10.0, 11.0, 10.5, 12.0];
        let v = vec![100.0, 200.0, 150.0, 300.0];
        let obv = obv_series(&c, &v);
        assert!((obv[1] - 200.0).abs() < 1e-10);
        assert!((obv[2] - 50.0).abs() < 1e-10);
    }

    #[test]
    fn test_sar() {
        let h = vec![10.0, 11.0, 12.0, 13.0, 14.0, 15.0];
        let l = vec![9.0, 10.0, 11.0, 12.0, 13.0, 14.0];
        let s = sar_series(&h, &l, 0.02, 0.02, 0.20);
        assert!(s.iter().all(|&x| x.is_finite() || x.is_nan()));
    }
}
