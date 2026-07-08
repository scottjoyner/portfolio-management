/// Trading strategy implementations as pure functions.
/// Each strategy takes price data slices and returns an optional signal.
/// All 25 technical strategies are implemented. Crossover detection uses
/// data slice comparison (no state tracking needed).

use crate::indicators;

#[derive(Debug, Clone)]
pub struct Signal {
    pub action: String,
    pub confidence: f64,
    pub reason: String,
}

/// ── Strategy 1: EMA Crossover ─────────────────────────────────────

pub fn ema_crossover(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 60 {
        return None;
    }
    let (prev_fast, curr_fast) = indicators::ema_last_two(closes, 9);
    let (prev_slow, curr_slow) = indicators::ema_last_two(closes, 21);
    if !prev_fast.is_finite() || !curr_fast.is_finite() || !prev_slow.is_finite() || !curr_slow.is_finite() {
        return None;
    }
    let bullish_cross = prev_fast <= prev_slow && curr_fast > curr_slow;
    let bearish_cross = prev_fast >= prev_slow && curr_fast < curr_slow;
    if bullish_cross {
        let conf = ((curr_fast - curr_slow) / curr_slow * 50.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "EMA bullish cross".into() })
    } else if bearish_cross {
        let conf = ((curr_slow - curr_fast) / curr_slow * 50.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "EMA bearish cross".into() })
    } else {
        None
    }
}

/// ── Strategy 2: RSI Mean Reversion ────────────────────────────────

pub fn rsi_mean_reversion(closes: &[f64]) -> Option<Signal> {
    let rsi_val = indicators::rsi(closes, 14);
    if rsi_val < 30.0 {
        let conf = ((30.0 - rsi_val) / 30.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("RSI oversold {:.1}", rsi_val) })
    } else if rsi_val > 70.0 {
        let conf = ((rsi_val - 70.0) / (100.0 - 70.0)).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("RSI overbought {:.1}", rsi_val) })
    } else {
        None
    }
}

/// ── Strategy 3: Bollinger Breakout ────────────────────────────────

pub fn bollinger_breakout(closes: &[f64]) -> Option<Signal> {
    let (lower, _mean, upper, _bw) = indicators::bollinger(closes, 20, 2.0);
    if !lower.is_finite() || !upper.is_finite() {
        return None;
    }
    let current_price = closes[closes.len() - 1];
    if current_price <= lower {
        let conf = ((lower - current_price) / lower * 20.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "Price below lower Bollinger band".into() })
    } else if current_price >= upper {
        let conf = ((current_price - upper) / upper * 20.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "Price above upper Bollinger band".into() })
    } else {
        None
    }
}

/// ── Strategy 4: Z-Score Reversion ─────────────────────────────────

pub fn zscore_reversion(closes: &[f64]) -> Option<Signal> {
    let (z, _mean, _std) = indicators::zscore(closes, 30);
    if !z.is_finite() {
        return None;
    }
    if z < -2.0 {
        let conf = (z.abs() / 4.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Z-score {:.2} extreme low", z) })
    } else if z > 2.0 {
        let conf = (z / 4.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Z-score {:.2} extreme high", z) })
    } else {
        None
    }
}

/// ── Strategy 5: Volume Momentum ───────────────────────────────────

pub fn volume_momentum(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 15 {
        return None;
    }
    let avg_vol: f64 = volumes[n - 14..].iter().sum::<f64>() / 14.0;
    let last_vol = volumes[n - 1];
    if last_vol < avg_vol * 1.5 {
        return None;
    }
    let price_change = (closes[n - 1] - closes[n - 15]) / closes[n - 15];
    if price_change > 0.05 {
        let conf = (price_change * 2.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Volume surge +{:.1}%", price_change * 100.0) })
    } else if price_change < -0.05 {
        let conf = (price_change.abs() * 2.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Volume surge {:.1}%", price_change * 100.0) })
    } else {
        None
    }
}

/// ── Strategy 6: MACD Crossover ────────────────────────────────────

pub fn macd_crossover(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 60 {
        return None;
    }
    let (_macd_line, _sig_line, hist) = indicators::macd(closes, 12, 26, 9);
    if !hist.is_finite() {
        return None;
    }
    // Compute prev histogram
    let prev_closes = &closes[..n - 1];
    let (_, _, prev_hist) = indicators::macd(prev_closes, 12, 26, 9);
    if !prev_hist.is_finite() {
        return None;
    }
    let bullish = prev_hist <= 0.0 && hist > 0.0;
    let bearish = prev_hist >= 0.0 && hist < 0.0;
    if bullish {
        let conf = (hist.abs() / _sig_line.abs().max(0.0001) * 2.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "MACD bullish cross".into() })
    } else if bearish {
        let conf = (hist.abs() / _sig_line.abs().max(0.0001) * 2.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "MACD bearish cross".into() })
    } else {
        None
    }
}

/// ── Strategy 7: VWAP Reversion ────────────────────────────────────

pub fn vwap_reversion(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 10 {
        return None;
    }
    let sum_pv: f64 = closes.iter().zip(volumes.iter()).map(|(p, v)| p * v).sum();
    let sum_v: f64 = volumes.iter().sum();
    if sum_v == 0.0 {
        return None;
    }
    let vwap = sum_pv / sum_v;
    let current_price = closes[n - 1];
    let deviation = (current_price - vwap) / vwap;
    if deviation < -0.03 {
        let conf = (deviation.abs() / 0.10).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("VWAP reversion: {:.1}% below VWAP", deviation * 100.0) })
    } else if deviation > 0.03 {
        let conf = (deviation / 0.10).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("VWAP reversion: {:.1}% above VWAP", deviation * 100.0) })
    } else {
        None
    }
}

/// ── Strategy 8: OBV Divergence ────────────────────────────────────

pub fn obv_divergence(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 15 {
        return None;
    }
    let obv = indicators::obv_series(closes, volumes);
    let lookback = 14.min(n - 1);
    let price_low = indicators::lowest(closes, lookback).min(closes[n - 1]);
    let price_high = indicators::highest(closes, lookback).max(closes[n - 1]);
    let obv_low = indicators::lowest(&obv, lookback);
    let obv_high = indicators::highest(&obv, lookback);
    let current_price = closes[n - 1];
    let current_obv = obv[n - 1];

    // Bullish divergence: price near low, OBV above its low
    if current_price <= price_low * 1.01 && current_obv > obv_low * 1.02 && obv_low != 0.0 {
        let divergence = ((current_obv - obv_low) / obv_low.abs().max(0.0001)).min(1.0);
        let conf = (divergence * 0.8).min(1.0);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: "Bullish OBV divergence: price low, OBV rising".into() });
    }
    // Bearish divergence: price near high, OBV below its high
    if current_price >= price_high * 0.99 && current_obv < obv_high * 0.98 && obv_high != 0.0 {
        let divergence = ((obv_high - current_obv) / obv_high.abs().max(0.0001)).min(1.0);
        let conf = (divergence * 0.8).min(1.0);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: "Bearish OBV divergence: price high, OBV falling".into() });
    }
    None
}

/// ── Strategy 9: Chande Momentum Oscillator ────────────────────────

pub fn chande_momentum(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 16 {
        return None;
    }
    let mut gains = 0.0;
    let mut losses = 0.0;
    for i in n - 14..n {
        let delta = closes[i] - closes[i - 1];
        if delta > 0.0 {
            gains += delta;
        } else {
            losses -= delta;
        }
    }
    let denom = gains + losses;
    if denom == 0.0 {
        return None;
    }
    let cmo = (gains - losses) / denom * 100.0;
    if cmo < -50.0 {
        let conf = ((-50.0 - cmo) / 50.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("CMO {:.0} oversold", cmo) })
    } else if cmo > 50.0 {
        let conf = ((cmo - 50.0) / 50.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("CMO {:.0} overbought", cmo) })
    } else {
        None
    }
}

/// ── Strategy 10: TRIX ─────────────────────────────────────────────

pub fn trix_signal(closes: &[f64]) -> Option<Signal> {
    let trix = indicators::trix_series(closes, 15);
    let n = trix.len();
    if n < 3 {
        return None;
    }
    let last = trix[n - 1];
    let prev = trix[n - 2];
    if !last.is_finite() || !prev.is_finite() {
        return None;
    }
    let trix_raw = last * 10000.0; // Python multiplies by 10000
    let conf = (trix_raw.abs() / 100.0).min(1.0);
    if last > 0.0 && prev < last {
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("TRIX positive rising {:.4}", last) })
    } else if last < 0.0 && prev > last {
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("TRIX negative falling {:.4}", last) })
    } else {
        None
    }
}

/// ── Strategy 11: ADX ──────────────────────────────────────────────

pub fn adx_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 30 {
        return None;
    }
    let period = 14;
    // Compute +DM, -DM, TR series
    let mut plus_dm = Vec::with_capacity(n - 1);
    let mut minus_dm = Vec::with_capacity(n - 1);
    let mut tr = Vec::with_capacity(n - 1);
    for i in 1..n {
        let up_move = highs[i] - highs[i - 1];
        let down_move = lows[i - 1] - lows[i];
        if up_move > down_move && up_move > 0.0 {
            plus_dm.push(up_move);
        } else {
            plus_dm.push(0.0);
        }
        if down_move > up_move && down_move > 0.0 {
            minus_dm.push(down_move);
        } else {
            minus_dm.push(0.0);
        }
        let high_low = highs[i] - lows[i];
        let high_close = (highs[i] - closes[i - 1]).abs();
        let low_close = (lows[i] - closes[i - 1]).abs();
        tr.push(high_low.max(high_close).max(low_close));
    }
    if tr.len() < period {
        return None;
    }
    // Wilder smooth +DM, -DM, TR
    let smoothed_plus = indicators::wilder_smooth(&plus_dm, period);
    let smoothed_minus = indicators::wilder_smooth(&minus_dm, period);
    let smoothed_tr = indicators::wilder_smooth(&tr, period);
    if smoothed_tr == 0.0 {
        return None;
    }
    let plus_di = smoothed_plus / smoothed_tr * 100.0;
    let minus_di = smoothed_minus / smoothed_tr * 100.0;

    // Compute full DX series for ADX smoothing
    let mut dx_series: Vec<f64> = Vec::new();
    let mut pd_smooth = plus_dm[..period].iter().sum::<f64>() / period as f64;
    let mut md_smooth = minus_dm[..period].iter().sum::<f64>() / period as f64;
    let mut tr_smooth = tr[..period].iter().sum::<f64>() / period as f64;
    for i in period..tr.len() {
        let pd = plus_dm[i];
        let md = minus_dm[i];
        let tr_val = tr[i];
        pd_smooth = pd * (1.0 / period as f64) + pd_smooth * ((period - 1) as f64 / period as f64);
        md_smooth = md * (1.0 / period as f64) + md_smooth * ((period - 1) as f64 / period as f64);
        tr_smooth = tr_val * (1.0 / period as f64) + tr_smooth * ((period - 1) as f64 / period as f64);
        if tr_smooth != 0.0 {
            let pdi = pd_smooth / tr_smooth * 100.0;
            let mdi = md_smooth / tr_smooth * 100.0;
            let d = (pdi - mdi).abs() / (pdi + mdi).max(0.0001) * 100.0;
            dx_series.push(d);
        }
    }
    if dx_series.len() < period as usize {
        return None;
    }
    let adx = indicators::wilder_smooth(&dx_series, period);
    if !adx.is_finite() || adx < 25.0 {
        return None;
    }

    // We also need previous +DI/-DI for crossover detection
    // Compute one step back
    let mut pd_prev = plus_dm[..period].iter().sum::<f64>() / period as f64;
    let mut md_prev = minus_dm[..period].iter().sum::<f64>() / period as f64;
    let mut tr_prev = tr[..period].iter().sum::<f64>() / period as f64;
    // Walk to n-2 (skip last)
    let end = tr.len().saturating_sub(1);
    for i in period..end {
        let pd_val = plus_dm[i];
        let md_val = minus_dm[i];
        let tr_v = tr[i];
        pd_prev = pd_val * (1.0 / period as f64) + pd_prev * ((period - 1) as f64 / period as f64);
        md_prev = md_val * (1.0 / period as f64) + md_prev * ((period - 1) as f64 / period as f64);
        tr_prev = tr_v * (1.0 / period as f64) + tr_prev * ((period - 1) as f64 / period as f64);
    }
    let prev_plus_di = if tr_prev != 0.0 { pd_prev / tr_prev * 100.0 } else { 0.0 };
    let prev_minus_di = if tr_prev != 0.0 { md_prev / tr_prev * 100.0 } else { 0.0 };

    let conf = ((adx - 25.0) / 50.0).min(1.0);
    let bullish = prev_plus_di <= prev_minus_di && plus_di > minus_di;
    let bearish = prev_plus_di >= prev_minus_di && plus_di < minus_di;
    if prev_plus_di == 0.0 && prev_minus_di == 0.0 {
        return None;
    }
    if bullish {
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("ADX {:.1} +DI crossover (trend up)", adx) })
    } else if bearish {
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("ADX {:.1} -DI crossover (trend down)", adx) })
    } else {
        None
    }
}

/// ── Strategy 12: Keltner Channels ─────────────────────────────────

pub fn keltner_channels(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 25 {
        return None;
    }
    let ema_mid = indicators::ema(closes, 20);
    let atr_val = indicators::atr(highs, lows, closes, 14);
    if !ema_mid.is_finite() || !atr_val.is_finite() || atr_val <= 0.0 {
        return None;
    }
    let upper = ema_mid + atr_val * 2.0;
    let lower = ema_mid - atr_val * 2.0;
    let current_price = closes[n - 1];

    if current_price > upper {
        let conf = ((current_price - upper) / (atr_val * 2.0).max(0.0001)).min(1.0).max(0.1);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Keltner breakout above {:.4}", upper) })
    } else if current_price < lower {
        let conf = ((lower - current_price) / (atr_val * 2.0).max(0.0001)).min(1.0).max(0.1);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Keltner breakdown below {:.4}", lower) })
    } else {
        None
    }
}

/// ── Strategy 13: Chaikin Money Flow ──────────────────────────────

pub fn chaikin_money_flow(closes: &[f64], volumes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len()).min(highs.len()).min(lows.len());
    if n < 22 {
        return None;
    }
    let period = 21;
    let mut mfv_sum = 0.0;
    let mut vol_sum = 0.0;
    for i in n - period..n {
        let range = highs[i] - lows[i];
        if range == 0.0 {
            continue;
        }
        let mfm = ((closes[i] - lows[i]) - (highs[i] - closes[i])) / range;
        mfv_sum += mfm * volumes[i];
        vol_sum += volumes[i];
    }
    if vol_sum == 0.0 {
        return None;
    }
    let cmf = mfv_sum / vol_sum;
    if cmf > 0.1 {
        let conf = (cmf / 0.3).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("CMF {:.2} accumulation", cmf) })
    } else if cmf < -0.1 {
        let conf = (cmf.abs() / 0.3).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("CMF {:.2} distribution", cmf) })
    } else {
        None
    }
}

/// ── Strategy 14: Williams %R ─────────────────────────────────────

pub fn williams_r_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 15 {
        return None;
    }
    let highest_h = indicators::highest(highs, 14);
    let lowest_l = indicators::lowest(lows, 14);
    let range = highest_h - lowest_l;
    if range == 0.0 {
        return None;
    }
    let wr = (highest_h - closes[n - 1]) / range * -100.0;
    if wr < -80.0 {
        let conf = ((-80.0 - wr) / 80.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Williams %R {:.0} oversold", wr) })
    } else if wr > -20.0 {
        let conf = ((wr + 20.0) / 20.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Williams %R {:.0} overbought", wr) })
    } else {
        None
    }
}

/// ── Strategy 15: Parabolic SAR ────────────────────────────────────

pub fn parabolic_sar(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 5 {
        return None;
    }
    let sar = indicators::sar_series(highs, lows, 0.02, 0.02, 0.20);
    if sar.len() < 3 {
        return None;
    }
    // Determine current and previous trend from SAR position relative to price
    let curr_sar = sar[sar.len() - 1];
    let prev_sar = sar[sar.len() - 2];
    if !curr_sar.is_finite() || !prev_sar.is_finite() {
        return None;
    }
    let current_price = closes[n - 1];
    let prev_price = closes[n - 2];

    // Trend is UP when price > SAR
    let curr_trend_up = current_price > curr_sar;
    let prev_trend_up = prev_price > prev_sar;

    if curr_trend_up && !prev_trend_up {
        let conf = ((current_price - curr_sar) / curr_sar * 10.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("PSAR reversal: uptrend (SAR={:.4})", curr_sar) })
    } else if !curr_trend_up && prev_trend_up {
        let conf = ((curr_sar - current_price) / curr_sar * 10.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("PSAR reversal: downtrend (SAR={:.4})", curr_sar) })
    } else {
        None
    }
}

/// ── Strategy 16: Hull Moving Average ──────────────────────────────

pub fn hull_ma(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 30 {
        return None;
    }
    // Compute HMA(9) and HMA(21) — need full WMA series
    let fast_half = 4.max(9 / 2); // sqrt(9) ≈ 5, but we need WMA of raw
    let slow_half = 10.max(21 / 2);

    // HMA = WMA(2 * WMA(n/2) - WMA(n), sqrt(n))
    let wma_full_9 = indicators::wma_slice(closes, 9);
    let wma_half_4 = indicators::wma_slice(closes, fast_half);
    let mut raw_9: Vec<f64> = Vec::with_capacity(n);
    let sqrt_9 = (9.0_f64).sqrt().ceil() as usize;
    for i in 0..n {
        if wma_half_4[i].is_finite() && wma_full_9[i].is_finite() {
            raw_9.push(2.0 * wma_half_4[i] - wma_full_9[i]);
        } else {
            raw_9.push(f64::NAN);
        }
    }
    let hma_fast = indicators::wma_slice(&raw_9, sqrt_9); // HMA(9)

    let wma_full_21 = indicators::wma_slice(closes, 21);
    let wma_half_10 = indicators::wma_slice(closes, slow_half);
    let mut raw_21: Vec<f64> = Vec::with_capacity(n);
    let sqrt_21 = (21.0_f64).sqrt().ceil() as usize;
    for i in 0..n {
        if wma_half_10[i].is_finite() && wma_full_21[i].is_finite() {
            raw_21.push(2.0 * wma_half_10[i] - wma_full_21[i]);
        } else {
            raw_21.push(f64::NAN);
        }
    }
    let hma_slow = indicators::wma_slice(&raw_21, sqrt_21); // HMA(21)

    // Get last two valid values for crossover detection
    let mut last_fast = f64::NAN;
    let mut prev_fast = f64::NAN;
    let mut last_slow = f64::NAN;
    let mut prev_slow = f64::NAN;
    for i in (0..n).rev() {
        if hma_fast[i].is_finite() {
            if last_fast.is_nan() { last_fast = hma_fast[i]; }
            else if prev_fast.is_nan() { prev_fast = hma_fast[i]; break; }
        }
    }
    for i in (0..n).rev() {
        if hma_slow[i].is_finite() {
            if last_slow.is_nan() { last_slow = hma_slow[i]; }
            else if prev_slow.is_nan() { prev_slow = hma_slow[i]; break; }
        }
    }

    if !last_fast.is_finite() || !prev_fast.is_finite() || !last_slow.is_finite() || !prev_slow.is_finite() {
        return None;
    }
    if prev_fast <= 0.0 || prev_slow <= 0.0 {
        return None; // Need at least one prior value to detect crossover
    }
    let bullish_cross = prev_fast <= prev_slow && last_fast > last_slow;
    let bearish_cross = prev_fast >= prev_slow && last_fast < last_slow;
    if bullish_cross {
        let conf = ((last_fast - last_slow) / last_slow * 30.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("HMA crossover {:.4}/{:.4}", last_fast, last_slow) })
    } else if bearish_cross {
        let conf = ((last_slow - last_fast) / last_slow * 30.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("HMA crossover {:.4}/{:.4}", last_fast, last_slow) })
    } else {
        None
    }
}

/// ── Strategy 17: Force Index ──────────────────────────────────────

pub fn force_index(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 15 {
        return None;
    }
    // FI(1) = V * (C - C_prev)
    let mut fi_series: Vec<f64> = Vec::with_capacity(n - 1);
    for i in 1..n {
        fi_series.push(volumes[i] * (closes[i] - closes[i - 1]));
    }
    // Smoothed = EMA(FI, 13)
    let smoothed = indicators::ema(&fi_series, 13);
    if !smoothed.is_finite() {
        return None;
    }
    // Previous smoothed
    let prev = &fi_series[..fi_series.len() - 1];
    let prev_smoothed = indicators::ema(prev, 13);
    if !prev_smoothed.is_finite() || prev_smoothed == 0.0 {
        return None;
    }
    let bullish = prev_smoothed <= 0.0 && smoothed > 0.0;
    let bearish = prev_smoothed >= 0.0 && smoothed < 0.0;
    if bullish {
        let conf = (smoothed / prev_smoothed.abs().max(0.0001)).min(1.0).max(0.1);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "Force Index bullish crossover".into() })
    } else if bearish {
        let conf = (smoothed.abs() / prev_smoothed.abs().max(0.0001)).min(1.0).max(0.1);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "Force Index bearish crossover".into() })
    } else {
        None
    }
}

/// ── Strategy 18: Volume Price Trend ───────────────────────────────

pub fn volume_price_trend(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 25 {
        return None;
    }
    let vpt = indicators::vpt_series(closes, volumes);
    if vpt.len() < 22 {
        return None;
    }
    let vpt_ema = indicators::ema(&vpt, 21);
    if !vpt_ema.is_finite() {
        return None;
    }
    let current_diff = vpt[vpt.len() - 1] - vpt_ema;
    // Previous diff
    let prev_vpt = &vpt[..vpt.len() - 1];
    let prev_vpt_ema = indicators::ema(prev_vpt, 21);
    if !prev_vpt_ema.is_finite() {
        return None;
    }
    let prev_diff = prev_vpt[prev_vpt.len() - 1] - prev_vpt_ema;
    if prev_diff == 0.0 {
        return None;
    }
    let bullish = prev_diff <= 0.0 && current_diff > 0.0;
    let bearish = prev_diff >= 0.0 && current_diff < 0.0;
    if bullish {
        let conf = ((current_diff / vpt_ema.abs().max(0.0001)) * 2.0).min(1.0).max(0.1);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "VPT bullish (accumulation)".into() })
    } else if bearish {
        let conf = ((current_diff.abs() / vpt_ema.abs().max(0.0001)) * 2.0).min(1.0).max(0.1);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "VPT bearish (distribution)".into() })
    } else {
        None
    }
}

/// ── Strategy 19: Donchian Channels ────────────────────────────────

pub fn donchian_channels(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 21 {
        return None;
    }
    let upper = indicators::highest(highs, 20);
    let lower = indicators::lowest(lows, 20);
    let range = upper - lower;
    if range == 0.0 {
        return None;
    }
    let current_price = closes[n - 1];
    if current_price > upper {
        let conf = ((current_price - upper) / range).min(1.0).max(0.1);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Donchian breakout above {:.4}", upper) })
    } else if current_price < lower {
        let conf = ((lower - current_price) / range).min(1.0).max(0.1);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Donchian breakdown below {:.4}", lower) })
    } else {
        None
    }
}

/// ── Strategy 20: Aroon ────────────────────────────────────────────

pub fn aroon_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 27 {
        return None;
    }
    let period = 25;
    let days_since_high = indicators::index_of_highest(highs, period);
    let days_since_low = indicators::index_of_lowest(lows, period);
    let aroon_up = (period - days_since_high) as f64 / period as f64 * 100.0;
    let aroon_down = (period - days_since_low) as f64 / period as f64 * 100.0;
    let osc = aroon_up - aroon_down;

    // Previous values (shift window back by 1)
    let prev_highs = &highs[..n - 1];
    let prev_lows = &lows[..n - 1];
    let prev_since_high = indicators::index_of_highest(prev_highs, period);
    let prev_since_low = indicators::index_of_lowest(prev_lows, period);
    let prev_aroon_up = (period - prev_since_high) as f64 / period as f64 * 100.0;
    let prev_aroon_down = (period - prev_since_low) as f64 / period as f64 * 100.0;
    let prev_osc = prev_aroon_up - prev_aroon_down;

    if prev_osc == 0.0 {
        return None;
    }
    let bullish = prev_osc <= 0.0 && osc > 0.0 && aroon_up > 50.0;
    let bearish = prev_osc >= 0.0 && osc < 0.0 && aroon_down > 50.0;
    if bullish {
        let conf = (aroon_up / 100.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Aroon bullish (up={:.0} down={:.0})", aroon_up, aroon_down) })
    } else if bearish {
        let conf = (aroon_down / 100.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Aroon bearish (up={:.0} down={:.0})", aroon_up, aroon_down) })
    } else {
        None
    }
}

/// ── Strategy 21: Price Efficiency Ratio ───────────────────────────

pub fn price_efficiency_ratio(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 25 {
        return None;
    }
    let period = 21;
    // Compute efficiency series
    let mut eff_series: Vec<f64> = Vec::with_capacity(n);
    let mut sum_p = 0.0;
    let mut sum_v = 0.0;
    for i in 0..n {
        sum_p += closes[i];
        sum_v += volumes[i];
        if i == 0 {
            eff_series.push(1.0);
        } else {
            let p_mean = sum_p / (i + 1) as f64;
            let v_mean = sum_v / (i + 1) as f64;
            if p_mean > 0.0 && v_mean > 0.0 {
                let eff = closes[i] * volumes[i] / (p_mean * v_mean.sqrt());
                eff_series.push(eff);
            } else {
                eff_series.push(1.0);
            }
        }
    }
    // WMA smooth the efficiency series
    let output_len = eff_series.len();
    if output_len < period + 7 {
        return None;
    }
    let wma_window = 7;
    // Compute last two WMA values
    let get_wma_last = |data: &[f64], win: usize| -> (f64, f64) {
        let sz = data.len();
        if sz < win + 1 {
            return (f64::NAN, f64::NAN);
        }
        let ws = (win * (win + 1)) / 2;
        let prev: f64 = data[sz - win - 1..sz - 1].iter()
            .enumerate().map(|(j, &v)| v * (j + 1) as f64).sum::<f64>() / ws as f64;
        let curr: f64 = data[sz - win..].iter()
            .enumerate().map(|(j, &v)| v * (j + 1) as f64).sum::<f64>() / ws as f64;
        (prev, curr)
    };
    let (prev_eff, curr_eff) = get_wma_last(&eff_series, wma_window);
    if !prev_eff.is_finite() || !curr_eff.is_finite() || prev_eff == 0.0 {
        return None;
    }
    let threshold = 0.8;
    if prev_eff <= threshold && curr_eff > threshold {
        let conf = ((curr_eff - threshold) / (1.0 - threshold)).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Efficiency bullish ({:.2})", curr_eff) })
    } else if prev_eff >= threshold && curr_eff < threshold {
        let conf = ((threshold - curr_eff) / threshold).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Efficiency bearish ({:.2})", curr_eff) })
    } else {
        None
    }
}

/// ── Strategy 22: Simplified CCI ───────────────────────────────────

pub fn simplified_cci(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 29 {
        return None;
    }
    let sma_val = indicators::sma(closes, 28);
    if !sma_val.is_finite() || sma_val == 0.0 {
        return None;
    }
    let cci = (closes[n - 1] / sma_val - 1.0) * 100.0;
    if cci > 30.0 {
        let conf = ((cci - 30.0) / 50.0).min(1.0).max(0.1);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("sCCI {:.1} overbought", cci) })
    } else if cci < -30.0 {
        let conf = ((-cci - 30.0) / 50.0).min(1.0).max(0.1);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("sCCI {:.1} oversold", cci) })
    } else {
        None
    }
}

/// ── Strategy 23: Range Expansion Index ────────────────────────────

pub fn range_expansion_index(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 23 {
        return None;
    }
    let period = 21;
    let current_high = indicators::highest(highs, period);
    let current_low = indicators::lowest(lows, period);
    let prev_high = highs[n - period - 1];
    let prev_low = lows[n - period - 1];

    let range_expansion = if prev_high != 0.0 { (current_high - prev_high) / prev_high } else { 0.0 };
    let range_contraction = if prev_low != 0.0 { (prev_low - current_low) / prev_low } else { 0.0 };
    let rei = range_expansion + range_contraction;

    // Previous REI (shift back by 1)
    if n < period + 2 {
        return None;
    }
    let prev_h = indicators::highest(&highs[..n - 1], period);
    let prev_l = indicators::lowest(&lows[..n - 1], period);
    let prev_prev_h = highs[n - period - 2];
    let prev_prev_l = lows[n - period - 2];
    let prev_re_exp = if prev_prev_h != 0.0 { (prev_h - prev_prev_h) / prev_prev_h } else { 0.0 };
    let prev_re_contr = if prev_prev_l != 0.0 { (prev_prev_l - prev_l) / prev_prev_l } else { 0.0 };
    let prev_rei = prev_re_exp + prev_re_contr;

    if prev_rei == 0.0 || rei.abs() <= 0.05 {
        return None;
    }
    let bullish = prev_rei <= 0.0 && rei > 0.0;
    let bearish = prev_rei >= 0.0 && rei < 0.0;
    let conf = (rei.abs() / 0.05).min(1.0).max(0.1);
    if bullish {
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "REI bullish (range expanding)".into() })
    } else if bearish {
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "REI bearish (range contracting)".into() })
    } else {
        None
    }
}

/// ── Strategy 24: EMA Deviation ────────────────────────────────────

pub fn ema_deviation(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 16 {
        return None;
    }
    let ema_val = indicators::ema(closes, 14);
    if !ema_val.is_finite() || ema_val == 0.0 {
        return None;
    }
    let current_price = closes[n - 1];
    let dev = (current_price - ema_val) / ema_val;

    // Previous deviation
    let prev_closes = &closes[..n - 1];
    let prev_ema = indicators::ema(prev_closes, 14);
    if !prev_ema.is_finite() || prev_ema == 0.0 {
        return None;
    }
    let prev_dev = (closes[n - 2] - prev_ema) / prev_ema;

    if prev_dev == 0.0 || dev.abs() <= 0.05 {
        return None;
    }
    let bullish = prev_dev <= 0.0 && dev > 0.0;
    let bearish = prev_dev >= 0.0 && dev < 0.0;
    let conf = (dev.abs() / 0.05).min(1.0).max(0.1);
    if bullish {
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("EMA deviation bullish ({:.3})", dev) })
    } else if bearish {
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("EMA deviation bearish ({:.3})", dev) })
    } else {
        None
    }
}

/// ── Strategy 25: Signal-to-Noise Ratio ────────────────────────────

pub fn signal_to_noise_ratio(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 16 {
        return None;
    }
    let period = 14;
    let slice = &closes[n - period..];
    let mean_close: f64 = slice.iter().sum::<f64>() / period as f64;
    if mean_close == 0.0 {
        return None;
    }
    let variance: f64 = slice.iter().map(|v| (v - mean_close).powi(2)).sum::<f64>() / period as f64;
    let std = variance.sqrt();
    if std == 0.0 {
        return None;
    }
    let price_change_pct = (closes[n - 1] - closes[n - period]) / closes[n - period] * 100.0;
    let direction_snr = price_change_pct / (std / mean_close * 100.0);

    // Previous SNR
    let prev_slice = &closes[n - period - 1..n - 1];
    let prev_mean: f64 = prev_slice.iter().sum::<f64>() / period as f64;
    let prev_var: f64 = prev_slice.iter().map(|v| (v - prev_mean).powi(2)).sum::<f64>() / period as f64;
    let prev_std = prev_var.sqrt();
    let prev_change = (closes[n - 2] - closes[n - period - 1]) / closes[n - period - 1] * 100.0;
    let prev_snr = if prev_std > 0.0 && prev_mean > 0.0 {
        prev_change / (prev_std / prev_mean * 100.0)
    } else {
        0.0
    };

    if prev_snr == 0.0 {
        return None;
    }
    let bullish = prev_snr <= 0.0 && direction_snr > 1.0;
    let bearish = prev_snr >= 0.0 && direction_snr < -1.0;
    let conf = (direction_snr.abs()).min(1.0).max(0.1);
    if bullish {
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("SNR bullish conviction ({:.2})", direction_snr) })
    } else if bearish {
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("SNR bearish conviction ({:.2})", direction_snr) })
    } else {
        None
    }
}

/// ── Strategy 26: Candlestick Pattern Recognition ─────────────────

pub fn candlestick_patterns(closes: &[f64], opens: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(opens.len()).min(highs.len()).min(lows.len());
    if n < 5 {
        return None;
    }
    let o = opens[n - 1];
    let c = closes[n - 1];
    let h = highs[n - 1];
    let l = lows[n - 1];
    let body = indicators::candle_body(o, c);
    let range = h - l;
    if range <= 0.0 {
        return None;
    }
    let body_ratio = body / range;
    let upper_wick = indicators::candle_upper_wick(h, l, o, c);
    let lower_wick = indicators::candle_lower_wick(h, l, o, c);
    let _wick_ratio = upper_wick.max(lower_wick) / range;

    // Doji: tiny body
    if body_ratio < 0.08 && range > 0.0 {
        return Some(Signal { action: "HOLD".into(), confidence: 0.3, reason: "Doji pattern".into() });
    }

    // Hammer: lower wick > 2x body, small upper wick, at bottom of downtrend
    if lower_wick > body * 2.0 && upper_wick < body && c > o && n >= 10 {
        let prev_close = closes[n - 2];
        let prev_open = if n >= 2 { opens[n - 2] } else { prev_close };
        if prev_close < prev_open && closes[n - 5] > closes[n - 1] {
            let conf = (lower_wick / range).min(1.0);
            return Some(Signal { action: "BUY".into(), confidence: conf, reason: "Hammer pattern".into() });
        }
    }

    // Shooting Star: upper wick > 2x body, small lower wick, at top of uptrend
    if upper_wick > body * 2.0 && lower_wick < body && c < o && n >= 10 {
        let prev_close = closes[n - 2];
        let prev_open = if n >= 2 { opens[n - 2] } else { prev_close };
        if prev_close > prev_open && closes[n - 5] < closes[n - 1] {
            let conf = (upper_wick / range).min(1.0);
            return Some(Signal { action: "SELL".into(), confidence: conf, reason: "Shooting star pattern".into() });
        }
    }

    // Bullish Engulfing: current open < prev close, current close > prev open, current body engulfs prev
    if n >= 3 {
        let po = opens[n - 2];
        let pc = closes[n - 2];
        let prev_body = indicators::candle_body(po, pc);
        if o <= pc && c >= po && body > prev_body * 1.1 && c > o && pc < po {
            let conf = (body / range).min(1.0).max(0.3);
            return Some(Signal { action: "BUY".into(), confidence: conf, reason: "Bullish engulfing".into() });
        }
        // Bearish Engulfing
        if o >= pc && c <= po && body > prev_body * 1.1 && c < o && pc > po {
            let conf = (body / range).min(1.0).max(0.3);
            return Some(Signal { action: "SELL".into(), confidence: conf, reason: "Bearish engulfing".into() });
        }
    }

    // Morning Star (3-bar reversal): bear candle, small body, bull candle
    if n >= 4 {
        let c1 = closes[n - 3]; let o1 = opens[n - 3]; let h1 = highs[n - 3]; let l1 = lows[n - 3];
        let c2 = closes[n - 2]; let o2 = opens[n - 2];
        let b1 = indicators::candle_body(o1, c1);
        let b2 = indicators::candle_body(o2, c2);
        let r1 = h1 - l1;
        if c1 < o1 && b1 / r1.max(0.0001) > 0.5 && b2 / b1.max(0.0001) < 0.3 && c > o && c > (o1 + c1) / 2.0 {
            return Some(Signal { action: "BUY".into(), confidence: 0.6, reason: "Morning star".into() });
        }
        // Evening Star (3-bar reversal): bull candle, small body, bear candle
        if c1 > o1 && b1 / r1.max(0.0001) > 0.5 && b2 / b1.max(0.0001) < 0.3 && c < o && c < (o1 + c1) / 2.0 {
            return Some(Signal { action: "SELL".into(), confidence: 0.6, reason: "Evening star".into() });
        }
    }

    None
}

/// ── Strategy 27: Support/Resistance Break & Retest ───────────────

pub fn support_resistance(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 30 {
        return None;
    }
    // Find recent swing points
    let sw_highs = indicators::swing_highs(highs, 5);
    let sw_lows = indicators::swing_lows(lows, 5);
    if sw_highs.is_empty() || sw_lows.is_empty() {
        return None;
    }
    let current_price = closes[n - 1];
    let prev_price = closes[n - 2];
    let atr_val = indicators::atr(highs, lows, closes, 14);
    if !atr_val.is_finite() || atr_val <= 0.0 {
        return None;
    }

    // Find nearest resistance level (recent swing high within 2 ATR of current price)
    let mut nearest_resistance = f64::MAX;
    let mut nearest_support = f64::MIN;
    for (_, val) in &sw_highs {
        if *val > current_price && *val - current_price < nearest_resistance - current_price {
            nearest_resistance = *val;
        }
    }
    for (_, val) in &sw_lows {
        if *val < current_price && current_price - *val < current_price - nearest_support {
            nearest_support = *val;
        }
    }

    // Breakout above resistance
    if nearest_resistance.is_finite() && nearest_resistance != f64::MAX {
        if prev_price <= nearest_resistance && current_price > nearest_resistance {
            let dist = (current_price - nearest_resistance) / atr_val;
            if dist > 0.5 {
                let conf = (dist / 3.0).min(1.0);
                return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Resistance breakout {:.4}", nearest_resistance) });
            }
        }
    }
    // Breakdown below support
    if nearest_support.is_finite() && nearest_support != f64::MIN {
        if prev_price >= nearest_support && current_price < nearest_support {
            let dist = (nearest_support - current_price) / atr_val;
            if dist > 0.5 {
                let conf = (dist / 3.0).min(1.0);
                return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Support breakdown {:.4}", nearest_support) });
            }
        }
    }

    // Bounce from support
    if nearest_support.is_finite() && nearest_support != f64::MIN {
        let dist_from_support = (current_price - nearest_support) / atr_val;
        if dist_from_support < 1.0 && current_price > nearest_support && closes[n - 1] > closes[n - 2] {
            return Some(Signal { action: "BUY".into(), confidence: 0.4, reason: format!("Support bounce {:.4}", nearest_support) });
        }
    }
    // Rejection at resistance
    if nearest_resistance.is_finite() && nearest_resistance != f64::MAX {
        let dist_from_resistance = (nearest_resistance - current_price) / atr_val;
        if dist_from_resistance < 1.0 && current_price < nearest_resistance && closes[n - 1] < closes[n - 2] {
            return Some(Signal { action: "SELL".into(), confidence: 0.4, reason: format!("Resistance rejection {:.4}", nearest_resistance) });
        }
    }

    None
}

/// ── Strategy 28: Liquidity Vacuum Reversal ──────────────────────

pub fn liquidity_vacuum(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n < 15 {
        return None;
    }
    let range = highs[n - 1] - lows[n - 1];
    if range <= 0.0 {
        return None;
    }
    let body = (closes[n - 1] - _opens[n - 1]).abs();
    let upper_wick = highs[n - 1] - _opens[n - 1].max(closes[n - 1]);
    let lower_wick = _opens[n - 1].min(closes[n - 1]) - lows[n - 1];
    let max_wick = upper_wick.max(lower_wick);
    let wick_ratio = max_wick / range;

    // Wick must dominate the candle
    if wick_ratio < 0.55 {
        return None;
    }

    // Volume must be elevated
    let avg_vol: f64 = volumes[n - 14..].iter().sum::<f64>() / 14.0;
    if avg_vol <= 0.0 || volumes[n - 1] < avg_vol * 1.5 {
        return None;
    }

    // Fade the wick direction
    if upper_wick > lower_wick && upper_wick > body * 1.5 && closes[n - 1] < _opens[n - 1] {
        // Upper wick rejection → short
        let conf = (wick_ratio.min(1.0) * 0.7).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Liquidity vacuum: upper wick {:.1}%", wick_ratio * 100.0) });
    }
    if lower_wick > upper_wick && lower_wick > body * 1.5 && closes[n - 1] > _opens[n - 1] {
        // Lower wick rejection → long
        let conf = (wick_ratio.min(1.0) * 0.7).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Liquidity vacuum: lower wick {:.1}%", wick_ratio * 100.0) });
    }

    None
}

/// ── Strategy 29: Smart Money Flow / CVD Proxy ───────────────────

pub fn smart_money_flow(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n < 25 {
        return None;
    }
    // Estimate CVD from OHLCV proxies
    let mut cvd: Vec<f64> = Vec::with_capacity(n);
    cvd.push(0.0);
    for i in 1..n {
        let range = highs[i] - lows[i];
        if range <= 0.0 {
            cvd.push(cvd[i - 1]);
            continue;
        }
        let est_bid = volumes[i] * (highs[i] - closes[i]) / range;
        let est_ask = volumes[i] * (closes[i] - lows[i]) / range;
        cvd.push(cvd[i - 1] + (est_ask - est_bid));
    }
    let cvd_ema = indicators::ema(&cvd, 10);
    let price_ema = indicators::ema(closes, 10);
    if !cvd_ema.is_finite() || !price_ema.is_finite() || price_ema <= 0.0 {
        return None;
    }

    // Divergence: price rising but CVD falling (distribution)
    let price_trend = closes[n - 1] - price_ema;
    let cvd_trend = cvd[n - 1] - cvd_ema;

    if price_trend > 0.0 && cvd_trend < 0.0 && cvd_trend.abs() > 0.01 {
        let conf = (cvd_trend.abs() * 10.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("CVD bearish divergence: price up {:.4}, CVD {:.4}", price_trend, cvd_trend) });
    }
    if price_trend < 0.0 && cvd_trend > 0.0 && cvd_trend.abs() > 0.01 {
        let conf = (cvd_trend.abs() * 10.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("CVD bullish divergence: price down {:.4}, CVD {:.4}", price_trend, cvd_trend) });
    }

    None
}

/// ── Strategy 30: Volatility Compression Breakout (VCP/Coil) ────

pub fn volatility_compression(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 30 {
        return None;
    }
    let window = 20;
    let current_range = indicators::highest(highs, window) - indicators::lowest(lows, window);
    let mid_price = indicators::sma(closes, window);
    if !mid_price.is_finite() || mid_price <= 0.0 || current_range <= 0.0 {
        return None;
    }
    let current_width = current_range / mid_price;

    // Compare to lookback period (10 bars before current window)
    if n < window + 10 {
        return None;
    }
    let prev_highs = &highs[n - window - 10..n - 10];
    let prev_lows = &lows[n - window - 10..n - 10];
    let prev_closes = &closes[n - window - 10..n - 10];
    let prev_range = indicators::highest(prev_highs, window) - indicators::lowest(prev_lows, window);
    let prev_mid = indicators::sma(prev_closes, window);
    if !prev_mid.is_finite() || prev_mid <= 0.0 {
        return None;
    }
    let prev_width = prev_range / prev_mid;

    // Compression: current width < 60% of previous width
    if prev_width > 0.0 && current_width / prev_width < 0.6 {
        let atr_val = indicators::atr(highs, lows, closes, 14);
        if !atr_val.is_finite() || atr_val <= 0.0 {
            return None;
        }
        let current_price = closes[n - 1];
        let break_high = indicators::highest(highs, 10);
        let break_low = indicators::lowest(lows, 10);

        // Breakout direction
        if current_price > break_high {
            let conf = ((current_price - break_high) / atr_val).min(1.0).max(0.3);
            return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("VCP breakout {:.1}% compression", (1.0 - current_width / prev_width) * 100.0) });
        }
        if current_price < break_low {
            let conf = ((break_low - current_price) / atr_val).min(1.0).max(0.3);
            return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("VCP breakdown {:.1}% compression", (1.0 - current_width / prev_width) * 100.0) });
        }
    }

    None
}

/// ── Strategy 31: Impulse Exhaustion Reversal ───────────────────

pub fn impulse_exhaustion(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n < 15 {
        return None;
    }
    let range = highs[n - 1] - lows[n - 1];
    if range <= 0.0 {
        return None;
    }
    let body = (closes[n - 1] - _opens[n - 1]).abs();
    let upper_wick = highs[n - 1] - _opens[n - 1].max(closes[n - 1]);
    let lower_wick = _opens[n - 1].min(closes[n - 1]) - lows[n - 1];

    // Momentum over last 5 bars
    let momentum = indicators::roc(closes, 5);
    if !momentum.is_finite() {
        return None;
    }

    // Impulse bar: large body + elevated volume
    let avg_range: f64 = (1..n).map(|i| highs[i] - lows[i]).sum::<f64>() / (n - 1) as f64;
    let avg_vol: f64 = volumes[n - 14..].iter().sum::<f64>() / 14.0;
    if avg_range <= 0.0 || avg_vol <= 0.0 {
        return None;
    }

    let is_impulse = range > avg_range * 1.5 && volumes[n - 1] > avg_vol * 1.3;
    if !is_impulse {
        return None;
    }

    // Fade strong impulse: after a big momentum move, look for exhaustion
    let upper_wick_ratio = upper_wick / range;
    let lower_wick_ratio = lower_wick / range;

    // Strong up-move stalling with upper wick → short
    if momentum > 0.08 && body / range > 0.3 && upper_wick_ratio > 0.45 && closes[n - 1] < _opens[n - 1] {
        let conf = (momentum * 5.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Impulse exhaustion at top (mom={:.1}%)", momentum * 100.0) });
    }
    // Strong down-move stalling with lower wick → long
    if momentum < -0.08 && body / range > 0.3 && lower_wick_ratio > 0.45 && closes[n - 1] > _opens[n - 1] {
        let conf = (momentum.abs() * 5.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Impulse exhaustion at bottom (mom={:.1}%)", momentum * 100.0) });
    }

    None
}

/// ── Strategy 32: Momentum Acceleration ─────────────────────────

pub fn momentum_acceleration(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 30 {
        return None;
    }
    // ROC of price (velocity)
    let velocity = indicators::roc_series(closes, 5);
    if velocity.len() < 15 {
        return None;
    }
    let last_valid = velocity.len() - 1;
    let v_curr = velocity[last_valid];
    let v_prev = velocity[last_valid - 2];
    if !v_curr.is_finite() || !v_prev.is_finite() {
        return None;
    }
    // Acceleration = change in velocity
    let accel = v_curr - v_prev;

    // Volume confirmation
    let avg_vol: f64 = volumes[n - 10..].iter().sum::<f64>() / 10.0;
    let vol_ok = volumes[n - 1] > avg_vol * 0.8;

    // Price above/below SMA(20) for trend context
    let sma20 = indicators::sma(closes, 20);
    if !sma20.is_finite() {
        return None;
    }
    let above_sma = closes[n - 1] > sma20;

    // Bullish acceleration: velocity turning positive and accelerating
    if accel > 0.02 && v_curr > 0.0 && above_sma && vol_ok {
        let conf = (accel * 10.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Momentum accelerating {:.2} (v={:.2})", accel, v_curr) });
    }
    // Bearish acceleration: velocity turning negative and accelerating
    if accel < -0.02 && v_curr < 0.0 && !above_sma && vol_ok {
        let conf = (accel.abs() * 10.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Momentum decelerating {:.2} (v={:.2})", accel, v_curr) });
    }

    None
}

/// ── Strategy 33: RSI Failure Swing ─────────────────────────────

pub fn rsi_failure_swing(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 30 {
        return None;
    }
    // Compute RSI series
    let mut rsi_vals = Vec::with_capacity(n);
    for i in 14..=n {
        let slice = &closes[..i];
        rsi_vals.push(indicators::rsi(slice, 14));
    }
    if rsi_vals.len() < 10 {
        return None;
    }

    let last_idx = rsi_vals.len() - 1;
    let rsi_curr = rsi_vals[last_idx];
    let _rsi_prev = rsi_vals[last_idx - 1];
    let rsi_prev2 = if last_idx >= 2 { rsi_vals[last_idx - 2] } else { 50.0 };
    let rsi_prev3 = if last_idx >= 3 { rsi_vals[last_idx - 3] } else { 50.0 };
    let rsi_prev4 = if last_idx >= 4 { rsi_vals[last_idx - 4] } else { 50.0 };

    // Bullish failure swing: RSI oversold, bounce above 30, then dip but stays above prior low + crosses back up
    if rsi_prev4 < 30.0 && rsi_prev3 > 30.0 && rsi_curr < rsi_prev2 + 2.0
        && rsi_curr > rsi_prev4 && rsi_curr > 30.0 && rsi_curr > 50.0 {
        let conf = ((rsi_curr - 30.0) / 30.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("RSI failure swing bullish (RSI {:.0})", rsi_curr) });
    }

    // Bearish failure swing: RSI overbought, dip below 70, then rally but stays below prior high + crosses back down
    if rsi_prev4 > 70.0 && rsi_prev3 < 70.0 && rsi_curr > rsi_prev2 - 2.0
        && rsi_curr < rsi_prev4 && rsi_curr < 70.0 && rsi_curr < 50.0 {
        let conf = ((70.0 - rsi_curr) / 30.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("RSI failure swing bearish (RSI {:.0})", rsi_curr) });
    }

    None
}

/// ── Strategy 34: Anchored VWAP Mean Reversion ──────────────────

pub fn anchored_vwap(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n < 30 {
        return None;
    }
    // Rolling VWAP over last 20 bars
    let window = 20;
    let start = n - window;
    let sum_pv: f64 = closes[start..].iter().zip(volumes[start..].iter()).map(|(p, v)| p * v).sum();
    let sum_v: f64 = volumes[start..].iter().sum();
    if sum_v <= 0.0 {
        return None;
    }
    let vwap = sum_pv / sum_v;
    let current_price = closes[n - 1];
    let deviation = (current_price - vwap) / vwap;

    // Z-score of closing prices around VWAP
    let variance: f64 = closes[start..].iter().map(|p| (p - vwap).powi(2)).sum::<f64>() / window as f64;
    let std_dev = variance.sqrt();
    if std_dev <= 0.0 {
        return None;
    }
    let z = (current_price - vwap) / std_dev;

    // Mean reversion when |z| > 1.5 with confirmation from previous bar
    if z < -1.5 && deviation < -0.01 && closes[n - 1] > closes[n - 2] {
        let conf = (z.abs() / 4.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Anchored VWAP reversion (z={:.1})", z) });
    }
    if z > 1.5 && deviation > 0.01 && closes[n - 1] < closes[n - 2] {
        let conf = (z.abs() / 4.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Anchored VWAP reversion (z={:.1})", z) });
    }

    None
}

/// ── Strategy 35: Donchian Pullback Continuation ───────────────

pub fn donchian_pullback(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 25 {
        return None;
    }
    let upper = indicators::highest(highs, 20);
    let lower = indicators::lowest(lows, 20);
    let mid = (upper + lower) / 2.0;
    let current_price = closes[n - 1];
    let prev_price = closes[n - 2];
    let atr_val = indicators::atr(highs, lows, closes, 14);
    if !atr_val.is_finite() || atr_val <= 0.0 {
        return None;
    }

    // Bullish: price broke above upper before, now pulling back to mid
    // Check if we were above upper within last 5 bars
    let mut was_above_upper = false;
    for i in (n - 6).max(0)..n - 1 {
        if closes[i] > upper {
            was_above_upper = true;
            break;
        }
    }
    if was_above_upper && current_price <= mid + atr_val && current_price > mid - atr_val
        && prev_price <= current_price {
        let conf = ((mid - current_price) / atr_val).abs().min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Donchian pullback to {:.4}", mid) });
    }

    // Bearish: price broke below lower before, now pulling back to mid
    let mut was_below_lower = false;
    for i in (n - 6).max(0)..n - 1 {
        if closes[i] < lower {
            was_below_lower = true;
            break;
        }
    }
    if was_below_lower && current_price >= mid - atr_val && current_price < mid + atr_val
        && prev_price >= current_price {
        let conf = ((current_price - mid) / atr_val).abs().min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Donchian pullback to {:.4}", mid) });
    }

    None
}

/// ── Strategy 36: Volume Profile / Market Profile ──────────────

pub fn volume_profile_strategy(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 30 {
        return None;
    }
    let vp = indicators::volume_profile(closes, volumes, n.min(20), 20)?;
    let current_price = closes[n - 1];
    let atr_val = indicators::atr(highs, lows, closes, 14);
    let atr_val = atr_val.max(vp.bin_width * 2.0);

    // Price at POC → reaction/reversal area
    let dist_to_poc = (current_price - vp.poc).abs();
    if dist_to_poc < atr_val * 0.5 {
        if current_price > vp.poc {
            return Some(Signal { action: "SELL".into(), confidence: 0.4, reason: format!("VP: at POC {:.4}, selling", vp.poc) });
        } else {
            return Some(Signal { action: "BUY".into(), confidence: 0.4, reason: format!("VP: at POC {:.4}, buying", vp.poc) });
        }
    }

    // Price at VAL (below value area) → bounce / support
    let val_dist = (current_price - vp.val).abs();
    if val_dist < atr_val * 0.5 && current_price > vp.val - atr_val {
        return Some(Signal { action: "BUY".into(), confidence: 0.5, reason: format!("VP: value area low {:.4}", vp.val) });
    }

    // Price at VAH (above value area) → rejection / resistance
    let vah_dist = (current_price - vp.vah).abs();
    if vah_dist < atr_val * 0.5 && current_price < vp.vah + atr_val {
        return Some(Signal { action: "SELL".into(), confidence: 0.5, reason: format!("VP: value area high {:.4}", vp.vah) });
    }

    // Price breaking out of value area with volume
    if current_price > vp.vah + atr_val && volumes[n - 1] > vp.total_volume / vp.n_bins as f64 {
        return Some(Signal { action: "BUY".into(), confidence: 0.5, reason: format!("VP: breakout above VAH {:.4}", vp.vah) });
    }
    if current_price < vp.val - atr_val && volumes[n - 1] > vp.total_volume / vp.n_bins as f64 {
        return Some(Signal { action: "SELL".into(), confidence: 0.5, reason: format!("VP: breakdown below VAL {:.4}", vp.val) });
    }

    None
}

/// ── Strategy 37: Bollinger Squeeze ─────────────────────────────

pub fn bollinger_squeeze(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 30 {
        return None;
    }
    let (lower, _mid, upper, bw) = indicators::bollinger(closes, 20, 2.0);
    if !lower.is_finite() || !upper.is_finite() || !bw.is_finite() || bw <= 0.0 {
        return None;
    }

    // Compare current bandwidth to historical bandwidth (20 bars ago)
    if n < 40 {
        return None;
    }
    let prev_closes = &closes[..n - 1];
    let (_, _, _, prev_bw) = indicators::bollinger(prev_closes, 20, 2.0);
    if !prev_bw.is_finite() || prev_bw <= 0.0 {
        return None;
    }

    // Squeeze: bandwidth contracted significantly
    let squeeze_ratio = bw / prev_bw;
    if squeeze_ratio > 0.8 {
        return None; // not compressed enough
    }

    let atr_val = indicators::atr(highs, lows, closes, 14);
    if !atr_val.is_finite() || atr_val <= 0.0 {
        return None;
    }

    let current_price = closes[n - 1];
    let recent_high = indicators::highest(highs, 5);
    let recent_low = indicators::lowest(lows, 5);

    // Breakout from squeeze
    if current_price > recent_high && current_price > upper {
        let conf = ((current_price - upper) / atr_val).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Bollinger squeeze breakout {:.1}%", (1.0 - squeeze_ratio) * 100.0) });
    }
    if current_price < recent_low && current_price < lower {
        let conf = ((lower - current_price) / atr_val).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Bollinger squeeze breakdown {:.1}%", (1.0 - squeeze_ratio) * 100.0) });
    }

    None
}

/// ── Strategy 38: Multi-Timeframe RSI ──────────────────────────

pub fn multi_tf_rsi(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 30 {
        return None;
    }
    let rsi_fast = indicators::rsi(closes, 7);
    let rsi_slow = indicators::rsi(closes, 21);
    if !rsi_fast.is_finite() || !rsi_slow.is_finite() {
        return None;
    }

    // Previous values for crossover detection
    let prev_closes = &closes[..n - 1];
    let rsi_fast_prev = indicators::rsi(prev_closes, 7);
    let rsi_slow_prev = indicators::rsi(prev_closes, 21);
    if !rsi_fast_prev.is_finite() || !rsi_slow_prev.is_finite() {
        return None;
    }

    // Fast RSI crosses above slow RSI (bullish)
    if rsi_fast_prev <= rsi_slow_prev && rsi_fast > rsi_slow {
        let strength = ((rsi_fast - rsi_slow) / 30.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: strength, reason: format!("Multi-TF RSI bullish cross {:.0}/{:.0}", rsi_fast, rsi_slow) });
    }

    // Fast RSI crosses below slow RSI (bearish)
    if rsi_fast_prev >= rsi_slow_prev && rsi_fast < rsi_slow {
        let strength = ((rsi_slow - rsi_fast) / 30.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: strength, reason: format!("Multi-TF RSI bearish cross {:.0}/{:.0}", rsi_fast, rsi_slow) });
    }

    // Extreme readings on fast RSI with slow RSI confirming direction
    if rsi_fast < 25.0 && rsi_slow < 40.0 {
        return Some(Signal { action: "BUY".into(), confidence: 0.5, reason: format!("Multi-TF RSI oversold {:.0}/{:.0}", rsi_fast, rsi_slow) });
    }
    if rsi_fast > 75.0 && rsi_slow > 60.0 {
        return Some(Signal { action: "SELL".into(), confidence: 0.5, reason: format!("Multi-TF RSI overbought {:.0}/{:.0}", rsi_fast, rsi_slow) });
    }

    None
}

/// ── Strategy 39: Linear Regression Slope ──────────────────────

pub fn linreg_slope_strategy(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 30 {
        return None;
    }
    let slope = indicators::linreg_slope(closes, 14);
    let prev_closes = &closes[..n - 1];
    let prev_slope = indicators::linreg_slope(prev_closes, 14);
    if !slope.is_finite() || !prev_slope.is_finite() || prev_slope == 0.0 {
        return None;
    }

    let current_price = closes[n - 1];
    let sma50 = indicators::sma(closes, 50.min(n));
    let above_sma = sma50.is_finite() && current_price > sma50;

    // Slope crossing from negative to positive → trend reversal up
    if prev_slope <= 0.0 && slope > 0.0 && above_sma {
        let conf = (slope * 50.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("LinReg slope bullish crossover {:.4}", slope) });
    }

    // Slope crossing from positive to negative → trend reversal down
    if prev_slope >= 0.0 && slope < 0.0 && !above_sma {
        let conf = (slope.abs() * 50.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("LinReg slope bearish crossover {:.4}", slope) });
    }

    // Strong positive slope with volume confirmation
    let avg_vol: f64 = volumes[n - 10..].iter().sum::<f64>() / 10.0;
    if slope > 0.05 && above_sma && volumes[n - 1] > avg_vol * 1.2 {
        let conf = (slope * 10.0).min(1.0).max(0.3);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("LinReg slope strong uptrend {:.4}", slope) });
    }

    // Strong negative slope
    if slope < -0.05 && !above_sma && volumes[n - 1] > avg_vol * 1.2 {
        let conf = (slope.abs() * 10.0).min(1.0).max(0.3);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("LinReg slope strong downtrend {:.4}", slope) });
    }

    None
}

/// ── Strategy 40: Hurst Regime Strategy ────────────────────────

pub fn hurst_regime(closes: &[f64], _opens: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 40 {
        return None;
    }

    // Compute Hurst exponent from closing prices
    let hurst = hurst_compute(closes, 30);
    if !hurst.is_finite() || hurst <= 0.0 {
        return None;
    }

    let current_price = closes[n - 1];
    let sma20 = indicators::sma(closes, 20);
    let sma50 = indicators::sma(closes, 50.min(n));
    if !sma20.is_finite() || !sma50.is_finite() {
        return None;
    }

    let above_sma = current_price > sma20 && current_price > sma50;
    let slope = indicators::linreg_slope(closes, 10);
    let atr_val = indicators::atr(highs, lows, closes, 14).max(0.001);

    // Mean-reverting regime (Hurst < 0.4): trade reversions
    if hurst < 0.4 {
        let z = (current_price - sma20) / sma20;
        if z < -0.02 && closes[n - 1] > closes[n - 2] {
            return Some(Signal { action: "BUY".into(), confidence: 0.5, reason: format!("Hurst mean-rev {:.2} oversold", hurst) });
        }
        if z > 0.02 && closes[n - 1] < closes[n - 2] {
            return Some(Signal { action: "SELL".into(), confidence: 0.5, reason: format!("Hurst mean-rev {:.2} overbought", hurst) });
        }
    }

    // Trending regime (Hurst > 0.6): follow momentum
    if hurst > 0.6 {
        let avg_vol: f64 = volumes[n - 10..].iter().sum::<f64>() / 10.0;
        let vol_ok = volumes[n - 1] > avg_vol * 0.8;
        if slope > 0.02 && above_sma && vol_ok {
            let conf = ((hurst - 0.6) * 3.0).min(1.0).max(0.3);
            return Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Hurst trend {:.2} momentum up", hurst) });
        }
        if slope < -0.02 && !above_sma && vol_ok {
            let conf = ((hurst - 0.6) * 3.0).min(1.0).max(0.3);
            return Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Hurst trend {:.2} momentum down", hurst) });
        }
    }

    // Neutral regime: look for regime transition signals
    if hurst >= 0.4 && hurst <= 0.6 {
        let price_range = indicators::highest(closes, 20) - indicators::lowest(closes, 20);
        if price_range > atr_val * 3.0 && slope.abs() > 0.01 {
            // Price expanding — could be transitioning to trend
            if slope > 0.0 && above_sma {
                return Some(Signal { action: "BUY".into(), confidence: 0.35, reason: format!("Hurst transition {:.2} to trend", hurst) });
            }
            if slope < 0.0 && !above_sma {
                return Some(Signal { action: "SELL".into(), confidence: 0.35, reason: format!("Hurst transition {:.2} to trend", hurst) });
            }
        }
    }

    None
}

/// Internal Hurst exponent computation (R/S analysis).
fn hurst_compute(prices: &[f64], max_lag: usize) -> f64 {
    let n = prices.len();
    if n < max_lag * 2 || max_lag < 4 {
        return f64::NAN;
    }
    let mut rs_values = Vec::new();
    let mut lags = Vec::new();

    // Compute R/S for various lag sizes
    let mut lag = 4;
    while lag <= max_lag && lag <= n / 2 {
        let n_chunks = n / lag;
        if n_chunks < 2 {
            lag *= 2;
            continue;
        }
        let mut rs_sum = 0.0;
        for chunk in 0..n_chunks {
            let start = chunk * lag;
            let end = start + lag;
            let slice = &prices[start..end];
            let mean: f64 = slice.iter().sum::<f64>() / lag as f64;
            let devs: Vec<f64> = slice.iter().map(|&x| x - mean).collect();
            let mut cum_sum = 0.0;
            let mut min_cum = 0.0;
            let mut max_cum = 0.0;
            for &d in &devs {
                cum_sum += d;
                if cum_sum < min_cum { min_cum = cum_sum; }
                if cum_sum > max_cum { max_cum = cum_sum; }
            }
            let r = max_cum - min_cum;
            let s = devs.iter().map(|d| d * d).sum::<f64>() / lag as f64;
            if s > 0.0 {
                rs_sum += r / s.sqrt();
            }
        }
        rs_values.push((rs_sum / n_chunks as f64).ln());
        lags.push((lag as f64).ln());
        lag *= 2;
    }

    if rs_values.len() < 3 {
        return f64::NAN;
    }

    // Linear regression of ln(R/S) on ln(lag)
    let m = rs_values.len();
    let sum_x: f64 = lags.iter().sum();
    let sum_y: f64 = rs_values.iter().sum();
    let sum_xy: f64 = lags.iter().zip(rs_values.iter()).map(|(x, y)| x * y).sum();
    let sum_x2: f64 = lags.iter().map(|x| x * x).sum();
    let denom = (m as f64) * sum_x2 - sum_x * sum_x;
    if denom == 0.0 {
        return f64::NAN;
    }
    let slope = ((m as f64) * sum_xy - sum_x * sum_y) / denom;
    slope // Hurst exponent = slope of ln(R/S) vs ln(lag)
}

// ── Strategy 41: Elder Ray Index ───────────────────────────────────
/// Bull Power = high - EMA(13), Bear Power = low - EMA(13).
/// BUY when Bull Power is positive and rising above recent avg.
/// SELL when Bear Power is negative and falling below recent avg.
fn elder_ray_index(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let period = 13_usize;
    if closes.len() < period + 5 || highs.len() < period + 5 || lows.len() < period + 5 {
        return None;
    }
    let ema_v = indicators::ema_slice(closes, period);
    if ema_v.len() < 3 {
        return None;
    }
    let last = ema_v.len() - 1;
    let bull_power = highs[highs.len() - 1] - ema_v[last];
    let bear_power = lows[lows.len() - 1] - ema_v[last];
    let prev_bull = highs[highs.len() - 2] - ema_v[last - 1];
    let prev_bear = lows[lows.len() - 2] - ema_v[last - 1];

    // Average bull/bear over last 10 bars for context
    let n = ema_v.len().min(10);
    let avg_bull: f64 = ema_v.iter().rev().skip(1).take(n)
        .zip(highs.iter().rev().skip(1).take(n))
        .map(|(e, h)| h - e).sum::<f64>() / n as f64;
    let avg_bear: f64 = ema_v.iter().rev().skip(1).take(n)
        .zip(lows.iter().rev().skip(1).take(n))
        .map(|(e, l)| l - e).sum::<f64>() / n as f64;

    if bull_power > avg_bull && bull_power > prev_bull {
        let conf = (bull_power / ema_v[last]).abs().min(0.05) / 0.05 * 0.5 + 0.5;
        return Some(Signal {
            action: "BUY".into(),
            confidence: conf.min(0.95),
            reason: format!("Bull Power {:.2} > avg {:.2}", bull_power, avg_bull),
        });
    }
    if bear_power < avg_bear && bear_power < prev_bear {
        let conf = (bear_power / ema_v[last]).abs().min(0.05) / 0.05 * 0.5 + 0.5;
        return Some(Signal {
            action: "SELL".into(),
            confidence: conf.min(0.95),
            reason: format!("Bear Power {:.2} < avg {:.2}", bear_power, avg_bear),
        });
    }
    None
}

// ── Strategy 42: Klinger Oscillator ────────────────────────────────
/// Volume-based: Klinger = EMA(13) of Volume Force - EMA(55) of Volume Force.
/// Volume Force = V * ((high+low+close)/3 - (prev_high+prev_low+prev_close)/3)
/// BUY when Klinger > 0 and rising, SELL when < 0 and falling.
fn klinger_oscillator(closes: &[f64], volumes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    if closes.len() < 60 || volumes.len() < 60 || highs.len() < 60 || lows.len() < 60 {
        return None;
    }
    let n = closes.len();
    let mut vf = Vec::with_capacity(n - 1);
    for i in 1..n {
        let tp = (highs[i] + lows[i] + closes[i]) / 3.0;
        let prev_tp = (highs[i - 1] + lows[i - 1] + closes[i - 1]) / 3.0;
        let trend = if closes[i] > closes[i - 1] { 1.0 } else { -1.0 };
        vf.push(volumes[i] as f64 * trend * (tp - prev_tp).abs() * 100.0);
    }
    let fast = 13_usize;
    let slow = 55_usize;
    if vf.len() < slow + 2 {
        return None;
    }
    let fast_ema = indicators::ema_last_two(&vf, fast);
    let slow_ema = indicators::ema_last_two(&vf, slow);
    let klinger = fast_ema.1 - slow_ema.1;
    let prev_klinger = fast_ema.0 - slow_ema.0;

    if klinger > 0.0 && klinger > prev_klinger {
        let conf = (klinger / fast_ema.1.abs().max(0.001)).abs().min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "BUY".into(),
            confidence: conf.min(0.95),
            reason: format!("Klinger + rising ({:.2})", klinger),
        });
    }
    if klinger < 0.0 && klinger < prev_klinger {
        let conf = (klinger / fast_ema.1.abs().max(0.001)).abs().min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "SELL".into(),
            confidence: conf.min(0.95),
            reason: format!("Klinger - falling ({:.2})", klinger),
        });
    }
    None
}

// ── Strategy 43: Pivot Points (S/R) ───────────────────────────────
/// Find recent swing highs/lows. If price breaks above nearest swing high → BUY.
/// If price breaks below nearest swing low → SELL.
fn pivot_point_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    if closes.len() < 30 || highs.len() < 30 || lows.len() < 30 {
        return None;
    }
    let lookback = 10_usize;
    let sh = indicators::swing_highs(highs, lookback);
    let sl = indicators::swing_lows(lows, lookback);
    if sh.is_empty() && sl.is_empty() {
        return None;
    }
    let current = closes[closes.len() - 1];
    // Nearest swing high above current
    let mut nearest_resist = f64::MAX;
    let mut nearest_support = 0.0_f64;
    for (_, val) in &sh {
        if *val > current && (*val - current).abs() < (nearest_resist - current).abs() {
            nearest_resist = *val;
        }
    }
    for (_, val) in &sl {
        if *val < current && (current - *val).abs() < (current - nearest_support).abs() {
            nearest_support = *val;
        }
    }
    // Breakout above resistance
    if nearest_resist < f64::MAX && current > nearest_resist * 1.005 {
        let pct = (current - nearest_resist) / nearest_resist;
        let conf = (pct * 20.0).min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "BUY".into(),
            confidence: conf.min(0.95),
            reason: format!("Pivot breakout above {:.4}", nearest_resist),
        });
    }
    // Breakdown below support
    if nearest_support > 0.0 && current < nearest_support * 0.995 {
        let pct = (nearest_support - current) / nearest_support;
        let conf = (pct * 20.0).min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "SELL".into(),
            confidence: conf.min(0.95),
            reason: format!("Pivot breakdown below {:.4}", nearest_support),
        });
    }
    None
}

// ── Strategy 44: Ichimoku Cloud (Simplified) ──────────────────────
/// Conversion Line (9) vs Base Line (26) crossover.
/// BUY on conversion cross above base, SELL on cross below base.
fn ichimoku_cloud(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let conv = 9_usize;
    let base = 26_usize;
    if closes.len() < base + 3 || highs.len() < base + 3 || lows.len() < base + 3 {
        return None;
    }

    fn tenkan(highs: &[f64], lows: &[f64], period: usize) -> f64 {
        (indicators::highest(highs, period) + indicators::lowest(lows, period)) / 2.0
    }

    fn prev_tenkan(highs: &[f64], lows: &[f64], period: usize) -> f64 {
        let h = &highs[..highs.len() - 1];
        let l = &lows[..lows.len() - 1];
        (indicators::highest(h, period) + indicators::lowest(l, period)) / 2.0
    }

    let conv_now = tenkan(highs, lows, conv);
    let base_now = tenkan(highs, lows, base);
    let conv_prev = prev_tenkan(highs, lows, conv);
    let base_prev = prev_tenkan(highs, lows, base);

    // Displacement: is current price above/below span?
    let span_a_now = (conv_now + base_now) / 2.0;
    let price = closes[closes.len() - 1];

    if conv_prev <= base_prev && conv_now > base_now {
        let conf = if price > span_a_now { 0.75 } else { 0.60 };
        return Some(Signal {
            action: "BUY".into(),
            confidence: conf,
            reason: format!("Ichimoku cross up (conv={:.2}, base={:.2})", conv_now, base_now),
        });
    }
    if conv_prev >= base_prev && conv_now < base_now {
        let conf = if price < span_a_now { 0.75 } else { 0.60 };
        return Some(Signal {
            action: "SELL".into(),
            confidence: conf,
            reason: format!("Ichimoku cross down (conv={:.2}, base={:.2})", conv_now, base_now),
        });
    }
    None
}

// ── Strategy 45: Choppiness Index ──────────────────────────────────
/// CI = 100 * ln(∑TR / (high_n - low_n)) / ln(n)
/// > 60 → ranging (mean reversion signals); < 30 → trending (momentum signals)
fn choppiness_index(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let period = 14_usize;
    if closes.len() < period + 5 || highs.len() < period + 5 || lows.len() < period + 5 {
        return None;
    }
    let n = highs.len();
    // True Range series
    let mut tr_sum = 0.0;
    for i in (n - period)..n {
        let tr = (highs[i] - lows[i])
            .max((highs[i] - closes[i - 1]).abs())
            .max((lows[i] - closes[i - 1]).abs());
        tr_sum += tr;
    }
    let hi_range = indicators::highest(highs, period) - indicators::lowest(lows, period);
    if hi_range <= 0.0 {
        return None;
    }
    let ci = 100.0 * (tr_sum / hi_range).ln() / (period as f64).ln();
    let price = closes[closes.len() - 1];
    let sma20 = indicators::sma_slice(closes, 20);
    if sma20.is_empty() {
        return None;
    }
    let avg = sma20[sma20.len() - 1];

    if ci > 60.0 {
        // Ranging → mean reversion
        if price < avg * 0.97 {
            return Some(Signal {
                action: "BUY".into(),
                confidence: 0.55,
                reason: format!("Choppiness {:.1}% → ranging, near low", ci),
            });
        }
        if price > avg * 1.03 {
            return Some(Signal {
                action: "SELL".into(),
                confidence: 0.55,
                reason: format!("Choppiness {:.1}% → ranging, near high", ci),
            });
        }
    }
    if ci < 30.0 {
        // Trending → momentum
        if price > indicators::highest(highs, 10) {
            return Some(Signal {
                action: "BUY".into(),
                confidence: 0.70,
                reason: format!("Choppiness {:.1}% → trending up", ci),
            });
        }
        if price < indicators::lowest(lows, 10) {
            return Some(Signal {
                action: "SELL".into(),
                confidence: 0.70,
                reason: format!("Choppiness {:.1}% → trending down", ci),
            });
        }
    }
    None
}

// ── Strategy 46: Full CCI (Commodity Channel Index) ───────────────
/// CCI(20) using mean deviation.
/// CCI < -100 → BUY; CCI > 100 → SELL.
fn commodity_channel_index(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let period = 20_usize;
    if closes.len() < period + 3 || highs.len() < period + 3 || lows.len() < period + 3 {
        return None;
    }
    let n = closes.len();
    let mut tp = Vec::with_capacity(n);
    for i in 0..n {
        tp.push((highs[i] + lows[i] + closes[i]) / 3.0);
    }
    let tp_slice = &tp[n - period..];
    let mean: f64 = tp_slice.iter().sum::<f64>() / period as f64;
    let md: f64 = tp_slice.iter().map(|&x| (x - mean).abs()).sum::<f64>() / period as f64;
    if md < 1e-10 {
        return None;
    }
    let cci = (tp[n - 1] - mean) / (0.015 * md);
    let prev_tp_slice = &tp[n - period - 1..n - 1];
    let prev_mean: f64 = prev_tp_slice.iter().sum::<f64>() / period as f64;
    let prev_md: f64 = prev_tp_slice.iter().map(|&x| (x - prev_mean).abs()).sum::<f64>() / period as f64;
    let prev_cci = if prev_md < 1e-10 { 0.0 } else { (tp[n - 2] - prev_mean) / (0.015 * prev_md) };

    if cci < -100.0 && cci > prev_cci {
        let conf = ((-100.0 - cci) / 100.0).min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "BUY".into(),
            confidence: conf.min(0.95),
            reason: format!("CCI {:.0} < -100, rising", cci),
        });
    }
    if cci > 100.0 && cci < prev_cci {
        let conf = ((cci - 100.0) / 100.0).min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "SELL".into(),
            confidence: conf.min(0.95),
            reason: format!("CCI {:.0} > 100, falling", cci),
        });
    }
    None
}

// ── Strategy 47: DPO (Detrended Price Oscillator) ─────────────────
/// DPO(20) = close(n - period/2 - 1) - SMA(period, n - period/2 - 1)
/// > 0 → BUY, < 0 → SELL (with cross confirmation)
fn detrended_price_oscillator(closes: &[f64]) -> Option<Signal> {
    let period = 20_usize;
    let shift = period / 2 + 1;
    if closes.len() < period + shift + 2 {
        return None;
    }
    let sma_v = indicators::sma_slice(closes, period);
    if sma_v.len() < shift + 2 {
        return None;
    }
    let n = closes.len();
    let dpo = closes[n - shift - 1] - sma_v[sma_v.len() - shift - 1];
    let prev_dpo = closes[n - shift - 2] - sma_v[sma_v.len() - shift - 2];

    if dpo > 0.0 && prev_dpo <= 0.0 {
        return Some(Signal {
            action: "BUY".into(),
            confidence: 0.60,
            reason: format!("DPO cross positive ({:.4})", dpo),
        });
    }
    if dpo < 0.0 && prev_dpo >= 0.0 {
        return Some(Signal {
            action: "SELL".into(),
            confidence: 0.60,
            reason: format!("DPO cross negative ({:.4})", dpo),
        });
    }
    None
}

// ── Strategy 48: KST (Know Sure Thing) ────────────────────────────
/// Sum of 4 smoothed ROCs:
/// KST = 1×ROC(10,SMA10) + 2×ROC(15,SMA10) + 3×ROC(20,SMA10) + 4×ROC(30,SMA15)
/// BUY when KST > signal (its SMA(9)), SELL when KST < signal.
fn know_sure_thing(closes: &[f64]) -> Option<Signal> {
    if closes.len() < 60 {
        return None;
    }
    let roc10 = indicators::roc_series(closes, 10);
    let roc15 = indicators::roc_series(closes, 15);
    let roc20 = indicators::roc_series(closes, 20);
    let roc30 = indicators::roc_series(closes, 30);
    if roc10.len() < 20 || roc15.len() < 20 || roc20.len() < 20 || roc30.len() < 25 {
        return None;
    }
    let sma10_r1 = indicators::sma_slice(&roc10, 10);
    let sma10_r2 = indicators::sma_slice(&roc15, 10);
    let sma10_r3 = indicators::sma_slice(&roc20, 10);
    let sma15_r4 = indicators::sma_slice(&roc30, 15);
    if sma10_r1.is_empty() || sma10_r2.is_empty() || sma10_r3.is_empty() || sma15_r4.is_empty() {
        return None;
    }
    let i = sma10_r1.len() - 1;
    let kst = sma10_r1[i] * 1.0 + sma10_r2[i] * 2.0 + sma10_r3[i] * 3.0 + sma15_r4[sma15_r4.len() - 1] * 4.0;
    let prev_kst = sma10_r1[i - 1] * 1.0 + sma10_r2[i - 1] * 2.0 + sma10_r3[i - 1] * 3.0
        + sma15_r4[sma15_r4.len() - 2] * 4.0;

    let signal_period = 9_usize;
    // Need enough KST values to compute signal line — we use recent 9 values
    let n_vals = sma10_r1.len();
    if n_vals < signal_period + 2 {
        return None;
    }
    let mut kst_series = Vec::with_capacity(n_vals);
    for j in 0..n_vals {
        let v = sma10_r1[j] * 1.0 + sma10_r2[j] * 2.0 + sma10_r3[j] * 3.0
            + if j < sma15_r4.len() { sma15_r4[j] * 4.0 } else { 0.0 };
        kst_series.push(v);
    }
    let sig = indicators::sma(&kst_series, signal_period);
    let prev_sig = indicators::sma_slice(&kst_series, signal_period);

    if prev_kst <= prev_sig[prev_sig.len() - 1] && kst > sig {
        let conf = (kst.abs() / 50.0).min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "BUY".into(),
            confidence: conf.min(0.95),
            reason: format!("KST {:.1} > signal {:.1}", kst, sig),
        });
    }
    if prev_kst >= prev_sig[prev_sig.len() - 1] && kst < sig {
        let conf = (kst.abs() / 50.0).min(1.0) * 0.3 + 0.5;
        return Some(Signal {
            action: "SELL".into(),
            confidence: conf.min(0.95),
            reason: format!("KST {:.1} < signal {:.1}", kst, sig),
        });
    }
    None
}

// ── Strategy 49: Mass Index ────────────────────────────────────────
/// Sum of (high[i]-low[i]) / (high[i-1]-low[i-1]) over 25 periods.
/// > 27 → reversal imminent → fade recent move.
fn mass_index(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let period = 25_usize;
    if closes.len() < period + 3 || highs.len() < period + 3 || lows.len() < period + 3 {
        return None;
    }
    let n = highs.len();
    let mut ratio_sum = 0.0;
    for i in (n - period)..n {
        let range = highs[i] - lows[i];
        let prev_range = highs[i - 1] - lows[i - 1];
        if prev_range > 0.0 {
            ratio_sum += range / prev_range;
        }
    }
    let mi = ratio_sum;
    let price = closes[closes.len() - 1];
    let sma20 = indicators::sma_slice(closes, 20);

    if mi > 27.0 && sma20.len() > 0 {
        let avg = sma20[sma20.len() - 1];
        // Reversal: if price extended above SMA, expect SELL; if below, expect BUY
        if price > avg * 1.03 {
            return Some(Signal {
                action: "SELL".into(),
                confidence: 0.60,
                reason: format!("Mass Index {:.1} > 27, reversal down signal", mi),
            });
        }
        if price < avg * 0.97 {
            return Some(Signal {
                action: "BUY".into(),
                confidence: 0.60,
                reason: format!("Mass Index {:.1} > 27, reversal up signal", mi),
            });
        }
    }
    None
}

// ── Strategy 50: Ulcer Index ──────────────────────────────────────
/// UI = sqrt(∑(drawdown_i)^2 / n) where drawdown = (close - peak) / peak.
/// High UI indicates risk; when UI drops from extreme → BUY.
fn ulcer_index(closes: &[f64]) -> Option<Signal> {
    let period = 14_usize;
    if closes.len() < period * 2 {
        return None;
    }
    let n = closes.len();
    let recent = &closes[n - period..];
    let older = &closes[n - period * 2..n - period];

    fn compute_ui(prices: &[f64]) -> f64 {
        let mut peak = 0.0_f64;
        let mut sum_sq = 0.0;
        for &p in prices {
            if p > peak { peak = p; }
            let dd = if peak > 0.0 { (p - peak) / peak } else { 0.0 };
            sum_sq += dd * dd;
        }
        (sum_sq / prices.len() as f64).sqrt()
    }

    let ui_recent = compute_ui(recent);
    let ui_older = compute_ui(older);

    // Ulcer dropping from high → drawdown recovering → BUY
    if ui_older > 0.15 && ui_recent < ui_older * 0.6 {
        return Some(Signal {
            action: "BUY".into(),
            confidence: 0.55,
            reason: format!("Ulcer {:.3} → {:.3}, drawdown recovering", ui_older, ui_recent),
        });
    }
    // Ulcer rising → increasing risk → SELL
    if ui_older < 0.05 && ui_recent > ui_older * 2.0 && ui_recent > 0.08 {
        return Some(Signal {
            action: "SELL".into(),
            confidence: 0.50,
            reason: format!("Ulcer {:.3} → {:.3}, risk rising", ui_older, ui_recent),
        });
    }
    None
}

/// ── Strategy 51: Money Flow Index ────────────────────────────────
/// MFI(14) < 20 oversold → BUY, MFI > 80 overbought → SELL

pub fn money_flow_index(closes: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let mfi_val = indicators::mfi(highs, lows, closes, volumes, 14);
    if !mfi_val.is_finite() {
        return None;
    }
    if mfi_val < 20.0 {
        let conf = ((20.0 - mfi_val) / 20.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("MFI oversold {:.1}", mfi_val) })
    } else if mfi_val > 80.0 {
        let conf = ((mfi_val - 80.0) / 20.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("MFI overbought {:.1}", mfi_val) })
    } else {
        None
    }
}

/// ── Strategy 52: Stochastic %K/%D ─────────────────────────────────
/// %K(14,3) < 20 and rising above %D → BUY; > 80 and falling below %D → SELL

pub fn stochastic_oscillator(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    if closes.len() < 20 || highs.len() < 20 || lows.len() < 20 || highs.len() < closes.len() {
        return None;
    }
    let (k, d) = indicators::stochastic_kd(closes, highs, lows, 14, 3);
    if !k.is_finite() || !d.is_finite() {
        return None;
    }
    // Compute prev %K and %D for cross detection
    let prev_n = closes.len().saturating_sub(1);
    let (prev_k, prev_d) = if prev_n >= 20 {
        indicators::stochastic_kd(&closes[..prev_n], &highs[..prev_n], &lows[..prev_n], 14, 3)
    } else {
        (k, d)
    };
    if k < 20.0 && prev_k <= prev_d && k > d {
        let conf = ((20.0 - k) / 20.0 * 0.8 + 0.2).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Stoch bullish cross K={:.1} D={:.1}", k, d) })
    } else if k > 80.0 && prev_k >= prev_d && k < d {
        let conf = ((k - 80.0) / 20.0 * 0.8 + 0.2).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Stoch bearish cross K={:.1} D={:.1}", k, d) })
    } else {
        None
    }
}

/// ── Strategy 53: Ease of Movement ─────────────────────────────────
/// EMV sma(14) > 0 → BUY (easy upward movement); < 0 → SELL (easy downward)

pub fn ease_of_movement(highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = highs.len().min(lows.len()).min(volumes.len());
    if n < 15 {
        return None;
    }
    // Build EMV series and compute SMA(14)
    let mut emv_values = Vec::with_capacity(n);
    for i in 1..n {
        let mid = (highs[i] + lows[i]) / 2.0;
        let prev_mid = (highs[i - 1] + lows[i - 1]) / 2.0;
        let distance = mid - prev_mid;
        let range = highs[i] - lows[i];
        let br = if range > 0.0 { volumes[i] / range / 100_000.0 } else { 0.0 };
        let e = if br != 0.0 { distance / br } else { 0.0 };
        emv_values.push(e);
    }
    if emv_values.len() < 14 {
        return None;
    }
    let emv_sma = indicators::sma(&emv_values, 14);
    if !emv_sma.is_finite() {
        return None;
    }
    if emv_sma > 0.5 {
        let conf = (emv_sma / 5.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("EMV +{:.3} easy upward move", emv_sma) })
    } else if emv_sma < -0.5 {
        let conf = (emv_sma.abs() / 5.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("EMV {:.3} easy downward move", emv_sma) })
    } else {
        None
    }
}

/// ── Strategy 54: A/D Line Divergence ──────────────────────────────
/// Price makes new high/low but A/D line fails to confirm → reversal signal

pub fn ad_divergence(closes: &[f64], highs: &[f64], lows: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len()).min(volumes.len());
    if n < 22 {
        return None;
    }
    let ad = indicators::ad_line_series(closes, highs, lows, volumes);
    let lookback = 20.min(n - 2);
    let price_high = indicators::highest(closes, lookback);
    let price_low = indicators::lowest(closes, lookback);
    let ad_high = indicators::highest(&ad, lookback);
    let ad_low = indicators::lowest(&ad, lookback);
    let current_price = closes[n - 1];
    let current_ad = ad[n - 1];

    // Bullish divergence: price at/near low, A/D above its low
    if current_price <= price_low * 1.01 && current_ad > ad_low * 1.02 && ad_low != 0.0 {
        let div = ((current_ad - ad_low) / ad_low.abs().max(0.0001)).min(1.0);
        let conf = (div * 0.8).min(1.0);
        return Some(Signal { action: "BUY".into(), confidence: conf, reason: "Bullish A/D divergence: price low, A/D rising".into() });
    }
    // Bearish divergence: price at/near high, A/D below its high
    if current_price >= price_high * 0.99 && current_ad < ad_high * 0.98 && ad_high != 0.0 {
        let div = ((ad_high - current_ad) / ad_high.abs().max(0.0001)).min(1.0);
        let conf = (div * 0.8).min(1.0);
        return Some(Signal { action: "SELL".into(), confidence: conf, reason: "Bearish A/D divergence: price high, A/D falling".into() });
    }
    None
}

/// ── Strategy 55: Envelope Channels ─────────────────────────────────
/// SMA(20) ± 2.5% bands. Price below lower envelope → BUY; above upper → SELL

pub fn envelope_channels(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 20 {
        return None;
    }
    let env_sma = indicators::sma(closes, 20);
    if !env_sma.is_finite() {
        return None;
    }
    let upper = env_sma * 1.025;
    let lower = env_sma * 0.975;
    let current_price = closes[n - 1];

    if current_price <= lower {
        let dev = (lower - current_price) / lower;
        let conf = (dev * 20.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Price ${:.2} below lower envelope ${:.2}", current_price, lower) })
    } else if current_price >= upper {
        let dev = (current_price - upper) / upper;
        let conf = (dev * 20.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Price ${:.2} above upper envelope ${:.2}", current_price, upper) })
    } else {
        None
    }
}

/// ── Strategy 56: ATR Channel ──────────────────────────────────────
/// SMA(20) ± 2×ATR(14). Price above upper channel → overextended SELL;
/// below lower channel → oversold BUY.

pub fn atr_channel_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 22 {
        return None;
    }
    let ch_sma = indicators::sma(closes, 20);
    let ch_atr = indicators::atr(highs, lows, closes, 14);
    if !ch_sma.is_finite() || !ch_atr.is_finite() || ch_atr <= 0.0 {
        return None;
    }
    let upper = ch_sma + 2.0 * ch_atr;
    let lower = ch_sma - 2.0 * ch_atr;
    let current_price = closes[n - 1];

    if current_price >= upper {
        let dev = (current_price - upper) / upper;
        let conf = (dev * 20.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Price ${:.2} above ATR channel ${:.2} (ATR=${:.2})", current_price, upper, ch_atr) })
    } else if current_price <= lower {
        let dev = (lower - current_price) / lower;
        let conf = (dev * 20.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Price ${:.2} below ATR channel ${:.2} (ATR=${:.2})", current_price, lower, ch_atr) })
    } else {
        None
    }
}

/// ── Strategy 57: Kaufman's Adaptive MA ────────────────────────────
/// Price > KAMA(10,2,30) → BUY; price < KAMA → SELL (with threshold)

pub fn kama_strategy(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 40 {
        return None;
    }
    let kama_val = indicators::kama(closes, 10, 2, 30);
    if !kama_val.is_finite() {
        return None;
    }
    let price = closes[n - 1];
    let dev = (price - kama_val) / kama_val;
    if dev > 0.01 {
        let conf = (dev * 5.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Price ${:.2} > KAMA ${:.2}", price, kama_val) })
    } else if dev < -0.01 {
        let conf = (dev.abs() * 5.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Price ${:.2} < KAMA ${:.2}", price, kama_val) })
    } else {
        None
    }
}

/// ── Strategy 58: DMI Cross (DI+ / DI-) ────────────────────────────
/// +DI(14) > -DI(14) → BUY; -DI(14) > +DI(14) → SELL

pub fn dmi_cross_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let plus = indicators::plus_di(highs, lows, closes, 14);
    let minus = indicators::minus_di(highs, lows, closes, 14);
    if !plus.is_finite() || !minus.is_finite() {
        return None;
    }
    if plus > minus + 2.0 {
        Some(Signal { action: "BUY".into(), confidence: 0.55, reason: format!("DMI +DI {:.1} > -DI {:.1}", plus, minus) })
    } else if minus > plus + 2.0 {
        Some(Signal { action: "SELL".into(), confidence: 0.55, reason: format!("DMI -DI {:.1} > +DI {:.1}", minus, plus) })
    } else {
        None
    }
}

/// ── Strategy 59: Variable MA ──────────────────────────────────────
/// Period adapts by volatility. VMA rising → BUY, falling → SELL.

pub fn vma_strategy(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 50 {
        return None;
    }
    let base_period = 20;
    // Compute volatility ratio: recent std / longer std
    let std_recent = {
        let slice = &closes[n - base_period..];
        let mean = slice.iter().sum::<f64>() / base_period as f64;
        (slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / base_period as f64).sqrt()
    };
    let long_period = 40.min(n);
    let std_long = {
        let slice = &closes[n - long_period..];
        let mean = slice.iter().sum::<f64>() / long_period as f64;
        (slice.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / long_period as f64).sqrt()
    };
    let vr = if std_long > 0.0 { std_recent / std_long } else { 1.0 };
    let adj_period = (base_period as f64 * (0.5 + vr).min(2.0)).max(5.0) as usize;
    let vma = indicators::ema(closes, adj_period);
    if !vma.is_finite() {
        return None;
    }
    // Compare current and prior EMA value to detect direction
    let prev = indicators::ema(&closes[..n - 1], adj_period);
    if !prev.is_finite() {
        return None;
    }
    if vma > prev * 1.001 {
        Some(Signal { action: "BUY".into(), confidence: 0.50, reason: format!("VMA rising (vol_adj_period={})", adj_period) })
    } else if vma < prev * 0.999 {
        Some(Signal { action: "SELL".into(), confidence: 0.50, reason: format!("VMA falling (vol_adj_period={})", adj_period) })
    } else {
        None
    }
}

/// ── Strategy 60: Vortex Indicator ──────────────────────────────────
/// VI+(14) > VI-(14) → BUY; VI-(14) > VI+(14) → SELL

pub fn vortex_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let (vi_plus, vi_minus) = indicators::vortex(highs, lows, closes, 14);
    if !vi_plus.is_finite() || !vi_minus.is_finite() {
        return None;
    }
    if vi_plus > vi_minus + 0.05 {
        Some(Signal { action: "BUY".into(), confidence: 0.50, reason: format!("Vortex VI+ {:.3} > VI- {:.3}", vi_plus, vi_minus) })
    } else if vi_minus > vi_plus + 0.05 {
        Some(Signal { action: "SELL".into(), confidence: 0.50, reason: format!("Vortex VI- {:.3} > VI+ {:.3}", vi_minus, vi_plus) })
    } else {
        None
    }
}

/// ── Strategy 61: Relative Volatility Index ────────────────────────
/// RVI(14) < 30 → BUY (low vol regime, reversion); > 70 → SELL

pub fn rvi_strategy(closes: &[f64]) -> Option<Signal> {
    let rvi_val = indicators::rvi(closes, 14);
    if !rvi_val.is_finite() {
        return None;
    }
    if rvi_val < 30.0 {
        let conf = ((30.0 - rvi_val) / 30.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("RVI oversold {:.1}", rvi_val) })
    } else if rvi_val > 70.0 {
        let conf = ((rvi_val - 70.0) / 30.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("RVI overbought {:.1}", rvi_val) })
    } else {
        None
    }
}

/// ── Strategy 62: Coppock Curve ────────────────────────────────────
/// Coppock(10) crossing above 0 → BUY (long-term bullish); below 0 → SELL

pub fn coppock_strategy(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 30 {
        return None;
    }
    let curr = indicators::coppock(closes, 10);
    let prev = indicators::coppock(&closes[..n - 1], 10);
    if !curr.is_finite() || !prev.is_finite() {
        return None;
    }
    if prev <= 0.0 && curr > 0.0 {
        Some(Signal { action: "BUY".into(), confidence: 0.60, reason: format!("Coppock crossed above 0 ({:.1})", curr) })
    } else if prev >= 0.0 && curr < 0.0 {
        Some(Signal { action: "SELL".into(), confidence: 0.60, reason: format!("Coppock crossed below 0 ({:.1})", curr) })
    } else {
        None
    }
}

/// ── Strategy 63: Regression Channel ───────────────────────────────
/// Price above upper band → SELL (overextended); below lower band → BUY

pub fn std_channel_strategy(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 20 {
        return None;
    }
    let (lower, _mid, upper) = indicators::linreg_channel(closes, 20, 2.0);
    if !lower.is_finite() || !upper.is_finite() {
        return None;
    }
    let price = closes[n - 1];
    if price >= upper {
        let dev = (price - upper) / upper;
        let conf = (dev * 20.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Price ${:.2} above upper channel ${:.2}", price, upper) })
    } else if price <= lower {
        let dev = (lower - price) / lower;
        let conf = (dev * 20.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Price ${:.2} below lower channel ${:.2}", price, lower) })
    } else {
        None
    }
}

/// ── Strategy 64: Volatility Ratio ─────────────────────────────────
/// VR(14) > 1.5 (vol expansion) + price direction → breakout signal

pub fn vol_ratio_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len().min(highs.len()).min(lows.len());
    if n < 16 {
        return None;
    }
    let vr = indicators::volatility_ratio(highs, lows, 14);
    if !vr.is_finite() {
        return None;
    }
    if vr < 1.5 {
        return None;
    }
    let price_chg = (closes[n - 1] - closes[n - 2]) / closes[n - 2];
    if price_chg > 0.015 {
        let conf = (vr / 4.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Vol ratio {:.1}x + bullish breakout", vr) })
    } else if price_chg < -0.015 {
        let conf = (vr / 4.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Vol ratio {:.1}x + bearish breakout", vr) })
    } else {
        None
    }
}

/// ── Strategy 65: Volume-Weighted MACD ─────────────────────────────
/// MACD(12,26,9) on VWAP series. Histogram cross → signal

pub fn vwap_macd_strategy(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 35 {
        return None;
    }
    // Build VWAP series: cumulative TPV / cumulative volume at each bar
    let mut vwap_series = Vec::with_capacity(n);
    let mut cum_tpv = 0.0;
    let mut cum_vol = 0.0;
    for i in 0..n {
        cum_tpv += closes[i] * volumes[i];
        cum_vol += volumes[i];
        vwap_series.push(if cum_vol > 0.0 { cum_tpv / cum_vol } else { closes[i] });
    }
    let (_macd_line, sig_line, hist) = indicators::macd(&vwap_series, 12, 26, 9);
    if !hist.is_finite() {
        return None;
    }
    // Compute prev histogram
    let prev_macd = indicators::macd(&vwap_series[..n - 1], 12, 26, 9);
    let prev_hist = prev_macd.2;
    if !prev_hist.is_finite() {
        return None;
    }
    let bullish = prev_hist <= 0.0 && hist > 0.0;
    let bearish = prev_hist >= 0.0 && hist < 0.0;
    if bullish {
        let conf = (hist.abs() / sig_line.abs().max(0.0001) * 2.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: "VWAP-MACD bullish cross".into() })
    } else if bearish {
        let conf = (hist.abs() / sig_line.abs().max(0.0001) * 2.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: "VWAP-MACD bearish cross".into() })
    } else {
        None
    }
}

/// ── Strategy 66: Negative Volume Index ────────────────────────────
/// NVI rising → smart money accumulating → BUY; falling → SELL

pub fn nvi_strategy(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len().min(volumes.len());
    if n < 15 {
        return None;
    }
    let mut nvi = 1.0;
    let mut nvi_values = Vec::with_capacity(n);
    nvi_values.push(1.0);
    for i in 1..n {
        if volumes[i] < volumes[i - 1] {
            let pct = (closes[i] - closes[i - 1]) / closes[i - 1];
            nvi += pct;
        }
        nvi_values.push(nvi);
    }
    // Compare recent NVI vs older NVI
    let recent_nvi = indicators::sma(&nvi_values, 5);
    let older_nvi = if nvi_values.len() >= 10 {
        indicators::sma(&nvi_values[..nvi_values.len() - 5], 5)
    } else {
        return None;
    };
    if !recent_nvi.is_finite() || !older_nvi.is_finite() {
        return None;
    }
    if recent_nvi > older_nvi * 1.005 {
        Some(Signal { action: "BUY".into(), confidence: 0.50, reason: format!("NVI rising {:.4} > {:.4}", recent_nvi, older_nvi) })
    } else if recent_nvi < older_nvi * 0.995 {
        Some(Signal { action: "SELL".into(), confidence: 0.50, reason: format!("NVI falling {:.4} < {:.4}", recent_nvi, older_nvi) })
    } else {
        None
    }
}

/// ── Strategy 67: DeMarker Oscillator ──────────────────────────────
/// DeM(14) < 0.3 → BUY; > 0.7 → SELL

pub fn demarker_strategy(highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let dem = indicators::demarker(highs, lows, 14);
    if !dem.is_finite() {
        return None;
    }
    if dem < 0.3 {
        let conf = ((0.3 - dem) / 0.3).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("DeM oversold {:.3}", dem) })
    } else if dem > 0.7 {
        let conf = ((dem - 0.7) / 0.3).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("DeM overbought {:.3}", dem) })
    } else {
        None
    }
}

/// ── Strategy 68: Gap Reversion ────────────────────────────────────
/// Large close-to-close gap (>2%) fades back. Approximation without opens.

pub fn gap_reversion_strategy(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 3 {
        return None;
    }
    let gap = (closes[n - 1] - closes[n - 2]) / closes[n - 2];
    if gap > 0.02 {
        let conf = ((gap - 0.02) * 5.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("Gap up {:.1}% fade SELL", gap * 100.0) })
    } else if gap < -0.02 {
        let conf = ((gap.abs() - 0.02) * 5.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("Gap down {:.1}% fade BUY", gap * 100.0) })
    } else {
        None
    }
}

/// ── Strategy 69: SuperTrend ────────────────────────────────────────
/// Trend-following with ATR-based trailing stop. ATR(10) * 3.0.

pub fn supertrend_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 15 {
        return None;
    }
    let period = 10;
    let mult = 3.0;
    let mut supertrend = closes[0];
    let mut trend = 1i32; // 1 = up, -1 = down
    
    for i in 1..n {
        // Compute ATR for the current bar using rolling window
        let atr_start = i.saturating_sub(period);
        let window_highs = &highs[atr_start..=i];
        let window_lows = &lows[atr_start..=i];
        let window_closes = &closes[atr_start..=i];
        let atr = indicators::atr(window_highs, window_lows, window_closes, period);
        
        if atr <= 0.0 {
            continue;
        }
        
        let hl2 = (highs[i] + lows[i]) / 2.0;
        let basic_up = hl2 - mult * atr;
        let basic_down = hl2 + mult * atr;
        
        if trend == 1 {
            supertrend = supertrend.max(basic_up);
            if closes[i] < supertrend {
                trend = -1;
            }
        } else {
            supertrend = supertrend.min(basic_down);
            if closes[i] > supertrend {
                trend = 1;
            }
        }
    }
    
    let price = closes[n - 1];
    if trend == 1 && price > supertrend {
        let conf = ((price - supertrend) / price * 100.0).min(1.0);
        Some(Signal { action: "BUY".into(), confidence: conf, reason: format!("SuperTrend BUY: price {:.2} > ST {:.2}", price, supertrend) })
    } else if trend == -1 && price < supertrend {
        let conf = ((supertrend - price) / price * 100.0).min(1.0);
        Some(Signal { action: "SELL".into(), confidence: conf, reason: format!("SuperTrend SELL: price {:.2} < ST {:.2}", price, supertrend) })
    } else {
        None
    }
}

/// ── Strategy 70: Fisher Transform ──────────────────────────────────
/// Mean reversion oscillator. Normalizes price to Gaussian distribution.

pub fn fisher_transform_strategy(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 20 {
        return None;
    }
    let period = 10;
    let mut fisher = 0.0;
    
    let start = n - period;
    let hi = closes[start..].iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let lo = closes[start..].iter().copied().fold(f64::INFINITY, f64::min);
    let mid = (hi + lo) / 2.0;
    let range = hi - lo;
    if range > 0.0 {
        let value = ((closes[n - 1] - mid) / range).clamp(-0.999, 0.999);
        fisher = 0.5 * (value.ln_1p() - (-value).ln_1p()) + 0.5 * fisher;
    }
    
    if fisher > 1.5 {
        Some(Signal { action: "SELL".into(), confidence: (fisher / 3.0).min(1.0), reason: format!("Fisher Transform {:.2} overbought", fisher) })
    } else if fisher < -1.5 {
        Some(Signal { action: "BUY".into(), confidence: (fisher.abs() / 3.0).min(1.0), reason: format!("Fisher Transform {:.2} oversold", fisher) })
    } else {
        None
    }
}

/// ── Strategy 71: Ultimate Oscillator ───────────────────────────────
/// Multi-timeframe momentum (7, 14, 28 periods).

pub fn ultimate_oscillator_strategy(closes: &[f64], highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 30 {
        return None;
    }
    
    fn bp(highs: &[f64], lows: &[f64], closes: &[f64], i: usize) -> f64 {
        let prev_close = closes[i - 1];
        let tr = highs[i].max(prev_close) - lows[i].min(prev_close);
        let bp = closes[i] - lows[i].min(prev_close);
        if tr > 0.0 { bp / tr } else { 0.0 }
    }
    
    let mut uo = 0.0;
    for (period, weight) in [(7, 4.0), (14, 2.0), (28, 1.0)] {
        let mut sum_bp = 0.0;
        let mut sum_tr = 0.0;
        for i in (n - period)..n {
            let tr = highs[i].max(closes[i - 1]) - lows[i].min(closes[i - 1]);
            sum_bp += bp(highs, lows, closes, i);
            sum_tr += tr;
        }
        if sum_tr > 0.0 {
            uo += weight * sum_bp / sum_tr;
        }
    }
    uo /= 7.0;
    uo *= 100.0;
    
    if uo > 70.0 {
        Some(Signal { action: "SELL".into(), confidence: ((uo - 70.0) / 30.0).min(1.0), reason: format!("Ultimate Osc {:.1} overbought", uo) })
    } else if uo < 30.0 {
        Some(Signal { action: "BUY".into(), confidence: ((30.0 - uo) / 30.0).min(1.0), reason: format!("Ultimate Osc {:.1} oversold", uo) })
    } else {
        None
    }
}

/// ── Strategy 72: Volume-Weighted RSI ───────────────────────────────
/// RSI weighted by volume for confirmation.

pub fn vw_rsi_strategy(closes: &[f64], volumes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 15 || volumes.len() < n {
        return None;
    }
    let period = 14;
    let mut vw_gain = 0.0;
    let mut vw_loss = 0.0;
    
    for i in (n - period)..n {
        let change = closes[i] - closes[i - 1];
        let vol = volumes[i].max(1.0);
        if change > 0.0 {
            vw_gain += change * vol;
        } else {
            vw_loss += (-change) * vol;
        }
    }
    
    if vw_loss <= 0.0 {
        return Some(Signal { action: "BUY".into(), confidence: 0.9, reason: "VW-RSI extreme bullish".into() });
    }
    let vw_rs = vw_gain / vw_loss;
    let vw_rsi = 100.0 - 100.0 / (1.0 + vw_rs);
    
    if vw_rsi > 70.0 {
        Some(Signal { action: "SELL".into(), confidence: ((vw_rsi - 70.0) / 30.0).min(1.0), reason: format!("VW-RSI {:.1} overbought", vw_rsi) })
    } else if vw_rsi < 30.0 {
        Some(Signal { action: "BUY".into(), confidence: ((30.0 - vw_rsi) / 30.0).min(1.0), reason: format!("VW-RSI {:.1} oversold", vw_rsi) })
    } else {
        None
    }
}

/// ── Conjugate Gradient HP Filter Helper ───────────────────────────

/// Solve (I + λ * D'D) * trend = y using conjugate gradient.
/// D is the (n-2)×n second-difference matrix.
fn hp_filter_cg(y: &[f64], lambda: f64, max_iter: usize, tol: f64) -> Vec<f64> {
    let n = y.len();
    let mut x = vec![0.0_f64; n];
    let mut r = y.to_vec();
    let mut p = r.clone();
    let mut rr: f64 = r.iter().map(|v| v * v).sum();

    for _ in 0..max_iter {
        if rr.sqrt() < tol {
            break;
        }

        // Compute Ap = A @ p = p + λ * D' @ (D @ p)
        // Step 1: Dp = second diff of p (n-2 elements)
        let mut dp = Vec::with_capacity(n - 2);
        for i in 0..n - 2 {
            dp.push(p[i] - 2.0 * p[i + 1] + p[i + 2]);
        }

        // Step 2: D' @ Dp (n elements)
        let mut dtdp = vec![0.0_f64; n];
        for i in 0..n {
            if i >= 2 {
                dtdp[i] += dp[i - 2];
            }
            if i >= 1 && i < n - 1 {
                dtdp[i] -= 2.0 * dp[i - 1];
            }
            if i < n - 2 {
                dtdp[i] += dp[i];
            }
        }

        // Step 3: Ap = p + λ * dtdp
        let mut ap = vec![0.0_f64; n];
        for i in 0..n {
            ap[i] = p[i] + lambda * dtdp[i];
        }

        // alpha = r·r / p·Ap
        let pap: f64 = p.iter().zip(ap.iter()).map(|(pi, ai)| pi * ai).sum();
        if pap.abs() < 1e-15 {
            break;
        }
        let alpha = rr / pap;

        // Update x and r
        for i in 0..n {
            x[i] += alpha * p[i];
            r[i] -= alpha * ap[i];
        }

        let rr_new: f64 = r.iter().map(|v| v * v).sum();
        if rr_new.sqrt() < tol {
            break;
        }
        let beta = rr_new / rr;
        rr = rr_new;

        for i in 0..n {
            p[i] = r[i] + beta * p[i];
        }
    }

    x
}

/// ── Strategy 73: Kalman Filter Mean Reversion ─────────────────────

/// Uses a 1D Kalman filter to estimate the adaptive mean of price.
/// When price deviates > 2σ from the state estimate, trade mean reversion.
/// Genuinely different from z-score (fixed window) — Kalman adaptively
/// adjusts its uncertainty estimate over time.
pub fn kalman_filter_mr(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 30 {
        return None;
    }

    let q = 0.01; // process noise (how fast the mean can drift)
    let r = 0.1;  // observation noise (price noise relative to mean)

    let mut x = closes[0]; // state estimate (estimated true price)
    let mut p = 1.0;       // estimate covariance

    let mut deviations = Vec::with_capacity(n);

    for &z in closes {
        p = p + q;
        let k = p / (p + r);
        let innovation = z - x;
        x = x + k * innovation;
        p = (1.0 - k) * p;
        let std_est = p.sqrt();
        let dev = if std_est > 1e-10 {
            innovation / std_est
        } else {
            0.0
        };
        deviations.push(dev);
    }

    let last_dev = deviations[n - 1];
    let prev_dev = if n >= 2 {
        deviations[n - 2]
    } else {
        last_dev
    };

    if last_dev > 2.0 && prev_dev >= 0.0 {
        let conf = ((last_dev - 2.0) / 3.0).min(1.0);
        Some(Signal {
            action: "SELL".into(),
            confidence: conf.max(0.1),
            reason: format!("Kalman MR: price {:.1}σ above adaptive mean", last_dev),
        })
    } else if last_dev < -2.0 && prev_dev <= 0.0 {
        let conf = ((-last_dev - 2.0) / 3.0).min(1.0);
        Some(Signal {
            action: "BUY".into(),
            confidence: conf.max(0.1),
            reason: format!("Kalman MR: price {:.1}σ below adaptive mean", -last_dev),
        })
    } else {
        None
    }
}

/// ── Strategy 74: HP Trend-Cycle Decomposition ─────────────────────

/// Decomposes price into trend + cycle using the Hodrick-Prescott filter.
/// Trades cycle extremes: SELL when cycle > 1.5σ above zero (cycle top),
/// BUY when < -1.5σ below zero (cycle bottom).
/// Also captures zero-crossings for early trend-change detection.
/// This is NOT a moving-average strategy — HP filter uses a global
/// optimization (minimizing sum of squares + second-difference penalty).
pub fn hp_trend_cycle(closes: &[f64]) -> Option<Signal> {
    let n = closes.len();
    if n < 40 {
        return None;
    }

    let lambda = 1600.0; // standard smoothing for daily data

    // Compute trend via CG (converges in ~20 iterations for n=200)
    let trend = hp_filter_cg(closes, lambda, 100, 1e-6);

    // Cycle = price - trend
    let mut cycle = Vec::with_capacity(n);
    let mut c_sum = 0.0_f64;
    let mut c_sq_sum = 0.0_f64;
    let mut c_count = 0;

    for i in 0..n {
        let c = closes[i] - trend[i];
        cycle.push(c);
        if i >= 20 {
            c_sum += c;
            c_sq_sum += c * c;
            c_count += 1;
        }
    }

    if c_count < 10 {
        return None;
    }

    let c_mean = c_sum / c_count as f64;
    let c_var = (c_sq_sum / c_count as f64) - c_mean * c_mean;
    let c_std = c_var.max(0.0).sqrt();
    if c_std < 1e-10 {
        return None;
    }

    let last_c = cycle[n - 1];
    let norm_c = (last_c - c_mean) / c_std;

    // Check zero-crossing with 3-bar lag
    let prev_c = if n >= 4 { cycle[n - 4] } else { cycle[n - 2] };

    if norm_c > 1.5 {
        let conf = ((norm_c - 1.5) / 3.0).min(1.0);
        Some(Signal {
            action: "SELL".into(),
            confidence: conf.max(0.1),
            reason: format!("HP cycle: {:.1}σ above trend (extreme)", norm_c),
        })
    } else if norm_c < -1.5 {
        let conf = ((-norm_c - 1.5) / 3.0).min(1.0);
        Some(Signal {
            action: "BUY".into(),
            confidence: conf.max(0.1),
            reason: format!("HP cycle: {:.1}σ below trend (extreme)", -norm_c),
        })
    } else if last_c > 0.0 && prev_c <= 0.0 && norm_c > 0.5 {
        // Cycle crossed zero upward with momentum
        Some(Signal {
            action: "BUY".into(),
            confidence: 0.5,
            reason: format!("HP cycle: zero-cross up ({:.1}σ)", norm_c),
        })
    } else if last_c < 0.0 && prev_c >= 0.0 && -norm_c > 0.5 {
        Some(Signal {
            action: "SELL".into(),
            confidence: 0.5,
            reason: format!("HP cycle: zero-cross down ({:.1}σ)", -norm_c),
        })
    } else {
        None
    }
}

/// ── Master Dispatch ───────────────────────────────────────────────

/// Run a single strategy by name.
pub fn evaluate(name: &str, closes: &[f64], volumes: &[f64],
                 highs: &[f64], lows: &[f64]) -> Option<Signal> {
    let opens = &[];
    evaluate_opens(name, closes, opens, volumes, highs, lows)
}

/// Run a single strategy with opens data.
pub fn evaluate_opens(name: &str, closes: &[f64], opens: &[f64], volumes: &[f64],
                 highs: &[f64], lows: &[f64]) -> Option<Signal> {
    match name {
        "ema_cross" => ema_crossover(closes),
        "rsi_revert" => rsi_mean_reversion(closes),
        "boll_break" => bollinger_breakout(closes),
        "zscore_revert" => zscore_reversion(closes),
        "vol_mom" => volume_momentum(closes, volumes),
        "macd" => macd_crossover(closes),
        "vwap_revert" => vwap_reversion(closes, volumes),
        "obv_div" => obv_divergence(closes, volumes),
        "cmo" => chande_momentum(closes),
        "trix" => trix_signal(closes),
        "adx" => adx_strategy(closes, highs, lows),
        "keltner" => keltner_channels(closes, highs, lows),
        "chaikin_mf" => chaikin_money_flow(closes, volumes, highs, lows),
        "williams_r" => williams_r_strategy(closes, highs, lows),
        "psar" => parabolic_sar(closes, highs, lows),
        "hma" => hull_ma(closes),
        "force_idx" => force_index(closes, volumes),
        "vpt" => volume_price_trend(closes, volumes),
        "donchian" => donchian_channels(closes, highs, lows),
        "aroon" => aroon_strategy(closes, highs, lows),
        "price_eff" => price_efficiency_ratio(closes, volumes),
        "scci" => simplified_cci(closes),
        "range_exp_idx" => range_expansion_index(closes, highs, lows),
        "ema_dev" => ema_deviation(closes),
        "snr_idx" => signal_to_noise_ratio(closes),
        "candle_pat" => candlestick_patterns(closes, opens, highs, lows),
        "sup_res" => support_resistance(closes, opens, highs, lows),
        "liq_vac" => liquidity_vacuum(closes, opens, highs, lows, volumes),
        "cvd_flow" => smart_money_flow(closes, opens, highs, lows, volumes),
        "vcp" => volatility_compression(closes, opens, highs, lows),
        "impulse_exh" => impulse_exhaustion(closes, opens, highs, lows, volumes),
        "mom_accel" => momentum_acceleration(closes, volumes),
        "rsi_fail" => rsi_failure_swing(closes),
        "avwap" => anchored_vwap(closes, opens, highs, lows, volumes),
        "donch_pull" => donchian_pullback(closes, opens, highs, lows),
        "vol_prof" => volume_profile_strategy(closes, opens, highs, lows, volumes),
        "bb_squeeze" => bollinger_squeeze(closes, opens, highs, lows),
        "multi_rsi" => multi_tf_rsi(closes),
        "linreg_slope" => linreg_slope_strategy(closes, volumes),
        "hurst" => hurst_regime(closes, opens, highs, lows, volumes),
        // ── 10 new strategies (41-50) ──
        "elder_ray" => elder_ray_index(closes, highs, lows),
        "klinger" => klinger_oscillator(closes, volumes, highs, lows),
        "pivot_points" => pivot_point_strategy(closes, highs, lows),
        "ichimoku" => ichimoku_cloud(closes, highs, lows),
        "choppiness" => choppiness_index(closes, highs, lows),
        "true_cci" => commodity_channel_index(closes, highs, lows),
        "dpo" => detrended_price_oscillator(closes),
        "kst" => know_sure_thing(closes),
        "mass_idx" => mass_index(closes, highs, lows),
        "ulcer" => ulcer_index(closes),
        // ── 6 new OHLCV strategies (51-56) ──
        "mfi" => money_flow_index(closes, highs, lows, volumes),
        "stoch" => stochastic_oscillator(closes, highs, lows),
        "emv" => ease_of_movement(highs, lows, volumes),
        "ad_div" => ad_divergence(closes, highs, lows, volumes),
        "envelope" => envelope_channels(closes),
        "atr_channel" => atr_channel_strategy(closes, highs, lows),
        // ── 12 new strategies (57-68) ──
        "kama" => kama_strategy(closes),
        "dmi_cross" => dmi_cross_strategy(closes, highs, lows),
        "vma" => vma_strategy(closes),
        "vortex" => vortex_strategy(closes, highs, lows),
        "rvi" => rvi_strategy(closes),
        "coppock" => coppock_strategy(closes),
        "std_channel" => std_channel_strategy(closes),
        "vol_ratio" => vol_ratio_strategy(closes, highs, lows),
        "vwap_macd" => vwap_macd_strategy(closes, volumes),
        "nvi" => nvi_strategy(closes, volumes),
        "de_marker" => demarker_strategy(highs, lows),
        "gap_revert" => gap_reversion_strategy(closes),
        "supertrend" => supertrend_strategy(closes, highs, lows),
        "fisher" => fisher_transform_strategy(closes),
        "ultimate_osc" => ultimate_oscillator_strategy(closes, highs, lows),
        "vw_rsi" => vw_rsi_strategy(closes, volumes),
        "kalman_mr" => kalman_filter_mr(closes),
        "hp_trend" => hp_trend_cycle(closes),
        _ => None,
    }
}

/// Run all 68 technical strategies. Returns Vec of (name, Signal) for non-HOLD signals.
pub fn evaluate_all(closes: &[f64], volumes: &[f64],
                     highs: &[f64], lows: &[f64]) -> Vec<(String, Signal)> {
    // Synthesize opens from closes (each bar's open = previous close)
    let opens: Vec<f64> = if closes.len() > 1 {
        let mut o = Vec::with_capacity(closes.len());
        o.push(closes[0]);
        o.extend_from_slice(&closes[..closes.len()-1]);
        o
    } else {
        closes.to_vec()
    };
    evaluate_all_opens(closes, &opens, volumes, highs, lows)
}

/// Run all 68 technical strategies with opens data.
pub fn evaluate_all_opens(closes: &[f64], opens: &[f64], volumes: &[f64],
                     highs: &[f64], lows: &[f64]) -> Vec<(String, Signal)> {
    let strategies: &[(&str, fn(&[f64], &[f64], &[f64], &[f64], &[f64]) -> Option<Signal>)] = &[
        ("ema_cross",    |c, _, _, _, _| ema_crossover(c)),
        ("rsi_revert",   |c, _, _, _, _| rsi_mean_reversion(c)),
        ("boll_break",   |c, _, _, _, _| bollinger_breakout(c)),
        ("zscore_revert",|c, _, _, _, _| zscore_reversion(c)),
        ("vol_mom",      |c, _, v, _, _| volume_momentum(c, v)),
        ("macd",         |c, _, _, _, _| macd_crossover(c)),
        ("vwap_revert",  |c, _, v, _, _| vwap_reversion(c, v)),
        ("obv_div",      |c, _, v, _, _| obv_divergence(c, v)),
        ("cmo",          |c, _, _, _, _| chande_momentum(c)),
        ("trix",         |c, _, _, _, _| trix_signal(c)),
        ("adx",          |c, _, _, h, l| adx_strategy(c, h, l)),
        ("keltner",      |c, _, _, h, l| keltner_channels(c, h, l)),
        ("chaikin_mf",   |c, _, v, h, l| chaikin_money_flow(c, v, h, l)),
        ("williams_r",   |c, _, _, h, l| williams_r_strategy(c, h, l)),
        ("psar",         |c, _, _, h, l| parabolic_sar(c, h, l)),
        ("hma",          |c, _, _, _, _| hull_ma(c)),
        ("force_idx",    |c, _, v, _, _| force_index(c, v)),
        ("vpt",          |c, _, v, _, _| volume_price_trend(c, v)),
        ("donchian",     |c, _, _, h, l| donchian_channels(c, h, l)),
        ("aroon",        |c, _, _, h, l| aroon_strategy(c, h, l)),
        ("price_eff",    |c, _, v, _, _| price_efficiency_ratio(c, v)),
        ("scci",         |c, _, _, _, _| simplified_cci(c)),
        ("range_exp_idx",|c, _, _, h, l| range_expansion_index(c, h, l)),
        ("ema_dev",      |c, _, _, _, _| ema_deviation(c)),
        ("snr_idx",      |c, _, _, _, _| signal_to_noise_ratio(c)),
        // ── 10 new strategies ──
        ("candle_pat",   |c, o, _, h, l| candlestick_patterns(c, o, h, l)),
        ("sup_res",      |c, o, _, h, l| support_resistance(c, o, h, l)),
        ("liq_vac",      |c, o, v, h, l| liquidity_vacuum(c, o, h, l, v)),
        ("cvd_flow",     |c, o, v, h, l| smart_money_flow(c, o, h, l, v)),
        ("vcp",          |c, o, _, h, l| volatility_compression(c, o, h, l)),
        ("impulse_exh",  |c, o, v, h, l| impulse_exhaustion(c, o, h, l, v)),
        ("mom_accel",    |c, _, v, _, _| momentum_acceleration(c, v)),
        ("rsi_fail",     |c, _, _, _, _| rsi_failure_swing(c)),
        ("avwap",        |c, o, v, h, l| anchored_vwap(c, o, h, l, v)),
        ("donch_pull",   |c, o, _, h, l| donchian_pullback(c, o, h, l)),
        ("vol_prof",     |c, o, v, h, l| volume_profile_strategy(c, o, h, l, v)),
        ("bb_squeeze",   |c, o, _, h, l| bollinger_squeeze(c, o, h, l)),
        ("multi_rsi",    |c, _, _, _, _| multi_tf_rsi(c)),
        ("linreg_slope", |c, _, v, _, _| linreg_slope_strategy(c, v)),
        ("hurst",        |c, o, v, h, l| hurst_regime(c, o, h, l, v)),
        // ── 10 new strategies (41-50) ──
        ("elder_ray",    |c, _, _, h, l| elder_ray_index(c, h, l)),
        ("klinger",      |c, _, v, h, l| klinger_oscillator(c, v, h, l)),
        ("pivot_points", |c, _, _, h, l| pivot_point_strategy(c, h, l)),
        ("ichimoku",     |c, _, _, h, l| ichimoku_cloud(c, h, l)),
        ("choppiness",   |c, _, _, h, l| choppiness_index(c, h, l)),
        ("true_cci",     |c, _, _, h, l| commodity_channel_index(c, h, l)),
        ("dpo",          |c, _, _, _, _| detrended_price_oscillator(c)),
        ("kst",          |c, _, _, _, _| know_sure_thing(c)),
        ("mass_idx",     |c, _, _, h, l| mass_index(c, h, l)),
        ("ulcer",        |c, _, _, _, _| ulcer_index(c)),
        // ── 6 new OHLCV strategies (51-56) ──
        ("mfi",          |c, _, v, h, l| money_flow_index(c, h, l, v)),
        ("stoch",        |c, _, _, h, l| stochastic_oscillator(c, h, l)),
        ("emv",          |_, _, v, h, l| ease_of_movement(h, l, v)),
        ("ad_div",       |c, _, v, h, l| ad_divergence(c, h, l, v)),
        ("envelope",     |c, _, _, _, _| envelope_channels(c)),
        ("atr_channel",  |c, _, _, h, l| atr_channel_strategy(c, h, l)),
        // ── 12 new strategies (57-68) ──
        ("kama",         |c, _, _, _, _| kama_strategy(c)),
        ("dmi_cross",    |c, _, _, h, l| dmi_cross_strategy(c, h, l)),
        ("vma",          |c, _, _, _, _| vma_strategy(c)),
        ("vortex",       |c, _, _, h, l| vortex_strategy(c, h, l)),
        ("rvi",          |c, _, _, _, _| rvi_strategy(c)),
        ("coppock",      |c, _, _, _, _| coppock_strategy(c)),
        ("std_channel",  |c, _, _, _, _| std_channel_strategy(c)),
        ("vol_ratio",    |c, _, _, h, l| vol_ratio_strategy(c, h, l)),
        ("vwap_macd",    |c, _, v, _, _| vwap_macd_strategy(c, v)),
        ("nvi",          |c, _, v, _, _| nvi_strategy(c, v)),
        ("de_marker",    |_, _, _, h, l| demarker_strategy(h, l)),
        ("gap_revert",   |c, _, _, _, _| gap_reversion_strategy(c)),
        ("supertrend",   |c, _, _, h, l| supertrend_strategy(c, h, l)),
        ("fisher",       |c, _, _, _, _| fisher_transform_strategy(c)),
        ("ultimate_osc", |c, _, _, h, l| ultimate_oscillator_strategy(c, h, l)),
        ("vw_rsi",       |c, _, v, _, _| vw_rsi_strategy(c, v)),
        ("kalman_mr",    |c, _, _, _, _| kalman_filter_mr(c)),
        ("hp_trend",     |c, _, _, _, _| hp_trend_cycle(c)),
    ];

    let mut results = Vec::new();
    for (name, func) in strategies {
        if let Some(signal) = func(closes, opens, volumes, highs, lows) {
            results.push((name.to_string(), signal));
        }
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rsi_signal() {
        let v: Vec<f64> = (0..50).map(|i| 100.0 + i as f64).collect();
        let sig = rsi_mean_reversion(&v);
        assert!(sig.is_none() || sig.unwrap().action == "SELL");
    }

    #[test]
    fn test_evaluate_all_returns() {
        let closes: Vec<f64> = (0..100).map(|i| 100.0 + (i as f64 * 0.1)).collect();
        let volumes: Vec<f64> = vec![1000.0; 100];
        let highs: Vec<f64> = closes.iter().map(|c| c + 1.0).collect();
        let lows: Vec<f64> = closes.iter().map(|c| c - 1.0).collect();
        let results = evaluate_all(&closes, &volumes, &highs, &lows);
        for (name, sig) in &results {
            assert!(sig.action == "BUY" || sig.action == "SELL" || sig.action == "HOLD",
                    "{} had invalid action {}", name, sig.action);
            assert!(sig.confidence >= 0.0 && sig.confidence <= 1.0,
                    "{} had invalid confidence {}", name, sig.confidence);
        }
    }
}
