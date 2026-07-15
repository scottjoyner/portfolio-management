/// Streaming (incremental) indicators — O(1) per-tick updates.
///
/// Maintains running state for EMA, SMA(+Bollinger), RSI, MACD, Z-score
/// so each new tick processes in O(1) instead of recomputing over all N candles.
///
/// Mirrors trading_system/core/streaming.py in Rust.

use std::collections::HashMap;

/// Fixed-size circular buffer for streaming data.
#[derive(Clone)]
pub struct RingBuffer {
    data: Vec<f64>,
    maxlen: usize,
    head: usize,
    size: usize,
}

impl RingBuffer {
    pub fn new(maxlen: usize) -> Self {
        Self {
            data: vec![0.0; maxlen],
            maxlen,
            head: 0,
            size: 0,
        }
    }

    pub fn append(&mut self, value: f64) {
        self.data[self.head] = value;
        self.head = (self.head + 1) % self.maxlen;
        if self.size < self.maxlen {
            self.size += 1;
        }
    }

    pub fn get(&self, index: usize) -> Option<f64> {
        if index >= self.size {
            return None;
        }
        let pos = (self.head + self.maxlen - self.size + index) % self.maxlen;
        Some(self.data[pos])
    }

    pub fn last(&self) -> Option<f64> {
        if self.size == 0 {
            None
        } else {
            self.get(self.size - 1)
        }
    }

    pub fn oldest(&self) -> Option<f64> {
        self.get(0)
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn to_vec(&self) -> Vec<f64> {
        (0..self.size).map(|i| self.get(i).unwrap()).collect()
    }
}

/// Running state for one indicator type.  Not public — use StreamingIndicators.
struct EmaState(HashMap<usize, f64>);

impl EmaState {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn update(&mut self, price: f64) {
        for (period, prev) in self.0.iter_mut() {
            let k = 2.0 / (*period as f64 + 1.0);
            *prev = price * k + *prev * (1.0 - k);
        }
    }

    fn seed(&mut self, period: usize, value: f64) {
        self.0.insert(period, value);
    }

    fn get(&self, period: usize) -> Option<f64> {
        self.0.get(&period).copied()
    }
}

struct SmaState(HashMap<usize, (f64, f64)>); // period -> (sma, sq_sum)

impl SmaState {
    fn new() -> Self {
        Self(HashMap::new())
    }

    fn update(&mut self, price: f64, buffer: &RingBuffer) {
        let n = buffer.len();
        let sq2 = price * price;
        let periods: Vec<usize> = self.0.keys().copied().collect();
        for period in periods {
            let (prev_sma, prev_sq) = match self.0.get(&period) {
                Some(v) => *v,
                None => continue,
            };
            if n > period {
                if let Some(oldest) = buffer.get(n - period - 1) {
                    let new_sma = prev_sma + (price - oldest) / period as f64;
                    let new_sq = prev_sq + sq2 - oldest * oldest;
                    self.0.insert(period, (new_sma, new_sq));
                }
            } else {
                let new_sma = prev_sma + (price - prev_sma) / period as f64;
                let new_sq = prev_sq + sq2;
                self.0.insert(period, (new_sma, new_sq));
            }
        }
    }

    fn seed(&mut self, period: usize, sma: f64, sq_sum: f64) {
        self.0.insert(period, (sma, sq_sum));
    }

    fn get(&self, period: usize) -> Option<f64> {
        self.0.get(&period).map(|v| v.0)
    }

    fn bollinger(&self, period: usize) -> Option<(f64, f64, f64)> {
        let (mid, sq_sum) = self.0.get(&period).copied()?;
        let variance = sq_sum / period as f64 - mid * mid;
        let std = variance.max(0.0).sqrt();
        Some((mid, mid + 2.0 * std, mid - 2.0 * std))
    }
}

/// Streaming indicator engine for one product.
pub struct StreamingIndicators {
    pub product_id: String,
    pub closes: RingBuffer,
    pub volumes: RingBuffer,

    ema: EmaState,
    sma: SmaState,

    // RSI state (Wilder's smoothing)
    rsi_avg_gain: Option<f64>,
    rsi_avg_loss: Option<f64>,
    rsi_prev_close: Option<f64>,

    // MACD state
    macd_ema_fast: Option<f64>,
    macd_ema_slow: Option<f64>,
    macd_signal: Option<f64>,
}

impl StreamingIndicators {
    pub fn new(product_id: &str, maxlen: usize) -> Self {
        Self {
            product_id: product_id.to_string(),
            closes: RingBuffer::new(maxlen),
            volumes: RingBuffer::new(maxlen),
            ema: EmaState::new(),
            sma: SmaState::new(),
            rsi_avg_gain: None,
            rsi_avg_loss: None,
            rsi_prev_close: None,
            macd_ema_fast: None,
            macd_ema_slow: None,
            macd_signal: None,
        }
    }

    pub fn update(&mut self, close: f64, volume: f64) {
        self.closes.append(close);
        self.volumes.append(volume);
        self.sma.update(close, &self.closes);
        self.ema.update(close);
        self.update_rsi(close);
        self.update_macd(close);
    }

    // ── EMA ────────────────────────────────────────────────────────

    pub fn seed_ema(&mut self, period: usize, value: f64) {
        self.ema.seed(period, value);
    }

    pub fn ema(&self, period: usize) -> Option<f64> {
        self.ema.get(period)
    }

    // ── SMA + Bollinger ────────────────────────────────────────────

    pub fn seed_sma(&mut self, period: usize, sma: f64, sq_sum: f64) {
        self.sma.seed(period, sma, sq_sum);
    }

    pub fn sma(&self, period: usize) -> Option<f64> {
        self.sma.get(period)
    }

    pub fn bollinger(&self, period: usize) -> Option<(f64, f64, f64)> {
        self.sma.bollinger(period)
    }

    // ── RSI ────────────────────────────────────────────────────────

    fn update_rsi(&mut self, price: f64) {
        let prev = match self.rsi_prev_close {
            Some(v) => v,
            None => {
                self.rsi_prev_close = Some(price);
                return;
            }
        };
        let change = price - prev;
        let gain = change.max(0.0);
        let loss = (-change).max(0.0);
        self.rsi_prev_close = Some(price);

        match (self.rsi_avg_gain, self.rsi_avg_loss) {
            (None, _) => {
                self.rsi_avg_gain = Some(gain);
                self.rsi_avg_loss = Some(loss);
            }
            (Some(avg_gain), Some(avg_loss)) => {
                self.rsi_avg_gain = Some((avg_gain * 13.0 + gain) / 14.0);
                self.rsi_avg_loss = Some((avg_loss * 13.0 + loss) / 14.0);
            }
            _ => {
                self.rsi_avg_gain = Some(gain);
                self.rsi_avg_loss = Some(loss);
            }
        }
    }

    pub fn seed_rsi(&mut self, closes: &[f64], period: usize) -> f64 {
        if closes.len() < period + 1 {
            return 50.0;
        }
        let mut gains = 0.0_f64;
        let mut losses = 0.0_f64;
        let start = closes.len() - period;
        for i in start..closes.len() {
            let change = closes[i] - closes[i - 1];
            gains += change.max(0.0);
            losses += (-change).max(0.0);
        }
        self.rsi_avg_gain = Some(gains / period as f64);
        self.rsi_avg_loss = Some(losses / period as f64);
        self.rsi_prev_close = closes.last().copied();
        self.rsi()
    }

    pub fn rsi(&self) -> f64 {
        match (self.rsi_avg_gain, self.rsi_avg_loss) {
            (_, Some(l)) if l == 0.0 => 100.0,
            (Some(g), Some(l)) => 100.0 - 100.0 / (1.0 + g / l),
            _ => 50.0,
        }
    }

    // ── MACD ───────────────────────────────────────────────────────

    fn update_macd(&mut self, price: f64) {
        match (self.macd_ema_fast, self.macd_ema_slow) {
            (Some(fast), Some(slow)) => {
                let k12 = 2.0 / 13.0;
                let k26 = 2.0 / 27.0;
                self.macd_ema_fast = Some(price * k12 + fast * (1.0 - k12));
                self.macd_ema_slow = Some(price * k26 + slow * (1.0 - k26));
                let macd_line = self.macd_ema_fast.unwrap() - self.macd_ema_slow.unwrap();
                self.macd_signal = Some(match self.macd_signal {
                    Some(sig) => {
                        let k9 = 2.0 / 10.0;
                        macd_line * k9 + sig * (1.0 - k9)
                    }
                    None => macd_line,
                });
            }
            _ => {}
        }
    }

    pub fn seed_macd(&mut self, ema_fast: f64, ema_slow: f64) {
        self.macd_ema_fast = Some(ema_fast);
        self.macd_ema_slow = Some(ema_slow);
        self.macd_signal = Some(ema_fast - ema_slow);
    }

    pub fn macd(&self) -> Option<(f64, f64, f64)> {
        let fast = self.macd_ema_fast?;
        let slow = self.macd_ema_slow?;
        let macd_line = fast - slow;
        let sig = self.macd_signal.unwrap_or(macd_line);
        Some((macd_line, sig, macd_line - sig))
    }
}

// ── StreamingEngine (thin manager for multiple products) ───────────

pub struct StreamingEngine {
    pub products: HashMap<String, StreamingIndicators>,
}

impl StreamingEngine {
    pub fn new() -> Self {
        Self {
            products: HashMap::new(),
        }
    }

    pub fn get_or_create(&mut self, product_id: &str, maxlen: usize) -> &mut StreamingIndicators {
        self.products
            .entry(product_id.to_string())
            .or_insert_with(|| StreamingIndicators::new(product_id, maxlen))
    }

    pub fn update(&mut self, product_id: &str, close: f64, volume: f64) {
        if let Some(ind) = self.products.get_mut(product_id) {
            ind.update(close, volume);
        }
    }

    pub fn ema(&self, product_id: &str, period: usize) -> Option<f64> {
        self.products.get(product_id)?.ema(period)
    }

    pub fn rsi(&self, product_id: &str) -> Option<f64> {
        self.products.get(product_id).map(|p| p.rsi())
    }

    pub fn macd(&self, product_id: &str) -> Option<(f64, f64, f64)> {
        self.products.get(product_id)?.macd()
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    #[test]
    fn test_ring_buffer_get_oob() {
        let mut buf = RingBuffer::new(3);
        buf.append(1.0);
        assert_eq!(buf.get(5), None); // index >= size
        assert_eq!(buf.get(0), Some(1.0));
    }

    #[test]
    fn test_sma_seeded_and_bollinger() {
        let mut ind = StreamingIndicators::new("TST", 100);
        // unseeded period -> None paths
        assert!(ind.sma(99).is_none());
        assert!(ind.bollinger(99).is_none());
        assert!(ind.ema(99).is_none());

        // seed SMA and exercise both n<=period and n>period branches
        ind.seed_sma(3, 100.0, 30000.0);
        ind.update(101.0, 10.0); // n=1 -> else branch
        ind.update(102.0, 10.0); // n=2 -> else branch
        ind.update(103.0, 10.0); // n=3 -> else branch
        ind.update(104.0, 10.0); // n=4 -> Some(oldest) branch
        assert!(ind.sma(3).is_some());
        let (mid, up, lo) = ind.bollinger(3).unwrap();
        assert!(mid.is_finite() && up > lo);
    }

    #[test]
    fn test_rsi_fresh_and_seed_edges() {
        let mut ind = StreamingIndicators::new("TST", 100);
        // rsi() before any update -> (None, None) -> 50
        assert_eq!(ind.rsi(), 50.0);
        // first update: prev None branch
        ind.update(50000.0, 10.0);
        // second update: (None, _) branch sets avg
        ind.update(50100.0, 10.0);
        assert!(ind.rsi().is_finite());
        // seed_rsi with too-short closes -> 50
        let short = vec![1.0, 2.0];
        assert_eq!(ind.seed_rsi(&short, 14), 50.0);
    }

    #[test]
    fn test_macd_unseeded() {
        let mut ind = StreamingIndicators::new("TST", 100);
        // update before seed -> _ branch in update_macd
        ind.update(50100.0, 10.0);
        // macd() before seed -> fast None -> None
        assert!(ind.macd().is_none());
    }

    #[test]
    fn test_engine_missing_product() {
        let mut engine = StreamingEngine::new();
        // update on unknown product -> no-op
        engine.update("NOPE", 1.0, 1.0);
        // accessors on unknown product -> None
        assert!(engine.ema("NOPE", 9).is_none());
        assert!(engine.rsi("NOPE").is_none());
        assert!(engine.macd("NOPE").is_none());
        // with a real product
        engine.get_or_create("BTC-USD", 100).seed_ema(9, 50000.0);
        engine.update("BTC-USD", 50100.0, 10.0);
        assert!(engine.ema("BTC-USD", 9).is_some());
        assert!(engine.rsi("BTC-USD").is_some());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer() {
        let mut buf = RingBuffer::new(5);
        assert!(buf.last().is_none());
        buf.append(1.0);
        buf.append(2.0);
        buf.append(3.0);
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.get(0), Some(1.0));
        assert_eq!(buf.get(2), Some(3.0));
        assert_eq!(buf.last(), Some(3.0));
        assert_eq!(buf.oldest(), Some(1.0));
    }

    #[test]
    fn test_ring_buffer_wrap() {
        let mut buf = RingBuffer::new(3);
        buf.append(1.0);
        buf.append(2.0);
        buf.append(3.0);
        buf.append(4.0); // wraps, overwrites index 0
        assert_eq!(buf.len(), 3);
        assert_eq!(buf.get(0), Some(2.0)); // oldest
        assert_eq!(buf.get(2), Some(4.0)); // newest
    }

    #[test]
    fn test_ema_update() {
        let mut ind = StreamingIndicators::new("BTC-USD", 100);
        ind.seed_ema(9, 50000.0);

        ind.update(50100.0, 100.0);
        let ema = ind.ema(9).unwrap();
        assert!(ema > 50000.0 && ema < 50100.0);
    }

    #[test]
    fn test_rsi_update() {
        let mut ind = StreamingIndicators::new("BTC-USD", 100);

        // Seed with 16 flat closes → no gains/losses → RSI should be 100
        let closes: Vec<f64> = vec![50000.0; 16];
        assert!((ind.seed_rsi(&closes, 14) - 100.0).abs() < 0.1);

        // Update with a price spike up
        ind.update(60000.0, 100.0);
        let rsi2 = ind.rsi();
        assert!(rsi2 > 50.0);
    }

    #[test]
    fn test_macd_update() {
        let mut ind = StreamingIndicators::new("BTC-USD", 100);
        ind.seed_macd(50000.0, 50000.0);

        ind.update(50100.0, 100.0);
        let macd = ind.macd().unwrap();
        // After one update with up-tick, macd_line should be positive
        assert!(macd.0 > 0.0);
    }

    #[test]
    fn test_streaming_engine() {
        let mut engine = StreamingEngine::new();
        engine.get_or_create("BTC-USD", 100);
        engine.update("BTC-USD", 50000.0, 100.0);
        engine.update("BTC-USD", 50100.0, 150.0);
        assert!(engine.products.contains_key("BTC-USD"));
    }

    #[test]
    fn test_to_vec() {
        let mut buf = RingBuffer::new(10);
        buf.append(1.0);
        buf.append(2.0);
        buf.append(3.0);
        let v = buf.to_vec();
        assert_eq!(v, vec![1.0, 2.0, 3.0]);
    }
}
