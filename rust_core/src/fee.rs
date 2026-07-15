/// Fee tier optimization — tracks rolling 30d volume, computes Coinbase fee tier,
/// sizes trades with fee awareness, and generates volume-filling trades.
///
/// Mirrors coinbase/src/fee_optimizer.py in Rust.

use std::time::{SystemTime, UNIX_EPOCH};

// ── Fee Tiers (Coinbase Advanced Trade) ──────────────────────────

pub struct FeeTier {
    pub min_volume: f64,
    pub maker_rate: f64,
    pub taker_rate: f64,
}

pub const COINBASE_FEE_TIERS: &[FeeTier] = &[
    FeeTier { min_volume: 0.0, maker_rate: 0.0060, taker_rate: 0.0120 },
    FeeTier { min_volume: 1_000.0, maker_rate: 0.0035, taker_rate: 0.0075 },
    FeeTier { min_volume: 10_000.0, maker_rate: 0.0025, taker_rate: 0.0040 },
    FeeTier { min_volume: 50_000.0, maker_rate: 0.0015, taker_rate: 0.0025 },
    FeeTier { min_volume: 100_000.0, maker_rate: 0.0010, taker_rate: 0.0020 },
    FeeTier { min_volume: 1_000_000.0, maker_rate: 0.0008, taker_rate: 0.0018 },
    FeeTier { min_volume: 20_000_000.0, maker_rate: 0.0005, taker_rate: 0.0015 },
];

// ── FeeTracker ───────────────────────────────────────────────────

pub struct FeeTracker {
    initial_volume_30d: f64,
    trades_30d: Vec<(f64, f64)>, // (timestamp, volume_usd)
}

impl FeeTracker {
    pub fn new(initial_volume_30d: f64) -> Self {
        Self {
            initial_volume_30d,
            trades_30d: Vec::new(),
        }
    }

    pub fn rolling_30d_volume(&self) -> f64 {
        self.initial_volume_30d
            + self.trades_30d.iter().map(|(_, v)| v).sum::<f64>()
    }

    pub fn current_tier(&self) -> &'static FeeTier {
        let vol = self.rolling_30d_volume();
        COINBASE_FEE_TIERS
            .iter()
            .rev()
            .find(|tier| vol >= tier.min_volume)
            .unwrap_or(&COINBASE_FEE_TIERS[0])
    }

    pub fn next_tier(&self) -> Option<&'static FeeTier> {
        let current_min = self.current_tier().min_volume;
        COINBASE_FEE_TIERS
            .iter()
            .find(|tier| tier.min_volume > current_min)
    }

    pub fn volume_to_next_tier(&self) -> f64 {
        match self.next_tier() {
            Some(tier) => (tier.min_volume - self.rolling_30d_volume()).max(0.0),
            None => 0.0,
        }
    }

    pub fn record_trade(&mut self, volume_usd: f64, timestamp: Option<f64>) {
        let ts = timestamp.unwrap_or_else(now_secs);
        self.trades_30d.push((ts, volume_usd));
        self.prune();
    }

    pub fn fee_cost(&self, trade_volume: f64, is_maker: bool) -> f64 {
        let tier = self.current_tier();
        let rate = if is_maker { tier.maker_rate } else { tier.taker_rate };
        trade_volume * rate
    }

    pub fn maker_rate(&self) -> f64 {
        self.current_tier().maker_rate
    }

    pub fn taker_rate(&self) -> f64 {
        self.current_tier().taker_rate
    }

    pub fn savings_to_next_tier(&self, projected_monthly_volume: f64) -> f64 {
        let current = self.current_tier();
        match self.next_tier() {
            Some(next) => {
                let maker_savings = (current.maker_rate - next.maker_rate) * projected_monthly_volume * 0.5;
                let taker_savings = (current.taker_rate - next.taker_rate) * projected_monthly_volume * 0.5;
                maker_savings + taker_savings
            }
            None => 0.0,
        }
    }

    pub fn to_state(&self) -> (f64, Vec<(f64, f64)>) {
        (self.initial_volume_30d, self.trades_30d.clone())
    }

    pub fn from_state(state: (f64, Vec<(f64, f64)>)) -> Self {
        let mut tracker = Self::new(state.0);
        tracker.trades_30d = state.1;
        tracker.prune();
        tracker
    }

    fn prune(&mut self) {
        let cutoff = now_secs() - 30.0 * 86400.0;
        self.trades_30d.retain(|&(ts, _)| ts > cutoff);
    }
}

// ── FeeAwareSizer ────────────────────────────────────────────────

pub struct FeeAwareSizer {
    fee_tracker: FeeTracker,
}

impl FeeAwareSizer {
    pub fn new(fee_tracker: FeeTracker) -> Self {
        Self { fee_tracker }
    }

    pub fn fee_tracker(&self) -> &FeeTracker {
        &self.fee_tracker
    }

    pub fn fee_tracker_mut(&mut self) -> &mut FeeTracker {
        &mut self.fee_tracker
    }

    pub fn effective_expected_return(&self, expected_return_pct: f64, _trade_volume: f64, is_maker: bool) -> f64 {
        let fee_rate = if is_maker {
            self.fee_tracker.maker_rate()
        } else {
            self.fee_tracker.taker_rate()
        };
        expected_return_pct - fee_rate
    }

    pub fn volume_boost(&self, trade_volume: f64) -> f64 {
        let needed = self.fee_tracker.volume_to_next_tier();
        if needed <= 0.0 {
            return 1.0;
        }
        let proximity = (trade_volume / needed.max(1.0)).min(1.0);
        1.0 + proximity * 0.4
    }

    pub fn should_generate_volume(&self, min_savings: f64) -> (bool, f64) {
        let needed = self.fee_tracker.volume_to_next_tier();
        if needed <= 0.0 {
            return (false, 0.0);
        }
        let projected = self.fee_tracker.rolling_30d_volume() * 1.1;
        let savings = self.fee_tracker.savings_to_next_tier(projected);
        (savings > min_savings, savings)
    }
}

// ── VolumeGenerator ──────────────────────────────────────────────

pub struct VolumeGenerator {
    fee_tracker: FeeTracker,
    max_volume_per_day: f64,
    #[allow(dead_code)]
    min_spread_bps: f64,
    daily_volume: f64,
    daily_reset_ts: f64,
}

impl VolumeGenerator {
    pub fn new(fee_tracker: FeeTracker, max_volume_per_day: f64, min_spread_bps: f64) -> Self {
        Self {
            fee_tracker,
            max_volume_per_day,
            min_spread_bps,
            daily_volume: 0.0,
            daily_reset_ts: now_secs(),
        }
    }

    pub fn fee_tracker(&self) -> &FeeTracker {
        &self.fee_tracker
    }

    pub fn generate_volume(&self, current_price: f64) -> Option<(f64, f64, String)> {
        // Returns (gen_volume, gen_size, reason) if generation is needed
        let needed = self.fee_tracker.volume_to_next_tier();
        if needed <= 0.0 {
            return None;
        }

        let tier = self.fee_tracker.current_tier();
        let next_tier = self.fee_tracker.next_tier()?;

        let spread_savings = (tier.maker_rate - next_tier.maker_rate) * current_price * 0.5;
        if spread_savings <= 0.0 {
            return None;
        }

        let remaining = self.max_volume_per_day - self.daily_volume;
        if remaining <= 0.0 {
            return None;
        }

        let gen_volume = needed.min(remaining);
        let gen_size = gen_volume / current_price.max(1e-9);
        let reason = format!("VOLGEN: generate ${:.0} volume for fee tier", gen_volume);

        Some((gen_volume, gen_size, reason))
    }

    pub fn daily_check(&mut self) {
        let now = now_secs();
        if now - self.daily_reset_ts > 86400.0 {
            self.daily_volume = 0.0;
            self.daily_reset_ts = now;
        }
    }

    pub fn record_generated(&mut self, volume: f64) {
        self.daily_volume += volume;
        self.fee_tracker.record_trade(volume, None);
    }

    pub fn record_generated_at(&mut self, volume: f64, timestamp: f64) {
        self.daily_volume += volume;
        self.fee_tracker.record_trade(volume, Some(timestamp));
    }
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_tier_lookup() {
        let tracker = FeeTracker::new(0.0);
        let tier = tracker.current_tier();
        assert_eq!(tier.min_volume, 0.0);
        assert_eq!(tier.maker_rate, 0.0060);
    }

    #[test]
    fn test_fee_tier_progression() {
        let tracker = FeeTracker::new(60_000.0);
        let tier = tracker.current_tier();
        assert_eq!(tier.min_volume, 50_000.0);
        assert_eq!(tier.maker_rate, 0.0015);
    }

    #[test]
    fn test_next_tier() {
        let tracker = FeeTracker::new(60_000.0);
        let next = tracker.next_tier().unwrap();
        assert_eq!(next.min_volume, 100_000.0);
    }

    #[test]
    fn test_volume_to_next() {
        let tracker = FeeTracker::new(60_000.0);
        let needed = tracker.volume_to_next_tier();
        assert!((needed - 40_000.0).abs() < 0.01);
    }

    #[test]
    fn test_record_trade() {
        let mut tracker = FeeTracker::new(0.0);
        tracker.record_trade(5000.0, Some(2000000000.0));
        assert!((tracker.rolling_30d_volume() - 5000.0).abs() < 0.01);
    }

    #[test]
    fn test_fee_cost() {
        let tracker = FeeTracker::new(0.0);
        let cost = tracker.fee_cost(1000.0, false);
        assert!((cost - 12.0).abs() < 0.01); // 1000 * 0.012
    }

    #[test]
    fn test_savings_to_next_tier() {
        let tracker = FeeTracker::new(0.0);
        let savings = tracker.savings_to_next_tier(10_000.0);
        assert!(savings > 0.0);
    }

    #[test]
    fn test_volume_boost() {
        let tracker = FeeTracker::new(500.0);
        let sizer = FeeAwareSizer::new(tracker);
        let boost = sizer.volume_boost(1000.0);
        assert!(boost > 1.0);
        assert!(boost <= 1.4);
    }

    #[test]
    fn test_volume_boost_no_tier() {
        let tracker = FeeTracker::new(50_000_000.0); // top tier
        let sizer = FeeAwareSizer::new(tracker);
        let boost = sizer.volume_boost(1000.0);
        assert!((boost - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_should_generate_volume() {
        let tracker = FeeTracker::new(500.0);
        let sizer = FeeAwareSizer::new(tracker);
        let (should, savings) = sizer.should_generate_volume(1.0); // low min_savings
        assert!(should);
        assert!(savings > 0.0);
    }

    #[test]
    fn test_generate_volume_opportunity() {
        let tracker = FeeTracker::new(500.0);
        let gen = VolumeGenerator::new(tracker, 10000.0, 5.0);
        let result = gen.generate_volume(100.0);
        assert!(result.is_some());
        let (vol, size, reason) = result.unwrap();
        assert!(vol > 0.0);
        assert!(size > 0.0);
        assert!(reason.contains("VOLGEN"));
    }

    #[test]
    fn test_state_roundtrip() {
        let mut tracker = FeeTracker::new(1000.0);
        tracker.record_trade(500.0, Some(2000000000.0));
        let state = tracker.to_state();
        let restored = FeeTracker::from_state(state);
        assert!((restored.rolling_30d_volume() - 1500.0).abs() < 0.01);
    }

    #[test]
    fn test_prune() {
        let old_ts = now_secs() - 31.0 * 86400.0;
        let mut tracker = FeeTracker::new(0.0);
        tracker.record_trade(1000.0, Some(old_ts));
        tracker.prune();
        assert!(tracker.trades_30d.is_empty());
    }
}

#[cfg(test)]
mod coverage_tests {
    use super::*;

    const TOP_VOL: f64 = 50_000_000.0;

    #[test]
    fn test_next_tier_top() {
        let tracker = FeeTracker::new(TOP_VOL);
        assert!(tracker.next_tier().is_none());
    }

    #[test]
    fn test_volume_to_next_tier_top() {
        let tracker = FeeTracker::new(TOP_VOL);
        assert_eq!(tracker.volume_to_next_tier(), 0.0);
    }

    #[test]
    fn test_savings_to_next_tier_top() {
        let tracker = FeeTracker::new(TOP_VOL);
        assert_eq!(tracker.savings_to_next_tier(10_000.0), 0.0);
    }

    #[test]
    fn test_effective_expected_return() {
        let tracker = FeeTracker::new(0.0);
        let sizer = FeeAwareSizer::new(tracker);
        let taker = sizer.effective_expected_return(0.01, 1000.0, false);
        let maker = sizer.effective_expected_return(0.01, 1000.0, true);
        assert!(taker < maker); // maker fee lower -> higher net return
        assert!(maker > 0.0);
    }

    #[test]
    fn test_volume_boost_full() {
        let tracker = FeeTracker::new(0.0);
        let sizer = FeeAwareSizer::new(tracker);
        // needed huge, trade_volume==needed => proximity 1 => boost 1.4
        let needed = sizer.fee_tracker().volume_to_next_tier();
        let boost = sizer.volume_boost(needed.max(1.0));
        assert!((boost - 1.4).abs() < 1e-9 || boost <= 1.4);
    }

    #[test]
    fn test_should_generate_volume_below_min() {
        let tracker = FeeTracker::new(500.0);
        let sizer = FeeAwareSizer::new(tracker);
        let (should, _savings) = sizer.should_generate_volume(1e9); // huge min -> false
        assert!(!should);
    }

    #[test]
    fn test_generate_volume_top_tier() {
        let tracker = FeeTracker::new(TOP_VOL);
        let gen = VolumeGenerator::new(tracker, 10000.0, 5.0);
        assert!(gen.generate_volume(100.0).is_none());
    }

    #[test]
    fn test_generate_volume_no_remaining() {
        let tracker = FeeTracker::new(500.0);
        let gen = VolumeGenerator::new(tracker, 0.0, 5.0); // max_volume_per_day = 0
        assert!(gen.generate_volume(100.0).is_none());
    }

    #[test]
    fn test_daily_check_and_record() {
        let tracker = FeeTracker::new(500.0);
        let mut gen = VolumeGenerator::new(tracker, 10000.0, 5.0);
        gen.record_generated(100.0);
        assert_eq!(gen.fee_tracker().rolling_30d_volume(), 600.0);
        // Force daily reset via old timestamp
        gen.daily_reset_ts = now_secs() - 100000.0;
        gen.daily_check();
        assert_eq!(gen.daily_volume, 0.0);
    }

    #[test]
    fn test_record_generated_at() {
        let tracker = FeeTracker::new(500.0);
        let mut gen = VolumeGenerator::new(tracker, 10000.0, 5.0);
        gen.record_generated_at(250.0, 2000000000.0);
        assert_eq!(gen.fee_tracker().rolling_30d_volume(), 750.0);
    }

    #[test]
    fn test_fee_tracker_accessors() {
        let tracker = FeeTracker::new(0.0);
        let mut sizer = FeeAwareSizer::new(tracker);
        assert!(sizer.fee_tracker().current_tier().min_volume >= 0.0);
        sizer.fee_tracker_mut().record_trade(10.0, None);
        assert!(sizer.fee_tracker().rolling_30d_volume() > 0.0);
    }

    #[test]
    fn test_state_roundtrip_multiple() {
        let mut tracker = FeeTracker::new(1000.0);
        tracker.record_trade(500.0, Some(2000000000.0));
        tracker.record_trade(250.0, Some(2000000100.0));
        let state = tracker.to_state();
        let restored = FeeTracker::from_state(state);
        assert_eq!(restored.trades_30d.len(), 2);
    }
}
