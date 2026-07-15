//! Multi-asset rebalancing and range-bound stair-step profit-taking.
//!
//! These two engines are the Rust core behind the "set an allocation and let it
//! run" rebalancer and the "stay in a range, harvest volatility" stair-step bot.
//! Both are pure, allocation-agnostic and fully unit-tested so they can be driven
//! from the Coinbase trader / portfolio optimizer with zero Python-side math.

use std::collections::HashMap;

// ───────────────────────────── Allocation ─────────────────────────────

/// A normalized target allocation: asset symbol -> target weight (fraction of
/// portfolio value). Weights need not sum to 1.0; they are normalized on use.
#[derive(Debug, Clone)]
pub struct Allocation {
    pub targets: HashMap<String, f64>,
}

impl Allocation {
    /// Build and validate a target allocation.
    pub fn new(targets: HashMap<String, f64>) -> Result<Self, String> {
        if targets.is_empty() {
            return Err("allocation must contain at least one asset".to_string());
        }
        let sum: f64 = targets.values().sum();
        if sum <= 0.0 {
            return Err("allocation weights must sum to a positive value".to_string());
        }
        for (asset, w) in &targets {
            if !w.is_finite() || *w <= 0.0 {
                return Err(format!("weight for {asset} must be > 0"));
            }
        }
        Ok(Self { targets })
    }

    /// Normalize weights in place so they sum to 1.0.
    pub fn normalize(&mut self) {
        let sum: f64 = self.targets.values().sum();
        if sum > 0.0 {
            for w in self.targets.values_mut() {
                *w /= sum;
            }
        }
    }

    /// Return a copy of the targets normalized to sum to 1.0.
    pub fn normalized(&self) -> HashMap<String, f64> {
        let sum: f64 = self.targets.values().sum();
        self.targets
            .iter()
            .map(|(k, v)| (k.clone(), if sum > 0.0 { *v / sum } else { *v }))
            .collect()
    }

    pub fn assets(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }
}

// ───────────────────────────── Rebalancer ─────────────────────────────

/// A single rebalancing order produced by [`Rebalancer::compute_orders`].
#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceOrder {
    pub asset: String,
    /// "BUY" | "SELL" | "HOLD" (HOLD only appears when drift is within threshold).
    pub side: String,
    /// Notional USD to trade.
    pub notional: f64,
    pub current_weight: f64,
    pub target_weight: f64,
    /// current_weight - target_weight (positive => overweight => SELL candidate).
    pub drift: f64,
}

/// Periodic / drift-threshold rebalancer with "slim profit" partial selling.
///
/// Given current per-asset market values and a target allocation, it returns the
/// trades required to bring the book back toward target. Two behaviours keep it
/// cheap and gentle:
///   * `drift_threshold` — assets whose |drift| is below this are skipped entirely
///     (so the engine only emits orders when something has actually moved).
///   * `profit_take_pct` — when an asset is overweight, only this fraction of the
///     excess is sold, banking a slim profit instead of fully flattening.
pub struct Rebalancer {
    allocation: Allocation,
    drift_threshold: f64,
    profit_take_pct: f64,
    min_trade_notional: f64,
}

impl Rebalancer {
    pub fn new(
        targets: HashMap<String, f64>,
        drift_threshold: f64,
        profit_take_pct: f64,
        min_trade_notional: f64,
    ) -> Result<Self, String> {
        if !drift_threshold.is_finite() || drift_threshold < 0.0 {
            return Err("drift_threshold must be >= 0".to_string());
        }
        if !profit_take_pct.is_finite() || profit_take_pct <= 0.0 || profit_take_pct > 1.0 {
            return Err("profit_take_pct must be in (0, 1]".to_string());
        }
        if !min_trade_notional.is_finite() || min_trade_notional < 0.0 {
            return Err("min_trade_notional must be >= 0".to_string());
        }
        let allocation = Allocation::new(targets)?;
        Ok(Self {
            allocation,
            drift_threshold,
            profit_take_pct,
            min_trade_notional,
        })
    }

    /// Effective total portfolio value (explicit arg preferred; falls back to sum).
    fn total_value(current_values: &HashMap<String, f64>, total: f64) -> f64 {
        if total > 0.0 {
            total
        } else {
            current_values.values().sum()
        }
    }

    /// Current weight of every target asset.
    pub fn current_weights(
        &self,
        current_values: &HashMap<String, f64>,
        total: f64,
    ) -> HashMap<String, f64> {
        let t = Self::total_value(current_values, total);
        let norm = self.allocation.normalized();
        let mut out = HashMap::new();
        for asset in norm.keys() {
            let cv = current_values.get(asset).copied().unwrap_or(0.0);
            let w = if t > 0.0 { cv / t } else { 0.0 };
            out.insert(asset.clone(), w);
        }
        out
    }

    /// Per-asset drift (current_weight - target_weight).
    pub fn drift(&self, current_values: &HashMap<String, f64>, total: f64) -> HashMap<String, f64> {
        let weights = self.current_weights(current_values, total);
        let norm = self.allocation.normalized();
        weights
            .iter()
            .map(|(asset, w)| {
                let tw = norm.get(asset).copied().unwrap_or(0.0);
                (asset.clone(), w - tw)
            })
            .collect()
    }

    /// Largest absolute drift across all assets (used by the trader to decide
    /// whether a rebalance tick is even worth scheduling).
    pub fn max_abs_drift(&self, current_values: &HashMap<String, f64>, total: f64) -> f64 {
        self.drift(current_values, total)
            .values()
            .map(|d| d.abs())
            .fold(0.0, f64::max)
    }

    /// Compute the rebalancing orders for the current book.
    pub fn compute_orders(
        &self,
        current_values: &HashMap<String, f64>,
        total: f64,
    ) -> Vec<RebalanceOrder> {
        let t = Self::total_value(current_values, total);
        let norm = self.allocation.normalized();
        let weights = self.current_weights(current_values, t);
        let mut orders = Vec::new();

        for (asset, tw) in &norm {
            let cw = weights.get(asset).copied().unwrap_or(0.0);
            let drift = cw - tw;
            if drift.abs() <= self.drift_threshold {
                continue;
            }
            let target_value = tw * t;
            let current_value = cw * t;
            let mut delta = target_value - current_value; // > 0 => need to BUY
            if delta < 0.0 {
                // Overweight: take only a slim slice of the excess as profit.
                let excess = -delta;
                delta = -excess * self.profit_take_pct;
            }
            let notional = delta.abs();
            if notional < self.min_trade_notional {
                continue;
            }
            let side = if delta > 0.0 { "BUY" } else { "SELL" }.to_string();
            orders.push(RebalanceOrder {
                asset: asset.clone(),
                side,
                notional,
                current_weight: cw,
                target_weight: *tw,
                drift,
            });
        }
        orders
    }
}

// ───────────────────────── Stair-step profit taker ────────────────────

/// A single order emitted by [`StairStepProfitTaker::on_price`].
#[derive(Debug, Clone, PartialEq)]
pub struct StairStepOrder {
    pub side: String, // "BUY" | "SELL"
    pub price: f64,
    pub notional: f64,
}

/// Mutable state of a running stair-step bot.
#[derive(Debug, Clone)]
pub struct StairStepState {
    pub next_buy_index: usize,
    /// Stack of filled buy prices (most recent last).
    pub buys: Vec<f64>,
    pub inventory_value: f64,
    pub realized_pnl: f64,
    pub last_action: String,
    pub filled_buys: usize,
    pub filled_sells: usize,
}

/// Range-bound stair-step profit taker.
///
/// The bot lays a grid of `steps` buy levels evenly spaced between `high` and
/// `low`. As price falls it buys one unit at each lower level (scaling in).
/// Whenever price recovers to `avg_cost * (1 + take_profit_pct)` it sells one
/// unit (the most recently filled buy), banking the spread. Net effect: stay
/// inside the range and harvest volatility over time.
pub struct StairStepProfitTaker {
    cfg: StairStepConfig,
    state: StairStepState,
}

#[derive(Debug, Clone)]
pub struct StairStepConfig {
    pub low: f64,
    pub high: f64,
    pub steps: usize,
    pub budget: f64,
    pub take_profit_pct: f64,
    pub base_size_pct: f64,
}

impl StairStepProfitTaker {
    pub fn new(
        low: f64,
        high: f64,
        steps: usize,
        budget: f64,
        take_profit_pct: f64,
        base_size_pct: f64,
    ) -> Result<Self, String> {
        if !low.is_finite() || !high.is_finite() || low >= high {
            return Err("low must be < high".to_string());
        }
        if steps == 0 {
            return Err("steps must be > 0".to_string());
        }
        if !budget.is_finite() || budget <= 0.0 {
            return Err("budget must be > 0".to_string());
        }
        if !take_profit_pct.is_finite() || take_profit_pct <= 0.0 {
            return Err("take_profit_pct must be > 0".to_string());
        }
        if !base_size_pct.is_finite() || base_size_pct <= 0.0 || base_size_pct > 1.0 {
            return Err("base_size_pct must be in (0, 1]".to_string());
        }
        Ok(Self {
            cfg: StairStepConfig {
                low,
                high,
                steps,
                budget,
                take_profit_pct,
                base_size_pct,
            },
            state: StairStepState {
                next_buy_index: 0,
                buys: Vec::new(),
                inventory_value: 0.0,
                realized_pnl: 0.0,
                last_action: "INIT".to_string(),
                filled_buys: 0,
                filled_sells: 0,
            },
        })
    }

    /// Buy price for grid index `i` (0 = at `high`, steps-1 = near `low`).
    pub fn buy_level(&self, index: usize) -> f64 {
        let span = self.cfg.high - self.cfg.low;
        self.cfg.high - (index as f64) * span / (self.cfg.steps as f64)
    }

    /// All grid buy levels, high -> low.
    pub fn step_levels(&self) -> Vec<f64> {
        (0..self.cfg.steps).map(|i| self.buy_level(i)).collect()
    }

    /// Notional per single buy/sell unit.
    pub fn base_size(&self) -> f64 {
        self.cfg.budget * self.cfg.base_size_pct
    }

    pub fn state(&self) -> &StairStepState {
        &self.state
    }

    pub fn reset(&mut self) {
        self.state = StairStepState {
            next_buy_index: 0,
            buys: Vec::new(),
            inventory_value: 0.0,
            realized_pnl: 0.0,
            last_action: "INIT".to_string(),
            filled_buys: 0,
            filled_sells: 0,
        };
    }

    /// Feed a price tick; returns an order if the bot should act, else None.
    pub fn on_price(&mut self, price: f64) -> Option<StairStepOrder> {
        if !price.is_finite() {
            self.state.last_action = "HOLD".to_string();
            return None;
        }
        // Buy at the next lower grid level if price has reached it.
        if self.state.next_buy_index < self.cfg.steps {
            let level = self.buy_level(self.state.next_buy_index);
            if price <= level {
                let notional = self.base_size();
                self.state.buys.push(price);
                self.state.inventory_value += notional;
                self.state.next_buy_index += 1;
                self.state.filled_buys += 1;
                self.state.last_action = "BUY".to_string();
                return Some(StairStepOrder {
                    side: "BUY".to_string(),
                    price,
                    notional,
                });
            }
        }
        // Take profit when price recovers above avg cost * (1 + tp).
        if !self.state.buys.is_empty() {
            let avg = self.state.buys.iter().sum::<f64>() / (self.state.buys.len() as f64);
            let target = avg * (1.0 + self.cfg.take_profit_pct);
            if price >= target {
                let buy_price = self.state.buys.pop().unwrap();
                let notional = self.base_size();
                let pnl = (price - buy_price) / buy_price * notional;
                self.state.realized_pnl += pnl;
                self.state.inventory_value =
                    (self.state.inventory_value - notional).max(0.0);
                self.state.filled_sells += 1;
                self.state.last_action = "SELL".to_string();
                return Some(StairStepOrder {
                    side: "SELL".to_string(),
                    price,
                    notional,
                });
            }
        }
        self.state.last_action = "HOLD".to_string();
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alloc(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs
            .iter()
            .map(|(a, w)| (a.to_string(), *w))
            .collect()
    }

    #[test]
    fn allocation_rejects_empty() {
        let r = Allocation::new(HashMap::new());
        assert!(r.is_err());
    }

    #[test]
    fn allocation_rejects_nonpositive_weight() {
        let mut m = alloc(&[("BTC", 0.5), ("ETH", -0.2)]);
        assert!(Allocation::new(m.clone()).is_err());
        m.insert("ETH".to_string(), 0.0);
        assert!(Allocation::new(m).is_err());
    }

    #[test]
    fn allocation_normalizes() {
        let mut a = Allocation::new(alloc(&[("BTC", 2.0), ("ETH", 2.0)])).unwrap();
        a.normalize();
        let sum: f64 = a.targets.values().sum();
        assert!((sum - 1.0).abs() < 1e-9);
        assert!((a.targets["BTC"] - 0.5).abs() < 1e-9);
    }

    #[test]
    fn allocation_normalized_copy() {
        let a = Allocation::new(alloc(&[("BTC", 1.0), ("ETH", 3.0)])).unwrap();
        let n = a.normalized();
        assert!((n["ETH"] - 0.75).abs() < 1e-9);
        assert!((n["BTC"] - 0.25).abs() < 1e-9);
    }

    #[test]
    fn rebalancer_rejects_bad_params() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        assert!(Rebalancer::new(t.clone(), -0.1, 0.5, 1.0).is_err());
        assert!(Rebalancer::new(t.clone(), 0.1, 0.0, 1.0).is_err());
        assert!(Rebalancer::new(t.clone(), 0.1, 1.5, 1.0).is_err());
        assert!(Rebalancer::new(t, 0.1, 0.5, -1.0).is_err());
    }

    #[test]
    fn rebalancer_on_target_emits_no_orders() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        let rb = Rebalancer::new(t, 0.01, 1.0, 1.0).unwrap();
        let mut values = alloc(&[("BTC", 50.0), ("ETH", 50.0)]);
        let orders = rb.compute_orders(&values, 100.0);
        assert!(orders.is_empty());
        // A missing asset (zero value) reads as 0 weight -> bot issues a BUY to
        // bring it up to target.
        values.remove("ETH");
        let orders = rb.compute_orders(&values, 50.0);
        let eth = orders.iter().find(|o| o.asset == "ETH");
        assert!(eth.is_some());
        assert_eq!(eth.unwrap().side, "BUY");
    }

    #[test]
    fn rebalancer_buys_underweight() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        let rb = Rebalancer::new(t, 0.01, 1.0, 1.0).unwrap();
        // BTC overweight (60 vs 40 target)
        let values = alloc(&[("BTC", 60.0), ("ETH", 40.0)]);
        let orders = rb.compute_orders(&values, 100.0);
        let btc = orders.iter().find(|o| o.asset == "BTC").unwrap();
        assert_eq!(btc.side, "SELL");
        assert!((btc.notional - 10.0).abs() < 1e-9);
        let eth = orders.iter().find(|o| o.asset == "ETH").unwrap();
        assert_eq!(eth.side, "BUY");
        assert!((eth.notional - 10.0).abs() < 1e-9);
    }

    #[test]
    fn rebalancer_slim_profit_partial_sell() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        // profit_take_pct = 0.25 -> only sell 25% of the 20 overweight excess
        let rb = Rebalancer::new(t, 0.01, 0.25, 0.0).unwrap();
        let values = alloc(&[("BTC", 60.0), ("ETH", 40.0)]);
        let orders = rb.compute_orders(&values, 100.0);
        let btc = orders.iter().find(|o| o.asset == "BTC").unwrap();
        assert_eq!(btc.side, "SELL");
        assert!((btc.notional - 2.5).abs() < 1e-9);
    }

    #[test]
    fn rebalancer_skips_below_drift_threshold() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        let rb = Rebalancer::new(t, 0.05, 1.0, 0.0).unwrap();
        // 52/48 -> drift 0.02 < 0.05 threshold
        let values = alloc(&[("BTC", 52.0), ("ETH", 48.0)]);
        assert!(rb.compute_orders(&values, 100.0).is_empty());
    }

    #[test]
    fn rebalancer_min_notional_filter() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        let rb = Rebalancer::new(t, 0.01, 1.0, 50.0).unwrap();
        let values = alloc(&[("BTC", 60.0), ("ETH", 40.0)]);
        // both deltas are 10 < 50 min notional
        assert!(rb.compute_orders(&values, 100.0).is_empty());
    }

    #[test]
    fn rebalancer_falls_back_total_to_sum() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        let rb = Rebalancer::new(t, 0.01, 1.0, 0.0).unwrap();
        let values = alloc(&[("BTC", 60.0), ("ETH", 40.0)]);
        let orders = rb.compute_orders(&values, 0.0);
        assert_eq!(orders.len(), 2);
    }

    #[test]
    fn rebalancer_drift_and_max() {
        let t = alloc(&[("BTC", 0.5), ("ETH", 0.5)]);
        let rb = Rebalancer::new(t, 0.01, 1.0, 0.0).unwrap();
        let values = alloc(&[("BTC", 60.0), ("ETH", 40.0)]);
        let d = rb.drift(&values, 100.0);
        assert!((d["BTC"] - 0.1).abs() < 1e-9);
        assert!((d["ETH"] + 0.1).abs() < 1e-9);
        assert!((rb.max_abs_drift(&values, 100.0) - 0.1).abs() < 1e-9);
    }

    #[test]
    fn stair_step_rejects_bad_config() {
        assert!(StairStepProfitTaker::new(100.0, 100.0, 5, 100.0, 0.01, 0.2).is_err());
        assert!(StairStepProfitTaker::new(50.0, 100.0, 0, 100.0, 0.01, 0.2).is_err());
        assert!(StairStepProfitTaker::new(50.0, 100.0, 5, 0.0, 0.01, 0.2).is_err());
        assert!(StairStepProfitTaker::new(50.0, 100.0, 5, 100.0, 0.0, 0.2).is_err());
        assert!(StairStepProfitTaker::new(50.0, 100.0, 5, 100.0, 0.01, 1.5).is_err());
    }

    #[test]
    fn stair_step_levels_descending() {
        let bot = StairStepProfitTaker::new(50.0, 100.0, 5, 1000.0, 0.02, 0.2).unwrap();
        let levels = bot.step_levels();
        assert_eq!(levels.len(), 5);
        assert!((levels[0] - 100.0).abs() < 1e-9);
        assert!((levels[4] - 60.0).abs() < 1e-9);
        for i in 1..levels.len() {
            assert!(levels[i] < levels[i - 1]);
        }
        assert!((bot.base_size() - 200.0).abs() < 1e-9);
    }

    #[test]
    fn stair_step_buys_as_price_falls() {
        let mut bot = StairStepProfitTaker::new(50.0, 100.0, 5, 1000.0, 0.02, 0.2).unwrap();
        // price at top -> no buy yet (buy level 0 is exactly 100; price<=level)
        assert!(bot.on_price(100.0).is_some()); // buy at level 0
        assert!(bot.on_price(90.0).is_some()); // buy at level 1 (90)
        assert_eq!(bot.state().filled_buys, 2);
        assert_eq!(bot.state().next_buy_index, 2);
    }

    #[test]
    fn stair_step_takes_profit_on_recovery() {
        let mut bot = StairStepProfitTaker::new(50.0, 100.0, 5, 1000.0, 0.02, 0.2).unwrap();
        bot.on_price(100.0); // buy at 100
        let order = bot.on_price(102.0); // >= avg(100)*1.02
        assert!(order.is_some());
        let o = order.unwrap();
        assert_eq!(o.side, "SELL");
        assert!(bot.state().realized_pnl > 0.0);
        assert_eq!(bot.state().filled_sells, 1);
    }

    #[test]
    fn stair_step_no_action_when_above_range() {
        let mut bot = StairStepProfitTaker::new(50.0, 100.0, 5, 1000.0, 0.02, 0.2).unwrap();
        // price above the grid top -> no buy, no inventory to sell
        assert!(bot.on_price(110.0).is_none());
        assert_eq!(bot.state().last_action, "HOLD");
    }

    #[test]
    fn stair_step_reset_clears_state() {
        let mut bot = StairStepProfitTaker::new(50.0, 100.0, 5, 1000.0, 0.02, 0.2).unwrap();
        bot.on_price(100.0);
        bot.on_price(102.0);
        bot.reset();
        assert_eq!(bot.state().filled_buys, 0);
        assert_eq!(bot.state().filled_sells, 0);
        assert!((bot.state().realized_pnl).abs() < 1e-9);
        assert_eq!(bot.state().next_buy_index, 0);
    }

    #[test]
    fn stair_step_ignores_nan_price() {
        let mut bot = StairStepProfitTaker::new(50.0, 100.0, 5, 1000.0, 0.02, 0.2).unwrap();
        assert!(bot.on_price(f64::NAN).is_none());
    }
}
