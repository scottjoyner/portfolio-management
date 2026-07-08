/// Confidence Matrix — multi-strategy signal aggregation (v2).
///
/// Groups signals by asset and direction, computes aggregate confidence using:
///   1. Strategy independence (same-family strategies don't double-count)
///   2. Historical backtest performance per strategy-asset pair
///   3. Asset-class-specific strategy weighting
///   4. **Regime-aware weighting** — favor trend strategies in trending regimes,
///      momentum in ranging, volatility in volatile
///   5. **Correlation penalty** — when two strategies from the same group agree,
///      apply diminishing returns to prevent over-weighting
///
/// Now supports 68 strategies across 7 independence groups.
///
/// Mirrors confidence_matrix.py in Rust.

use std::collections::{HashMap, HashSet};

/// A raw signal from a strategy (input to the matrix).
#[derive(Debug, Clone)]
pub struct RawSignal {
    pub strategy: String,
    pub action: String,   // "BUY" | "SELL"
    pub confidence: f64,  // 0..1
    pub reason: String,
}

/// Aggregated signal after confidence matrix processing.
#[derive(Debug, Clone)]
pub struct AggregatedSignal {
    pub asset: String,
    pub direction: String,
    pub confidence: f64,
    pub raw_confidence: f64,
    pub agreeing_groups: usize,
    pub total_groups: usize,
    pub strategy_count: usize,
    pub strategies: Vec<String>,
    pub best_reason: String,
    pub asset_class: String,
}

// ── Independence groups (10 groups for 68 strategies) ──────────────

const TREND_STRATS: &[&str] = &[
    "ema_cross", "macd", "trix", "adx", "psar", "hma", "aroon",
    "elder_ray", "ichimoku", "dpo",
    "kama", "dmi_cross", "vma",
    "hp_trend",
];
const MOMENTUM_STRATS: &[&str] = &[
    "rsi_revert", "cmo", "williams_r", "zscore_revert", "force_idx",
    "true_cci", "kst", "mom_accel", "multi_rsi", "stoch",
    "vortex", "rvi", "coppock",
];
const VOLATILITY_STRATS: &[&str] = &[
    "boll_break", "vwap_revert", "keltner", "donchian",
    "bb_squeeze", "vcp", "choppiness", "mass_idx",
    "envelope", "atr_channel",
    "std_channel", "vol_ratio",
];
const VOLUME_STRATS: &[&str] = &[
    "vol_mom", "obv_div", "chaikin_mf", "vpt",
    "vol_prof", "klinger", "price_eff", "snr_idx",
    "mfi", "emv", "ad_div",
    "vwap_macd", "nvi",
];
const PATTERN_STRATS: &[&str] = &[
    "candle_pat", "pivot_points", "sup_res", "liq_vac",
    "donch_pull", "impulse_exh", "range_exp_idx",
    "de_marker", "gap_revert",
];
const MOMENTUM_ADV_STRATS: &[&str] = &[
    "rsi_fail", "cvd_flow", "avwap", "linreg_slope",
    "hurst", "scci", "ulcer", "ema_dev",
    "kalman_mr",
];
const PM_STRATS: &[&str] = &["kalshi", "polymarket"];
const SENTIMENT_STRATS: &[&str] = &["crypto_news"];
const DERIVATIVES_STRATS: &[&str] = &["funding_contrarian"];
const ONCHAIN_STRATS: &[&str] = &["exchange_flow"];
const ORDER_FLOW_STRATS: &[&str] = &["order_flow"];
const MACRO_RISK_STRATS: &[&str] = &["macro_risk", "btc_dxy_corr"];

fn strategy_group(name: &str) -> Option<&'static str> {
    if TREND_STRATS.contains(&name) { Some("trend") }
    else if MOMENTUM_STRATS.contains(&name) { Some("momentum") }
    else if VOLATILITY_STRATS.contains(&name) { Some("volatility") }
    else if VOLUME_STRATS.contains(&name) { Some("volume") }
    else if PATTERN_STRATS.contains(&name) { Some("pattern") }
    else if MOMENTUM_ADV_STRATS.contains(&name) { Some("momentum_adv") }
    else if PM_STRATS.contains(&name) { Some("prediction_market") }
    else if SENTIMENT_STRATS.contains(&name) { Some("sentiment") }
    else if DERIVATIVES_STRATS.contains(&name) { Some("derivatives") }
    else if ONCHAIN_STRATS.contains(&name) { Some("onchain") }
    else if ORDER_FLOW_STRATS.contains(&name) { Some("order_flow") }
    else if MACRO_RISK_STRATS.contains(&name) { Some("macro_risk") }
    else { None }
}

const GROUP_NAMES: &[&str] = &[
    "trend", "momentum", "volatility", "volume",
    "pattern", "momentum_adv", "prediction_market",
    "sentiment", "derivatives", "onchain",
    "order_flow", "macro_risk",
];

// ── Default weights ─────────────────────────────────────────────────

pub fn default_weight(strategy: &str) -> f64 {
    match strategy {
        // Core strategies
        "ema_cross" | "macd" | "hma" | "aroon" => 0.6,
        "rsi_revert" | "zscore_revert" | "vwap_revert" => 0.4,
        "adx" => 0.6,
        "psar" => 0.4,
        "boll_break" | "vol_mom" | "obv_div" | "cmo" | "trix"
        | "keltner" | "chaikin_mf" | "williams_r" | "force_idx" | "vpt"
        | "donchian" => 0.5,
        // New 15
        "candle_pat" | "sup_res" | "liq_vac" | "cvd_flow" | "vcp"
        | "impulse_exh" | "mom_accel" | "rsi_fail" | "avwap" | "donch_pull"
        | "vol_prof" | "bb_squeeze" | "multi_rsi" | "linreg_slope" | "hurst" => 0.5,
        // New 10
        "elder_ray" | "ichimoku" | "dpo" => 0.55,
        "klinger" => 0.5,
        "pivot_points" | "choppiness" | "true_cci" | "kst" | "mass_idx" | "ulcer" => 0.5,
        // Prediction market
        "kalshi" | "polymarket" => 0.5,
        // Sentiment (crypto news)
        "crypto_news" => 0.55,
        // Microstructure (order flow)
        "order_flow" => 0.5,
        // Macro risk (DXY, yields, VIX, gold)
        "macro_risk" => 0.5,
        // 6 new OHLCV strategies (51-56)
        "mfi" | "stoch" | "emv" | "ad_div" | "envelope" | "atr_channel" => 0.5,
        // 12 new strategies (57-68)
        "kama" | "vma" | "coppock" | "vortex" => 0.55,
        "dmi_cross" | "rvi" | "std_channel" | "vol_ratio"
        | "vwap_macd" | "nvi" | "de_marker" | "gap_revert" => 0.5,
        // External-data strategies
        "funding_contrarian" | "exchange_flow" | "btc_dxy_corr" => 0.5,
        // New Rust strategies (73-74)
        "kalman_mr" | "hp_trend" => 0.55,
        _ => 0.5,
    }
}

// ── Asset class boost ───────────────────────────────────────────────

pub fn class_boost(strategy: &str, asset_class: &str) -> f64 {
    let group = strategy_group(strategy).unwrap_or("momentum");
    let boosts = match asset_class {
        "safe" => [
            ("trend", 1.3), ("momentum", 0.7), ("volatility", 0.8),
            ("volume", 1.0), ("pattern", 0.9), ("momentum_adv", 0.7),
            ("prediction_market", 0.9), ("sentiment", 1.0),
            ("derivatives", 1.0), ("onchain", 1.0),
            ("order_flow", 1.1), ("macro_risk", 1.3),
        ],
        "speculative" => [
            ("trend", 0.8), ("momentum", 1.3), ("volatility", 1.2),
            ("volume", 1.1), ("pattern", 1.1), ("momentum_adv", 1.3),
            ("prediction_market", 1.2), ("sentiment", 1.1),
            ("derivatives", 1.2), ("onchain", 1.1),
            ("order_flow", 1.0), ("macro_risk", 0.8),
        ],
        _ => [ // growth
            ("trend", 1.1), ("momentum", 1.1), ("volatility", 1.0),
            ("volume", 1.0), ("pattern", 1.0), ("momentum_adv", 1.1),
            ("prediction_market", 1.0), ("sentiment", 1.0),
            ("derivatives", 1.1), ("onchain", 1.0),
            ("order_flow", 1.0), ("macro_risk", 1.0),
        ],
    };
    for (g, b) in &boosts {
        if *g == group { return *b; }
    }
    1.0
}

pub fn strategy_weight_from_cache(strategy: &str, cache: &HashMap<String, f64>) -> f64 {
    cache.get(strategy).copied().unwrap_or_else(|| default_weight(strategy))
}

pub fn compute_weight_from_bt(win_rate: f64, sharpe: f64) -> f64 {
    if win_rate > 0.0 && sharpe > 0.0 {
        (0.3 + win_rate * 0.4 + sharpe * 0.3).min(1.0)
    } else {
        0.5
    }
}

/// Regime modifier: adjust group weights based on market regime.
/// `regime`: "trending" | "ranging" | "volatile" | "" (default: balanced)
fn regime_group_multiplier(group: &str, regime: &str) -> f64 {
    match regime {
        "trending" => match group {
            "trend" => 1.4,
            "volume" => 1.2,
            "momentum_adv" => 0.8,
            "pattern" => 0.8,
            "volatility" => 0.7,
            "momentum" => 0.9,
            "sentiment" => 0.9,
            "derivatives" => 1.1,
            "onchain" => 1.0,
            "order_flow" => 1.1,
            "macro_risk" => 1.2,
            _ => 1.0,
        },
        "ranging" => match group {
            "momentum" => 1.3,
            "pattern" => 1.2,
            "volume" => 1.1,
            "trend" => 0.7,
            "volatility" => 0.8,
            "momentum_adv" => 1.3,
            "sentiment" => 1.0,
            "derivatives" => 1.0,
            "onchain" => 1.0,
            "order_flow" => 1.0,
            "macro_risk" => 1.0,
            _ => 1.0,
        },
        "volatile" => match group {
            "volatility" => 1.3,
            "momentum_adv" => 1.2,
            "pattern" => 1.1,
            "trend" => 0.8,
            "momentum" => 1.0,
            "volume" => 0.9,
            "sentiment" => 1.1,
            "derivatives" => 1.2,
            "onchain" => 1.0,
            "order_flow" => 1.2,
            "macro_risk" => 0.9,
            _ => 1.0,
        },
        _ => 1.0, // balanced — no adjustment
    }
}

/// Aggregate a batch of signals into BUY/SELL aggregated signals.
///
/// `regime` controls group weighting ("" = balanced, "trending"/"ranging"/"volatile" = regime-aware).
/// `prediction_market` and other groups act as independent confirmation sources.
pub fn aggregate(
    signals: &[RawSignal],
    asset_class: &str,
    currency: &str,
    bt_weights: &HashMap<String, f64>,
) -> Vec<AggregatedSignal> {
    aggregate_ext(signals, asset_class, currency, bt_weights, "")
}

/// Extended aggregation with regime-aware weighting.
pub fn aggregate_ext(
    signals: &[RawSignal],
    asset_class: &str,
    currency: &str,
    bt_weights: &HashMap<String, f64>,
    regime: &str,
) -> Vec<AggregatedSignal> {
    if signals.is_empty() {
        return vec![];
    }

    let mut buy_signals: Vec<&RawSignal> = Vec::new();
    let mut sell_signals: Vec<&RawSignal> = Vec::new();

    for s in signals {
        match s.action.as_str() {
            "BUY" => buy_signals.push(s),
            "SELL" => sell_signals.push(s),
            _ => {}
        }
    }

    let total_groups = GROUP_NAMES.len();
    let mut results = Vec::with_capacity(2);

    for (direction, dir_signals) in [("BUY", buy_signals), ("SELL", sell_signals)] {
        if dir_signals.is_empty() {
            continue;
        }

        // Collect unique strategy names, their groups, and track group counts
        let mut unique_names_set: HashSet<&str> = HashSet::new();
        let mut unique_groups: HashSet<&str> = HashSet::new();
        let mut group_strat_counts: HashMap<&str, usize> = HashMap::new();

        for s in &dir_signals {
            if unique_names_set.insert(&s.strategy) {
                if let Some(grp) = strategy_group(&s.strategy) {
                    unique_groups.insert(grp);
                    *group_strat_counts.entry(grp).or_insert(0) += 1;
                }
            }
        }

        let unique_names: Vec<String> = unique_names_set.into_iter().map(|s| s.to_string()).collect();

        // Compute weighted confidence with regime + correlation adjustments
        let mut total_weight = 0.0_f64;
        let mut weighted_conf = 0.0_f64;
        let mut best_reason = String::new();
        let mut best_conf = 0.0_f64;

        // Track per-group summed confidence and weight for correlation penalty
        let mut group_weight_sum: HashMap<&str, f64> = HashMap::new();
        let mut group_conf_weighted: HashMap<&str, f64> = HashMap::new();

        for s in &dir_signals {
            let bt_weight = bt_weights.get(&s.strategy).copied().unwrap_or_else(|| default_weight(&s.strategy));
            let cb = class_boost(&s.strategy, asset_class);
            let rm = if let Some(grp) = strategy_group(&s.strategy) {
                regime_group_multiplier(grp, regime)
            } else {
                1.0
            };

            // Correlation penalty: if multiple strategies from the same group,
            // apply diminishing returns (sqrt scaling for the group)
            let grp = strategy_group(&s.strategy).unwrap_or("");
            let cnt = group_strat_counts.get(grp).copied().unwrap_or(1) as f64;
            let corr_penalty = if cnt > 1.0 {
                1.0 / cnt.sqrt()  // 2 -> 0.71, 3 -> 0.58
            } else {
                1.0
            };

            let effective_weight = bt_weight * cb * rm * corr_penalty;
            weighted_conf += s.confidence * effective_weight;
            total_weight += effective_weight;

            *group_weight_sum.entry(grp).or_insert(0.0) += effective_weight;
            *group_conf_weighted.entry(grp).or_insert(0.0) += s.confidence * effective_weight;

            if s.confidence > best_conf {
                best_conf = s.confidence;
                best_reason.clone_from(&s.reason);
            }
        }

        let avg_conf = if total_weight > 0.0 {
            weighted_conf / total_weight
        } else {
            0.0
        };

        // Boost confidence based on agreeing independent groups
        let agreeing = unique_groups.len();
        let boosted_conf = if agreeing >= 2 {
            let raw_boost = 1.0 + (agreeing as f64 - 1.0) * 0.15;
            // Capped at 7 groups now
            (avg_conf * raw_boost.min(1.9)).min(1.0)
        } else if agreeing == 0 {
            avg_conf * 0.5
        } else {
            avg_conf
        };

        // Strategy count diversity bonus
        let final_conf = if unique_names.len() >= 5 {
            (boosted_conf * 1.15).min(1.0)
        } else if unique_names.len() >= 3 {
            (boosted_conf * 1.1).min(1.0)
        } else {
            boosted_conf
        };

        let raw_conf = if total_weight > 0.0 { weighted_conf / total_weight } else { 0.0 };

        results.push(AggregatedSignal {
            asset: currency.to_string(),
            direction: direction.to_string(),
            confidence: final_conf,
            raw_confidence: raw_conf,
            agreeing_groups: agreeing,
            total_groups,
            strategy_count: unique_names.len(),
            strategies: unique_names,
            best_reason,
            asset_class: asset_class.to_string(),
        });
    }

    results.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap_or(std::cmp::Ordering::Equal));
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_signal(strategy: &str, action: &str, confidence: f64, reason: &str) -> RawSignal {
        RawSignal {
            strategy: strategy.to_string(),
            action: action.to_string(),
            confidence,
            reason: reason.to_string(),
        }
    }

    #[test]
    fn test_empty_signals() {
        let result = aggregate(&[], "growth", "BTC-USD", &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn test_single_buy() {
        let signals = vec![make_signal("ema_cross", "BUY", 0.7, "EMA cross up")];
        let result = aggregate(&signals, "growth", "BTC-USD", &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].direction, "BUY");
        assert!(result[0].confidence > 0.0);
    }

    #[test]
    fn test_buy_and_sell() {
        let signals = vec![
            make_signal("ema_cross", "BUY", 0.7, "EMA cross up"),
            make_signal("rsi_revert", "SELL", 0.8, "RSI overbought"),
        ];
        let result = aggregate(&signals, "growth", "BTC-USD", &HashMap::new());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].direction, "SELL"); // higher confidence first
        assert_eq!(result[1].direction, "BUY");
    }

    #[test]
    fn test_group_agreement_boost() {
        let signals = vec![
            make_signal("ema_cross", "BUY", 0.5, "trend buy"),
            make_signal("rsi_revert", "BUY", 0.5, "momentum buy"),
            make_signal("boll_break", "BUY", 0.5, "volatility buy"),
        ];
        let result = aggregate(&signals, "growth", "BTC-USD", &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].agreeing_groups, 3);
        // Boost: 1.0 + (3-1) * 0.15 = 1.30; avg_conf ~0.5; boosted = 0.5 * 1.3 = 0.65
        // Then strategy_count >= 3: 0.65 * 1.1 = 0.715
        assert!((result[0].confidence - 0.715).abs() < 0.01);
    }

    #[test]
    fn test_no_group_penalty() {
        let signals = vec![make_signal("ema_cross", "BUY", 0.5, "buy")];
        let result = aggregate(&signals, "growth", "BTC-USD", &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].agreeing_groups, 1);
        // No boost for 1 group, no penalty
        assert!((result[0].confidence - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_three_plus_strategies_bonus() {
        let signals = vec![
            make_signal("ema_cross", "BUY", 0.4, "trend"),
            make_signal("rsi_revert", "BUY", 0.4, "momentum"),
            make_signal("boll_break", "BUY", 0.4, "volatility"),
        ];
        let result = aggregate(&signals, "growth", "BTC-USD", &HashMap::new());
        assert!(result[0].strategy_count >= 3);
    }

    #[test]
    fn test_regime_trending_boosts_trend() {
        // Use different confidences to make regime weighting visible
        let signals = vec![
            make_signal("ema_cross", "BUY", 0.8, "trend"),
            make_signal("rsi_revert", "BUY", 0.3, "momentum"),
        ];
        let balanced = aggregate(&signals, "growth", "BTC-USD", &HashMap::new());
        let trending = aggregate_ext(&signals, "growth", "BTC-USD", &HashMap::new(), "trending");
        // In trending regime, trend (ema_cross at 0.8) gets 1.4x multiplier
        // while momentum (rsi_revert at 0.3) gets 0.9x
        // This shifts avg_conf toward the stronger trend signal
        let balanced_conf = balanced[0].confidence;
        let trending_conf = trending[0].confidence;
        assert!(trending_conf > balanced_conf);
    }

    #[test]
    fn test_correlation_penalty() {
        // 3 strategies from same group should have less weight than 3 from different groups
        let same_group = vec![
            make_signal("ema_cross", "BUY", 0.6, "trend1"),
            make_signal("macd", "BUY", 0.6, "trend2"),
            make_signal("hma", "BUY", 0.6, "trend3"),
        ];
        let diff_groups = vec![
            make_signal("ema_cross", "BUY", 0.6, "trend"),
            make_signal("rsi_revert", "BUY", 0.6, "momentum"),
            make_signal("boll_break", "BUY", 0.6, "volatility"),
        ];
        let r1 = aggregate(&same_group, "growth", "BTC-USD", &HashMap::new());
        let r2 = aggregate(&diff_groups, "growth", "BTC-USD", &HashMap::new());
        // 3 different groups should have higher confidence than 3 from same group
        assert!(r2[0].confidence > r1[0].confidence);
    }
}
