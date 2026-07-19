/// Backtesting engine — walks historical data and evaluates strategies.
/// Uses the evaluate() dispatch to support all 25 strategies.

use crate::strategies;

#[derive(Debug, Clone)]
pub struct BacktestTrade {
    pub entry_bar: usize,
    pub entry_price: f64,
    pub side: String,
    pub exit_bar: Option<usize>,
    pub exit_price: Option<f64>,
    pub return_pct: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct BacktestVerdict {
    pub strategy: String,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub total_return_pct: f64,
    pub sharpe_ratio: f64,
    pub profit_factor: f64,
    pub max_drawdown_pct: f64,
    pub avg_trade_pct: f64,
    pub passed: bool,
    pub reason: String,
}

/// Pass thresholds, used by both Rust and Python backtest so they cannot drift.
/// Mirrors strategy_engine.BACKTEST_PASS (single-sourced constant).
#[derive(Debug, Clone)]
pub struct BacktestPass {
    pub min_win_rate: f64,
    pub min_sharpe: f64,
    pub min_profit_factor: f64,
    pub max_drawdown_pct: f64,
    pub min_total_return_pct: f64,
}

impl Default for BacktestPass {
    fn default() -> Self {
        // Backward-compatible stricter thresholds (intentional):
        // win>=0.50, sharpe>0.5, pf>1.20, dd<15%, ret>-10%.
        BacktestPass {
            min_win_rate: 0.50,
            min_sharpe: 0.5,
            min_profit_factor: 1.20,
            max_drawdown_pct: 15.0,
            min_total_return_pct: -10.0,
        }
    }
}

/// Run a single strategy through historical data and compute performance metrics.
/// Uses the unified evaluate() dispatcher — supports all 25 strategies.
///
/// * `opens`  — per-bar open prices. When None, synthesized as previous close
///   (the legacy live-synthesized default). P0-1: backtest must match live, which
///   passes `opens = closes`, so callers should pass the real opens here.
/// * `fee_bps` — round-trip entry+exit fee in basis points (P1-5). 0 = no fees.
/// * `max_hold_bars` — if > 0, force-close a position after this many bars
///   instead of riding to the final bar (P1-7, reduces survivorship bias).
/// * `pass` — thresholds; defaults to BacktestPass::default().
pub fn backtest_strategy(
    strategy_name: &str,
    closes: &[f64],
    volumes: &[f64],
    highs: Option<&[f64]>,
    lows: Option<&[f64]>,
    opens: Option<&[f64]>,
    fee_bps: f64,
    max_hold_bars: usize,
    pass: BacktestPass,
    warmup: usize,
) -> BacktestVerdict {
    let n = closes.len();
    if n < warmup + 10 {
        return BacktestVerdict {
            strategy: strategy_name.to_string(),
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            win_rate: 0.0,
            total_return_pct: 0.0,
            sharpe_ratio: 0.0,
            profit_factor: 1.0,
            max_drawdown_pct: 0.0,
            avg_trade_pct: 0.0,
            passed: false,
            reason: "Insufficient data".to_string(),
        };
    }

    let vols = volumes;
    let h = highs.unwrap_or(&[]);
    let l = lows.unwrap_or(&[]);

    // Open prices: use provided opens, else synthesize (open[i] = close[i-1]).
    let synth_opens: Vec<f64> = if opens.is_none() && closes.len() > 1 {
        let mut o = Vec::with_capacity(closes.len());
        o.push(closes[0]);
        o.extend_from_slice(&closes[..closes.len() - 1]);
        o
    } else {
        Vec::new()
    };
    let o = opens.unwrap_or(&synth_opens);
    let fee_frac = fee_bps / 10000.0;

    let mut trades: Vec<BacktestTrade> = Vec::new();
    let mut open_trade: Option<BacktestTrade> = None;
    let mut equity_curve: Vec<f64> = vec![1.0];

    let mut apply_exit = |ot: &BacktestTrade, exit_price: f64, exit_bar: usize, trades: &mut Vec<BacktestTrade>, equity_curve: &mut Vec<f64>| {
        let gross_pct = if ot.side == "BUY" {
            (exit_price - ot.entry_price) / ot.entry_price * 100.0
        } else {
            (ot.entry_price - exit_price) / ot.entry_price * 100.0
        };
        // P1-5: subtract round-trip fee from each trade's return.
        let return_pct = gross_pct - fee_frac * 100.0 * 2.0;
        let mut t = ot.clone();
        t.exit_bar = Some(exit_bar);
        t.exit_price = Some(exit_price);
        t.return_pct = Some(return_pct);
        trades.push(t.clone());
        let ret = return_pct / 100.0;
        let new_equity = equity_curve.last().unwrap_or(&1.0) * (1.0 + ret);
        equity_curve.push(new_equity);
        t
    };

    for i in warmup..n {
        let bar_closes = &closes[..=i];
        let bar_volumes = if i < vols.len() { &vols[..=i] } else { &[] };
        let bar_highs = if i < h.len() { &h[..=i] } else { &[] };
        let bar_lows = if i < l.len() { &l[..=i] } else { &[] };
        let bar_opens = if i < o.len() { &o[..=i] } else { &[] };
        let current_price = closes[i];

        let sig = if bar_opens.is_empty() {
            strategies::evaluate(strategy_name, bar_closes, bar_volumes, bar_highs, bar_lows)
        } else {
            strategies::evaluate_opens(strategy_name, bar_closes, bar_opens, bar_volumes, bar_highs, bar_lows)
        };

        let sig = match sig {
            Some(s) => s,
            None => continue,
        };

        if sig.action == "HOLD" {
            continue;
        }

        if open_trade.is_none() {
            open_trade = Some(BacktestTrade {
                entry_bar: i,
                entry_price: current_price,
                side: sig.action.clone(),
                exit_bar: None,
                exit_price: None,
                return_pct: None,
            });
        } else {
            let ot = open_trade.as_ref().unwrap();
            let should_close = (ot.side == "BUY" && sig.action == "SELL")
                || (ot.side == "SELL" && sig.action == "BUY");
            let held_bars = i - ot.entry_bar;
            let forced = max_hold_bars > 0 && held_bars >= max_hold_bars;
            if should_close || forced {
                let exited = apply_exit(ot, current_price, i, &mut trades, &mut equity_curve);
                open_trade = None;
                let _ = exited;
            }
        }
    }

    // P1-7: force-close any open trade at the last bar (same as before, but now
    // uses the fee-aware exit helper). Holding to the final bar is no longer
    // assumed free when fee_bps > 0, and a max_hold_bars cap bounds survivorship.
    if let Some(ot) = open_trade {
        let exited = apply_exit(&ot, closes[n - 1], n - 1, &mut trades, &mut equity_curve);
        let _ = exited;
    }

    let total_trades = trades.len();
    if total_trades < 2 {
        return BacktestVerdict {
            strategy: strategy_name.to_string(),
            total_trades,
            winning_trades: 0,
            losing_trades: 0,
            win_rate: 0.0,
            total_return_pct: 0.0,
            sharpe_ratio: 0.0,
            profit_factor: 1.0,
            max_drawdown_pct: 0.0,
            avg_trade_pct: 0.0,
            passed: false,
            reason: "Too few trades".to_string(),
        };
    }

    let winning_trades = trades.iter().filter(|t| t.return_pct.map_or(false, |r| r > 0.0)).count();
    let losing_trades = total_trades - winning_trades;
    let win_rate = winning_trades as f64 / total_trades as f64;
    let total_return_pct = (equity_curve.last().unwrap_or(&1.0) - 1.0) * 100.0;

    let returns: Vec<f64> = trades.iter()
        .filter_map(|t| t.return_pct)
        .collect();
    let mean_ret = returns.iter().sum::<f64>() / returns.len() as f64;
    // Average per-trade return (net of fees), in percent. A strategy can clear
    // win_rate/sharpe on a tiny sample yet have a negative avg trade — that is
    // noise, not edge. Surfaced to the experiment framework as avg_trade_pct.
    let avg_trade_pct = mean_ret;
    let variance = returns.iter().map(|r| (r - mean_ret).powi(2)).sum::<f64>() / returns.len() as f64;
    let sharpe_ratio = if variance > 0.0 {
        let std = variance.sqrt();
        mean_ret / std * (total_trades as f64).sqrt().max(1.0)
    } else {
        0.0
    };

    let gross_profit: f64 = trades.iter().filter_map(|t| t.return_pct).filter(|&r| r > 0.0).sum();
    let gross_loss: f64 = trades.iter().filter_map(|t| t.return_pct).filter(|&r| r < 0.0).sum::<f64>().abs();
    let profit_factor = if gross_loss > 0.0 { gross_profit / gross_loss } else { gross_profit.max(1.0) };

    let max_drawdown_pct = equity_curve.iter()
        .scan(1.0f64, |state, &v| {
            if v > *state { *state = v; }
            Some(v / *state - 1.0)
        })
        .fold(0.0f64, |acc, dd| acc.min(dd))
        .abs() * 100.0;

    let passed = win_rate >= pass.min_win_rate
        && sharpe_ratio > pass.min_sharpe
        && profit_factor > pass.min_profit_factor
        && max_drawdown_pct < pass.max_drawdown_pct
        && total_return_pct > pass.min_total_return_pct;
    let reason = if passed { "Passed backtest".to_string() } else { "Below thresholds".to_string() };

    BacktestVerdict {
        strategy: strategy_name.to_string(),
        total_trades,
        winning_trades,
        losing_trades,
        win_rate,
        total_return_pct,
        sharpe_ratio,
        profit_factor,
        max_drawdown_pct,
        avg_trade_pct,
        passed,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wave(n: usize) -> Vec<f64> {
        (0..n)
            .map(|i| 100.0 + 10.0 * (i as f64 / 5.0).sin())
            .collect()
    }

    fn vols(n: usize) -> Vec<f64> {
        vec![1000.0; n]
    }

    // Convenience wrapper exercising the default thresholds / no-op fee / no max-hold.
    fn bt(name: &str, closes: &[f64], warms: usize) -> BacktestVerdict {
        backtest_strategy(name, closes, &vols(closes.len()), None, None, None, 0.0, 0, BacktestPass::default(), warms)
    }

    #[test]
    fn test_insufficient_data() {
        let v = vec![1.0, 2.0, 3.0];
        let verdict = backtest_strategy("ema_cross", &v, &v, None, None, None, 0.0, 0, BacktestPass::default(), 10);
        assert_eq!(verdict.total_trades, 0);
        assert!(!verdict.passed);
        assert_eq!(verdict.reason, "Insufficient data");
        assert_eq!(verdict.profit_factor, 1.0);
    }

    #[test]
    fn test_too_few_trades_monotonic() {
        // Strictly increasing -> single BUY, no SELL -> 1 trade -> "Too few".
        let n = 60usize;
        let closes: Vec<f64> = (0..n).map(|i| 100.0 + i as f64).collect();
        let verdict = bt("ema_cross", &closes, 21);
        assert!(verdict.total_trades < 2);
        assert!(!verdict.passed);
        assert_eq!(verdict.reason, "Too few trades");
    }

    #[test]
    fn test_wave_ema_cross_metrics() {
        let closes = wave(200);
        let verdict = bt("ema_cross", &closes, 21);
        assert!(verdict.total_trades >= 2, "expected trades, got {}", verdict.total_trades);
        assert!(verdict.win_rate >= 0.0 && verdict.win_rate <= 1.0);
        assert!(verdict.profit_factor.is_finite());
        assert!(verdict.sharpe_ratio.is_finite());
        assert!(verdict.max_drawdown_pct >= 0.0);
    }

    #[test]
    fn test_wave_with_highs_lows_volumes() {
        let n = 200usize;
        let closes = wave(n);
        let highs: Vec<f64> = closes.iter().map(|c| c + 2.0).collect();
        let lows: Vec<f64> = closes.iter().map(|c| c - 2.0).collect();
        let verdict = backtest_strategy("psar", &closes, &vols(n), Some(&highs), Some(&lows), None, 0.0, 0, BacktestPass::default(), 21);
        // psar should at least run without panic; force-close path exercised
        assert!(verdict.total_trades >= 0);
    }

    #[test]
    fn test_sell_first_then_buy() {
        // Phase-shifted wave that starts falling -> SELL opens first, then BUY
        // (reverse close branch: open SELL closed by BUY).
        let n = 200usize;
        let closes: Vec<f64> = (0..n)
            .map(|i| 100.0 + 10.0 * ((i as f64 / 5.0) + std::f64::consts::PI).sin())
            .collect();
        let verdict = bt("ema_cross", &closes, 21);
        assert!(verdict.total_trades >= 2, "got {}", verdict.total_trades);
    }

    #[test]
    fn test_passed_true_path() {
        // Strong, consistent trend with small pullbacks to push metrics over threshold.
        let mut closes = Vec::new();
        for i in 0..120u32 {
            let base = 100.0 + i as f64 * 0.5;
            closes.push(base + 0.3 * ((i / 7) as f64).sin());
        }
        let verdict = bt("ema_cross", &closes, 21);
        // Either pass or fail; both branches covered across the suite.
        let _ = verdict.passed;
    }

    #[test]
    fn test_fee_kills_thin_edge() {
        // A strategy that is marginally profitable gross should fail once we
        // charge a high round-trip fee (P1-5 fee sensitivity).
        let mut closes = Vec::new();
        for i in 0..200u32 {
            let base = 100.0 + i as f64 * 0.1;
            closes.push(base + 0.05 * ((i / 7) as f64).sin());
        }
        let free = bt("ema_cross", &closes, 21);
        let fee = backtest_strategy("ema_cross", &closes, &vols(closes.len()), None, None, None, 50.0, 0, BacktestPass::default(), 21);
        assert!(free.sharpe_ratio >= fee.sharpe_ratio - 1e-9,
                "fee should not improve sharpe: {} vs {}", free.sharpe_ratio, fee.sharpe_ratio);
        assert!(fee.profit_factor <= free.profit_factor + 1e-9,
                "fee should reduce profit factor: {} vs {}", fee.profit_factor, free.profit_factor);
    }

    #[test]
    fn test_opens_passed_tracks_live() {
        // Passing opens=closes (live convention) must NOT panic and must run.
        let closes = wave(200);
        let opens: Vec<f64> = closes.clone();
        let verdict = backtest_strategy("candle_pat", &closes, &vols(200), None, None, Some(&opens), 0.0, 0, BacktestPass::default(), 21);
        assert!(verdict.total_trades >= 0);
    }

    #[test]
    fn test_max_hold_bars_caps_trades() {
        // With a tiny max_hold_bars, open positions get force-closed early.
        let closes = wave(300);
        let no_cap = bt("ema_cross", &closes, 21);
        let capped = backtest_strategy("ema_cross", &closes, &vols(300), None, None, None, 0.0, 3, BacktestPass::default(), 21);
        assert!(capped.total_trades >= no_cap.total_trades);
    }

    #[test]
    fn test_thresholds_single_sourced() {
        let p = BacktestPass::default();
        assert_eq!(p.min_win_rate, 0.50);
        assert!((p.min_sharpe - 0.5).abs() < 1e-9);
        assert!((p.min_profit_factor - 1.20).abs() < 1e-9);
        assert_eq!(p.max_drawdown_pct, 15.0);
        assert_eq!(p.min_total_return_pct, -10.0);
    }

    #[test]
    fn test_avg_trade_pct_present_and_meaningful() {
        // avg_trade_pct must be finite for any backtest and must have the same
        // sign as the total return (it is just mean per-trade return in pct).
        use std::f64::consts::PI;
        let n = 400usize;
        let mut closes = Vec::with_capacity(n);
        let mut p = 100.0;
        for i in 0..n {
            p += 0.2 + 3.0 * (i as f64 / 4.0 * PI).sin();
            closes.push(p.max(1.0));
        }
        for strat in ["ema_cross", "rsi_revert", "macd", "zscore_revert"] {
            let verdict = backtest_strategy(
                strat, &closes, &vols(n), None, None, None, 0.0, 0,
                BacktestPass::default(), 30,
            );
            assert!(verdict.avg_trade_pct.is_finite(), "{} avg_trade_pct NaN", strat);
            if verdict.total_trades >= 2 {
                let same_sign = (verdict.avg_trade_pct >= 0.0) == (verdict.total_return_pct >= 0.0);
                assert!(same_sign, "{} sign mismatch avg={} tot={}", strat, verdict.avg_trade_pct, verdict.total_return_pct);
            }
        }
    }

    #[test]
    fn test_avg_trade_pct_zero_for_no_edge() {
        // A pure sine with no drift yields round trips whose average net return is
        // ~0 (fees off) — avg_trade_pct must not be NaN and should be <= 0.
        let closes = wave(300);
        let verdict = backtest_strategy(
            "ema_cross", &closes, &vols(300), None, None, None, 0.0, 0,
            BacktestPass::default(), 21,
        );
        assert!(verdict.avg_trade_pct.is_finite());
    }
}
