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
    pub passed: bool,
    pub reason: String,
}

/// Run a single strategy through historical data and compute performance metrics.
/// Uses the unified evaluate() dispatcher — supports all 25 strategies.
pub fn backtest_strategy(
    strategy_name: &str,
    closes: &[f64],
    volumes: &[f64],
    highs: Option<&[f64]>,
    lows: Option<&[f64]>,
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
            passed: false,
            reason: "Insufficient data".to_string(),
        };
    }

    let vols = volumes;
    let h = highs.unwrap_or(&[]);
    let l = lows.unwrap_or(&[]);

    let mut trades: Vec<BacktestTrade> = Vec::new();
    let mut open_trade: Option<BacktestTrade> = None;
    let mut equity_curve: Vec<f64> = vec![1.0];

    for i in warmup..n {
        let bar_closes = &closes[..=i];
        let bar_volumes = if i < vols.len() { &vols[..=i] } else { &[] };
        let bar_highs = if i < h.len() { &h[..=i] } else { &[] };
        let bar_lows = if i < l.len() { &l[..=i] } else { &[] };
        let current_price = closes[i];

        let sig = strategies::evaluate(
            strategy_name, bar_closes, bar_volumes, bar_highs, bar_lows,
        );

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
            if should_close {
                let return_pct = if ot.side == "BUY" {
                    (current_price - ot.entry_price) / ot.entry_price * 100.0
                } else {
                    (ot.entry_price - current_price) / ot.entry_price * 100.0
                };
                let mut t = ot.clone();
                t.exit_bar = Some(i);
                t.exit_price = Some(current_price);
                t.return_pct = Some(return_pct);
                trades.push(t);
                open_trade = None;
                let ret = return_pct / 100.0;
                let new_equity = equity_curve.last().unwrap_or(&1.0) * (1.0 + ret);
                equity_curve.push(new_equity);
            }
        }
    }

    // Force-close any open trade at last bar
    if let Some(ot) = open_trade {
        let return_pct = if ot.side == "BUY" {
            (closes[n - 1] - ot.entry_price) / ot.entry_price * 100.0
        } else {
            (ot.entry_price - closes[n - 1]) / ot.entry_price * 100.0
        };
        let mut t = ot;
        t.exit_bar = Some(n - 1);
        t.exit_price = Some(closes[n - 1]);
        t.return_pct = Some(return_pct);
        trades.push(t);
        let ret = return_pct / 100.0;
        let new_equity = equity_curve.last().unwrap_or(&1.0) * (1.0 + ret);
        equity_curve.push(new_equity);
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

    let passed = win_rate >= 0.50 && sharpe_ratio > 0.5 && profit_factor > 1.20 && max_drawdown_pct < 15.0 && total_return_pct > -10.0;
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
        passed,
        reason,
    }
}
