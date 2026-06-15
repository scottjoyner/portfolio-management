"""Novel backtesting strategy implementations."""
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

@dataclass
class OHLCVBar:
    """Single candlestick bar."""
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_window: List[float] = field(default_factory=list)
    
    @property
    def body(self) -> float:
        return abs(self.close - self.open)
    
    @property
    def wick(self) -> float:
        return max(self.high, self.low) - min(self.open, self.close)

@dataclass  
class StrategyMetrics:
    """Performance metrics for a backtested strategy."""
    win_rate: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    profit_factor: float = 0.0
    total_return_pct: float = 0.0
    calmar_ratio: float = 0.0
    
    def is_strong(self) -> bool:
        return self.win_rate >= 0.6 and self.sharpe_ratio > 1.5

class BaseStrategy(ABC):
    """Abstract base for all backtesting strategies."""
    
    @abstractmethod
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        """Return 'BUY', 'SELL', or None on each new bar."""
        ...
    
    @abstractmethod
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        """Compute performance metrics from trade records."""
        ...

def _calc_metrics(trades: List[Dict]) -> StrategyMetrics:
    """Helper to compute metrics for all strategies."""
    if not trades:
        return StrategyMetrics(0.0, 0, 0, 0, 0.0, 0.0)
    
    wins = [t for t in trades if t.get('pnl_pct', 0) > 0]
    losses = [t for t in trades if t.get('pnl_pct', 0) <= 0]
    win_rate = len(wins) / max(1, len(trades))
    
    avg_win = sum(t['pnl_pct'] for t in wins) / len(wins) if wins else 0.0
    avg_loss = abs(sum(t['pnl_pct'] for t in losses) / len(losses)) if losses else 0.0
    
    pf = (sum(t['pnl_pct'] for t in wins)) / max(0.001, sum(abs(t['pnl_pct']) for t in losses)) if losses else float('inf')
    
    return StrategyMetrics(
        win_rate=win_rate * 100,
        total_trades=len(trades),
        winning_trades=len(wins),
        losing_trades=len(losses),
        avg_win=avg_win,
        avg_loss=float(avg_loss) if losses else float(0.0),
        profit_factor=float(pf) if pf != float('inf') else 999.0,
    )

class MultiTimeframeRSIMomentumStrategy(BaseStrategy):
    """Multi-timeframe RSI momentum strategy."""
    
    def __init__(self, short_period: int = 14, long_period: int = 28):
        self.short_period = short_period
        self.long_period = long_period
    
    def _rsi(self, closes: List[float], period: int) -> float:
        if len(closes) < period:
            return 50.0
        changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        gains = sum(max(0, c) for c in changes[-period:])
        losses = sum(abs(min(0, c)) for c in changes[-period:])
        rs = gains / max(losses, 0.0001)
        return 100 - (100 / (1 + rs))
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        closes = list(bar.close_window)
        if len(closes) < self.long_period:
            return None
        
        short_rsi = self._rsi(closes, self.short_period)
        long_rsi = self._rsi(closes, self.long_period)
        
        if short_rsi > 50 and short_rsi > long_rsi and short_rsi < 70:
            return "BUY"
        elif short_rsi < 50 and short_rsi < long_rsi and short_rsi > 30:
            return "SELL"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

class BollingerSqueezeBreakoutStrategy(BaseStrategy):
    """Buy on Bollinger Band squeeze, sell on expansion breakout."""
    
    def __init__(self, bb_period: int = 20, bb_mult: float = 2.0):
        self.bb_period = bb_period
        self.bb_mult = bb_mult
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        closes = list(bar.close_window)[-self.bb_period:] if len(bar.close_window) >= self.bb_period else []
        if len(closes) < self.bb_period:
            return None
        
        sma = sum(closes) / len(closes)
        variance = sum((c - sma)**2 for c in closes) / len(closes)
        std_dev = math.sqrt(variance)
        
        bb_upper = sma + self.bb_mult * std_dev
        bb_lower = sma - self.bb_mult * std_dev
        
        band_width = (bb_upper - bb_lower) / sma if sma else 0
        squeezed = bar.close > bb_lower and bar.close < bb_upper and band_width < 0.02
        
        if squeezed and bar.close > bb_upper:
            return "BUY"
        elif bar.close < bb_lower:
            return "SELL"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

class CrossExchangeMicrostructureArbStrategy(BaseStrategy):
    """Simulated cross-exchange arbitrage strategy."""
    
    def __init__(self, signal_window: int = 20):
        self.signal_window = signal_window
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        closes = list(bar.close_window)[-self.signal_window:] if len(bar.close_window) >= self.signal_window else []
        if len(closes) < 5:
            return None
        
        rs_signal = sum((closes[i] - closes[i-1])/max(abs(closes[i-1]), 0.01) for i in range(1, len(closes)))/max(len(closes), 1)
        
        if rs_signal > 0.5 and bar.close > bar.open:
            return "BUY"
        elif rs_signal < -0.5 and bar.close < bar.open:
            return "SELL"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

class RegimeAwareAdaptiveStrategy(BaseStrategy):
    """Combines trend, mean-reversion, and volatility signals dynamically."""
    
    def __init__(self, trend_ma_period: int = 50, reversion_threshold: float = 1.5):
        self.trend_ma_period = trend_ma_period
        self.reversion_threshold = reversion_threshold
    
    def _get_regime(self, closes: List[float], bar_close: float) -> str:
        if len(closes) < self.trend_ma_period:
            return "unknown"
        ma_50 = sum(closes[-self.trend_ma_period:]) / len(closes[-self.trend_ma_period:])
        std = math.sqrt(sum((c - ma_50)**2 for c in closes[-self.trend_ma_period:]) / len(closes[-self.trend_ma_period:]))
        
        if std < 0.01:
            return "range"
        elif bar_close > ma_50:
            return "uptrend"
        else:
            return "downtrend"
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        closes = list(bar.close_window)[-100:] if len(bar.close_window) >= 100 else []
        regime = self._get_regime(closes, bar.close)
        
        ma_20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else bar.close
        std = math.sqrt(sum((c-ma_20)**2 for c in closes[-20:])/20) if len(closes) >= 20 else 1
        z_score = (bar.close - ma_20) / max(std, 0.01)
        
        if regime == "uptrend" and bar.close > ma_50:
            return "BUY"
        elif regime == "downtrend" and bar.close < ma_50:
            return "SELL"
        elif regime == "range":
            if z_score > self.reversion_threshold:
                return "SELL"
            elif z_score < -self.reversion_threshold:
                return "BUY"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

class OnChainRegimeWhaleFlowStrategy(BaseStrategy):
    """Correlates price action with on-chain whale activity signals."""
    
    def __init__(self, whale_threshold: int = 1000000):
        self.whale_threshold = whale_threshold
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        price_change_pct = (bar.close - bar.open) / max(bar.open, 0.01)
        
        if bar.volume > 1e9 and price_change_pct > 0.02:
            return "BUY"
        elif bar.volume > 1e9 and price_change_pct < -0.02:
            return "SELL"
        
        closes = list(bar.close_window)[-50:] if len(bar.close_window) >= 50 else []
        if len(closes) >= 50:
            ma_50 = sum(closes[-50:]) / 50
            if bar.close < ma_50 * 0.97 and price_change_pct > -0.01:
                return "BUY"
            elif bar.close > ma_50 * 1.03 and price_change_pct < 0.01:
                return "SELL"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

class SentimentMomentumCompositeStrategy(BaseStrategy):
    """Combines price momentum with sentiment-like signals."""
    
    def __init__(self, trend_period: int = 20, short_period: int = 5):
        self.trend_period = trend_period
        self.short_period = short_period
    
    def _trend_signal(self, closes: List[float]) -> float:
        if len(closes) < self.trend_period:
            return 0.0
        ma_trend = sum(closes[-self.trend_period:]) / self.trend_period
        return (closes[-1] - ma_trend) / max(abs(ma_trend), 0.01)
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        closes = list(bar.close_window)[-self.trend_period:] if len(bar.close_window) >= self.trend_period else []
        
        if len(closes) < max(self.trend_period, self.short_period):
            return None
        
        trend_signal = self._trend_signal(closes)
        short_momentum = (closes[-1] - closes[-5]) / max(abs(closes[-5]), 0.01) if len(closes) >= 5 else 0
        price_position = (bar.close - min(closes[-20:]) / max(max(closes[-20:]), 0.01)) if len(closes) >= 20 else 0
        
        if trend_signal > 0.03 and short_momentum > 0 and price_position < 1.5:
            return "BUY"
        elif trend_signal < -0.03 and short_momentum < 0 and price_position > 0.8:
            return "SELL"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

class VolRegimeSwitchStrategy(BaseStrategy):
    """Switches between trend-following and mean-reversion based on volatility regime."""
    
    def __init__(self, atr_period: int = 14):
        self.atr_period = atr_period
    
    def on_bar(self, bar: OHLCVBar) -> Optional[str]:
        closes = list(bar.close_window)[-20:] if len(bar.close_window) >= 20 else []
        
        if not closes:
            return None
        
        returns = [(closes[i] - closes[i-1]) / max(abs(closes[i-1]), 0.01) for i in range(1, min(len(closes), 21))] if len(closes) > 1 else []
        mean_ret = sum(returns) / max(1, len(returns)) if returns else 0
        vol = math.sqrt(sum((r - mean_ret)**2 for r in returns) / max(1, len(returns))) if returns else 0
        atr = vol * abs(bar.close)
        
        if atr < 50:
            returns_1d = (bar.close - bar.open) / max(abs(bar.open), 0.01)
            
            if returns_1d > 0.03 and atr > 20:
                return "SELL"
            elif returns_1d < -0.03 and atr > 20:
                return "BUY"
        else:
            if bar.close > sum(closes[-5:]) / max(1, len(closes)) + atr * 0.5:
                return "BUY"
            elif bar.close < sum(closes[-5:]) / max(1, len(closes)) - atr * 0.5:
                return "SELL"
        
        return None
    
    def calculate_metrics(self, trades: List[Dict]) -> StrategyMetrics:
        return _calc_metrics(trades)

def backtest_strategy(strategy: BaseStrategy, symbol_data: List[OHLCVBar], initial_capital: float = 10000) -> Dict[str, Any]:
    """Run a strategy against historical data and return metrics."""
    
    position_open = False
    entry_price = 0.0
    trades = []
    capital = initial_capital
    prev_capital = [initial_capital]
    
    for bar in symbol_data[50:]:
        signal = strategy.on_bar(bar)
        
        if signal == "BUY" and not position_open:
            entry_price = bar.close
            position_open = True
        elif signal == "SELL" and position_open:
            exit_price = bar.close
            pnl_pct = (exit_price - entry_price) / max(entry_price, 0.01)
            
            trades.append({
                'type': 'sell',
                'entry': entry_price,
                'exit': exit_price, 
                'pnl_pct': pnl_pct,
                'bars_held': int(len(symbol_data)),
            })
            position_open = False
    
    metrics = _calc_metrics(trades)
    
    peak = initial_capital
    for t in trades:
        cap = initial_capital * (1 + sum(x['pnl_pct'] for x in trades[:trades.index(t)+1]))
        if cap > peak:
            peak = cap
        dd = (peak - cap) / max(peak, 0.01)
        metrics.max_drawdown_pct = max(metrics.max_drawdown_pct, dd)
    
    if trades and metrics.avg_win > 0 and metrics.avg_loss > 0:
        # Calculate sharpe ratio - handle edge cases where avg values might be special types
        aw = float(metrics.avg_win)
        al = float(metrics.avg_loss)
        sharpe = (aw - al) / math.sqrt(max(aw**2 + al**2, 0.01))
        metrics.sharpe_ratio = sharpe
    
    return {
        'strategy': strategy.__class__.__name__,
        'metrics': metrics,
        'trades': trades,
        'capital_path': prev_capital,
    }

def run_backtest(strategy: BaseStrategy, bars: List[OHLCVBar], initial_capital: float = 10000) -> Dict[str, Any]:
    """Run backtest with proper bar construction."""
    
    for bar in bars:
        closes = list(bar.close_window)
        if len(closes) < 20:
            closes.append(bar.close)
            bar.close_window = closes
    
    results = backtest_strategy(strategy, bars, initial_capital)
    
    total_return = sum(t['pnl_pct'] for t in results['trades'])
    results['total_return_pct'] = total_return * 100
    results['num_trades'] = len(results['trades'])
    
    return results

def simulate_mock_data(days: int, start_price: float = 25000.0) -> List[OHLCVBar]:
    """Generate synthetic crypto price data for backtesting."""
    import random
    
    random.seed(42)
    bars = []
    price = start_price
    
    daily_vol = 0.03
    
    for day in range(days):
        ret = (random.random() - 0.52) * 2 * daily_vol
        open_p = price
        close_p = price * (1 + ret)
        
        intraday_noise = random.uniform(-0.015, 0.015)
        high_p = max(open_p, close_p) * (1 + intraday_noise / 2)
        low_p = min(open_p, close_p) - abs(intraday_noise) / 2
        
        volume = random.uniform(5e8, 3e9) * (abs(ret) / daily_vol + 1)
        
        timestamp = f"2024-{(day % 12)+1:02d}-{(day % 28)+1:02d}"
        
        bar = OHLCVBar(
            timestamp=timestamp,
            open=open_p,
            high=high_p,
            low=low_p,
            close=close_p,
            volume=volume,
            close_window=[close_p]
        )
        bars.append(bar)
        
        price = close_p
    
    return bars

def run_mock_backtest(strategy: BaseStrategy):
    """Run backtest on simulated data."""
    
    days = 900
    bars = simulate_mock_data(days, 45000)
    
    print(f"Running {strategy.__class__.__name__}...")
    result = run_backtest(strategy, bars)
    
    m = result['metrics']
    print(f"  Win Rate: {m.win_rate:.1f}% | Trades: {m.total_trades}")
    print(f"  W/L: {m.winning_trades}/{m.losing_trades} | PfF: {m.profit_factor:.2f}")
    print(f"  Sharpe: {m.sharpe_ratio:.2f} | DD: {m.max_drawdown_pct*100:.1f}%")
    
    return m

def main():
    """Run all strategies on simulated data for testing."""
    
    from coinbase.src.backtest.new_strategies import (
        MultiTimeframeRSIMomentumStrategy,
        BollingerSqueezeBreakoutStrategy,
        CrossExchangeMicrostructureArbStrategy,
        RegimeAwareAdaptiveStrategy,
        OnChainRegimeWhaleFlowStrategy,
        SentimentMomentumCompositeStrategy,
        VolRegimeSwitchStrategy,
    )
    
    print("=" * 60)
    print("Running Backtest Framework against --simulate-data")
    print("=" * 60)
    
    strategies = [
        MultiTimeframeRSIMomentumStrategy(short_period=14, long_period=28),
        BollingerSqueezeBreakoutStrategy(bb_period=20, bb_mult=2.0),
        CrossExchangeMicrostructureArbStrategy(signal_window=20),
        RegimeAwareAdaptiveStrategy(trend_ma_period=50),
        OnChainRegimeWhaleFlowStrategy(whale_threshold=1e6),
        SentimentMomentumCompositeStrategy(trend_period=20, short_period=5),
        VolRegimeSwitchStrategy(atr_period=14),
    ]
    
    results = {}
    for strat in strategies:
        m = run_mock_backtest(strat)
        results[strat.__class__.__name__] = m
    
    print()
    print("=" * 60)
    print("PAPER TRADING CANDIDATES (>60% win rate)")
    print("=" * 60)
    
    for name, m in results.items():
        if m.win_rate >= 60:
            print(f"\n{name}:")
            print(f"  Win Rate: {m.win_rate:.1f}%")
            print(f"  Trades: {m.total_trades}")
            print(f"  Sharpe: {m.sharpe_ratio:.2f}")

if __name__ == "__main__":
    main()
