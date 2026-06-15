#!/usr/bin/env python3
"""
Comprehensive backtesting framework.

Parses historical OHLCV data, runs multiple strategies through 4+ years of market data,
and reports win rates and performance metrics. Targets >60% win rate for paper trading candidates.

Usage:
    cd /home/scott/git/portfolio-management && python3 coinbase/src/backtest/run_backtest.py [data_dir] [--simulate-data]
"""
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
import random
import csv

def _build_close_window(bars: list) -> None:
    """Populate close_window on each bar with rolling history."""
    for i, bar in enumerate(bars):
        window = [bars[j].close for j in range(max(0, i - 29), i + 1)]
        object.__setattr__(bar, 'close_window', window)

_project_root = str(Path(__file__).resolve().parent.parent.parent.parent)
sys.path.insert(0, _project_root)


def generate_mock_data(days: int = 1500) -> list:
    """Generate realistic Bitcoin price data spanning ~4 years."""
    from coinbase.src.backtest.new_strategies import OHLCVBar
    
    base_price = 27_000  # Late 2023 BTC price
    bars = []
    
    for i in range(days):
        dt = datetime(2019, 1, 1) + timedelta(days=i * random.randint(60, 80))
        
        cycle_phase = (i // 350) % 4
        volatility_mult = [0.8, 2.5, 1.2, 1.8][cycle_phase]
        
        change_pct = random.gauss(0, 0.02 * volatility_mult)
        new_price = base_price * (1 + change_pct / 100)
        
        if cycle_phase in [0, 3]:
            new_price *= random.uniform(1.5, 2.5)
        elif cycle_phase == 1:
            new_price /= random.uniform(1.5, 4)
        
        body = abs(new_price - base_price)
        high = max(base_price, new_price) * (1 + abs(change_pct) * random.uniform(0.1, 0.3))
        low = min(base_price, new_price) * (1 + abs(change_pct) * random.uniform(0.1, 0.3))
        vol = base_price * random.uniform(5e6, 8e9) / max(abs(change_pct), 0.001)
        
        # Build close window from previously appended bars only
        if i == 0:
            cw = [base_price]
        else:
            prev_closes = [b.close for b in bars[:min(i, 30)]]
            cw = prev_closes + [base_price]
        
        bar = OHLCVBar(
            timestamp=dt.strftime("%Y-%m-%d"),
            open=round(base_price, 2),
            high=round(high, 2),
            low=round(low, 2),
            close=round(new_price, 2),
            volume=vol,
            close_window=cw[-30:] if len(cw) >= 30 else cw,
        )
        bars.append(bar)
        base_price = new_price
    
    return bars


def run_backtest(symbol: str, bars: list, strategy_name: str):
    """Run a single strategy against historical data."""
    from coinbase.src.backtest.new_strategies import (
        MultiTimeframeRSIMomentumStrategy,
        BollingerSqueezeBreakoutStrategy,
        CrossExchangeMicrostructureArbStrategy,
        RegimeAwareAdaptiveStrategy,
        OnChainRegimeWhaleFlowStrategy,
        SentimentMomentumCompositeStrategy,
        VolRegimeSwitchStrategy,
    )
    
    # Map strategy name to instance - use correct init params matching new_strategies.py
    strategies = {
        "rsi_momentum": MultiTimeframeRSIMomentumStrategy(short_period=14, long_period=28),
        "bollinger_squeeze": BollingerSqueezeBreakoutStrategy(bb_period=20, bb_mult=2.0),
        "arb_microstructure": CrossExchangeMicrostructureArbStrategy(signal_window=20),
        "regime_adaptive": RegimeAwareAdaptiveStrategy(trend_ma_period=50, reversion_threshold=1.5),
        "whale_flow": OnChainRegimeWhaleFlowStrategy(whale_threshold=1e6),
        "sentiment_momentum": SentimentMomentumCompositeStrategy(trend_period=20, short_period=5),
        "vol_regime_switch": VolRegimeSwitchStrategy(atr_period=14),
    }
    
    strategy = strategies.get(strategy_name)
    if not strategy:
        return None
    
    # Run backtest
    from coinbase.src.backtest.new_strategies import OHLCVBar, backtest_strategy
    metrics = backtest_strategy(strategy, bars[:500])  # First 500 days for initial test
    
    if not metrics.get('win_rate'):
        return None
    
    result = {
        'symbol': symbol,
        'strategy_name': strategy_name,
        'period': f"{bars[0].timestamp[:10]} to {bars[-1].timestamp[:10]}",
        **metrics,
    }
    
    # Validate against target (>60% win rate)
    strong = metrics.get('win_rate', 0) >= 60
    
    return result


def main():
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "historical_data"
    simulate = "--simulate-data" in sys.argv
    
    print(f"\n{'='*70}")
    print(f"Running Backtest Framework against {data_dir}")
    print("=" * 70 + "\n")
    
    # Load or generate data
    if not simulate and Path(data_dir).exists():
        print(f"[OK] Loading existing data from {data_dir}...")
    else:
        print("[INFO] Generating mock data for backtesting...")
        bars = generate_mock_data(1500)  # ~4 years of daily data
    
    # Test all strategies against historical periods
    results = []
    strong_strats = []  # Collect strong performers for summary
    
    if not simulate:
        from coinbase.src.backtest.new_strategies import OHLCVBar
        
        csv_files = list(Path(data_dir).glob("*.csv"))
        
        for csv_file in csv_files[:3]:  # Test first 3 files
            try:
                with open(csv_file) as f:
                    reader = csv.DictReader(f)
                    bars = [OHLCVBar(
                        timestamp=row['timestamp'],
                        open=float(row.get('open', 0)),
                        high=float(row.get('high', 0)),
                        low=float(row.get('low', 0)),
                        close=float(row.get('close', 0)),
                        volume=float(row.get('volume', 0)),
                    ) for row in reader]
                
                _build_close_window(bars)
                print(f"[DATA] {csv_file.name}: {len(bars)} bars")
            except Exception as e:
                print(f"[WARN] Failed loading {csv_file.name}: {e}")
    
    # Test each strategy against available data
    strategies = [
        ("rsi_momentum", "Multi-Timeframe RSI Momentum"),
        ("bollinger_squeeze", "Bollinger Squeeze + Breakout"),
        ("arb_microstructure", "Cross-Exchange Microstructure Arb"),
        ("regime_adaptive", "Regime-Aware Adaptive"),
        ("whale_flow", "On-Chain Regime Whale Flow"),
        ("sentiment_momentum", "Sentiment-Momentum Composite"),
        ("vol_regime_switch", "Volatility Regime Switching"),
    ]
    
    for strat_name, strat_display in strategies:
        try:
            result = run_backtest("BTC-USD", bars[:500], strat_name)
            if result and result.get('win_rate', 0) > 10:
                print(f"[OK] {strat_display} ({strat_name}):")
                m = result['metrics']
                print(f"    Win Rate: {m.win_rate:.1f}% | Trades: {m.total_trades}")
                print(f"    W/L: {m.winning_trades}/{m.losing_trades} | PfF: {m.profit_factor:.2f}")
                print(f"    Sharpe: {m.sharpe_ratio:.2f} | DD: {m.max_drawdown_pct*100:.1f}%")
                
                if m.win_rate >= 60 and m.sharpe_ratio > 1.5:
                    strong_strats.append(result)
                results.append((strat_name, strat_display, result))
        except Exception as e:
            print(f"[ERROR] {strat_display}: {e}")
    
    # Show strong performers
    if strong_strats:
        print("\n" + "=" * 70)
        print("PAPER TRADING CANDIDATES (>)60% win rate)")
        print("=" * 70)
        
        for s in strong_strats:
            m = s['metrics']
            print(f"\n{s['strategy_name']:40s}")
            print(f"    Win Rate: {m.win_rate:.1f}%")
            print(f"    Trades:   {m.total_trades}")
            print(f"    Sharpe:   {m.sharpe_ratio:.2f}")

if __name__ == "__main__":
    main()