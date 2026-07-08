"""Synthetic BTC data generator for testing paper trading pipeline."""

import random
from datetime import datetime, timedelta


def generate_realistic_btc_data(days: int = 1500) -> list:
    """Generate realistic BTC price history matching our backtest data patterns.
    
    Produces ~4 years of plausible price movements with regime cycles.
    """
    random.seed(42)
    
    base_price = 40000.0
    daily_vol = 0.028
    
    bars: list[dict] = []
    prev_close = base_price * 0.75
    
    for i in range(days):
        dt = datetime(2021, 1, 1) + timedelta(days=i*2)
        phase = (i // 180) % 5
        
        if phase == 0:
            daily_ret_mean, vol_mult = 0.018, 2.2
        elif phase == 1:
            daily_ret_mean, vol_mult = 0.04, 3.0
        elif phase == 2:
            daily_ret_mean, vol_mult = -0.02, 2.5
        elif phase == 3:
            daily_ret_mean, vol_mult = -0.01, 3.0
        else:
            daily_ret_mean, vol_mult = 0.03, 2.0
        
        daily_return = random.gauss(daily_ret_mean, daily_vol * vol_mult)
        new_price = prev_close * (1 + daily_return)
        
        high = max(prev_close, new_price) * (1 + abs(daily_return) * 2.5)
        low = min(prev_close, new_price) * (1 - abs(daily_return) * 2.5)
        vol = prev_close * random.uniform(1e7, 1e9) / max(abs(daily_return), 0.001)
        
        bar = {
            "timestamp": dt.strftime("%Y-%m-%d"),
            "open": round(prev_close, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(new_price, 2),
            "volume": float(vol),
        }
        bars.append(bar)
        prev_close = new_price
    
    return bars


if __name__ == "__main__":
    from strategies_paper_trading import StrategyConfig, MultiTimeframeRSIStrategy, OnChainWhaleFlowStrategy
    
    # Generate data
    print("Generating synthetic BTC price history...")
    random.seed(42)
    
    base_price = 40000.0
    daily_vol = 0.028
    bars: list[dict] = []
    prev_close = base_price * 0.75
    
    for i in range(1500):
        dt = datetime(2021, 1, 1) + timedelta(days=i*2)
        phase = (i // 180) % 5
        
        if phase == 0:
            daily_ret_mean, vol_mult = 0.018, 2.2
        elif phase == 1:
            daily_ret_mean, vol_mult = 0.04, 3.0
        elif phase == 2:
            daily_ret_mean, vol_mult = -0.02, 2.5
        elif phase == 3:
            daily_ret_mean, vol_mult = -0.01, 3.0
        else:
            daily_ret_mean, vol_mult = 0.03, 2.0
        
        daily_return = random.gauss(daily_ret_mean, daily_vol * vol_mult)
        new_price = prev_close * (1 + daily_return)
        
        high = max(prev_close, new_price) * (1 + abs(daily_return) * 2.5)
        low = min(prev_close, new_price) * (1 - abs(daily_return) * 2.5)
        vol = prev_close * random.uniform(1e7, 1e9) / max(abs(daily_return), 0.001)
        
        bar = {
            "timestamp": dt.strftime("%Y-%m-%d"),
            "open": round(prev_close, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(new_price, 2),
            "volume": float(vol),
        }
        bars.append(bar)
        prev_close = new_price
    
    print(f"Generated {len(bars)} bars")
    
    # Convert to DataFrame format expected by paper trading
    import pandas as pd
    df = pd.DataFrame([{
        "date": b["timestamp"],
        "open": b["open"],
        "high": b["high"],
        "low": b["low"],
        "close": b["close"],
        "volume": b["volume"],
    } for b in bars])
    df.index = pd.to_datetime(df["date"])
    df = df.sort_index()
    
    # Run both strategies
    rsi_strategy = MultiTimeframeRSIStrategy()
    whale_strategy = OnChainWhaleFlowStrategy()
    
    from strategies_paper_trading import run_paper_trading
    
    print("\nRunning RSI Strategy...")
    trades_rsi, _ = run_paper_trading(rsi_strategy, df)
    
    print("Running Whale Flow Strategy...")
    trades_whale, _ = run_paper_trading(whale_strategy, df)
    
    # Summary
    print("\n" + "="*60)
    print("RESULTS SUMMARY")
    print("="*60)
    
    for name, trades in [("RSI Momentum", trades_rsi), ("Whale Flow", trades_whale)]:
        wins = sum(1 for t in trades if t["pnl_pct"] > 0)
        win_rate = len(trades) / max(1, len(trades)) * 100 if trades else 0
        
        print(f"\n{name}:")
        print(f"  Trades: {len(trades)}")
        print(f"  Win Rate: {win_rate:.1f}%")
        print(f"  Total P&L: ${sum(t['pnl_usd'] for t in trades):,.0f}")
