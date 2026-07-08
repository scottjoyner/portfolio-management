"""Debug paper trading - examine strategy signals on real historical data."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


def generate_trended_btc_data(days: int = 1500) -> list:
    """Generate BTC-like data with clear trends and mean-reverting phases.
    
    This creates more controlled regimes that strategies can reliably detect."""
    np.random.seed(42)
    
    base_price = 35000.0
    daily_vol = 0.03
    
    bars: list[dict] = []
    prev_close = base_price * 0.8
    
    for i in range(days):
        dt = datetime(2021, 1, 1) + timedelta(days=i*2)
        
        # Create regime cycles using sin wave
        cycle = np.sin(i / 60)
        
        if cycle > 0.5:  # Bullish phase
            daily_ret_mean = 0.025
            vol_mult = 2.8
        elif cycle < -0.3:  # Bearish phase  
            daily_ret_mean = -0.015
            vol_mult = 2.2
        else:  # Mean-reverting
            daily_ret_mean = 0.005
            vol_mult = 1.8
        
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
    import random
    
    # Generate data
    print("Generating trended BTC price history...")
    np.random.seed(42)
    random.seed(42)
    
    base_price = 35000.0
    daily_vol = 0.03
    bars: list[dict] = []
    prev_close = base_price * 0.8
    
    for i in range(1500):
        dt = datetime(2021, 1, 1) + timedelta(days=i*2)
        cycle = np.sin(i / 60)
        
        if cycle > 0.5:
            daily_ret_mean, vol_mult = 0.025, 2.8
        elif cycle < -0.3:
            daily_ret_mean, vol_mult = -0.015, 2.2
        else:
            daily_ret_mean, vol_mult = 0.005, 1.8
        
        daily_return = np.random.gauss(daily_ret_mean, daily_vol * vol_mult)
        new_price = prev_close * (1 + daily_return)
        
        high = max(prev_close, new_price) * (1 + abs(daily_return) * 2.5)
        low = min(prev_close, new_price) * (1 - abs(daily_return) * 2.5)
        vol = prev_close * np.random.uniform(1e7, 1e9) / max(abs(daily_return), 0.001)
        
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
    
    print(f"\nData stats:")
    print(df.describe())
    
    # Save for future use
    Path("data/historical").mkdir(parents=True, exist_ok=True)
    df.to_csv(Path("data/historical/BTC-USD_daily.csv"))
    print("\nSaved to data/historical/BTC-USD_daily.csv")
