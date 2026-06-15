"""Realistic synthetic BTC price data generator for backtesting.

Generates 4-6 years of plausible Bitcoin price history with proper regime cycles,
momentum patterns, and volatility clustering similar to actual crypto markets."""

import math
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class OHLCVBar:
    """Single candlestick bar."""
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_window: List[float] = None
    
    def __post_init__(self):
        if self.close_window is None:
            self.close_window = []


def generate_realistic_btc_data(
    days: int = 1800,
    seed: int = 42,
    start_date: str = "2020-01-01",
) -> List[OHLCVBar]:
    """Generate realistic BTC price history with regime cycles.
    
    This produces data that mimics actual crypto markets:
    - Strong bull runs with parabolic extensions
    - Deep bear market drawdowns (50-80%)
    - Mean-reverting consolidation phases
    - Volatility clustering
    
    Args:
        days: Number of trading days to generate
        seed: Random seed for reproducibility
        start_date: Starting date string
        
    Returns:
        List of OHLCVBar objects with proper close_window history.
    """
    import random
    
    random.seed(seed)
    
    # Base parameters - realistic BTC price levels and volatility
    base_price = 45000.0  # Mid-cycle reference price
    daily_vol = 0.025     # 2.5% baseline daily vol
    
    bars: List[OHLCVBar] = []
    prev_close = base_price * 0.8  # Start below mid-cycle
    
    # Regime cycle parameters - mimics ~18-24 month crypto cycles
    cycle_length = 700  # days per full cycle (bull + bear + consolidation)
    
    for i in range(days):
        dt = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=i * random.randint(1, 3))
        
        # Current regime phase within cycle
        phase = (i // 100) % 6
        
        # Regime-specific behavior
        if phase == 0:  # Strong bull accumulation - strong uptrend
            daily_ret_mean = 0.015
            daily_vol_mult = 2.0
        elif phase == 1:  # Parabolic mania - extreme momentum
            daily_ret_mean = 0.035
            daily_vol_mult = 3.5
        elif phase == 2:  # Distribution/top
            daily_ret_mean = -0.008
            daily_vol_mult = 2.5
        elif phase == 3:  # Bear market decline
            daily_ret_mean = -0.025
            daily_vol_mult = 3.0
        elif phase == 4:  # Capitulation/bottom
            daily_ret_mean = -0.018
            daily_vol_mult = 3.5
        else:  # Recovery/accumulation
            daily_ret_mean = 0.025
            daily_vol_mult = 1.8
        
        # Generate daily return with regime-appropriate distribution
        daily_return = random.gauss(daily_ret_mean, daily_vol * daily_vol_mult)
        
        # Apply mean reversion toward 200-day moving average (realistic feature)
        if len(bars) >= 200:
            ma_200 = sum(b.close for b in bars[-200:]) / 200
            mrt = math.log(prev_close / ma_200) * 0.05  # 5% reversion per day
            daily_return += mrt
        
        new_price = prev_close * (1 + daily_return)
        
        # Generate OHLC structure
        body = abs(new_price - prev_close)
        
        # Realistic wick generation (higher vol on extremes)
        high = max(prev_close, new_price) * (1 + abs(daily_return) * random.uniform(1.5, 3.0))
        low = min(prev_close, new_price) * (1 - abs(daily_return) * random.uniform(1.5, 3.0))
        
        # Volume: inversely proportional to price movement magnitude
        vol = prev_close * random.uniform(1e7, 1e9) / max(abs(daily_return), 0.001)
        
        # Build close_window from prior bars
        prev_closes = [b.close for b in bars[-30:]] if len(bars) >= 30 else [b.close for b in bars]
        cw = prev_closes + [prev_close]
        
        bar = OHLCVBar(
            timestamp=dt.strftime("%Y-%m-%d"),
            open=round(prev_close, 2),
            high=round(high, 2),
            low=round(low, 2),
            close=round(new_price, 2),
            volume=float(vol),
            close_window=cw[-30:] if len(cw) >= 30 else cw,
        )
        bars.append(bar)
        prev_close = new_price
    
    return bars


def save_btc_data(bars: List[OHLCVBar], path: str):
    """Save BTC data to CSV file."""
    import csv
    import os
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, 
            fieldnames=["date", "open", "high", "low", "close", "volume"]
        )
        writer.writeheader()
        for bar in bars:
            writer.writerow({
                "date": bar.timestamp,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
            })


def main():
    """Generate and save realistic BTC price history."""
    print("Generating 5 years of realistic BTC price data...")
    
    days = 1825  # ~5 years
    bars = generate_realistic_btc_data(days)
    
    output_path = "data/historical/BTC-USD_synthetic_5yrs.csv"
    save_btc_data(bars, output_path)
    
    print(f"✓ Generated {len(bars)} bars ({(days/365):.1f} years)")
    print(f"  Price range: ${min(b.close for b in bars):,.0f} - ${max(b.close for b in bars):,.0f}")
    print(f"  Daily vol (std): {math.sqrt(sum((float((b.close - sum(a.close for a in bars[-31:])/31)**2 for b in bars[-31:]))/30) / 30)*100:.1f}%")
    print(f"  Saved to: {output_path}")


if __name__ == "__main__":
    main()
