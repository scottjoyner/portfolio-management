"""Standalone strategy tester - no external dependencies."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any
import random


def generate_realistic_btc(days: int = 1200) -> pd.DataFrame:
    """Generate realistic BTC price history with regime cycles."""
    np.random.seed(42)
    random.seed(42)
    
    base_price = 35000.0
    daily_vol = 0.03
    
    prices, dates, volumes = [], [], []
    prev_close = base_price * 0.75
    
    for i in range(days):
        dt = datetime(2021, 1, 1) + timedelta(days=i*2)
        
        cycle = np.sin(i / 60)
        if cycle > 0.5:
            daily_ret_mean, vol_mult = 0.025, 2.8
        elif cycle < -0.3:
            daily_ret_mean, vol_mult = -0.015, 2.2
        else:
            daily_ret_mean, vol_mult = 0.005, 1.8
        
        daily_return = random.gauss(daily_ret_mean, daily_vol * vol_mult)
        new_price = prev_close * (1 + daily_return)
        
        high = max(prev_close, new_price) * (1 + abs(daily_return) * 2.5)
        low = min(prev_close, new_price) * (1 - abs(daily_return) * 2.5)
        vol = prev_close * random.uniform(1e7, 1e9) / max(abs(daily_return), 0.001)
        
        prices.append(new_price)
        dates.append(dt.date())
        volumes.append(vol)
        prev_close = new_price
    
    df = pd.DataFrame({
        "date": dates,
        "open": [prev*0.98 for prev in prices],
        "high": [p*(1+0.02) for p in prices],
        "low": [p*0.97 for p in prices],
        "close": prices,
        "volume": volumes,
    })
    df.index = pd.to_datetime(df["date"])
    return df


class SignalWindow:
    """Signal window with RSI calculations."""
    
    def __init__(self):
        self.closes: List[float] = []
        
    def add(self, close: float):
        self.closes.append(close)
    
    @property
    def close_window(self) -> List[float]:
        return self.closes[-30:] if len(self.closes) >= 30 else self.closes[:]
    
    def calculate_rsi(self, period: int = 14) -> float:
        """Calculate RSI value."""
        if len(self.closes) < period + 1:
            return 50.0
        
        changes = [self.closes[i] - self.closes[i-1] for i in range(1, len(self.closes))]
        gains = [max(0, c) for c in changes]
        losses = [-min(0, c) for c in changes]
        
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        
        rs = (avg_gain / max(avg_loss, 1e-9)) if avg_loss > 0 else 100.0
        rsi = 100 - (100 / (1 + rs))
        return float(rsi)


class MultiTimeframeRSIStrategy:
    """Multi-Timeframe RSI Momentum Strategy."""
    
    def __init__(self):
        self.name = "MultiTimeframe RSI Momentum"
        
    def run(self, df: pd.DataFrame) -> Tuple[List[Dict], pd.DataFrame]:
        trades = []
        position: Optional[Dict] = None
        
        for ts in df.index:
            row = df.loc[ts]
            current_price = float(row["close"])
            
            window = SignalWindow()
            history = df.loc[:ts]
            if len(history) < 28:
                continue
            
            for c in history["close"]:
                window.add(float(c))
            
            # Check entry signal
            if position is None:
                rsi_short = window.calculate_rsi(period=5)
                rsi_long = window.calculate_rsi(period=28)
                
                if rsi_short > rsi_long and rsi_long > 50:
                    position = {
                        "side": "long",
                        "entry_price": current_price,
                        "size": 1.0 / current_price,  # $1 in BTC
                        "open_ts": ts,
                    }
            
            # Check exit signal  
            elif position["side"] == "long" and current_price <= position["entry_price"]:
                pnl_pct = (current_price - position["entry_price"]) / position["entry_price"]
                
                if current_price >= position["entry_price"] * 1.15:  # Target hit
                    reason = "target"
                else:
                    reason = "stop"
                
                trade = {
                    "strategy": self.name,
                    "side": position["side"],
                    "open_ts": position["open_ts"],
                    "close_ts": ts,
                    "entry_price": position["entry_price"],
                    "exit_price": current_price,
                    "size": position["size"],
                    "bars_held": max(1, (ts - position["open_ts"]).days),
                    "pnl_pct": pnl_pct,
                    "pnl_usd": pnl_pct * 1.0,  # $1 position
                    "reason": reason,
                }
                trades.append(trade)
                position = None
        
        return trades


def main():
    """Run RSI strategy on generated data."""
    
    print("Generating BTC price history...")
    df = generate_realistic_btc(1200)
    print(f"Loaded {len(df)} bars from {df.index[0]} to {df.index[-1]}")
    
    strategy = MultiTimeframeRSIStrategy()
    trades = strategy.run(df)  # Returns just the list
    
    # Summary statistics
    wins = sum(1 for t in trades if t["pnl_pct"] > 0)
    win_rate = len(trades) / max(1, len(trades)) * 100 if trades else 0
    
    print("\n" + "="*60)
    print("RESULTS SUMMARY")
    print("="*60)
    print(f"\n{strategy.name}:")
    print(f"  Trades: {len(trades)}")
    print(f"  Win Rate: {win_rate:.1f}%")
    print(f"  Total P&L: ${sum(t['pnl_usd'] for t in trades):,.0f}")


if __name__ == "__main__":
    main()
