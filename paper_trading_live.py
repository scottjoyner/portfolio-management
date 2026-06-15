"""Enhanced paper trading monitor - full signal reasoning with entry/exit levels."""

from __future__ import annotations
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class LivePaperTrader:
    """Live paper trading monitor with comprehensive signal reasoning."""
    
    ASSETS = ["BTC-USD", "MON-USD", "ATOM-USD", "AVAX-USD", "XRP-USD", "SOL-USD"]
    
    def __init__(self):
        pass


def _fetch_coingecko(coin_id: str, currency='usd', days=90) -> dict | None:
    """Fetch historical market chart data from CoinGecko."""
    try:
        import requests
        url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
        params = {'vs_currency': currency, 'days': days}
        resp = requests.get(url, params=params)
        
        if not (200 <= resp.status_code < 300):
            return None
        
        return resp.json()
    except Exception as e:
        print(f"CoinGecko error: {str(e)[:60]}")
        return None


def _coingecko_id(asset: str) -> str | None:
    """Map asset ticker to CoinGecko coin ID."""
    mapping = {
        "BTC-USD": "bitcoin",
        "MON-USD": "monad", 
        "ATOM-USD": "cosmos",
        "AVAX-USD": "avalanche-2",
        "XRP-USD": "ripple",
        "SOL-USD": "solana",
    }
    return mapping.get(asset)


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI on a price series."""
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100 - (100 / (1 + rs))


def analyze_price_history(prices: list[float]) -> dict | None:
    """Analyze price history for entry/exit signals."""
    if len(prices) < 30:
        return {"error": "Insufficient data"}
    
    df = pd.Series(prices, index=pd.date_range('2024-01-01', periods=len(prices), freq='h'))
    
    rsi_short = float(_rsi(df, period=5).iloc[-1])
    rsi_long = float(_rsi(df, period=28).iloc[-1])
    
    current_price = df.iloc[-1] if isinstance(df.iloc[-1], (int, float)) else 0
    
    # Key metrics
    high_7d = max(prices[-(7*6):]) if len(prices) >= 42 else None
    low_7d = min(prices[-(7*6):]) if len(prices) >= 42 else None
    high_30d = max(prices[-(30*6):]) if len(prices) >= 180 else None
    low_30d = min(prices[-(30*6):]) if len(prices) >= 180 else None
    
    recent_prices = df.iloc[-(7*6):] if len(df) >= 42 else df
    daily_returns = recent_prices.pct_change().dropna()
    vol_7d = float(daily_returns.std() * np.sqrt(365)) * 100
    
    max_dd_scalar = None
    if len(prices) >= 540:
        prices_array = pd.Series(prices[-540:])
        rolling_high = prices_array.rolling(20).max()
        # Use .iloc to extract scalar values from Series
        high_max = float(rolling_high.iloc[-1]) if not rolling_high.empty else 0
        price_min = float(prices_array.min())
        max_dd_scalar = float((high_max - price_min) / high_max * 100) if high_max > 0 else None
    
    return {
        "current_price": current_price,
        "rsi_short": rsi_short,
        "rsi_long": rsi_long,
        "high_7d": high_7d,
        "low_7d": low_7d,
        "volatility_pct": vol_7d,
        "max_drawdown": max_dd_scalar if isinstance(max_dd_scalar, (int, float)) else None,
    }


def generate_signal(prices: list[float], asset_name: str) -> dict | None:
    """Generate a detailed trade signal with reasoning."""
    analysis = analyze_price_history(prices)
    
    if not isinstance(analysis, dict) or "error" in analysis.get('status', ''):
        return {"asset": asset_name, "status": "INSUFFICIENT_DATA"}
    
    rsi_short = analysis["rsi_short"]
    rsi_long = analysis["rsi_long"]
    current = analysis["current_price"]
    high_7d = analysis.get("high_7d", 0) or 0
    low_7d = analysis.get("low_7d", 0) or 0
    
    if rsi_short > rsi_long and rsi_long > 50:
        recent_prices = pd.Series(prices[-(7*6):]) if len(prices) >= 42 else pd.Series(prices[-14:])
        sma_short = float(recent_prices.mean())
        
        volatility_factor = min(float(recent_prices.std()), float(recent_prices.std() * 2))
        take_profit_1 = current + (volatility_factor * 0.5)
        take_profit_2 = current + (volatility_factor * 1.5)
        stop_loss = max(low_7d, current - (volatility_factor * 0.8))
        
        return {
            "asset": asset_name,
            "signal_type": "BUY",
            "reasoning": f"RSI momentum confirmed: short-term RSI({rsi_short:.1f}) > long-term RSI({rsi_long:.1f}), both above 50 threshold",
            "entry_price": round(current, 4),
            "target_1": round(take_profit_1, 4),
            "target_2": round(take_profit_2, 4),
            "stop_loss": round(stop_loss, 4),
            "risk_reward_ratio": f"~{abs((current - stop_loss) / (take_profit_1 - current)):.2f}R",
            "7d_high": round(high_7d, 4) if high_7d else None,
            "7d_low": round(low_7d, 4) if low_7d else None,
            "volatility_pct": round(analysis["volatility_pct"], 2),
            "max_drawdown_pct": round(analysis.get("max_drawdown", None), 2) if analysis.get("max_drawdown") is not None else None,
        }
    
    elif rsi_short < rsi_long and rsi_short < 30:
        return {
            "asset": asset_name,
            "signal_type": "WAIT",
            "reasoning": f"Oversold condition detected (RSI(5)={rsi_short:.1f}), potential bounce setup forming",
            "entry_price": round(current, 4),
        }
    
    else:
        return {
            "asset": asset_name,
            "signal_type": "HOLD",
            "reasoning": f"RSI(5)={rsi_short:.1f}, RSI(28)={rsi_long:.1f} - No momentum alignment for new position",
        }


def print_detailed_signals(trader: LivePaperTrader) -> None:
    """Print comprehensive signal output with full reasoning."""
    print("\n" + "="*76)
    print("PAPER TRADING SIGNALS - DETAILED REASONING")
    print("="*76)
    
    for asset in trader.ASSETS:
        coin_id = _coingecko_id(asset)
        
        if not coin_id:
            print(f"\n{asset}: SKIPPED (no mapping)")
            continue
        
        data = _fetch_coingecko(coin_id, currency='usd', days=90)
        
        if not data or 'prices' not in data:
            print(f"\n{asset}: NO DATA AVAILABLE")
            continue
        
        prices_list = [p[1] for p in data.get('prices', [])]
        signal = generate_signal(prices_list, asset)
        
        if not signal or "error" in signal:
            print(f"\n{asset}: INSUFFICIENT DATA")
            continue
        
        signal_type = signal["signal_type"]
        status_emoji = {"BUY": "[BUY]", "HOLD": "[HOLD]", "WAIT": "[WAIT]"}[signal_type]
        
        print(f"\n{asset}: {status_emoji}")
        print("-" * 60)
        print(f"Reasoning:    {signal['reasoning']}")
        
        if signal["signal_type"] == "BUY":
            entry = signal.get("entry_price", 0)
            tp1 = signal.get("target_1", 0)
            tp2 = signal.get("target_2", 0)
            sl = signal.get("stop_loss", 0)
            
            print(f"Entry Price:  ${entry:.4f}")
            print(f"Take Profit:  ${tp1:.4f} (TP1), ${tp2:.4f} (TP2)")
            print(f"Stop Loss:    ${sl:.4f}")
            print(f"Risk/Reward:  {signal.get('risk_reward_ratio', 'N/A')}")
            
            if signal.get("7d_high"):
                print(f"Historical Context:")
                print(f"  • 7D High:  ${signal['7d_high']:.4f}")
                print(f"  • 7D Low:   ${signal['7d_low']:.4f}")
                print(f"  • Volatility: {signal['volatility_pct']}%")
                
            if signal.get("max_drawdown_pct"):
                print(f"Max Drawdown Risk: {signal['max_drawdown_pct']}%")
            
        elif signal["signal_type"] == "WAIT":
            print(f"Entry Price: ${signal['entry_price']:.4f}")
            print(f"Note: Monitor for oversold RSI < 30 + bullish reversal patterns")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Live Paper Trading Monitor - Detailed Signals")
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--once", action="store_true")
    
    args = parser.parse_args()
    
    trader = LivePaperTrader()
    
    while True:
        print_detailed_signals(trader)
        
        if args.once:
            break
        
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
