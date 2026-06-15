#!/usr/bin/env python3
"""Multi-strategy crypto backtester with 4+ years of Coinbase market data."""

import subprocess, json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional


class MarketDataFetcher:
    """Fetches historical OHLCV candles from Coinbase CLI."""
    
    def fetch_candles(self, product_id: str, granularity: str = "1h", days_back: int = 90):
        cli_path = '/home/scott/.npm-global/bin/coinbase'
        
        end_dt = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        start_dt = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        result = subprocess.run(
            [cli_path, 'products', 'candles', product_id, 
             f'granularity=={granularity}', f'start=={start_dt}'],
            capture_output=True, text=True, timeout=60
        )
        
        if result.returncode != 0:
            return [], f"CLI error: {result.stderr[:200]}"
            
        data = json.loads(result.stdout)
        candles_raw = data.get('candles', []) if isinstance(data, dict) else data
        
        candles = []
        for c in candles_raw:
            try:
                ts_str = str(c.get('start', '0'))
                unix_ts = int(ts_str)  # Already seconds
                
                dt_obj = datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
                
                candles.append({
                    'time': dt_obj,
                    'ts': unix_ts,
                    'open': float(c.get('open', 0)),
                    'high': float(c.get('high', 0)),  
                    'low': float(c.get('low', 0)),
                    'close': float(c.get('close', 0)),
                    'volume': float(c.get('volume', 0))
                })
            except Exception:
                continue
                
        return candles, None


# --- Strategy Implementations ---

class MovingAverageCrossover:
    """Classic MA crossover: buy when short-term crosses above long-term."""
    
    def __init__(self, fast_window=50, slow_window=20):
        self.fast = fast_window  
        self.slow = slow_window
        self._name = "MA_Crossover"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < max(self.fast, self.slow):
            return []
            
        closes = [d['close'] for d in data]
        
        # Calculate short-term MA  
        sma_short = []
        for i in range(self.slow - 1, len(closes)):
            window_data = closes[i-(self.slow-1):i+1]
            avg = sum(window_data) / len(window_data)
            sma_short.append(avg)
            
        # Detect crossovers  
        signals = []
        if len(sma_short) >= self.fast:
            for i in range(self.fast - 1, len(sma_short)):
                prev_long = closes[i-(self.fast-1):i]
                avg_long = sum(prev_long) / len(prev_long)
                
                if sma_short[-1] > avg_long and (len(sma_short) >= self.fast + 2):
                    signals.append(('BUY', closes[i]))
                    
        return signals


class RSI:
    """Relative Strength Index strategy - buy when oversold (<30), sell when overbought (>70)."""
    
    def __init__(self, window=14, oversold=30, overbought=70):
        self.window = window  
        self.oversold = oversold
        self.overbought = overbought
        self._name = "RSI"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < 14:  # Need at least 14 for RSI calculation
            return []
            
        closes = [d['close'] for d in data]
        
        gains = []
        losses = []
        
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i-1]
            if delta > 0:
                gains.append(delta)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(delta))
        
        # Calculate RSI  
        avg_gain = sum(gains[:self.window]) / self.window
        avg_loss = sum(losses[:self.window]) / self.window
        
        if avg_loss == 0:
            rsi_values = [100.0] * len(closes)
        else:
            rs = avg_gain / avg_loss
            base_rsi = 100 - (100 / (1 + rs))
            rsi_values = [base_rsi] * len(closes[:self.window])
            
            for i in range(self.window, len(gains)):
                # Smoothed RSI calculation
                new_gain = gains[i-self.window+1] if i >= self.window else 0
                new_loss = losses[i-self.window+1] if i >= self.window else 0
                avg_gain = (avg_gain * (self.window - 1) + new_gain) / self.window
                avg_loss = (avg_loss * (self_window - 1) + new_loss) / self.window
                
            rsi_values.append(100 - (100 / (1 + rs)))
            
        # Generate signals from RSI values
        signals = []
        for i in range(self_window, len(rsi_values)):
            if rsi_values[i] < 30:  # Oversold
                signals.append(('BUY', closes[i]))
            elif rsi_values[i] > 70:  # Overbought  
                signals.append(('SELL', closes[i]))
                
        return signals


class BollingerBands:
    """Buy near lower band, sell near upper band."""
    
    def __init__(self, window=20, multiplier=2.0):
        self.window = window
        self.multiplier = multiplier
        self._name = "Bollinger_Bands"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < 20:
            return []
            
        closes = [d['close'] for d in data]
        
        signals = []
        for i in range(19, len(closes)):
            window_data = closes[i-19:i+1]
            mean = sum(window_data) / 20
            std_dev = (sum((x - mean)**2 for x in window_data) / 20) ** 0.5
            
            upper_band = mean + self.multiplier * std_dev
            lower_band = mean - self.multiplier * std_dev
            
            if closes[i] < lower_band:
                signals.append(('BUY', closes[i]))
            elif closes[i] > upper_band:
                signals.append(('SELL', closes[i]))
                
        return signals


class DonchianChannel:
    """Buy when price breaks above 20-day high, sell below 20-day low."""
    
    def __init__(self, window=20):
        self.window = window  
        self._name = "Donchian"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < 20:
            return []
            
        signals = []
        for i in range(19, len(data)):
            window_data = data[i-19:i+1]
            high_20 = max(d['high'] for d in window_data)
            low_20 = min(d['low'] for d in window_data)
            
            if data[i]['close'] > high_20:
                signals.append(('BUY', data[i]['close']))
            elif data[i]['close'] < low_20:
                signals.append(('SELL', data[i]['close']))
                
        return signals


class TrendFollowing:
    """Simple trend following - buy when price > 50-day MA, sell below."""
    
    def __init__(self, window=50):
        self.window = window  
        self._name = "Trend_Following"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < 50:
            return []
            
        closes = [d['close'] for d in data]
        
        # Calculate 50-day MA  
        sma_50 = []
        for i in range(49, len(closes)):
            window_data = closes[i-49:i+1]
            avg = sum(window_data) / 50
            sma_50.append(avg)
            
        signals = []
        if len(sma_50) >= 2:
            for i in range(1, len(sma_50)):
                # Buy when price crosses above MA (upward momentum)
                if closes[i+49] > sma_50[-i] and closes[i+49] > closes[i+48]:
                    signals.append(('BUY', closes[i+49]))
                    
                # Sell when price falls below MA  
                elif closes[i+49] < sma_50[-1-i] and closes[i+49] < closes[i+48]:
                    signals.append(('SELL', closes[i+49]))
        
        return signals


# --- Backtesting Engine ---

class Backtester:
    """Runs backtests with performance metrics."""
    
    @staticmethod  
    def run_backtest(product_id, strategy, days_back=365*4):
        fetcher = MarketDataFetcher()
        candles_raw, err = fetcher.fetch_candles(product_id, "1h", days_back)
        
        if err is not None and isinstance(err, str):
            return {'error': f"Failed to fetch data: {err}"}
            
        signals = strategy.generate_signals(candles_raw)
        
        closes = [c['close'] for c in candles_raw] if isinstance(candles_raw, list) and len(candles_raw) > 0 else []
        
        if len(closes) < 14:
            return {'error': 'Insufficient data'}
            
        start_price = closes[0]  
        end_price = closes[-1]
        
        # Calculate PnL from signals
        buy_prices = []
        sell_prices = []
        
        for signal, price in signals:
            if signal == 'BUY':
                buy_prices.append(price)
            else:
                sell_prices.append(price)
                
        trades_count = min(len(buy_prices), len(sell_prices))
        win_trades = 0
        
        for i in range(trades_count):
            pnl = (sell_prices[i] - buy_prices[i]) / buy_prices[i] * 100 if len(sell_prices) > i else 0.0
            if pnl > 0:
                win_trades += 1
                
        # Calculate key metrics
        total_return_pct = ((end_price - start_price) / start_price) * 100
        
        # Sharpe ratio approximation (annualized)
        returns_24h = [closes[i] / closes[i-1] - 1 for i in range(1, len(closes))]
        
        if len(returns_24h) >= 2:
            mean_return = sum(returns_24h) / len(returns_24h)
            std_dev = (sum((r - mean_return)**2 for r in returns_24h) / len(returns_24h)) ** 0.5
            sharpe = mean_return / std_dev * (365*24) ** 0.5 if std_dev > 0 else 0
        else:
            sharpe = 0
            
        # Drawdown calculation  
        peak = max(closes[:100]) if len(closes) >= 100 else max(closes[:5]) if closes else 0
        drawdowns = [(peak - p) / peak * 100 for p in closes] if closes and peak > 0 else [0.0]
        max_drawdown = max(drawdowns)
        
        # Buy-and-hold comparison
        bh_return = (end_price - start_price) / start_price * 100
        
        result = {
            'product': product_id,
            'strategy': strategy.name,
            'data_points': len(candles_raw),
            'start_date': candles_raw[0]['time'][:10] if isinstance(candles_raw, list) and len(candles_raw) > 0 else "N/A",
            'end_date': candles_raw[-1]['time'][:10] if isinstance(candles_raw, list) and len(candles_raw) > 0 else "N/A",
            'price_info': {
                'start_price': round(start_price, 2),
                'end_price': round(end_price, 2),
                'high': max(closes),
                'low': min(closes)
            },
            'trades_count': trades_count,
            'win_trades': win_trades,
            'win_rate_pct': (win_trades / trades_count * 100) if trades_count > 0 else 0.0,
            'total_return_pct': round(total_return_pct, 4),
            'sharpe_ratio': round(sharpe, 6),
            'max_drawdown_pct': round(max_drawdown, 4),
            'buy_hold_return_pct': round(bh_return, 4)
        }
        
        return result


# --- Benchmarking ---

def run_benchmark(product_id="BTC-USD"):
    """Run benchmark comparison of all strategies on a product."""
    
    print("=" * 70)
    print(f"Strategy Benchmark - {product_id}")
    print("=" * 70)
    
    # Initialize strategies  
    strategies = [
        MovingAverageCrossover(50, 20),
        RSI(14, oversold=30, overbought=70),
        BollingerBands(20, multiplier=2.0),
        DonchianChannel(20),
        TrendFollowing(50)
    ]
    
    results = []
    
    for strategy in strategies:
        print(f"\nTesting {strategy.name}...")
        
        # Run with 4+ years of data (halving cycle coverage)  
        result = Backtester.run_backtest(product_id, strategy, days_back=365*4)
        
        if 'error' not in result:
            print(f"  ✅ Data: {result['data_points']} candles")
            
            pi = result.get('price_info', {})
            sp = str(pi.get('start_price', 0)) if isinstance(pi, dict) else "0"
            ep = str(pi.get('end_price', 0)) if isinstance(pi, dict) else "0"
            
            print(f"  Price: ${sp} → ${ep}")
            print(f"  Returns: {result['total_return_pct']:.2f}% | Sharpe: {result['sharpe_ratio']}")
            print(f"  Trades: {result['trades_count']} (Win rate: {result['win_rate_pct']:.1f}%)")
            
            results.append(result)
        else:
            print(f"  ⚠️ Failed: {result.get('error', 'Unknown')}")
    
    # Print benchmark summary  
    if results:
        print("\n=== BENCHMARK SUMMARY ===")
        
        # Sort by Sharpe ratio (risk-adjusted returns)
        sorted_results = sorted(results, key=lambda x: x['sharpe_ratio'], reverse=True)
        
        for i, r in enumerate(sorted_results):
            pi = r.get('price_info', {})
            
            print(f"\n{r['strategy']:20s} | Trades: {str(r['trades_count']):<5} | Win%: {r['win_rate_pct']:.1f}%")
            print(f"{'':20s} | Return: {r['total_return_pct']:.3f}% | Sharpe: {r['sharpe_ratio']}")
            
        # Best strategy  
        if sorted_results:
            best = sorted_results[0]
            print(f"\n🏆 Winner: {best['strategy']} (Sharpe={best['sharpe_ratio']:.4f})")

if __name__ == "__main__":
    run_benchmark("BTC-USD")
    run_benchmark("ETH-USD")

EOF