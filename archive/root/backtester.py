#!/usr/bin/env python3
"""Multi-strategy crypto backtester with 4+ years of Coinbase market data."""

import subprocess, json
import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Tuple, Optional
import os


class MarketDataFetcher:
    """Fetches historical OHLCV candles from Coinbase CLI."""
    
    def fetch_candles(self, product_id: str, granularity: str = "1h", days_back: int = 90):
        cli_path = os.environ.get('COINBASE_CLI_PATH', 'coinbase')
        
        end_dt = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        start_dt = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        result = subprocess.run(
            [cli_path, 'products', 'candles', product_id, 
             f'granularity=={granularity}', f'start=={start_dt}'],
            capture_output=True, text=True, timeout=60
        )
        
        if result.returncode != 0:
            try:
                from data.fetch_multi_source import MultiSourceDataFetcher
                fetcher = MultiSourceDataFetcher()
                gran = 'hour' if granularity.endswith('h') else 'day'
                candles = fetcher.fetch_coinbase(
                    product_id,
                    granularity=gran,
                    start_date=datetime.now(timezone.utc) - timedelta(days=days_back),
                    end_date=datetime.now(timezone.utc),
                )
                if candles:
                    normalized = []
                    for c in candles:
                        normalized.append({
                            'time': c.get('time') or '',
                            'ts': int(c.get('ts', 0) or 0),
                            'open': float(c.get('open', 0)),
                            'high': float(c.get('high', 0)),
                            'low': float(c.get('low', 0)),
                            'close': float(c.get('close', 0)),
                            'volume': float(c.get('volume', 0)),
                        })
                    return normalized, None
            except Exception as exc:
                return [], f"CLI error: {result.stderr[:200]} | fallback error: {exc}"
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
    
    def fetch_all_btc_pairs(self, granularity: str = "1h", days_back: int = 90):
        """Fetch data for all BTC market pairs."""
        # Common BTC trading pairs
        btc_pairs = [
            "BTC-USD",      # Bitcoin/USD
            "BTC-ETH",      # Bitcoin/Ethereum
            "BTC-SOL",      # Bitcoin/Solana
            "BTC-DOGE",     # Bitcoin/Dogecoin
            "BTC-XRP",      # Bitcoin/XRP
            "BTC-BTC",      # Bitcoin/BTC (if exists)
            "BTC-ADA",      # Bitcoin/Cardano
            "BTC-DOT",      # Bitcoin/Polkadot
            "BTC-MATIC",    # Bitcoin/Polygon
            "BTC-SHIB",     # Bitcoin/Shiba Inu
            "BTC-AVAX",     # Bitcoin/Avalanche
            "BTC-UNI",      # Bitcoin/Uniswap
            "BTC-SNX",      # Bitcoin/Synthetix
            "BTC-YFI",      # Bitcoin/YFI
            "BTC-AAVE",     # Bitcoin/AAVE
            "BTC-MKR",      # Bitcoin/Maker
            "BTC-COMP",     # Bitcoin/Compound
            "BTC-LINK",     # Bitcoin/Chainlink
            "BTC-BAT",      # Bitcoin/Basic Attention Token
            "BTC-ZRX",      # Bitcoin/0x
        ]
        
        all_data = {}
        errors = []
        
        for pair in btc_pairs:
            print(f"Fetching data for {pair}...")
            candles, err = self.fetch_candles(pair, granularity, days_back)
            
            if err is None and candles:
                all_data[pair] = candles
                print(f"  ✅ Successfully fetched {len(candles)} candles for {pair}")
            else:
                errors.append(f"{pair}: {err}")
                print(f"  ⚠️ Failed to fetch data for {pair}: {err}")
        
        return all_data, errors


# --- Strategy Implementations ---

class MovingAverageCrossover:
    """Classic MA crossover: buy when short-term crosses above long-term."""
    
    def __init__(self, fast_window=20, slow_window=50):
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

        sma_fast = []
        for i in range(self.fast - 1, len(closes)):
            window = closes[i - self.fast + 1:i + 1]
            sma_fast.append(sum(window) / self.fast)

        sma_slow = []
        for i in range(self.slow - 1, len(closes)):
            window = closes[i - self.slow + 1:i + 1]
            sma_slow.append(sum(window) / self.slow)

        signals = []
        for i in range(1, min(len(sma_fast), len(sma_slow))):
            if sma_fast[i - 1] <= sma_slow[i - 1] and sma_fast[i] > sma_slow[i]:
                signals.append(('BUY', closes[i + self.slow - 1]))
            elif sma_fast[i - 1] >= sma_slow[i - 1] and sma_fast[i] < sma_slow[i]:
                signals.append(('SELL', closes[i + self.slow - 1]))

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

        # Compute RSI for each index using Wilder's smoothed method
        rsi_values = []
        avg_gain = 0.0
        avg_loss = 0.0

        for i in range(1, len(closes)):
            delta = closes[i] - closes[i - 1]
            gain = max(delta, 0)
            loss = max(-delta, 0)

            if i < self.window:
                avg_gain += gain
                avg_loss += loss
                if i == self.window - 1:
                    avg_gain /= self.window
                    avg_loss /= self.window
                    if avg_loss == 0:
                        rsi_values.append(100.0)
                    else:
                        rsi_values.append(100 - (100 / (1 + avg_gain / avg_loss)))
                else:
                    rsi_values.append(50.0)  # neutral before we have enough data
            else:
                avg_gain = (avg_gain * (self.window - 1) + gain) / self.window
                avg_loss = (avg_loss * (self.window - 1) + loss) / self.window
                if avg_loss == 0:
                    rsi_values.append(100.0)
                else:
                    rsi_values.append(100 - (100 / (1 + avg_gain / avg_loss)))

        # Generate signals from RSI values
        signals = []
        for i in range(self.window, len(rsi_values)):
            if rsi_values[i] < self.oversold:  # Oversold
                signals.append(('BUY', closes[i]))
            elif rsi_values[i] > self.overbought:  # Overbought
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

        # Calculate 50-day SMA for every valid index
        sma_50 = []
        for i in range(49, len(closes)):
            window_data = closes[i - 49:i + 1]
            sma_50.append(sum(window_data) / 50)

        signals = []
        for i in range(1, len(sma_50)):
            close_idx = i + 49  # corresponding index in closes
            prev_close = closes[close_idx - 1]
            curr_close = closes[close_idx]
            curr_sma = sma_50[i]
            prev_sma = sma_50[i - 1]

            # Buy when price crosses above SMA with upward momentum
            if curr_close > curr_sma and curr_close > prev_close:
                signals.append(('BUY', curr_close))

            # Sell when price crosses below SMA with downward momentum
            elif curr_close < curr_sma and curr_close < prev_close:
                signals.append(('SELL', curr_close))

        return signals


class BTCVolatilityStacking:
    """BTC volatility stacking strategy - capitalize on crypto volatility with stacking and tax loss harvesting."""
    
    def __init__(self, volatility_window=20, atr_window=14, rsi_window=14, stop_loss_pct=0.02, take_profit_pct=0.04):
        self.volatility_window = volatility_window
        self.atr_window = atr_window
        self.rsi_window = rsi_window
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct
        self._name = "BTC_Volatility_Stacking"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < max(self.volatility_window, self.atr_window, self.rsi_window):
            return []
            
        closes = [d['close'] for d in data]
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        
        signals = []
        
        # Calculate ATR for volatility-based position sizing
        atr_values = []
        for i in range(self.atr_window - 1, len(data)):
            window_data = data[i - self.atr_window + 1:i + 1]
            tr_values = []
            for j in range(1, len(window_data)):
                tr = max(
                    window_data[j]['high'] - window_data[j]['low'],
                    abs(window_data[j]['high'] - window_data[j - 1]['close']),
                    abs(window_data[j]['low'] - window_data[j - 1]['close'])
                )
                tr_values.append(tr)
            atr = sum(tr_values) / len(tr_values)
            atr_values.append(atr)
        
        # Calculate RSI for oversold/overbought signals
        rsi_values = []
        for i in range(self.rsi_window - 1, len(closes)):
            window_closes = closes[i - self.rsi_window + 1:i + 1]
            
            gains = []
            losses = []
            for j in range(1, len(window_closes)):
                delta = window_closes[j] - window_closes[j - 1]
                if delta > 0:
                    gains.append(delta)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(delta))
            
            avg_gain = sum(gains) / len(gains)
            avg_loss = sum(losses) / len(losses)
            
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            rsi_values.append(rsi)
        
        # Calculate volatility bands
        vol_bands = []
        for i in range(self.volatility_window - 1, len(closes)):
            window_closes = closes[i - self.volatility_window + 1:i + 1]
            mean_price = sum(window_closes) / len(window_closes)
            variance = sum((x - mean_price) ** 2 for x in window_closes) / len(window_closes)
            std_dev = variance ** 0.5
            
            upper_band = mean_price + 2 * std_dev
            lower_band = mean_price - 2 * std_dev
            vol_bands.append((upper_band, lower_band))
        
        # Generate signals based on multiple factors
        for i in range(self.volatility_window - 1, len(data)):
            current_price = closes[i]
            atr = atr_values[i - (self.atr_window - 1)]
            rsi = rsi_values[i - (self.rsi_window - 1)]
            upper_band, lower_band = vol_bands[i - (self.volatility_window - 1)]
            
            # Calculate dynamic stop loss and take profit based on ATR
            dynamic_stop_loss = current_price - (self.stop_loss_pct * atr)
            dynamic_take_profit = current_price + (self.take_profit_pct * atr)
            
            # Signal 1: Volatility mean reversion with RSI confirmation
            if current_price <= lower_band and rsi < 30:
                signals.append(('BUY', current_price))
            
            # Signal 2: Tax loss harvesting - sell when price drops below previous entry with significant loss
            elif current_price < dynamic_stop_loss:
                signals.append(('SELL', current_price))
            
            # Signal 3: Stacking - accumulate on small pullbacks during uptrend
            elif current_price > upper_band * 0.98 and rsi > 50:
                signals.append(('BUY', current_price))
            
            # Signal 4: Take profit - sell when reaching dynamic take profit
            elif current_price >= dynamic_take_profit and rsi > 70:
                signals.append(('SELL', current_price))
        
        return signals


class BTCVolatilityBreakout:
    """BTC volatility breakout strategy - trade breakouts with volatility targeting."""
    
    def __init__(self, breakout_window=20, volatility_window=10, atr_window=14, position_size_pct=0.02):
        self.breakout_window = breakout_window
        self.volatility_window = volatility_window
        self.atr_window = atr_window
        self.position_size_pct = position_size_pct
        self._name = "BTC_Volatility_Breakout"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < max(self.breakout_window, self.volatility_window, self.atr_window):
            return []
            
        closes = [d['close'] for d in data]
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        
        signals = []
        
        # Calculate ATR for position sizing and stop loss
        atr_values = []
        for i in range(self.atr_window - 1, len(data)):
            window_data = data[i - self.atr_window + 1:i + 1]
            tr_values = []
            for j in range(1, len(window_data)):
                tr = max(
                    window_data[j]['high'] - window_data[j]['low'],
                    abs(window_data[j]['high'] - window_data[j - 1]['close']),
                    abs(window_data[j]['low'] - window_data[j - 1]['close'])
                )
                tr_values.append(tr)
            atr = sum(tr_values) / len(tr_values)
            atr_values.append(atr)
        
        # Calculate volatility bands
        vol_bands = []
        for i in range(self.volatility_window - 1, len(closes)):
            window_closes = closes[i - self.volatility_window + 1:i + 1]
            mean_price = sum(window_closes) / len(window_closes)
            variance = sum((x - mean_price) ** 2 for x in window_closes) / len(window_closes)
            std_dev = variance ** 0.5
            
            upper_band = mean_price + 2 * std_dev
            lower_band = mean_price - 2 * std_dev
            vol_bands.append((upper_band, lower_band))
        
        # Calculate breakout levels
        breakout_levels = []
        for i in range(self.breakout_window - 1, len(data)):
            window_data = data[i - self.breakout_window + 1:i + 1]
            prev_high = max(d['high'] for d in window_data[:-1])
            prev_low = min(d['low'] for d in window_data[:-1])
            current_close = window_data[-1]['close']
            
            breakout_levels.append((prev_high, prev_low, current_close))
        
        # Generate signals based on volatility breakouts
        for i in range(self.breakout_window - 1, len(data)):
            current_price = closes[i]
            atr = atr_values[i - (self.atr_window - 1)]
            upper_band, lower_band = vol_bands[i - (self.volatility_window - 1)]
            prev_high, prev_low, prev_close = breakout_levels[i - (self.breakout_window - 1)]
            
            # Signal 1: Volatility breakout - buy when price breaks above upper band
            if current_price > upper_band:
                signals.append(('BUY', current_price))
            
            # Signal 2: Volatility breakdown - sell when price breaks below lower band
            elif current_price < lower_band:
                signals.append(('SELL', current_price))
            
            # Signal 3: Consolidation exit - sell when price reverts to mean
            elif abs(current_price - (upper_band + lower_band) / 2) < atr * 0.5:
                signals.append(('SELL', current_price))
            
            # Signal 4: Momentum continuation - buy on strong breakout with high volume
            elif current_price > prev_high * 1.02 and data[i]['volume'] > data[i - 1]['volume'] * 1.5:
                signals.append(('BUY', current_price))
        
        return signals


class BTCVolatilityMeanReversion:
    """BTC volatility mean reversion strategy - trade mean reversion with volatility targeting."""
    
    def __init__(self, volatility_window=30, zscore_window=20, entry_zscore=2.0, exit_zscore=0.5, atr_window=14):
        self.volatility_window = volatility_window
        self.zscore_window = zscore_window
        self.entry_zscore = entry_zscore
        self.exit_zscore = exit_zscore
        self.atr_window = atr_window
        self._name = "BTC_Volatility_Mean_Reversion"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < max(self.volatility_window, self.zscore_window, self.atr_window):
            return []
            
        closes = [d['close'] for d in data]
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        
        signals = []
        
        # Calculate ATR for stop loss
        atr_values = []
        for i in range(self.atr_window - 1, len(data)):
            window_data = data[i - self.atr_window + 1:i + 1]
            tr_values = []
            for j in range(1, len(window_data)):
                tr = max(
                    window_data[j]['high'] - window_data[j]['low'],
                    abs(window_data[j]['high'] - window_data[j - 1]['close']),
                    abs(window_data[j]['low'] - window_data[j - 1]['close'])
                )
                tr_values.append(tr)
            atr = sum(tr_values) / len(tr_values)
            atr_values.append(atr)
        
        # Calculate rolling volatility
        vol_values = []
        for i in range(self.volatility_window - 1, len(closes)):
            window_closes = closes[i - self.volatility_window + 1:i + 1]
            mean_price = sum(window_closes) / len(window_closes)
            variance = sum((x - mean_price) ** 2 for x in window_closes) / len(window_closes)
            volatility = variance ** 0.5
            vol_values.append(volatility)
        
        # Calculate z-scores
        z_scores = []
        for i in range(self.zscore_window - 1, len(closes)):
            window_closes = closes[i - self.zscore_window + 1:i + 1]
            mean_price = sum(window_closes) / len(window_closes)
            variance = sum((x - mean_price) ** 2 for x in window_closes) / len(window_closes)
            std_dev = variance ** 0.5
            
            if std_dev == 0:
                z_score = 0.0
            else:
                z_score = (closes[i] - mean_price) / std_dev
            
            z_scores.append(z_score)
        
        # Generate signals based on z-score and volatility
        for i in range(self.zscore_window - 1, len(data)):
            current_price = closes[i]
            atr = atr_values[i - (self.atr_window - 1)]
            volatility = vol_values[i - (self.volatility_window - 1)]
            z_score = z_scores[i - (self.zscore_window - 1)]

            # Mean reversion entry - buy when z-score is oversold
            if z_score < -self.entry_zscore:
                signals.append(('BUY', current_price))

            # Mean reversion exit - sell when z-score reverts
            if z_score > -self.exit_zscore and z_score < self.exit_zscore:
                signals.append(('SELL', current_price))

            # Volatility-based stop loss (current vs prior close entry)
            if i > 0 and current_price < closes[i - 1] - (2 * atr):
                signals.append(('SELL', current_price))

            # Volatility-based take profit
            if i > 0 and current_price > closes[i - 1] + (2 * atr):
                signals.append(('SELL', current_price))
        
        return signals


class BTCVolatilityMomentum:
    """BTC volatility momentum strategy - combine volatility with momentum for enhanced returns."""
    
    def __init__(self, volatility_window=20, momentum_window=10, atr_window=14, rsi_window=14, stop_loss_pct=0.02):
        self.volatility_window = volatility_window
        self.momentum_window = momentum_window
        self.atr_window = atr_window
        self.rsi_window = rsi_window
        self.stop_loss_pct = stop_loss_pct
        self._name = "BTC_Volatility_Momentum"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < max(self.volatility_window, self.momentum_window, self.atr_window, self.rsi_window):
            return []
            
        closes = [d['close'] for d in data]
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        
        signals = []
        
        # Calculate ATR for position sizing and stop loss
        atr_values = []
        for i in range(self.atr_window - 1, len(data)):
            window_data = data[i - self.atr_window + 1:i + 1]
            tr_values = []
            for j in range(1, len(window_data)):
                tr = max(
                    window_data[j]['high'] - window_data[j]['low'],
                    abs(window_data[j]['high'] - window_data[j - 1]['close']),
                    abs(window_data[j]['low'] - window_data[j - 1]['close'])
                )
                tr_values.append(tr)
            atr = sum(tr_values) / len(tr_values)
            atr_values.append(atr)
        
        # Calculate momentum
        momentum_values = []
        for i in range(self.momentum_window - 1, len(closes)):
            window_closes = closes[i - self.momentum_window + 1:i + 1]
            momentum = (window_closes[-1] - window_closes[0]) / window_closes[0] * 100
            momentum_values.append(momentum)
        
        # Calculate RSI
        rsi_values = []
        for i in range(self.rsi_window - 1, len(closes)):
            window_closes = closes[i - self.rsi_window + 1:i + 1]
            
            gains = []
            losses = []
            for j in range(1, len(window_closes)):
                delta = window_closes[j] - window_closes[j - 1]
                if delta > 0:
                    gains.append(delta)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(delta))
            
            avg_gain = sum(gains) / len(gains)
            avg_loss = sum(losses) / len(losses)
            
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            rsi_values.append(rsi)
        
        # Calculate volatility bands
        vol_bands = []
        for i in range(self.volatility_window - 1, len(closes)):
            window_closes = closes[i - self.volatility_window + 1:i + 1]
            mean_price = sum(window_closes) / len(window_closes)
            variance = sum((x - mean_price) ** 2 for x in window_closes) / len(window_closes)
            std_dev = variance ** 0.5
            
            upper_band = mean_price + 1.5 * std_dev
            lower_band = mean_price - 1.5 * std_dev
            vol_bands.append((upper_band, lower_band))
        
        # Generate signals based on volatility and momentum
        for i in range(self.volatility_window - 1, len(data)):
            current_price = closes[i]
            atr = atr_values[i - (self.atr_window - 1)]
            momentum = momentum_values[i - (self.momentum_window - 1)]
            rsi = rsi_values[i - (self.rsi_window - 1)]
            upper_band, lower_band = vol_bands[i - (self.volatility_window - 1)]
            
            # Calculate dynamic stop loss
            dynamic_stop_loss = current_price - (self.stop_loss_pct * atr)
            
            # Signal 1: Volatility momentum - buy when momentum is strong and price is above lower band
            if momentum > 2.0 and current_price > lower_band and rsi > 50:
                signals.append(('BUY', current_price))
            
            # Signal 2: Volatility momentum exit - sell when momentum reverses
            elif momentum < -1.0 and rsi < 50:
                signals.append(('SELL', current_price))
            
            # Signal 3: Volatility stop loss
            elif current_price < dynamic_stop_loss:
                signals.append(('SELL', current_price))
            
            # Signal 4: Volatility take profit - sell when price reaches upper band
            elif current_price >= upper_band and rsi > 70:
                signals.append(('SELL', current_price))
        
        return signals


class CoinbaseMomentumStrategy:
    """RSI-based momentum strategy with adaptive timeframes for BTC-XXX pairs."""
    
    def __init__(self, initial_capital=10000.0):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.position_size = 0.0
        self.entry_price = None
        self.rsi_periods = [14, 28, 56]
        self.overbought_thresholds = {14: 70, 28: 75, 56: 80}
        self.oversold_thresholds = {14: 30, 28: 25, 56: 20}
        self._name = "Coinbase_Momentum"
        
    @property
    def name(self) -> str:
        return self._name
    
    @name.setter  
    def name(self, value):
        self._name = value
        
    def generate_signals(self, data: List[Dict]) -> List[Tuple[str, float]]:
        if len(data) < 100:
            return []
            
        closes = [d['close'] for d in data]
        
        signals = []
        
        # Calculate RSI for multiple periods
        for period in self.rsi_periods:
            if len(closes) < period + 1:
                continue
                
            rsi = self._compute_rsi(closes, period)
            if rsi is None or len(rsi) == 0:
                continue
                
            current_rsi = rsi[-1]
            threshold = self.oversold_thresholds.get(period, 30)
            
            # Generate buy signals when RSI is oversold
            if current_rsi < threshold:
                signals.append(('BUY', closes[-1]))
            
            # Generate sell signals when RSI is overbought
            elif current_rsi > self.overbought_thresholds.get(period, 70):
                signals.append(('SELL', closes[-1]))
        
        return signals
    
    def _compute_rsi(self, prices: List[float], period: int) -> Optional[List[float]]:
        """Compute RSI with proper handling of initial values."""
        if not prices or len(prices) < period + 1:
            return None
        
        rsi = []
        gains, losses = [], []
        
        # Initial gain/loss
        for i in range(1, period + 1):
            change = prices[i] - prices[i-1]
            gains.append(max(change, 0))
            losses.append(max(-change, 0))
        
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        rs = avg_gain / max(avg_loss, 1e-8)
        rsi.append(100 - (100 / (1 + rs)))
        
        # Continue for remaining bars
        for i in range(period + 1, len(prices)):
            change = prices[i] - prices[i-1]
            gain = max(change, 0)
            loss = max(-change, 0)
            avg_gain = ((avg_gain * (period - 1)) + gain) / period
            avg_loss = ((avg_loss * (period - 1)) + loss) / period

            rs = avg_gain / max(avg_loss, 1e-8)
            rsi.append(100 - (100 / (1 + rs)))
        
        return rsi


class CoinbaseMeanReversionStrategy:
    """Bollinger Band mean reversion with volatility breakout for BTC-XXX pairs."""
    
    def __init__(self, bb_period: int = 20, bb_std: float = 2.0):
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.sma_values = []
        self.upper_band = []
        self.lower_band = []
        self.band_width_history = []
        self._name = "Coinbase_Mean_Reversion"
        
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
        
        # Calculate SMA and Bollinger Bands
        self.sma_values = self._compute_sma(closes, self.bb_period)
        self.upper_band = []
        self.lower_band = []

        for i in range(len(closes)):
            start = max(0, i - self.bb_period + 1)
            window = closes[start:i + 1]
            sma = sum(window) / len(window)
            variance = sum((x - sma) ** 2 for x in window) / len(window)
            std = variance ** 0.5
            self.upper_band.append(sma + self.bb_std * std)
            self.lower_band.append(sma - self.bb_std * std)
        
        signals = []
        
        # Generate signals based on band width and price action
        for i in range(self.bb_period - 1, len(data)):
            current_price = closes[i]
            
            # Calculate band width
            if self.upper_band[i] and self.lower_band[i]:
                bw = (self.upper_band[i] - self.lower_band[i]) / max(self.lower_band[i], 1e-8)
                self.band_width_history.append(bw)
            
            # Squeeze detection: buy on breakout from contraction
            if len(self.band_width_history) >= 20:
                recent_bw = sum(self.band_width_history[-10:]) / 10
                older_bw = sum(self.band_width_history[:-10]) / 10
                
                # Significant squeeze detected
                if recent_bw < older_bw * 0.7 and current_price > self.upper_band[i]:
                    signals.append(('BUY', current_price))  # Bullish breakout from squeeze
                elif recent_bw < older_bw * 0.7 and current_price < self.lower_band[i]:
                    signals.append(('SELL', current_price))  # Bearish breakdown from squeeze
            
            # Mean reversion: buy at lower band with confirmation
            if self.lower_band[i] and current_price <= self.lower_band[i]:
                # Check for bullish reversal candle
                if (data[i]['high'] - data[i]['low']) / max(current_price, 1e-8) > 0.02:
                    signals.append(('BUY', current_price))
            
            # Mean reversion: sell at upper band with confirmation
            elif self.upper_band[i] and current_price >= self.upper_band[i]:
                # Check for bearish reversal candle
                if (data[i]['high'] - data[i]['low']) / max(current_price, 1e-8) > 0.02:
                    signals.append(('SELL', current_price))
        
        return signals
    
    def _compute_sma(self, prices: List[float], period: int) -> List[Optional[float]]:
        """Compute Simple Moving Average."""
        sma = []
        for i in range(len(prices)):
            start_idx = max(0, i - period + 1)
            window = prices[start_idx:i + 1]
            sma.append(sum(window) / len(window))
        return sma


class VolatilityBreakoutStrategy:
    """ATR-based volatility breakout with squeeze detection for BTC-XXX pairs."""
    
    def __init__(self, bb_period: int = 20, bb_std: float = 2.0, atr_period: int = 14):
        self.bb_period = bb_period
        self.bb_std = bb_std
        self.atr_period = atr_period
        self.highs = []
        self.lows = []
        self.trues_range = []
        self._name = "Volatility_Breakout"
        
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
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]

        # Build true range series locally (no state leak)
        tr_series = []
        for i in range(len(data)):
            h = highs[i] if highs[i] else closes[i]
            l = lows[i] if lows[i] else closes[i]
            if i == 0:
                tr_series.append(h - l)
            else:
                prev_h = highs[i - 1] if highs[i - 1] else closes[i - 1]
                prev_l = lows[i - 1] if lows[i - 1] else closes[i - 1]
                tr = max(h - l, abs(h - prev_h), abs(l - prev_l))
                tr_series.append(tr)

        signals = []
        for i in range(49, len(data)):
            window_highs = highs[i - 19:i + 1]
            window_lows = lows[i - 19:i + 1]
            bb_upper = max(window_highs)
            bb_lower = min(window_lows)

            atr = sum(tr_series[i - self.atr_period + 1:i + 1]) / self.atr_period

            recent_tr = sum(tr_series[i - 19:i + 1]) / 20
            older_tr = sum(tr_series[max(0, i - 39):i - 19]) / 20 if i >= 39 else recent_tr

            recent_vol = sum(d['volume'] or 1e6 for d in data[i - 19:i + 1]) / 20
            older_vol = sum(d['volume'] or 1e6 for d in data[max(0, i - 39):i - 19]) / 20 if i >= 39 else recent_vol

            if recent_tr < older_tr * 0.6 and recent_vol > older_vol * 1.5:
                if closes[i] > bb_upper:
                    signals.append(('BUY', closes[i]))
                elif closes[i] < bb_lower:
                    signals.append(('SELL', closes[i]))

        return signals


class RegimeAwareAdaptiveStrategy:
    """Regime-aware adaptive parameter tuning for BTC-XXX pairs."""
    
    def __init__(self):
        self.regime_history = []
        self.performance_buffer = []
        self.optimal_params = {
            'trending_up': {'rsi_period': 14, 'stop_multiplier': 2.0, 'position_size_pct': 0.15},
            'trending_down': {'rsi_period': 14, 'stop_multiplier': 2.0, 'position_size_pct': 0.15},
            'ranging_high_vol': {'rsi_period': 28, 'stop_multiplier': 3.0, 'position_size_pct': 0.10},
            'ranging_low_vol': {'rsi_period': 7, 'stop_multiplier': 1.5, 'position_size_pct': 0.20},
        }
        self._name = "Regime_Aware_Adaptive"
        
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
        
        signals = []
        
        # Classify regime and get optimal parameters
        regime = self._classify_regime(data)
        self.regime_history.append(regime)
        if len(self.regime_history) > 30:
            self.regime_history.pop(0)
        
        # Get adaptive parameters
        params = self._get_interpolated_params(regime)
        
        # Generate signal using regime-adaptive approach
        rsi_period = params['rsi_period']
        sma_val = self._compute_sma(closes, rsi_period)[-1]

        if regime == 'trending_up' and closes[-1] > sma_val:
            signals.append(('BUY', closes[-1]))
        elif regime == 'trending_down' and closes[-1] < sma_val:
            signals.append(('SELL', closes[-1]))
        elif regime == 'ranging_low_vol' and abs(closes[-1] - sma_val) / max(sma_val, 1e-8) > 0.02:
            signals.append(('BUY', closes[-1]))
        elif regime == 'ranging_high_vol' and abs(closes[-1] - sma_val) / max(sma_val, 1e-8) > 0.03:
            if closes[-1] > sma_val:
                signals.append(('SELL', closes[-1]))
            else:
                signals.append(('BUY', closes[-1]))
        
        return signals
    
    def _classify_regime(self, data: List[Dict]) -> str:
        """Classify current market regime."""
        if len(data) < 50:
            return 'unknown'
        
        # Calculate trend strength
        price_changes = [data[i]['close'] - data[i-1]['close'] 
                        for i in range(1, min(20, len(data)))]
        avg_change = sum(price_changes) / len(price_changes)
        abs_changes = [abs(c) for c in price_changes]
        avg_abs_change = sum(abs_changes) / len(abs_changes)
        
        # Trend strength ratio
        trend_strength = avg_change / max(avg_abs_change, 1e-8)
        
        # Volatility calculation
        recent_returns = [(data[i]['close'] - data[i-1]['close']) / 
                         max(data[i-1]['close'], 1e-8)
                         for i in range(1, min(20, len(data)))]
        volatility = sum(r**2 for r in recent_returns) ** 0.5
        
        # Classify regime
        if trend_strength > 0.1:
            return 'trending_up'
        elif trend_strength < -0.1:
            return 'trending_down'
        else:
            if volatility > 0.03:
                return 'ranging_high_vol'
            else:
                return 'ranging_low_vol'
    
    def _get_interpolated_params(self, current_regime: str) -> Dict[str, float]:
        """Get optimal parameters for current regime with interpolation."""
        if current_regime not in self.optimal_params:
            return self.optimal_params['ranging_low_vol']
        
        params = self.optimal_params[current_regime].copy()
        
        # Add performance-based adjustment
        if len(self.performance_buffer) >= 10:
            recent_perf = sum(p.get('return', 0) for p in self.performance_buffer[-10:]) / 10
            # Adjust position size based on recent performance (momentum)
            perf_factor = min(1.2, max(0.8, 1.0 + recent_perf * 5))
            params['position_size_pct'] *= perf_factor
        
        return params
    
    def _compute_sma(self, prices: List[float], period: int) -> List[float]:
        """Compute Simple Moving Average."""
        sma = []
        for i in range(len(prices)):
            start_idx = max(0, i - period + 1)
            window = prices[start_idx:i + 1]
            sma.append(sum(window) / len(window))
        return sma


# --- Backtesting Engine ---

# ── Novel Strategies ──

class VolumeProfileStrategy:
    """Volume Profile — identifies High Volume Nodes (HVN) as support/resistance
    and Low Volume Nodes (LVN) as targets. Trades bounces off HVN and
    breakouts through LVN.

    Gap: No existing strategy uses volume-at-price profiling.
    """
    def __init__(self, lookback=48, num_bins=20, vol_threshold=1.5):
        self.lookback = lookback
        self.num_bins = num_bins
        self.vol_threshold = vol_threshold
        self._name = "Volume_Profile"

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def generate_signals(self, data):
        if len(data) < self.lookback:
            return []
        closes = [d['close'] for d in data]
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        vols = [d['volume'] for d in data]
        signals = []

        for i in range(self.lookback - 1, len(data)):
            window_lows = lows[i - self.lookback + 1:i + 1]
            window_highs = highs[i - self.lookback + 1:i + 1]
            window_vols = vols[i - self.lookback + 1:i + 1]

            price_min = min(window_lows)
            price_max = max(window_highs)
            bin_size = (price_max - price_min) / self.num_bins
            if bin_size <= 0:
                continue

            bins = {b: 0.0 for b in range(self.num_bins)}
            for j in range(len(window_lows)):
                mid = (window_lows[j] + window_highs[j]) / 2
                bin_idx = min(int((mid - price_min) / bin_size), self.num_bins - 1)
                bins[bin_idx] += window_vols[j]

            avg_vol = sum(bins.values()) / self.num_bins if self.num_bins else 1
            poc_bin = max(bins, key=bins.get)
            poc_price = price_min + (poc_bin + 0.5) * bin_size
            hvns = [b for b, v in bins.items() if v > avg_vol * self.vol_threshold]
            lvns = [b for b, v in bins.items() if v < avg_vol * 0.3]

            current = closes[i]

            hvn_prices = [price_min + (b + 0.5) * bin_size for b in hvns]
            lvn_prices = [price_min + (b + 0.5) * bin_size for b in lvns]

            nearest_hvn_above = min([p for p in hvn_prices if p > current], default=None)
            nearest_hvn_below = max([p for p in hvn_prices if p < current], default=None)
            nearest_lvn_above = min([p for p in lvn_prices if p > current], default=None)
            nearest_lvn_below = max([p for p in lvn_prices if p < current], default=None)

            price_structure = bins[poc_bin] / avg_vol if avg_vol > 0 else 1

            if nearest_hvn_below and (current - nearest_hvn_below) / nearest_hvn_below < 0.02:
                pct_diff = (current - nearest_hvn_below) / nearest_hvn_below * 100
                strength = min(abs(pct_diff) / 2, 1.0) * min(price_structure / 2, 1.0)
                if strength > 0.3:
                    signals.append(('BUY', current))

            if nearest_hvn_above and (nearest_hvn_above - current) / current < 0.02:
                pct_diff = (nearest_hvn_above - current) / current * 100
                strength = min(abs(pct_diff) / 2, 1.0) * min(price_structure / 2, 1.0)
                if strength > 0.3:
                    signals.append(('SELL', current))

            if nearest_lvn_above and current > poc_price and (nearest_lvn_above - current) / current < 0.01:
                signals.append(('BUY', current))
            if nearest_lvn_below and current < poc_price and (current - nearest_lvn_below) / current < 0.01:
                signals.append(('SELL', current))

        return signals


class MultiTimeframeConfluenceStrategy:
    """Multi-timeframe confluence — detects when multiple time horizons
    (short, medium, long) agree on direction using rate-of-change.
    Higher conviction when more timeframes align.

    Gap: No strategy explicitly checks cross-timeframe alignment.
    """
    def __init__(self, short_window=6, medium_window=24, long_window=72, threshold=0.01):
        self.short_window = short_window
        self.medium_window = medium_window
        self.long_window = long_window
        self.threshold = threshold
        self._name = "MultiTF_Confluence"

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def generate_signals(self, data):
        if len(data) < self.long_window:
            return []
        closes = [d['close'] for d in data]
        signals = []

        def roc(window, idx):
            return (closes[idx] - closes[idx - window]) / closes[idx - window]

        for i in range(self.long_window - 1, len(data)):
            roc_s = roc(self.short_window, i)
            roc_m = roc(self.medium_window, i)
            roc_l = roc(self.long_window, i)

            bullish = sum([1 for r in [roc_s, roc_m, roc_l] if r > self.threshold])
            bearish = sum([1 for r in [roc_s, roc_m, roc_l] if r < -self.threshold])

            net = bullish - bearish
            if net >= 2:
                signals.append(('BUY', closes[i]))
            elif net <= -2:
                signals.append(('SELL', closes[i]))

        return signals


class OrderFlowPressureStrategy:
    """Order flow pressure inferred from candle structure.
    Analyzes wick-to-body ratios, consecutive close positions, and
    volume-contextualized directional pressure.

    Gap: No strategy uses candle microstructure (wick vs body analysis).
    """
    def __init__(self, window=14, pressure_threshold=0.6):
        self.window = window
        self.pressure_threshold = pressure_threshold
        self._name = "OrderFlow_Pressure"

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def generate_signals(self, data):
        if len(data) < self.window + 1:
            return []
        signals = []

        for i in range(self.window, len(data)):
            window_data = data[i - self.window + 1:i + 1]
            bull_score = 0.0
            bear_score = 0.0

            for j, c in enumerate(window_data):
                o, h, l, cl, v = c['open'], c['high'], c['low'], c['close'], c['volume']
                candle_range = h - l
                if candle_range <= 0:
                    continue
                body = abs(cl - o)
                upper_wick = h - max(o, cl)
                lower_wick = min(o, cl) - l
                vol_factor = min(v / 1000, 10.0) if v > 0 else 0.1

                is_bull = cl > o
                if is_bull:
                    body_pct = body / candle_range
                    lower_wick_ratio = lower_wick / candle_range
                    if lower_wick_ratio > 0.4 and body_pct > 0.3:
                        bull_score += 0.3 * vol_factor
                    if body_pct > 0.6:
                        bull_score += 0.2 * vol_factor
                    if j > 0 and cl > window_data[j - 1]['close']:
                        bull_score += 0.15 * vol_factor
                else:
                    body_pct = body / candle_range
                    upper_wick_ratio = upper_wick / candle_range
                    if upper_wick_ratio > 0.4 and body_pct > 0.3:
                        bear_score += 0.3 * vol_factor
                    if body_pct > 0.6:
                        bear_score += 0.2 * vol_factor
                    if j > 0 and cl < window_data[j - 1]['close']:
                        bear_score += 0.15 * vol_factor

                if lower_wick > upper_wick * 2 and lower_wick > candle_range * 0.3:
                    bull_score += 0.4 * vol_factor
                if upper_wick > lower_wick * 2 and upper_wick > candle_range * 0.3:
                    bear_score += 0.4 * vol_factor

            total = bull_score + bear_score
            if total > 0:
                bull_pct = bull_score / total
                bear_pct = bear_score / total
                if bull_pct > self.pressure_threshold:
                    signals.append(('BUY', data[i]['close']))
                elif bear_pct > self.pressure_threshold:
                    signals.append(('SELL', data[i]['close']))

        return signals


class VolatilityContractionExpansionStrategy:
    """Detects volatility contraction (tight consolidation) followed by
    expansion (breakout). Trades the direction of the expansion after
    a contraction phase.

    Gap: No strategy explicitly models the contraction-expansion cycle.
    """
    def __init__(self, contraction_window=10, expansion_window=5, atr_mult=1.5):
        self.contraction_window = contraction_window
        self.expansion_window = expansion_window
        self.atr_mult = atr_mult
        self._name = "Vol_Contraction_Expansion"

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def generate_signals(self, data):
        if len(data) < self.contraction_window + self.expansion_window:
            return []
        signals = []
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        closes = [d['close'] for d in data]
        volumes = [d['volume'] for d in data]

        def _atr(start, n):
            if start - n < 1:
                return 0
            trs = []
            for k in range(start - n + 1, start + 1):
                if k < 1:
                    continue
                tr = max(highs[k] - lows[k],
                         abs(highs[k] - closes[k - 1]),
                         abs(lows[k] - closes[k - 1]))
                trs.append(tr)
            return sum(trs) / len(trs) if trs else 0

        def _avg_range(start, n):
            if start - n < 0:
                return 0
            vals = [(highs[k] - lows[k]) for k in range(start - n + 1, start + 1)]
            return sum(vals) / len(vals) if vals else 0

        for i in range(self.contraction_window + self.expansion_window, len(data)):
            contract_range = _avg_range(i - self.expansion_window, self.contraction_window)
            recent_range = _avg_range(i, self.expansion_window)

            atr_val = _atr(i - self.expansion_window, self.contraction_window)
            recent_atr = _atr(i, self.expansion_window)

            if atr_val <= 0:
                continue

            contraction_ratio = contract_range / (sum(highs[k] - lows[k] for k in range(i - self.contraction_window - self.expansion_window + 1, i - self.expansion_window + 1)) / max(self.contraction_window, 1)) if i - self.contraction_window - self.expansion_window + 1 >= 0 else 1

            avg_vol = sum(volumes[i - self.expansion_window + 1:i + 1]) / self.expansion_window
            prev_avg_vol = sum(volumes[i - self.contraction_window - self.expansion_window + 1:i - self.expansion_window + 1]) / max(self.contraction_window, 1) if i - self.contraction_window - self.expansion_window + 1 >= 0 else 1
            vol_ratio = avg_vol / max(prev_avg_vol, 1)

            is_contracting = contract_range < atr_val * 0.6
            is_expanding = recent_range > atr_val * self.atr_mult
            vol_surge = vol_ratio > 1.5

            if is_contracting and is_expanding and vol_surge:
                direction = 1 if closes[i] > closes[i - self.expansion_window] else -1
                if direction > 0:
                    signals.append(('BUY', closes[i]))
                else:
                    signals.append(('SELL', closes[i]))
            elif is_expanding and vol_surge:
                direction = 1 if closes[i] > closes[i - 1] else -1
                if direction > 0 and closes[i] > closes[i - self.expansion_window]:
                    signals.append(('BUY', closes[i]))
                elif direction < 0 and closes[i] < closes[i - self.expansion_window]:
                    signals.append(('SELL', closes[i]))

        return signals


class StatisticalArbitrageZScorePairStrategy:
    """Statistical arbitrage on BTC-XXX pair ratios.
    Tracks the z-score of the price ratio between the first pair (reference)
    and the current data symbol. When z-score exceeds threshold, fades
    the divergence expecting mean reversion.

    Gap: No existing strategy uses z-score of pair ratios for mean reversion.
    """
    def __init__(self, z_score_entry=2.0, z_score_exit=0.5, lookback=48):
        self.z_score_entry = z_score_entry
        self.z_score_exit = z_score_exit
        self.lookback = lookback
        self._name = "ZScore_Pair_Arb"
        self._reference_price = None

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def _infer_reference(self, data):
        if not data:
            return None
        first = data[0]
        return first.get('close', 0)

    def generate_signals(self, data):
        if len(data) < self.lookback:
            return []
        closes = [d['close'] for d in data]
        if self._reference_price is None:
            self._reference_price = self._infer_reference(data)
        if not self._reference_price or self._reference_price == 0:
            return []

        ratios = [c / self._reference_price for c in closes]
        signals = []

        for i in range(self.lookback - 1, len(data)):
            window = ratios[i - self.lookback + 1:i + 1]
            mu = sum(window) / len(window)
            var = sum((x - mu) ** 2 for x in window) / len(window)
            std = var ** 0.5
            if std == 0:
                continue
            z = (ratios[i] - mu) / std

            if z > self.z_score_entry:
                signals.append(('SELL', closes[i]))
            elif z < -self.z_score_entry:
                signals.append(('BUY', closes[i]))

        return signals


class LiquidationHeatmapStrategy:
    """Detects potential stop-loss runs and liquidation cascades by
    analyzing volatility clustering, rapid price acceleration, and
    sharp reversals from extremes.

    Gap: No existing strategy models stop-hunt / liquidation sweep patterns.
    """
    def __init__(self, surge_window=3, lookback=20, vol_mult=2.0):
        self.surge_window = surge_window
        self.lookback = lookback
        self.vol_mult = vol_mult
        self._name = "Liquidation_Heatmap"

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    def generate_signals(self, data):
        if len(data) < self.lookback + self.surge_window:
            return []
        closes = [d['close'] for d in data]
        highs = [d['high'] for d in data]
        lows = [d['low'] for d in data]
        vols = [d['volume'] for d in data]
        signals = []

        for i in range(self.lookback + self.surge_window, len(data)):
            prev_vols = vols[i - self.lookback - self.surge_window + 1:i - self.surge_window + 1]
            recent_vols = vols[i - self.surge_window + 1:i + 1]

            avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
            vol_spike = recent_vols[-1] > avg_vol * self.vol_mult if avg_vol > 0 else False

            surge_returns = []
            for k in range(1, self.surge_window):
                if i - k < 0:
                    continue
                surge_returns.append((closes[i - k + 1] - closes[i - k]) / closes[i - k])

            avg_surge = sum(surge_returns) / len(surge_returns) if surge_returns else 0
            accel = 0
            if len(surge_returns) >= 2:
                accel = surge_returns[-1] - surge_returns[0]

            prev_range = [highs[k] - lows[k] for k in range(i - self.lookback - self.surge_window + 1, i - self.surge_window + 1)]
            recent_range = [highs[k] - lows[k] for k in range(i - self.surge_window + 1, i + 1)]
            avg_range = sum(prev_range) / len(prev_range) if prev_range else 1
            range_expansion = sum(recent_range) / len(recent_range) / max(avg_range, 1) if recent_range and avg_range else 0

            wick_ratio = 0
            for k in range(max(0, i - 2), i + 1):
                candle_range = highs[k] - lows[k]
                if candle_range > 0:
                    body = abs(closes[k] - data[k]['open'])
                    wick = candle_range - body
                    wick_ratio = max(wick_ratio, wick / candle_range)

            is_liquidation_sweep = False
            sweep_direction = 0
            for k in range(1, 4):
                if i - k < 0:
                    continue
                if highs[i - k] > highs[i] and closes[i - k] < lows[i - k] and closes[i] > closes[i - k]:
                    is_liquidation_sweep = True
                    sweep_direction = 1
                    break
                if lows[i - k] < lows[i] and closes[i - k] > highs[i - k] and closes[i] < closes[i - k]:
                    is_liquidation_sweep = True
                    sweep_direction = -1
                    break

            if is_liquidation_sweep and sweep_direction > 0:
                signals.append(('BUY', closes[i]))
            elif is_liquidation_sweep and sweep_direction < 0:
                signals.append(('SELL', closes[i]))
            elif vol_spike and range_expansion > 2.0 and avg_surge > 0.02:
                signals.append(('BUY', closes[i]))
            elif vol_spike and range_expansion > 2.0 and avg_surge < -0.02:
                signals.append(('SELL', closes[i]))
            elif vol_spike and wick_ratio > 0.6:
                last_dir = 1 if closes[i] > data[i]['open'] else -1
                if last_dir > 0:
                    signals.append(('BUY', closes[i]))
                else:
                    signals.append(('SELL', closes[i]))

        return signals


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
        
        # Calculate PnL from signals using FIFO queue
        open_buys = []
        total_trades = 0
        win_trades = 0

        for signal, price in signals:
            if signal == 'BUY':
                open_buys.append(price)
            elif signal == 'SELL' and open_buys:
                entry_price = open_buys.pop(0)
                pnl = (price - entry_price) / entry_price * 100
                total_trades += 1
                if pnl > 0:
                    win_trades += 1

        trades_count = total_trades
                
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
            
        # Drawdown calculation (full series peak-to-trough)
        peak = 0.0
        max_drawdown = 0.0
        for p in closes:
            if p > peak:
                peak = p
            drawdown = (peak - p) / peak * 100 if peak > 0 else 0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        
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
        TrendFollowing(50),
        BTCVolatilityStacking(20, 14, 14, 0.02, 0.04),
        BTCVolatilityBreakout(20, 10, 14, 0.02),
        BTCVolatilityMeanReversion(30, 20, 2.0, 0.5, 14),
        BTCVolatilityMomentum(20, 10, 14, 14, 0.02),
        CoinbaseMomentumStrategy(),
        CoinbaseMeanReversionStrategy(),
        VolatilityBreakoutStrategy(),
        RegimeAwareAdaptiveStrategy(),
        VolumeProfileStrategy(),
        MultiTimeframeConfluenceStrategy(),
        OrderFlowPressureStrategy(),
        VolatilityContractionExpansionStrategy(),
        StatisticalArbitrageZScorePairStrategy(),
        LiquidationHeatmapStrategy()
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
    run_benchmark("BTC-ETH")
    run_benchmark("BTC-SOL")
    run_benchmark("BTC-DOGE")
    run_benchmark("BTC-XRP")
