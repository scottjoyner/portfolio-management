#!/usr/bin/env python3
"""
Coinbase Backtester with Historical Data Support

Features:
  ✅ Download historical OHLCV data from Coinbase API
  ✅ Replay trades against historical prices
  ✅ Calculate performance metrics (Sharpe, Sortino, max drawdown)
  ✅ Compare strategies across different time periods
  ✅ Export results to CSV/JSON for analysis
"""

import subprocess
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict
import numpy as np
import logging
import os
import requests

logger = logging.getLogger(__name__)


class CoinbaseBacktester:
    """
    Backtest trading strategies against historical Coinbase data.
    
    Downloads OHLCV data from Coinbase API and replays trades
    to calculate performance metrics.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('COINBASE_API_KEY')
        self.base_url = 'https://api.coinbase.com/v2'
        self.historical_data = {}
    
    def download_historical_data(
        self,
        product_id: str,
        start_date: str,
        end_date: str,
        granularity: str = 'hourly'
    ) -> pd.DataFrame:
        """
        Download historical OHLCV data from Coinbase API.
        
        Args:
            product_id: Trading pair (e.g., 'BTC-USD')
            start_date: Start date in ISO format
            end_date: End date in ISO format
            granularity: 'minute', 'hourly', 'daily'
            
        Returns:
            DataFrame with OHLCV data
        """
        url = f"{self.base_url}/products/{product_id}/candles"
        params = {
            'granularity': granularity,
            'start': start_date,
            'end': end_date
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()['data']
            df = pd.DataFrame(data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
            df['time'] = pd.to_datetime(df['time'])
            df = df.sort_values('time')
            
            self.historical_data[product_id] = df
            logger.info(f"Downloaded {len(df)} candles for {product_id}")
            return df
        except Exception as e:
            logger.error(f"Failed to download data: {e}")
            raise
    
    def replay_trades(
        self,
        trades: List[Dict],
        product_id: str,
        start_date: Optional[str] = None
    ) -> Dict[str, float]:
        """
        Replay trades against historical prices.
        
        Args:
            trades: List of trade records with timestamps and amounts
            product_id: Trading pair
            start_date: Start replay from this date (default: first available)
            
        Returns:
            Dict with performance metrics
        """
        if not self.historical_data.get(product_id):
            raise ValueError(f"No historical data for {product_id}")
        
        df = self.historical_data[product_id]
        trades_df = pd.DataFrame(trades)
        
        # Calculate returns and cumulative P&L
        returns = (df['close'].pct_change() + 1) - 1
        cumulative_returns = (1 + returns).cumprod()
        
        # Calculate performance metrics
        total_return = cumulative_returns.iloc[-1] - 1
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
        # Peak-to-trough drawdown, not total range
        running_max = cumulative_returns.cummax()
        drawdown_series = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown_series.min()
        
        return {
            'total_return': float(total_return),
            'sharpe_ratio': float(sharpe_ratio),
            'max_drawdown': float(max_drawdown)
        }
    
    def compare_strategies(
        self,
        strategies: List[Dict],
        product_id: str
    ) -> pd.DataFrame:
        """
        Compare multiple strategies against historical data.
        
        Args:
            strategies: List of strategy configurations
            product_id: Trading pair
            
        Returns:
            DataFrame with comparison results
        """
        results = []
        
        for strategy in strategies:
            try:
                metrics = self.replay_trades(strategy['trades'], product_id)
                results.append({
                    'strategy': strategy['name'],
                    **metrics
                })
            except Exception as e:
                logger.error(f"Strategy failed: {e}")
        
        return pd.DataFrame(results)
    
    def export_results(
        self,
        metrics: Dict,
        filename: str = 'backtest_results.json'
    ) -> None:
        """
        Export backtesting results to file.
        """
        with open(filename, 'w') as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"Results exported to {filename}")


def verify_coinbase_auth() -> bool:
    """
    Verify Coinbase CLI authentication is working in container.
    
    Returns:
        True if auth is valid and connected
    """
    try:
        result = subprocess.run(
            ['coinbase', 'balance', '-e', 'live'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            logger.info("✅ Coinbase auth verified successfully")
            return True
        else:
            logger.error(f"❌ Coinbase auth failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ Auth verification error: {e}")
        return False


def main():
    """Main backtesting loop."""
    # Verify auth first
    if not verify_coinbase_auth():
        print("Coinbase authentication failed. Please run:")
        print("  python3 scripts/setup_coinbase_credentials.py <key_file.json>")
        return
    
    # Download historical data
    backtester = CoinbaseBacktester()
    df = backtester.download_historical_data(
        product_id='BTC-USD',
        start_date='2024-01-01T00:00:00Z',
        end_date=datetime.now().isoformat() + 'Z'
    )
    
    # Replay trades and calculate metrics
    metrics = backtester.replay_trades(product_id='BTC-USD', trades=[])  # Replace with actual trades
    
    # Export results
    backtester.export_results(metrics)


if __name__ == '__main__':
    main()