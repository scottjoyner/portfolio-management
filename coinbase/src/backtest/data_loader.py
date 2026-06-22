"""
Historical data loaders for backtesting.

Loads real market data from CSV files and converts to OHLCVBar format
with proper close_window history tracking."""

from pathlib import Path
from typing import List, Optional
import csv


def load_btc_ohlcv(
    csv_path: str = "/home/scott/git/portfolio-management/data/historical/BTC-USD_daily.csv",
    min_days: int = 365,
) -> List["OHLCVBar"]:
    """Load BTC daily OHLCV data from CSV.
    
    Args:
        csv_path: Path to the CSV file (must have columns: date,open,high,low,close,volume)
        min_days: Minimum required rows; raises ValueError if fewer
        
    Returns:
        List of OHLCVBar objects with close_window populated from prior bars.
        
    Raises:
        FileNotFoundError: If CSV doesn't exist
        ValueError: If insufficient data (< min_days rows after header)
    """
    from coinbase.src.backtest.coinbase_niche_strategies import OHLCVBar
    
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Historical data not found at {csv_path}")
    
    rows = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                bar_dict = {
                    "date": row["date"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row.get("volume", 0)),
                }
                rows.append(bar_dict)
            except (ValueError, KeyError):
                continue  # Skip malformed rows
    
    if len(rows) < min_days:
        raise ValueError(
            f"Insufficient data: {len(rows)} days found, "
            f"need at least {min_days} days. "
            f"Run fetch_historical_btc.py to download more history."
        )
    
    # Convert to OHLCVBar with close_window
    bars: List[OHLCVBar] = []
    prev_closes: list[float] = []
    
    for row in rows:
        cw = prev_closes[-30:] if len(prev_closes) >= 30 else prev_closes[:]
        
        bar = OHLCVBar(
            timestamp=row["date"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
            close_window=cw,
        )
        bars.append(bar)
        
        # Update close window for next iteration
        prev_closes.append(row["close"])
        if len(prev_closes) > 100:
            prev_closes.pop(0)
    
    return bars


def fetch_historical_btc(days: int = 2000, output_dir: str = "data/historical") -> str:
    """Fetch historical BTC price data using yfinance.
    
    Downloads ~5-6 years of daily BTC-USD history to CSV file.
    
    Args:
        days: Number of days to fetch (default 2000 ≈ 5.5 years)
        output_dir: Directory to save the CSV
        
    Returns:
        Path to saved CSV file.
    """
    import yfinance as yf
    from datetime import datetime, timedelta
    
    # Ensure directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    ticker = "BTC-USD"
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"Downloading {ticker} from {start_date.date()} to {end_date.date()}...")
    
    data = yf.download(ticker, start=start_date, end=end_date, progress=False)
    
    if len(data) == 0:
        raise RuntimeError("Failed to fetch data - check internet connection and API rate limits.")
    
    # Flatten MultiIndex columns
    data.columns = ["date", "open", "high", "low", "close", "volume"]
    data["date"] = data.index.strftime("%Y-%m-%d")
    output_path = Path(output_dir) / "BTC-USD_daily_full.csv"
    
    data.to_csv(output_path, index=False)
    
    print(f"Saved {len(data)} bars to {output_path}")
    return str(output_path)
