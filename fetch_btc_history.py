"""Fetch historical BTC data for backtesting."""
import yfinance as yf
from datetime import datetime, timedelta
import os

# Fetch 5+ years of daily BTC-USD data
ticker = "BTC-USD"
end_date = datetime.now()
start_date = end_date - timedelta(days=2000)

print(f'Downloading {ticker} from {start_date.date()} to {end_date.date()}...')
data = yf.download(ticker, start=start_date, end=end_date, progress=False)

if len(data) == 0:
    print("ERROR: No data fetched - check internet connection")
else:
    # Flatten MultiIndex columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = ["date", "open", "high", "low", "close", "volume"]
    else:
        data["date"] = data.index.strftime("%Y-%m-%d")
    
    output_path = "data/historical/BTC-USD_daily_5yrs.csv"
    os.makedirs("data/historical", exist_ok=True)
    data.to_csv(output_path, index=False)
    
    print(f"✓ Saved {len(data)} bars ({(len(data)/365):.1f} years)")
    print(f"  Price range: ${data['close'].min():,.0f} - ${data['close'].max():,.0f}")
    print(f"  Path: {output_path}")
