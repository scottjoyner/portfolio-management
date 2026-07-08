import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from data.fetch_coinbase_historical import CoinbaseDataFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Configuration
    ASSETS = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "DOT-USD", "MATIC-USD", "AVAX-USD", "LINK-USD"]
    GRANULARITY = "ONE_HOUR"
    DAYS_BACK = 30
    DATA_DIR = "data/raw"

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    fetcher = CoinbaseDataFetcher()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_BACK)

    for asset in ASSETS:
        logger.info(f"Starting download for {asset}...")
        data = fetcher.fetch_candles(
            product_id=asset,
            granularity=GRANULARITY,
            start_date=start_date,
            end_date=end_date
        )
        
        if data:
            df = pd.DataFrame(data)
            # Convert timestamps
            df['start'] = pd.to_datetime(df['start'], unit='s')
            df['end'] = pd.to_datetime(df['end'], unit='s')
            # Convert to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            # Sort by start time
            df = df.sort_values('start')
            
            file_path = os.path.join(DATA_DIR, f"{asset.replace('-', '_')}_{GRANULARITY.lower()}.csv")
            df.to_csv(file_path, index=False)
            logger.info(f"Saved {len(df)} rows to {file_path}")
        else:
            logger.error(f"No data for {asset}")

if __name__ == "__main__":
    main()
