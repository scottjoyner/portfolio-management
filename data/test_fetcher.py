import json
import logging
import time
from datetime import datetime, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoinbaseDataFetcher:
    def __init__(self, base_url: str = "https://api.coinbase.com/api/v3/brokerage/market/products"):
        self.base_url = base_url

    def fetch_candles(self, product_id: str, start_date: datetime, end_date: datetime, granularity: str = "ONE_HOUR") -> List[Dict[str, Any]]:
        """
        Fetches candles for a product.
        Granularity options: ONE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, TWO_HOUR, FOUR_HOUR, SIX_HOUR, ONE_DAY
        """
        all_candles = []
        current_start = start_date
        max_retries = 3

        while current_start < end_date:
            # For the last window, use end_date instead of current_start + granularity
            current_end = min(current_start + timedelta(hours=1), end_date)
            
            # If granularity is ONE_DAY, use 1 day
            if granularity == "ONE_DAY":
                current_end = min(current_start + timedelta(days=1), end_date)
            elif granularity == "SIX_HOUR":
                current_end = min(current_start + timedelta(hours=6), end_date)
            elif granularity == "FOUR_HOUR":
                current_end = min(current_start + timedelta(hours=4), end_date)
            elif granularity == "TWO_HOUR":
                current_end = min(current_start + timedelta(hours=2), end_date)
            elif granularity == "THIRTY_MINUTE":
                current_end = min(current_start + timedelta(minutes=30), end_date)
            elif granularity == "FIFTEEN_MINUTE":
                current_end = min(current_start + timedelta(minutes=15), end_date)
            elif granularity == "FIVE_MINUTE":
                current_end = min(current_start + timedelta(minutes=5), end_date)
            elif granularity == "ONE_MINUTE":
                current_end = min(current_start + timedelta(minutes=1), end_date)

            # Actually, the API handles the range. We'll just fetch in chunks of 300 if needed.
            # But to keep it simple, let's just try to fetch the whole range first.
            
            params = {
                "start": int(current_start.timestamp()),
                "end": int(current_end.timestamp()),
                "granularity": granularity
            }
            
            url = f"{self.base_url}/{product_id}/candles?{urlencode(params)}"
            logger.info(f"Fetching {product_id} from {current_start} to {current_end} ({granularity})")
            
            success = False
            for attempt in range(max_retries):
                try:
                    req = Request(url, headers={
                        'User-Agent': 'HermesPortfolio/1.0 (Backtesting)',
                        'Accept': 'application/json'
                    })
                    
                    with urlopen(req, timeout=10) as response:
                        data = json.loads(response.read().decode('utf-8'))
                        candles = data.get('candles', [])
                        if candles:
                            all_candles.extend(candles)
                        success = True
                        break
                except HTTPError as e:
                    if e.code == 429:
                        wait = int(e.headers.get('Retry-After', 5))
                        logger.warning(f"Rate limited (429). Waiting {wait}s...")
                        time.sleep(wait)
                    else:
                        logger.error(f"HTTP Error {e.code}: {e.reason}")
                        break
                except Exception as e:
                    logger.error(f"Error: {e}")
                    time.sleep(2 ** attempt)
            
            if not success:
                break
            
            if current_end >= end_date:
                break
            
            current_start = current_end
            if len(all_candles) > 0 and all_candles[-1]['start'] == current_end.timestamp():
                # We might be stuck in a loop if the API returns the same end timestamp
                current_start = current_end + timedelta(seconds=1)
            else:
                current_start = current_end

        return all_candles

if __name__ == "__main__":
    fetcher = CoinbaseDataFetcher()
    # Test BTC-USD
    start = datetime.now() - timedelta(days=2)
    end = datetime.now()
    data = fetcher.fetch_candles("BTC-USD", start, end, "ONE_HOUR")
    print(f"Fetched {len(data)} candles for BTC-USD")
    if data:
        print(f"First: {data[0]}")
        print(f"Last: {data[-1]}")
