import os
import time
import datetime
import requests
from typing import List, Dict, Optional
from dotenv import load_dotenv
from save_db import StockDataManager
from logger_config import setup_logger

logger = setup_logger("PolygonFetcher")

class StockDataFetcher:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("POLYGON_API_KEY")
        self.db_manager = StockDataManager()
        self.api_base_url = "https://api.polygon.io"
        self.symbol_fetch_limit = 1000
        
        if not self.api_key:
            logger.error("POLYGON_API_KEY missing from environment variables.")

    def fetch_all_stock_symbols(self, limit: Optional[int] = None) -> List[str]:
        if limit is None: limit = self.symbol_fetch_limit
        url = f"{self.api_base_url}/v3/reference/tickers"
        params = {"market": "stocks", "active": "true", "limit": limit, "apiKey": self.api_key}
        
        logger.info(f"Fetching up to {limit} ticker symbols from Polygon API...")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return [ticker['ticker'] for ticker in response.json().get('results', [])]
        else:
            logger.error(f"API Error GET /v3/reference/tickers: HTTP {response.status_code}")
            return []

    def fetch_stock_details(self, symbol: str) -> Optional[Dict]:
        url = f"{self.api_base_url}/v3/reference/tickers/{symbol}"
        params = {"apiKey": self.api_key}
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"API Error fetching details for {symbol}: HTTP {response.status_code}")
            return None

    def fetch_ohlc_stock_data(self, date: str) -> Dict:
        logger.info(f"Rate Limiting: Sleeping 12s before fetching OHLCV for {date}")
        time.sleep(12) 
        
        url = f"{self.api_base_url}/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {"apiKey": self.api_key, "adjusted": "true"}
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"API Error GET grouped OHLCV for {date}: HTTP {response.status_code}")
            return {}

    def close_db_connection(self):
        self.db_manager.close_connection()

def main():
    fetcher = StockDataFetcher()
    try:
        stock_symbols = fetcher.fetch_all_stock_symbols()
        logger.info(f"Retrieved {len(stock_symbols)} active tickers. Commencing detail fetch routine.")
        
        for symbol in stock_symbols:
            details = fetcher.fetch_stock_details(symbol)
            if details and details.get('results'):
                fetcher.db_manager.insert_ticker_details(details['results'])

        today = datetime.datetime.now()
        if today.weekday() == 5: target_date = today - datetime.timedelta(days=1)
        elif today.weekday() == 6: target_date = today - datetime.timedelta(days=2)
        else:
            target_date = today - datetime.timedelta(days=1)
            if target_date.weekday() == 6: target_date = target_date - datetime.timedelta(days=2)

        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(f"Targeting {date_str} for OHLCV bulk fetch.")
        
        stock_prices = fetcher.fetch_ohlc_stock_data(date_str)
        if stock_prices:
            fetcher.db_manager.insert_stock_prices(stock_prices)
            logger.info(f"OHLCV ingestion complete for {date_str}.")

    except Exception as e:
        logger.exception(f"Unhandled exception in StockDataFetcher execution: {e}")
    finally:
        fetcher.close_db_connection()

if __name__ == "__main__":
    main()