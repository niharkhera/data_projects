import os
import time
import logging
import requests
import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Import the SQLite data manager
from save_db import StockDataManager
symbol_fetch_limit = 2

class StockDataFetcher:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        self.api_key = self.get_api_key()
        self.db_manager = StockDataManager()
        self.api_base_url = "https://api.polygon.io"
        self.symbol_fetch_limit = 1      # Fetch 10 stock symbols

    def get_api_key(self) -> str:
        """
        Retrieves the API key from environment variables.

        Returns:
            str: API key for Polygon.io
        """
        api_key = os.getenv("POLYGON_API_KEY")
        if not api_key:
            logger.error("ERROR: API key not found in environment variables.")
            raise ValueError("API key not found. Please ensure it's set in the .env file.")
        return api_key

    def fetch_all_stock_symbols(self, limit: int = None) -> List[str]:
        """
        Fetch active stock symbols from Polygon API.

        Args:
            limit (int): Number of symbols to fetch.

        Returns:
            list: A list of stock symbols.
        """
        if limit is None:
            limit = self.symbol_fetch_limit

        url = f"{self.api_base_url}/v3/reference/tickers"
        params = {
            "market": "stocks",
            "active": "true",
            "order": "desc",
            "limit": limit,
            "sort": "market",
            "apiKey": self.api_key,
        }
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            return [ticker['ticker'] for ticker in data.get('results', [])]
        except requests.RequestException as e:
            logger.error(f"ERROR: Error fetching stock symbols: {e}")
            return []

    def fetch_stock_details(self, ticker: str, date: Optional[str] = None) -> Optional[Dict]:
        """
        Fetch stock details for a specific ticker from Polygon API.

        Args:
            ticker (str): The stock ticker symbol.
            date (str, optional): Specific date for retrieving ticker details.

        Returns:
            dict: Stock details information, or None if request fails.
        """
        url = f"{self.api_base_url}/v3/reference/tickers/{ticker}"
        params = {"apiKey": self.api_key}

        if date:
            params["date"] = date

        try:
            response = requests.get(url, params=params)
            time.sleep(12)  # Respect API rate limits
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"ERROR: Error fetching stock details for {ticker}: {e}")
            return None

    def fetch_ohlc_stock_data(self, date: str) -> Optional[Dict]:
        """
        Fetch OHLC (Open, High, Low, Close) stock data for a given date.

        Args:
            date (str): The date for stock data in YYYY-MM-DD format.

        Returns:
            dict: Stock market OHLC data, or None if request fails.
        """
        url = f"{self.api_base_url}/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {
            "apiKey": self.api_key,
            "adjusted": "true",
            "include_otc": "false"
        }
        try:
            response = requests.get(url, params=params)
            time.sleep(12)  # Respect API rate limits
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"ERROR: Error fetching stock price data: {e}")
            return None

    def close_db_connection(self):
        """Close the database connection."""
        self.db_manager.close_connection()

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Create an instance of the StockDataFetcher class
    fetcher = StockDataFetcher()

    try:
        # Fetch stock symbols
        stock_symbols = fetcher.fetch_all_stock_symbols()
        logger.info(f"INFO: Found {len(stock_symbols)} stock symbols")

        # Fetch and save ticker details
        for symbol in stock_symbols:
            details = fetcher.fetch_stock_details(symbol)
            if details and details.get('results'):
                fetcher.db_manager.insert_ticker_details(details['results'])

        # Get yesterday's date
        # date = (datetime.datetime.now() - datetime.timedelta(1)).strftime("%Y-%m-%d")

        # 5 free API requests in a min. Use time.sleep(12) in fetch_ohlc_stock_data function 
        # list1 = ['2023-01-24', '2023-01-25', '2023-01-26', '2023-01-27', '2023-01-28']
        # for date in list1:
        # Fetch and save stock prices
        date = '2021-01-24'
        stock_prices = fetcher.fetch_ohlc_stock_data(date)
        if stock_prices:
            fetcher.db_manager.insert_stock_prices(stock_prices)
            logger.info(f"INFO: Successfully inserted stock price for {date}")

    except Exception as e:
        logger.error(f"ERROR: An unexpected error occurred: {e}")
    finally:
        # print(fetcher.db_manager.display_ticker_details())
        # print(fetcher.db_manager.display_stock_prices())
        # Always close the database connection
        fetcher.close_db_connection()

if __name__ == "__main__":
    main()
