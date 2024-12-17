import os
import time
import logging
import requests
import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Import the SQLite data manager
from save_db import StockDataManager

# Load environment variables from .env file
load_dotenv() 

# Constants
API_BASE_URL = "https://api.polygon.io"
SYMBOL_FETCH_LIMIT = 1  # Fetch 10 stock symbols

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def get_api_key() -> str:
    """
    Retrieves the API key from environment variables.
    
    Returns:
        str: API key for Polygon.io
    """
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        logger.error("API key not found in environment variables.")
        raise ValueError("API key not found. Please ensure it's set in the .env file.")
    return api_key

def fetch_all_stock_symbols(api_key: str, limit: int = SYMBOL_FETCH_LIMIT) -> List[str]:
    """
    Fetch active stock symbols from Polygon API.
    
    Args:
        api_key (str): API key for Polygon.io.
        limit (int): Number of symbols to fetch.
    
    Returns:
        list: A list of stock symbols.
    """
    url = f"{API_BASE_URL}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "order": "desc",
        "limit": limit,
        "sort": "market",
        "apiKey": api_key,
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return [ticker['ticker'] for ticker in data.get('results', [])]
    except requests.RequestException as e:
        logger.error(f"API request error: {e}")
        return []

def fetch_stock_details(ticker: str, api_key: str, date: Optional[str] = None) -> Optional[Dict]:
    """
    Fetch stock details for a specific ticker from Polygon API.
    
    Args:
        ticker (str): The stock ticker symbol.
        api_key (str): API key for Polygon.io.
        date (str, optional): Specific date for retrieving ticker details.
    
    Returns:
        dict: Stock details information, or None if request fails.
    """
    url = f"{API_BASE_URL}/v3/reference/tickers/{ticker}"
    params = {"apiKey": api_key}
    
    if date:
        params["date"] = date

    try:
        response = requests.get(url, params=params)
        time.sleep(12)  # Respect API rate limits
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching ticker details for {ticker}: {e}")
        return None

def fetch_ohlc_stock_data(date: str, api_key: str) -> Optional[Dict]:
    """
    Fetch OHLC (Open, High, Low, Close) stock data for a given date.
    
    Args:
        date (str): The date for stock data in YYYY-MM-DD format.
        api_key (str): API key for Polygon.io.
    
    Returns:
        dict: Stock market OHLC data, or None if request fails.
    """  
    url = f"{API_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}"  
    params = {
        "apiKey": api_key,
        "adjusted": "true",
        "include_otc": "false"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching stock data: {e}")
        return None

def main():
    # Setup database manager
    db_manager = StockDataManager()
    
    try:
        # Retrieve the API key from environment
        api_key = get_api_key()

        # Fetch stock symbols
        stock_symbols = fetch_all_stock_symbols(api_key)
        logger.info(f"Found {len(stock_symbols)} stock symbols")
        
        # Fetch and save ticker details
        # for symbol in stock_symbols:
        #     details = fetch_stock_details(symbol, api_key)
        #     if details and details.get('results'):
        #         db_manager.insert_ticker_details(details['results'])

        # Get yesterday's date
        # date = (datetime.datetime.now() - datetime.timedelta(1)).strftime("%Y-%m-%d")

        # 5 free API requests in a min. Use time.sleep(12) in fetch_ohlc_stock_data function 
        # list1 = ['2023-01-24', '2023-01-25', '2023-01-26', '2023-01-27', '2023-01-28']
        # for date in list1:
        # # Fetch and save stock prices
        #     stock_prices = fetch_ohlc_stock_data(date, api_key)
        #     if stock_prices:
        #         db_manager.insert_stock_prices(stock_prices)
        #         logger.info(f"Successfully inserted stock price for {date}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        # print(db_manager.display_ticker_details())
        # print(db_manager.display_stock_prices())
        # Always close the database connection
        db_manager.close_connection()

if __name__ == "__main__":
    main()

