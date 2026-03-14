# services/ingestion/main.py

import os
import sys
import json
import time
import datetime
import requests
from pathlib import Path
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from apscheduler.schedulers.blocking import BlockingScheduler

# Allow importing from the shared directory
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.db import DatabaseManager
from shared.logger_config import setup_logger

logger = setup_logger("IngestionService")

class PolygonRateLimitError(Exception):
    pass

class StockDataIngester:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("POLYGON_API_KEY")
        self.db = DatabaseManager()
        self.api_base_url = "https://api.polygon.io"
        self.dlq_path = Path(__file__).resolve().parent / "dead_letter.jsonl"
        
        if not self.api_key or self.api_key == "your_actual_polygon_api_key_here":
            logger.error("POLYGON_API_KEY is missing or invalid.")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), retry=retry_if_exception_type(requests.exceptions.RequestException))
    def fetch_symbols(self, limit: int = 1000) -> list:
        """Fetches active stock symbols with exponential backoff on network failures."""
        url = f"{self.api_base_url}/v3/reference/tickers"
        params = {"market": "stocks", "active": "true", "limit": limit, "apiKey": self.api_key}
        
        logger.info(f"Fetching up to {limit} ticker symbols from Polygon...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        return [ticker['ticker'] for ticker in response.json().get('results', [])]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=12, max=60))
    def fetch_ohlcv(self, date: str) -> dict:
        """Fetches OHLCV data. Backs off significantly if rate limited."""
        logger.info(f"Rate Limiting: Sleeping 12s before fetching OHLCV for {date}")
        time.sleep(12) 
        
        url = f"{self.api_base_url}/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {"apiKey": self.api_key, "adjusted": "true"}
        response = requests.get(url, params=params)
        
        if response.status_code == 429:
            logger.warning("Polygon Rate Limit Hit! Triggering Tenacity backoff.")
            raise PolygonRateLimitError("Rate limited by Polygon.")
            
        response.raise_for_status()
        return response.json()

    def write_to_dlq(self, date: str, payload: dict):
        """Saves failed payloads to a Dead Letter Queue file to prevent data loss."""
        with open(self.dlq_path, "a") as f:
            dlq_record = {"date": date, "timestamp": datetime.datetime.now().isoformat(), "data": payload}
            f.write(json.dumps(dlq_record) + "\n")
        logger.warning(f"Data for {date} written to Dead Letter Queue (DLQ).")

    def insert_stock_prices(self, date: str, stock_prices: dict):
        """Translates Python dicts to Postgres SQL and executes the write."""
        results = stock_prices.get('results', [])
        if not results:
            logger.info(f"No price results found for {date}.")
            return

        query = """
            INSERT INTO stock_prices (ticker, timestamp, date, open, high, low, close, volume, transactions, volume_weighted_avg, is_otc, is_adjusted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, timestamp) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                close = EXCLUDED.close, volume = EXCLUDED.volume, transactions = EXCLUDED.transactions,
                volume_weighted_avg = EXCLUDED.volume_weighted_avg;
        """
        
        success_count = 0
        for stock in results:
            timestamp = stock.get('t')
            params = (
                stock.get('T'), timestamp, date, stock.get('o'), stock.get('h'), 
                stock.get('l'), stock.get('c'), stock.get('v'), stock.get('n'), 
                stock.get('vw'), stock.get('otc', False), stock_prices.get('adjusted', False)
            )
            # Utilizing the shared DatabaseManager
            if self.db.execute_write(query, params):
                success_count += 1
            else:
                self.write_to_dlq(date, {"ticker": stock.get('T'), "payload": stock})
                
        logger.info(f"Successfully UPSERTED {success_count}/{len(results)} price records for {date}.")

def run_daily_ingestion():
    """The main job triggered by the scheduler."""
    logger.info("--- Starting Daily Ingestion Job ---")
    ingester = StockDataIngester()
    
    # Calculate target date (yesterday, skipping weekends)
    today = datetime.datetime.now()
    if today.weekday() == 5: target_date = today - datetime.timedelta(days=1)
    elif today.weekday() == 6: target_date = today - datetime.timedelta(days=2)
    else:
        target_date = today - datetime.timedelta(days=1)
        if target_date.weekday() == 6: target_date = target_date - datetime.timedelta(days=2)

    date_str = target_date.strftime("%Y-%m-%d")
    
    try:
        prices = ingester.fetch_ohlcv(date_str)
        if prices:
            ingester.insert_stock_prices(date_str, prices)
    except Exception as e:
        logger.error(f"Ingestion job failed for {date_str}: {e}")

if __name__ == "__main__":
    logger.info("Ingestion Service Booting Up...")
    
    # Run once immediately on startup
    run_daily_ingestion()
    
    # Then schedule to run every day at 18:00 (6 PM)
    scheduler = BlockingScheduler()
    scheduler.add_job(run_daily_ingestion, 'cron', hour=18, minute=0)
    logger.info("Scheduler configured. Waiting for next run at 18:00...")
    scheduler.start()