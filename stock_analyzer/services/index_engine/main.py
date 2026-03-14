# services/index_engine/main.py

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Allow importing from the shared directory
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from shared.db import DatabaseManager
from shared.logger_config import setup_logger

logger = setup_logger("IndexEngine")

class IndexConstructor:
    def __init__(self):
        self.db = DatabaseManager()

    def construct_equal_weighted_index(self, date: str, top_n: int = 5) -> pd.DataFrame:
        query = """
        SELECT td.market_cap, td.ticker, sp.close as close_price
        FROM ticker_details td 
        JOIN stock_prices sp ON td.ticker = sp.ticker
        WHERE sp.date = %s AND td.market_cap IS NOT NULL 
        ORDER BY td.market_cap DESC LIMIT %s
        """
        # .copy() prevents ChainedAssignmentErrors in Pandas 3.0+
        top_stocks = self.db.execute_query(query, (date, top_n)).copy()

        if top_stocks.empty:
            logger.warning(f"EW Construction aborted: No price/metadata overlap found for {date}.")
            return pd.DataFrame()

        # Using .loc for explicit assignment
        equal_weight = 1 / len(top_stocks)
        top_stocks.loc[:, 'weight'] = equal_weight
        top_stocks.loc[:, 'date'] = date
        top_stocks.loc[:, 'index_type'] = 'Equal Weighted'

        self._bulk_insert_composition(top_stocks)
        logger.info(f"Constructed Equal-Weighted index for {date} ({len(top_stocks)} constituents).")
        return top_stocks

    def construct_market_cap_weighted_index(self, date: str, top_n: int = 5) -> pd.DataFrame:
        query = """
        SELECT td.market_cap, td.ticker, sp.close as close_price
        FROM ticker_details td 
        JOIN stock_prices sp ON td.ticker = sp.ticker
        WHERE sp.date = %s AND td.market_cap IS NOT NULL 
        ORDER BY td.market_cap DESC LIMIT %s
        """
        top_stocks = self.db.execute_query(query, (date, top_n)).copy()

        if top_stocks.empty:
            logger.warning(f"MCW Construction aborted: No price/metadata overlap found for {date}.")
            return pd.DataFrame()

        total_market_cap = top_stocks['market_cap'].sum()
        
        # Using .loc to ensure we are modifying the copy properly
        top_stocks.loc[:, 'weight'] = top_stocks['market_cap'] / total_market_cap
        top_stocks.loc[:, 'date'] = date
        top_stocks.loc[:, 'index_type'] = 'Market-Cap Weighted'

        self._bulk_insert_composition(top_stocks)
        logger.info(f"Constructed Market-Cap Weighted index for {date} ({len(top_stocks)} constituents).")
        return top_stocks

    def _bulk_insert_composition(self, df: pd.DataFrame):
        """Helper to insert index composition data into PostgreSQL."""
        insert_query = """
            INSERT INTO index_composition (date, ticker, close_price, weight, market_cap, index_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, ticker, index_type, update_time) DO NOTHING;
        """
        for _, row in df.iterrows():
            params = (row['date'], row['ticker'], row['close_price'], row['weight'], row['market_cap'], row['index_type'])
            self.db.execute_write(insert_query, params)


def seed_test_ticker_details():
    """Fetches 5 mega-cap stocks from Polygon to ensure the SQL JOIN succeeds for testing."""
    load_dotenv()
    api_key = os.getenv("POLYGON_API_KEY")
    db = DatabaseManager()
    tickers = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]
    
    logger.info("Seeding test metadata for JOIN dependencies...")
    for ticker in tickers:
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get('results', {})
            query = """
                INSERT INTO ticker_details (ticker, active, name, market, market_cap)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET market_cap = EXCLUDED.market_cap;
            """
            params = (data.get('ticker'), data.get('active'), data.get('name'), data.get('market'), data.get('market_cap'))
            db.execute_write(query, params)
    logger.info("Metadata seeding complete.")

if __name__ == "__main__":
    engine = IndexConstructor()
    
    try:
        # 1. Seed the metadata table
        seed_test_ticker_details()
        
        # 2. Get target date
        today = datetime.now()
        if today.weekday() == 5: target_date = today - timedelta(days=1)
        elif today.weekday() == 6: target_date = today - timedelta(days=2)
        elif today.weekday() == 0: target_date = today - timedelta(days=3)
        else: target_date = today - timedelta(days=1)
        
        date_str = target_date.strftime("%Y-%m-%d")
        
        # 3. Run the engine
        logger.info(f"Triggering Engine for date: {date_str}")
        
        ew_df = engine.construct_equal_weighted_index(date_str, top_n=5)
        mcw_df = engine.construct_market_cap_weighted_index(date_str, top_n=5)
        
        if not ew_df.empty:
            print("\n--- Equal Weighted Output ---")
            print(ew_df[['ticker', 'weight', 'close_price']])

        if not mcw_df.empty:
            print("\n--- Market-Cap Weighted Output ---")
            print(mcw_df[['ticker', 'weight', 'market_cap']])

    finally:
        if hasattr(engine.db, '_connection_pool') and engine.db._connection_pool:
            logger.info("Closing database connection pool...")
            # Change .closeall() to .close() for psycopg 3
            engine.db._connection_pool.close()