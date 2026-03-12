import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pathlib import Path
from logger_config import setup_logger

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / 'data' / 'stock_data.db'

class StockDataManager:
    def __init__(self, db_path_param: Optional[str] = None):
        db_path = db_path_param or DB_PATH
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.logger = setup_logger("StockDB")
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()
        self.logger.info(f"SQLite DB connected at {db_path}")
    
    def _create_tables(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS ticker_details (
            ticker TEXT PRIMARY KEY, active BOOLEAN, name TEXT, market TEXT, locale TEXT, 
            primary_exchange TEXT, type TEXT, currency_name TEXT, cik TEXT, description TEXT, 
            homepage_url TEXT, list_date TEXT, market_cap REAL, phone_number TEXT, 
            total_employees INTEGER, address1 TEXT, address2 TEXT, city TEXT, state TEXT, 
            postal_code TEXT, branding_icon_url TEXT, branding_logo_url TEXT, sic_code TEXT, 
            sic_description TEXT, ticker_root TEXT, ticker_suffix TEXT, weighted_shares_outstanding REAL)''')
        
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
            ticker TEXT, timestamp INTEGER, date TEXT, open REAL, high REAL, low REAL, 
            close REAL, volume REAL, transactions INTEGER, volume_weighted_avg REAL, 
            is_otc BOOLEAN, is_adjusted BOOLEAN, PRIMARY KEY (ticker, timestamp))''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS index_composition (
            id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL, ticker TEXT NOT NULL,
            close_price REAL NOT NULL, weight REAL NOT NULL, market_cap REAL, 
            index_type TEXT NOT NULL, update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (date, ticker, index_type, update_time))''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS index_performance (
            date TEXT NOT NULL, index_price REAL NOT NULL, daily_return REAL NOT NULL,
            index_type TEXT NOT NULL, update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, index_type, update_time))''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS index_composition_changes  (
            date TEXT NOT NULL, symbols TEXT NOT NULL, prev_date TEXT,    
            prev_symbols TEXT, update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, update_time))''')
    
    def insert_ticker_details(self, ticker_details: Dict):
        try:
            details = {
                'ticker': ticker_details.get('ticker'), 'active': ticker_details.get('active'),
                'name': ticker_details.get('name'), 'market': ticker_details.get('market'),
                'locale': ticker_details.get('locale'), 'primary_exchange': ticker_details.get('primary_exchange'),
                'type': ticker_details.get('type'), 'currency_name': ticker_details.get('currency_name'),
                'cik': ticker_details.get('cik'), 'description': ticker_details.get('description'),
                'homepage_url': ticker_details.get('homepage_url'), 'list_date': ticker_details.get('list_date'),
                'market_cap': ticker_details.get('market_cap'), 'phone_number': ticker_details.get('phone_number'),
                'total_employees': ticker_details.get('total_employees'), 'address1': ticker_details.get('address', {}).get('address1'),
                'address2': ticker_details.get('address', {}).get('address2'), 'city': ticker_details.get('address', {}).get('city'),
                'state': ticker_details.get('address', {}).get('state'), 'postal_code': ticker_details.get('address', {}).get('postal_code'),
                'branding_icon_url': ticker_details.get('branding', {}).get('icon_url'), 'branding_logo_url': ticker_details.get('branding', {}).get('logo_url'),
                'sic_code': ticker_details.get('sic_code'), 'sic_description': ticker_details.get('sic_description'),
                'ticker_root': ticker_details.get('ticker_root'), 'ticker_suffix': ticker_details.get('ticker_suffix'),
                'weighted_shares_outstanding': ticker_details.get('weighted_shares_outstanding')
            }
            
            self.cursor.execute('''INSERT OR REPLACE INTO ticker_details 
            (ticker, active, name, market, locale, primary_exchange, type, currency_name, cik, description, homepage_url, list_date, market_cap, phone_number,
             total_employees, address1, address2, city, state, postal_code, branding_icon_url, branding_logo_url, sic_code, sic_description, ticker_root, ticker_suffix, weighted_shares_outstanding)
            VALUES (:ticker, :active, :name, :market, :locale, :primary_exchange, :type, :currency_name, :cik, :description, :homepage_url, :list_date, :market_cap, :phone_number,
             :total_employees, :address1, :address2, :city, :state, :postal_code, :branding_icon_url, :branding_logo_url, :sic_code, :sic_description, :ticker_root, :ticker_suffix, :weighted_shares_outstanding)''', details)
            
            self.conn.commit()
            self.logger.debug(f"UPSERT successful for ticker_details: {details['ticker']}")
        except sqlite3.Error as e:
            self.logger.error(f"SQL Error UPSERTING ticker_details for {ticker_details.get('ticker')}: {e}")
            self.conn.rollback()

    def display_ticker_details(self, limit: int = 10) -> pd.DataFrame:
        try:
            return pd.read_sql_query(f"SELECT * FROM ticker_details LIMIT {limit}", self.conn)
        except Exception as e:
            self.logger.error(f"SQL Error reading ticker_details: {e}")
            return pd.DataFrame()
    
    def insert_stock_prices(self, stock_prices: Dict):
        try:
            price_data = []
            for stock in stock_prices.get('results', []):
                timestamp = stock.get('t')
                date = datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
                price_data.append((
                    stock.get('T'), timestamp, date, stock.get('o'), stock.get('h'), 
                    stock.get('l'), stock.get('c'), stock.get('v'), stock.get('n'), 
                    stock.get('vw'), stock.get('otc', False), stock_prices.get('adjusted', False)
                ))
            
            self.cursor.executemany('''INSERT OR REPLACE INTO stock_prices 
            (ticker, timestamp, date, open, high, low, close, volume, transactions, volume_weighted_avg, is_otc, is_adjusted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', price_data)
            self.conn.commit()
            self.logger.info(f"Bulk INSERT/REPLACE of {len(price_data)} records into stock_prices.")
        except sqlite3.Error as e:
            self.logger.error(f"SQL Error in insert_stock_prices batch execution: {e}")
            self.conn.rollback()
    
    def display_stock_prices(self, limit: int = 10) -> pd.DataFrame:
        try:
            return pd.read_sql_query(f"SELECT * FROM stock_prices LIMIT {limit}", self.conn)
        except Exception as e:
            self.logger.error(f"SQL Error reading stock_prices: {e}")
            return pd.DataFrame()

    def insert_or_update_index_composition(self, data: List[tuple]):
        try: 
            self.conn.executemany('''INSERT INTO index_composition (date, ticker, close_price, weight, market_cap, index_type)
            VALUES (?, ?, ?, ?, ?, ?)''', data)
            self.conn.commit()
            self.logger.info(f"Inserted {len(data)} records into index_composition.")
        except sqlite3.Error as e:
            self.logger.error(f"SQL Error in insert_or_update_index_composition: {e}")
            self.conn.rollback()

    def display_index_composition(self, date: Optional[str] = None, index_type: str = 'Equal Weighted') -> pd.DataFrame:
        try: 
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')

            query = """
            WITH RankedComposition AS (
                SELECT date, ticker, close_price, weight, market_cap, index_type,
                       ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY update_time DESC) as rn
                FROM index_composition WHERE date = ? AND index_type = ?
            )
            SELECT date, ticker, close_price, weight, market_cap, index_type
            FROM RankedComposition WHERE rn = 1 ORDER BY weight DESC
            """
            df = pd.read_sql_query(query, self.conn, params=(date, index_type))
            if not df.empty:
                self.logger.debug(f"Retrieved {len(df)} composition records for {index_type} on {date}.")
            else: 
                self.logger.debug(f"No records found in index_composition for {index_type} on {date}.")
            return df
        except Exception as e:
            self.logger.error(f"SQL Error querying index_composition: {e}")
            return pd.DataFrame()

    def insert_or_update_index_performance(self, data: List[tuple]):
        try: 
            self.conn.executemany("""INSERT INTO index_performance (date, index_price, daily_return, index_type)
            VALUES (?, ?, ?, ?)""", data)
            self.conn.commit()
            self.logger.info(f"Inserted {len(data)} records into index_performance.")
        except sqlite3.Error as e:
            self.logger.error(f"SQL Error in insert_or_update_index_performance: {e}")
            self.conn.rollback()

    def display_index_performance(self, start_date: Optional[str] = None, end_date: Optional[str] = None, index_type: str = 'Equal Weighted') -> pd.DataFrame:
        try: 
            if start_date is None: start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')

            query = """
                WITH RankedPerformance AS (
                    SELECT date, index_price, daily_return, index_type,
                           ROW_NUMBER() OVER(PARTITION BY date ORDER BY update_time DESC) as rn
                    FROM index_performance WHERE date BETWEEN ? AND ? AND index_type = ?
                )
                SELECT date, index_price, daily_return, index_type
                FROM RankedPerformance WHERE rn = 1 ORDER BY date ASC
            """
            df = pd.read_sql_query(query, self.conn, params=(start_date, end_date, index_type))
            if not df.empty:
                df['daily_return'] = df['index_price'].pct_change().fillna(0) * 100
            return df
        except Exception as e:
            self.logger.error(f"SQL Error querying index_performance: {e}")
            return pd.DataFrame()
        
    def insert_or_update_index_composition_changes(self, data: List[tuple]):
        try:
            self.conn.executemany("""INSERT INTO index_composition_changes (date, symbols, prev_date, prev_symbols)
            VALUES (?, ?, ?, ?)""", data)
            self.conn.commit()
            self.logger.info(f"Inserted {len(data)} records into index_composition_changes.")
        except sqlite3.Error as e:
            self.logger.error(f"SQL Error in insert_or_update_index_composition_changes: {e}")
            self.conn.rollback()

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        try:
            if params is None: return pd.read_sql_query(query, self.conn)
            return pd.read_sql_query(query, self.conn, params=params)
        except sqlite3.Error as e:
            self.logger.error(f"Raw query execution failed. Query: {query} | Error: {e}")
            return pd.DataFrame()
    
    def close_connection(self):
        try:
            self.conn.close()
            self.logger.info("SQLite connection closed.")
        except Exception as e:
            self.logger.error(f"Error closing SQLite connection: {e}")