import sqlite3
import os
import logging
import pandas as pd
import time
from datetime import datetime 
from typing import List, Dict, Optional


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DB_PATH = os.path.join(PROJECT_ROOT, 'market_env', 'db' , 'stock_data.db')
print("PROJECT_ROOT:", PROJECT_ROOT)
print("DB_PATH:", DB_PATH)


class StockDataManager:
    def __init__(self, db_path_param=None):
        """
        Initialize the database connection and create necessary tables.
        
        Args:
            db_path (str): Path to the SQLite database file
        """

        db_path = db_path_param or DB_PATH
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Database connection
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        
        # Create tables if they don't exist
        self._create_tables()
    
    def _create_tables(self):
        """
        Create necessary tables for stock data storage.
        """
        # Ticker Details Table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS ticker_details (
            ticker TEXT PRIMARY KEY,
            active BOOLEAN,
            name TEXT,
            market TEXT,
            locale TEXT,
            primary_exchange TEXT,
            type TEXT,
            currency_name TEXT,
            cik TEXT,
            description TEXT,
            homepage_url TEXT,
            list_date TEXT,
            market_cap REAL,
            phone_number TEXT,
            total_employees INTEGER,
            address1 TEXT,
            address2 TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            branding_icon_url TEXT,
            branding_logo_url TEXT,
            sic_code TEXT,
            sic_description TEXT,
            ticker_root TEXT,
            ticker_suffix TEXT,
            weighted_shares_outstanding REAL
        )
        ''')
        
        # Stock Prices Table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_prices (
            ticker TEXT,
            timestamp INTEGER,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            transactions INTEGER,
            volume_weighted_avg REAL,
            is_otc BOOLEAN,
            is_adjusted BOOLEAN,
            PRIMARY KEY (ticker, timestamp)
        )
        ''')

        #Index Composition Table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_composition (
            id INTEGER AUTO INCREMENT,
            ticker TEXT NOT NULL,
            close_price REAL NOT NULL, 
            weight REAL NOT NULL,
            date TEXT NOT NULL,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, date, ticker, update_time)
        ) 
        ''')

        #Index Performance Table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_performance (
            date TEXT NOT NULL,
            index_price REAL NOT NULL,
            daily_return REAL NOT NULL,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, update_time)
            )
        ''')

        #Index composition changes table
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS index_composition_changes  (
            date TEXT NOT NULL,
            symbols TEXT NOT NULL,
            prev_symbols TEXT,
            update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, update_time)
            )
        ''')
    
    def insert_ticker_details(self, ticker_details: Dict):
        """
        Insert ticker details into the database.
        
        Args:
            ticker_details (Dict): Dictionary containing ticker details
        """
        try:
            # Extract relevant details
            details = {
                'ticker': ticker_details.get('ticker'),
                'active': ticker_details.get('active'),
                'name': ticker_details.get('name'),
                'market': ticker_details.get('market'),
                'locale': ticker_details.get('locale'),
                'primary_exchange': ticker_details.get('primary_exchange'),
                'type': ticker_details.get('type'),
                'currency_name': ticker_details.get('currency_name'),
                'cik': ticker_details.get('cik'),
                'description': ticker_details.get('description'),
                'homepage_url': ticker_details.get('homepage_url'),
                'list_date': ticker_details.get('list_date'),
                'market_cap': ticker_details.get('market_cap'),
                'phone_number': ticker_details.get('phone_number'),
                'total_employees': ticker_details.get('total_employees'),
                'address1': ticker_details.get('address', {}).get('address1'),
                'address2': ticker_details.get('address', {}).get('address2'),
                'city': ticker_details.get('address', {}).get('city'),
                'state': ticker_details.get('address', {}).get('state'),
                'postal_code': ticker_details.get('address', {}).get('postal_code'),
                'branding_icon_url': ticker_details.get('branding', {}).get('icon_url'),
                'branding_logo_url': ticker_details.get('branding', {}).get('logo_url'),
                'sic_code': ticker_details.get('sic_code'),
                'sic_description': ticker_details.get('sic_description'),
                'ticker_root': ticker_details.get('ticker_root'),
                'ticker_suffix': ticker_details.get('ticker_suffix'),
                'weighted_shares_outstanding': ticker_details.get('weighted_shares_outstanding')
            }
            
            
            # Insert or replace existing record
            self.cursor.execute('''
            INSERT OR REPLACE INTO ticker_details 
            (ticker, active, name, market, locale, primary_exchange, type, 
             currency_name, cik, description, homepage_url, list_date, market_cap, phone_number,
             total_employees, address1, address2, city, state, postal_code, branding_icon_url, 
             branding_logo_url, sic_code, sic_description, ticker_root, ticker_suffix, 
             weighted_shares_outstanding
             )
            VALUES 
            (:ticker, :active, :name, :market, :locale, :primary_exchange, :type, 
             :currency_name, :cik, :description, :homepage_url, :list_date, :market_cap, :phone_number,
             :total_employees, :address1, :address2, :city, :state, :postal_code, :branding_icon_url, 
             :branding_logo_url, :sic_code, :sic_description, :ticker_root, :ticker_suffix, 
             :weighted_shares_outstanding
             )
            ''', details)
            
            self.conn.commit()
            self.logger.info(f"Inserted details for {details['ticker']}")
        
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting ticker details: {e}")
            self.conn.rollback()
    
    def insert_stock_prices(self, stock_prices: List[Dict]):
        """
        Insert stock price data into the database.
        
        Args:
            stock_prices (List[Dict]): List of dictionaries containing stock price data
        """
        try:
            # Prepare data for bulk insert
            price_data = []
            for stock in stock_prices.get('results', []):
                timestamp = stock.get('t')
                date = datetime.utcfromtimestamp(timestamp/1000).strftime('%Y-%m-%d')

                price_data.append((
                stock.get('T'),       # ticker
                timestamp, 
                date, 
                stock.get('o'),        # open
                stock.get('h'),        # high
                stock.get('l'),        # low
                stock.get('c'),        # close
                stock.get('v'),        # volume
                stock.get('n'),        # number of transactions
                stock.get('vw'),       # volume weighted average
                stock.get('otc', False),  # OTC flag
                stock_prices.get('adjusted', False)  # adjusted flag
                ))
            
            # Bulk insert
            self.cursor.executemany('''
            INSERT OR REPLACE INTO stock_prices 
            (ticker, timestamp, date, open, high, low, close, volume, 
            transactions, volume_weighted_avg, is_otc, is_adjusted)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', price_data)
            
            self.conn.commit()
            self.logger.info(f"Inserted {len(price_data)} stock price records")
        
        except sqlite3.Error as e:
            self.logger.error(f"Error inserting stock prices: {e}")
            self.conn.rollback()

    def display_ticker_details(self, limit: int = 10) -> pd.DataFrame:
        """
        Display ticker details with optional limit.
        
        Args:
            limit (int): Number of records to display
        
        Returns:
            pandas.DataFrame: DataFrame with ticker details
        """
        try:
            query = f"SELECT * FROM ticker_details LIMIT {limit}"
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error displaying ticker details: {e}")
            return pd.DataFrame()
    
    def display_stock_prices(self, limit: int = 10) -> pd.DataFrame:
        """
        Display stock prices with optional limit.
        
        Args:
            limit (int): Number of records to display
        
        Returns:
            pandas.DataFrame: DataFrame with stock prices
        """
        try:
            query = f"SELECT * FROM stock_prices LIMIT {limit}"
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"Error displaying stock prices: {e}")
            return pd.DataFrame()

    def insert_or_update_index_composition(self, data):
        """
        Inserts or updates the index_composition table with new data.

        Args:
            data (list of tuples): Each tuple contains (ticker, close_price, weight, date).
        """

        try: 
            query = '''
            INSERT OR REPLACE INTO index_composition (ticker, close_price, weight, date)
            VALUES (?, ?, ?, ?)
            '''
            self.conn.executemany(query, data)
            self.conn.commit()
            self.logger.info(f"Inserted index composition records for date")

        except sqlite3.Error as e:
            self.logger.error(f"Error inserting index composition records: {e}")
            self.conn.rollback()


    def insert_or_update_index_performance(self, data):
        """
        Inserts or updates the index_performance table with new data.

        Args:
            data (list of tuples): Each tuple contains (date, index_price, daily_return).
        """
        try: 
            query = """
            INSERT INTO index_performance (date, index_price, daily_return)
            VALUES (?, ?, ?)
            """
            self.conn.executemany(query, data)
            self.conn.commit()
            self.logger.info(f"Inserted index performance records")

        except sqlite3.Error as e:
            self.logger.error(f"Error inserting index performance records: {e}")
            self.conn.rollback()
        
    def insert_or_update_index_composition_changes(self, data):
        """
        Inserts or updates the index_composition_changes table with new data.

        Args:
            data (list of tuples): Each tuple contains (date, symbols, prev_symbols).
        """
        try:
            # SQL query to insert or update index composition changes
            query = """
            INSERT INTO index_composition_changes (date, symbols, prev_symbols)
            VALUES (?, ?, ?)
            """
            
            # Execute the batch insert operation
            self.conn.executemany(query, data)
            self.conn.commit()
            self.logger.info(f"Inserted index composition change records.")

        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(f"Error inserting index composition change records: {e}")



    def display_index_composition(self):
        """
        Display all records from the index_composition table.
        """
        query = "SELECT * FROM index_composition;"
        return pd.read_sql_query(query, self.conn)

    def display_index_performance(self):
        """
        Fetches all records from the index_performance table.
        """
        query = "SELECT * FROM index_performance;"
        return pd.read_sql_query(query, self.conn)


    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Execute a custom SQL query and return results as a pandas DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for parameterized queries
        
        Returns:
            pandas.DataFrame: Query results
        """
        try:
            # If no params provided, execute without parameters
            if params is None:
                return pd.read_sql_query(query, self.conn)
            
            # Execute parameterized query
            return pd.read_sql_query(query, self.conn, params=params)
        
        except sqlite3.Error as e:
            self.logger.error(f"Error executing query: {e}")
            return pd.DataFrame()

    
    def close_connection(self):
        """
        Close database connection.
        """
        self.conn.close()
        self.logger.info("Database connection closed")