import pandas as pd
from datetime import datetime, timedelta
import logging
from save_db import *
from fetch_data import *

class IndexConstructor:
    def __init__(self, db_path=None):
        """
        Initialize Index Constructor with database manager and data fetcher.
        
        Args:
            db_path (str): Path to SQLite database
        """
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)
        self.db_manager = StockDataManager(db_path)
        self.data_fetcher = StockDataFetcher()  # Initialize StockDataFetcher

    def construct_equal_weighted_index(self, date=None, top_n=100):
        """
        Construct an equal-weighted index for a given date.
        
        Args:
            date (str, optional): Date in 'YYYY-MM-DD' format
            top_n (int): Number of stocks in the index
        
        Returns:
            pandas.DataFrame: Index composition with weights
        """
        query_date = date or datetime.now().strftime('%Y-%m-%d')

        query = f"""
        SELECT td.market_cap, 
        td.ticker,
        sp.close as close_price
        FROM ticker_details td
        JOIN stock_prices sp ON td.ticker = sp.ticker
        WHERE sp.date = ? AND td.market_cap IS NOT NULL
        ORDER BY td.market_cap DESC
        LIMIT {top_n}
        """
        
        #Use StockDataFetcher to fetch OHLC data and save it
        try:
            self.logger.info(f"INFO: Fetching stock price data for {query_date}")
            stock_prices = self.data_fetcher.fetch_ohlc_stock_data(query_date)
            if stock_prices:
                self.db_manager.insert_stock_prices(stock_prices)
            else:
                self.logger.warning(f"WARN: No new stock price data fetched for {query_date}")
        except Exception as e:
            self.logger.error(f"ERROR: Error fetching stock price data: {e}")
            return pd.DataFrame() # Return an empty DataFrame in case of an error

        # Run the query to include any newly inserted data
        top_stocks = self.db_manager.execute_query(query, (query_date,))

        if top_stocks.empty:
            self.logger.warning(f"WARN: No stock price data available after API fetch for date: {query_date}. Cannot construct index.")
            return pd.DataFrame()  # Return an empty DataFrame if no data is available

        # Calculate equal weights 
        equal_weight = 1 / len(top_stocks)
        top_stocks['weight'] = equal_weight
        top_stocks['date'] = query_date

        # Convert DataFrame to list of tuples
        data = list(top_stocks[['date', 'ticker', 'close_price', 'weight', 'market_cap']].itertuples(index=False, name=None))

        # Insert or update index composition
        self.db_manager.insert_or_update_index_composition(data)

        return top_stocks

    def track_index_performance(self, start_date=None, end_date=None):
        """
        Track the performance of the constructed index.

        Args:
            start_date (str, optional): Start date of performance tracking
            end_date (str, optional): End date of performance tracking

        Returns:
            pandas.DataFrame: Index performance metrics
        """
        try: 
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')

            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')

            # Step 1: Fetch daily index prices
            query = """
                SELECT
                    sp.date AS date,
                    SUM(ic.weight * sp.close) AS index_price
                FROM stock_prices sp
                JOIN (
                        select ic.date, ic.ticker, ic.close_price, ic.weight, ic.market_cap, max(ic.update_time)
                        from index_composition ic
                        group by ic.date, ic.ticker, ic.close_price, ic.weight, ic.market_cap
                ) ic ON sp.ticker = ic.ticker AND sp.date = ic.date
                WHERE sp.date BETWEEN ? AND ?
                GROUP BY sp.date
                ORDER BY sp.date    
            """

            daily_prices = self.db_manager.execute_query(query, (start_date,end_date,))
            if daily_prices is None or daily_prices.empty:
                logger.warning(f"WARN: No data available for index performance between {start_date} and {end_date}.")
                return pd.DataFrame(columns=['date', 'index_price', 'daily_return'])

            # # Step 2: Calculate daily returns manually
            daily_prices['daily_return'] = daily_prices['index_price'].pct_change() * 100

            # Fill NaN values for the first row (first day) with 0 or another default value
            daily_prices['daily_return'].fillna(0, inplace=True)

            
            data = list(daily_prices[['date', 'index_price', 'daily_return']].itertuples(index=False, name=None))

            # Insert or update index performance
            self.db_manager.insert_or_update_index_performance(data)

            return daily_prices

        except Exception as e:
            # Log the exception and return an empty DataFrame
            logger.error(f"ERROR: An error occurred while tracking index performance: {e}", exc_info=True)
            return pd.DataFrame(columns=['date', 'index_price', 'daily_return'])


    def detect_index_changes(self, start_date=None, end_date=None):
        """
        Detect changes in index composition.
        
        Args:
            start_date (str, optional): Start date of change tracking
            end_date (str, optional): End date of change tracking
        
        Returns:
            pandas.DataFrame: Days with index composition changes
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        query = """

            WITH latest_index_composition AS (
            SELECT  
                date, 
                ticker, 
                close_price, 
                market_cap, 
                weight, 
                max(update_time) as latest_update_time 
            FROM index_composition
            GROUP BY date, ticker, close_price, weight, market_cap
                    ),
            daily_composition as (
            SELECT 
                date,
                GROUP_CONCAT(ticker) AS symbols 
            FROM latest_index_composition
            GROUP BY date
                    ),
            changes as (
            SELECT 
                dc.date,
                dc.symbols,
                    (SELECT date 
                    FROM daily_composition AS prev 
                    WHERE prev.date < dc.date 
                    ORDER BY prev.date DESC 
                    LIMIT 1) AS prev_date,
                    (SELECT symbols 
                    FROM daily_composition AS prev 
                    WHERE prev.date < dc.date 
                    ORDER BY prev.date DESC 
                    LIMIT 1) AS prev_symbols
            FROM daily_composition dc
            )
            SELECT *
            FROM changes
            WHERE (symbols != prev_symbols OR prev_symbols IS NULL)
            AND date BETWEEN ? and ? 

        """
        changes = self.db_manager.execute_query(query, (start_date, end_date))

        # Format the results into a list of tuples for insertion
        # data = list(daily_prices[['date', 'index_price', 'daily_return']].itertuples(index=False, name=None))
        data = list(changes[['date', 'symbols', 'prev_date', 'prev_symbols']].itertuples(index=False, name=None))
        
        # Insert or update the detected changes into the database
        self.db_manager.insert_or_update_index_composition_changes(data)

        # Return the changes as a DataFrame for further use (optional)
        return (changes)

    def close_db_connection(self):
        """Close the database connection."""
        self.db_manager.close_connection()


# Example usage
if __name__ == '__main__':
    index_constructor = IndexConstructor()
    
    # # Construct daily index
    daily_index = index_constructor.construct_equal_weighted_index('2023-01-31')
    print("\nDaily Index Composition:\n", daily_index)

    # # Track performance
    performance = index_constructor.track_index_performance('2024-12-22','2024-12-22')
    # print("\nIndex Performance:\n", performance)

    # # # Detect composition changes
    changes = index_constructor.detect_index_changes('2023-01-01','2024-01-30')
    print("\nIndex Composition Changes:\n", changes)
    
    index_constructor.close_db_connection()
