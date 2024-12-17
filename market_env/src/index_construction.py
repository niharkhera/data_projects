import pandas as pd
from datetime import datetime, timedelta
import logging
from save_db import StockDataManager


class IndexConstructor:
    def __init__(self, db_path=None):
        """
        Initialize Index Constructor with database manager.
        
        Args:
            db_path (str): Path to SQLite database
        """
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(levelname)s: %(message)s')
        self.logger = logging.getLogger(__name__)
        self.db_manager = StockDataManager(db_path)

    def construct_equal_weighted_index(self, date=None, top_n=100):
        """
        Construct an equal-weighted index for a given date.
        
        Args:
            date (str, optional): Date in 'YYYY-MM-DD' format
            top_n (int): Number of stocks in the index
        
        Returns:
            pandas.DataFrame: Index composition with weights
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        query = f"""
        SELECT td.ticker,
        sp.close as close_price
        FROM ticker_details td
        JOIN stock_prices sp ON td.ticker = sp.ticker
        WHERE sp.date = ? AND td.market_cap IS NOT NULL
        ORDER BY td.market_cap DESC
        LIMIT {top_n}
        """
        top_stocks = self.db_manager.execute_query(query, (date,))
        # print(top_stocks)
        
        if top_stocks.empty:
            self.logger.warning(f"No stock data found for date: {date}")
            return pd.DataFrame()

        # Calculate equal weights
        total_stocks = len(top_stocks)
        equal_weight = 1 / total_stocks
        top_stocks['weight'] = equal_weight
        top_stocks['date'] = date

        # Convert DataFrame to list of tuples
        data = list(top_stocks[['ticker', 'close_price', 'weight', 'date']].itertuples(index=False, name=None))

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
                    select ic.ticker, ic.close_price, ic.weight, ic.date, max(ic.update_time)
                    from index_composition ic
                    group by ic.ticker, ic.close_price, ic.weight, ic.date
            ) ic ON sp.ticker = ic.ticker AND sp.date = ic.date
            WHERE sp.date BETWEEN ? AND ?
            GROUP BY sp.date
            ORDER BY sp.date
        """

        daily_prices = self.db_manager.execute_query(query, (start_date,end_date,))



        # # Step 2: Calculate daily returns manually
        daily_prices['daily_return'] = daily_prices['index_price'].pct_change() * 100
        
        data = list(daily_prices[['date', 'index_price', 'daily_return']].itertuples(index=False, name=None))

        # Insert or update index composition
        self.db_manager.insert_or_update_index_performance(data)

        return daily_prices


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
        WITH daily_composition AS (
            SELECT date, GROUP_CONCAT(ticker) AS symbols
            FROM index_composition
            WHERE date BETWEEN ? AND ?
            GROUP BY date
        )
        SELECT 
            a.date, 
            a.symbols,
            b.symbols AS prev_symbols
        FROM daily_composition a
        LEFT JOIN daily_composition b
            ON a.date = DATE(b.date, '+1 day')
        WHERE a.symbols != b.symbols
        """
        changes = self.db_manager.execute_query(query, (start_date, end_date))
        
        # Format the results into a list of tuples for insertion
        # data = list(daily_prices[['date', 'index_price', 'daily_return']].itertuples(index=False, name=None))
        data = list(changes[['date', 'symbols', 'prev_symbols']].itertuples(index=False, name=None))
        
        # Insert or update the detected changes into the database
        self.db_manager.insert_or_update_index_composition_changes(data)

        # Return the changes as a DataFrame for further use (optional)
        return (changes)

    def close(self):
        """Close the database connection."""
        self.db_manager.close_connection()


# Example usage
if __name__ == '__main__':
    index_constructor = IndexConstructor()
    
    # # Construct daily index
    daily_index = index_constructor.construct_equal_weighted_index('2023-01-28')
    # # print("\nDaily Index Composition:\n", daily_index)

    # Track performance
    performance = index_constructor.track_index_performance('2023-01-01','2023-01-30')
    # print("\nIndex Performance:\n", performance)

    # Detect composition changes
    changes = index_constructor.detect_index_changes('2023-01-01','2023-01-30')
    # print("\nIndex Composition Changes:\n", changes)
    
    index_constructor.close()
