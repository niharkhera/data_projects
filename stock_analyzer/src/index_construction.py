import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from save_db import StockDataManager
from logger_config import setup_logger

class IndexConstructor:
    def __init__(self, db_path: Optional[str] = None):
        self.logger = setup_logger("IndexEngine")
        self.db_manager = StockDataManager(db_path)

    def construct_equal_weighted_index(self, date: Optional[str] = None, top_n: int = 100) -> pd.DataFrame:
        query_date = date or datetime.now().strftime('%Y-%m-%d')
        query = """
        SELECT td.market_cap, td.ticker, sp.close as close_price
        FROM ticker_details td JOIN stock_prices sp ON td.ticker = sp.ticker
        WHERE sp.date = ? AND td.market_cap IS NOT NULL ORDER BY td.market_cap DESC LIMIT ?
        """
        top_stocks = self.db_manager.execute_query(query, (query_date, top_n))

        if top_stocks.empty:
            self.logger.warning(f"Construction aborted: Missing dependency data for {query_date}.")
            return pd.DataFrame()

        equal_weight = 1 / len(top_stocks)
        top_stocks['weight'] = equal_weight
        top_stocks['date'] = query_date
        top_stocks['index_type'] = 'Equal Weighted'

        data = list(top_stocks[['date', 'ticker', 'close_price', 'weight', 'market_cap', 'index_type']].itertuples(index=False, name=None))
        self.db_manager.insert_or_update_index_composition(data)
        self.logger.info(f"Constructed Equal-Weighted index for {query_date} ({len(top_stocks)} constituents).")
        return top_stocks

    def construct_market_cap_weighted_index(self, date: Optional[str] = None, top_n: int = 100) -> pd.DataFrame:
        query_date = date or datetime.now().strftime('%Y-%m-%d')
        query = """
        SELECT td.market_cap, td.ticker, sp.close as close_price
        FROM ticker_details td JOIN stock_prices sp ON td.ticker = sp.ticker
        WHERE sp.date = ? AND td.market_cap IS NOT NULL ORDER BY td.market_cap DESC LIMIT ?
        """
        top_stocks = self.db_manager.execute_query(query, (query_date, top_n))

        if top_stocks.empty:
            self.logger.warning(f"Construction aborted: Missing dependency data for {query_date}.")
            return pd.DataFrame()

        total_market_cap = top_stocks['market_cap'].sum()
        top_stocks['weight'] = top_stocks['market_cap'] / total_market_cap
        top_stocks['date'] = query_date
        top_stocks['index_type'] = 'Market-Cap Weighted'

        data = list(top_stocks[['date', 'ticker', 'close_price', 'weight', 'market_cap', 'index_type']].itertuples(index=False, name=None))
        self.db_manager.insert_or_update_index_composition(data)
        self.logger.info(f"Constructed Market-Cap Weighted index for {query_date} ({len(top_stocks)} constituents).")
        return top_stocks

    def track_index_performance(self, start_date: Optional[str] = None, end_date: Optional[str] = None, index_type: str = 'Equal Weighted') -> pd.DataFrame:
        if start_date is None: start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')

        query = """
            SELECT sp.date AS date, SUM(ic.weight * sp.close) AS index_price
            FROM stock_prices sp JOIN (
                SELECT ticker, date, weight FROM (
                    SELECT ticker, date, weight, ROW_NUMBER() OVER(PARTITION BY date, ticker ORDER BY update_time DESC) as rn
                    FROM index_composition WHERE index_type = ?
                ) WHERE rn = 1
            ) ic ON sp.ticker = ic.ticker AND sp.date = ic.date
            WHERE sp.date BETWEEN ? AND ? GROUP BY sp.date ORDER BY sp.date    
        """
        daily_prices = self.db_manager.execute_query(query, (index_type, start_date, end_date))
        
        if daily_prices is None or daily_prices.empty:
            self.logger.info(f"Performance calculation yielded empty set for {index_type} between {start_date} and {end_date}.")
            return pd.DataFrame(columns=['date', 'index_price', 'daily_return', 'index_type'])

        daily_prices['daily_return'] = daily_prices['index_price'].pct_change().fillna(0) * 100
        daily_prices['index_type'] = index_type
        
        data = list(daily_prices[['date', 'index_price', 'daily_return', 'index_type']].itertuples(index=False, name=None))
        self.db_manager.insert_or_update_index_performance(data)
        self.logger.info(f"Tracked performance history generated for {len(daily_prices)} days.")
        return daily_prices

    def detect_index_changes(self, start_date: Optional[str] = None, end_date: Optional[str] = None, index_type: str = 'Equal Weighted') -> pd.DataFrame:
        if start_date is None: start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None: end_date = datetime.now().strftime('%Y-%m-%d')

        query = """
            WITH RankedComposition AS (
                SELECT date, ticker FROM (
                    SELECT date, ticker, ROW_NUMBER() OVER(PARTITION BY date, ticker ORDER BY update_time DESC) as rn
                    FROM index_composition WHERE index_type = ?
                ) WHERE rn = 1
            ),
            SortedComposition AS (SELECT date, ticker FROM RankedComposition ORDER BY date, ticker ASC),
            DailyComposition AS (SELECT date, GROUP_CONCAT(ticker) AS symbols FROM SortedComposition GROUP BY date),
            Changes AS (
                SELECT date, symbols, LAG(date) OVER(ORDER BY date ASC) as prev_date, LAG(symbols) OVER(ORDER BY date ASC) as prev_symbols
                FROM DailyComposition
            )
            SELECT * FROM Changes WHERE (symbols != prev_symbols OR prev_symbols IS NULL) AND date BETWEEN ? AND ? 
        """
        changes = self.db_manager.execute_query(query, (index_type, start_date, end_date))
        
        if not changes.empty:
            data = list(changes[['date', 'symbols', 'prev_date', 'prev_symbols']].itertuples(index=False, name=None))
            self.db_manager.insert_or_update_index_composition_changes(data)
            self.logger.info(f"Detected {len(changes)} discrete composition change events.")
        
        return changes

    def close_db_connection(self):
        self.db_manager.close_connection()