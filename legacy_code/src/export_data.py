import os
import pandas as pd
import logging
from save_db import StockDataManager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Define the path for the data directory in your virtual environment
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
EXPORT_PATH = os.path.join(PROJECT_ROOT, 'market_env', 'data', 'csv')
os.makedirs(EXPORT_PATH, exist_ok=True)


def export_to_csv(db_manager: StockDataManager, query: str, filename: str = "output.csv"):
    """
    Export data from a given SQL query to a CSV file.

    Args:
        db_manager (StockDataManager): Instance of StockDataManager for database connection.
        query (str): SQL query to fetch data.
        filename (str, optional): Name of the CSV file to export data. Defaults to 'output.csv'.
    """
    try:
        # Execute the query and fetch data into a DataFrame
        df = db_manager.execute_query(query)

        if df.empty:
            logger.warning(f"No data found for query: {query}")
            return

        # Export to CSV
        csv_file = os.path.join(EXPORT_PATH, filename)
        df.to_csv(csv_file, index=False)
        logger.info(f"Data exported to {csv_file}")

    except Exception as e:
        logger.error(f"Error exporting data to {filename}: {e}")


def export_index_performance(db_manager: StockDataManager):
    """Export index performance data to CSV."""
    query = "SELECT * FROM index_performance"
    export_to_csv(db_manager, query, "index_performance.csv")


def export_index_composition(db_manager: StockDataManager):
    """Export index composition data to CSV."""
    query = "SELECT * FROM index_composition"
    export_to_csv(db_manager, query, "index_composition.csv")


if __name__ == "__main__":
    db_manager = None
    try:
        # Initialize the database manager
        db_manager = StockDataManager()

        # Export data
        export_index_performance(db_manager)
        export_index_composition(db_manager)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    finally:
        # Safely close the database connection
        if db_manager:
            db_manager.close_connection()
