import pandas as pd
from pathlib import Path
from typing import Optional
from save_db import StockDataManager
from logger_config import setup_logger

logger = setup_logger("DataExporter")

class DataExporter:
    def __init__(self, db_manager: StockDataManager):
        self.db_manager = db_manager
        self.export_base_path = Path(__file__).resolve().parent.parent / 'data' / 'csv'
        self.export_base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Initialized CSV exporter targeting {self.export_base_path}")

    def _save_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        if df.empty:
            logger.warning(f"Dataframe is empty. Aborting export for {filename}.")
            return ""
        
        file_path = self.export_base_path / filename
        try:
            df.to_csv(file_path, index=False)
            logger.info(f"Successfully exported {len(df)} rows to {filename}.")
            return str(file_path)
        except Exception as e:
            logger.error(f"IOError saving CSV {filename}: {e}")
            return ""

    def export_performance(self, index_type: str, start_date: str, end_date: str) -> str:
        """
        Exports the performance history specifically for the requested date window.
        Format: performance_{index_type}_{start_date}_{end_date}.csv
        """
        query = "SELECT * FROM index_performance WHERE index_type = ? AND date BETWEEN ? AND ?"
        df = self.db_manager.execute_query(query, (index_type, start_date, end_date))
        
        formatted_index = index_type.lower().replace(' ', '_')
        filename = f"performance_{formatted_index}_{start_date}_{end_date}.csv"
        
        return self._save_to_csv(df, filename)

    def export_composition(self, date: str, index_type: str) -> str:
        """
        Exports the composition weights.
        Format: composition_{index_type}_{date}.csv
        """
        df = self.db_manager.display_index_composition(date, index_type)
        
        formatted_index = index_type.lower().replace(' ', '_')
        filename = f"composition_{formatted_index}_{date}.csv"
        
        return self._save_to_csv(df, filename)