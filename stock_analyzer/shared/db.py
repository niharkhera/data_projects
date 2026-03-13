# shared/db.py

import os
import pandas as pd
from contextlib import contextmanager
from psycopg2 import pool
from typing import Optional
from .logger_config import setup_logger

logger = setup_logger("DatabaseManager")

class DatabaseManager:
    """
    Handles PostgreSQL connection pooling and query execution for all microservices.
    """
    _connection_pool = None

    def __init__(self):
        self._initialize_pool()

    def _initialize_pool(self):
        """Initializes a thread-safe connection pool using environment variables."""
        if DatabaseManager._connection_pool is None:
            try:
                DatabaseManager._connection_pool = pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    user=os.getenv("POSTGRES_USER", "stockuser"),
                    password=os.getenv("POSTGRES_PASSWORD", "changeme"),
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=os.getenv("POSTGRES_PORT", "5432"),
                    database=os.getenv("POSTGRES_DB", "stock_data")
                )
                logger.info("PostgreSQL connection pool initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to initialize PostgreSQL connection pool: {e}")
                raise

    @contextmanager
    def get_connection(self):
        """Yields a database connection from the pool and ensures it is returned."""
        conn = DatabaseManager._connection_pool.getconn()
        try:
            yield conn
        finally:
            DatabaseManager._connection_pool.putconn(conn)

    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        """
        Executes a SELECT query and returns the results as a Pandas DataFrame.
        """
        try:
            with self.get_connection() as conn:
                if params:
                    return pd.read_sql_query(query, conn, params=params)
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"Query execution failed: {e} | Query: {query}")
            return pd.DataFrame()

    def execute_write(self, query: str, params: tuple) -> bool:
        """
        Executes an INSERT, UPDATE, or DELETE operation.
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Write operation failed: {e} | Query: {query}")
            return False
            
    def close_all_connections(self):
        """Closes all connections in the pool. Used during graceful shutdown."""
        if DatabaseManager._connection_pool:
            DatabaseManager._connection_pool.closeall()
            logger.info("All PostgreSQL connections closed.")