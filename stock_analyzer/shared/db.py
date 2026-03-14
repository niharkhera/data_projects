# shared/db.py

import os
import pandas as pd
from contextlib import contextmanager
import psycopg # Changed from psycopg2
from psycopg_pool import ConnectionPool # Changed from psycopg2.pool
from typing import Optional
from .logger_config import setup_logger

logger = setup_logger("DatabaseManager")

class DatabaseManager:
    _connection_pool = None

    def __init__(self):
        self._initialize_pool()

    def _initialize_pool(self):
        if DatabaseManager._connection_pool is None:
            try:
                # psycopg 3 uses a single conninfo string
                conninfo = f"dbname={ os.getenv('POSTGRES_DB', 'stock_data')} user={os.getenv('POSTGRES_USER', 'sudouser')} password={os.getenv('POSTGRES_PASSWORD', 'sudopass')} host={os.getenv('POSTGRES_HOST', 'localhost')} port={os.getenv('POSTGRES_PORT', '5432')}"
                DatabaseManager._connection_pool = ConnectionPool(conninfo, min_size=1, max_size=20)
                logger.info("PostgreSQL connection pool initialized successfully (psycopg 3).")
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

    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Executes a SELECT query and returns a pandas DataFrame."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    data = cur.fetchall()
                    
                    if not data:
                        # Clear state before returning early
                        conn.rollback() 
                        return pd.DataFrame()
                        
                    columns = [desc.name for desc in cur.description]
                
                # Clear the transaction state so the pool stays quiet
                conn.rollback() 
                return pd.DataFrame(data, columns=columns)
                    
        except Exception as e:
            logger.error(f"Read operation failed: {e} | Query: {query}")
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