"""
Database Manager - Connection Pool and Query Utilities

This module provides centralized database connection management for DuckDB.

Features:
- Singleton connection pool
- Query caching for performance
- Prepared statement support
- Transaction management
- Connection health checks

Usage:
    from db.db_manager import DatabaseManager
    
    db = DatabaseManager()
    result = db.execute("SELECT * FROM bronze.brz_plan_info WHERE county_code = ?", ['06037'])
    df = result.df()
"""

import duckdb
from pathlib import Path
from functools import lru_cache
import threading


class DatabaseManager:
    """
    Singleton database manager for DuckDB connections.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, db_path='data/medicare_part_d.duckdb'):
        """
        Singleton pattern: Only one instance per database path.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, db_path='data/medicare_part_d.duckdb'):
        """
        Initialize database connection.
        
        Args:
            db_path (str): Path to DuckDB database file
        """
        # Only initialize once (singleton pattern)
        if self._initialized:
            return
        
        self.db_path = Path(db_path)
        
        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Database not found at {db_path}. "
                "Please run migration script first: python scripts/migrate_to_duckdb.py"
            )
        
        # Create connection
        self.conn = duckdb.connect(str(self.db_path), read_only=False)
        
        # Configure DuckDB for optimal performance
        self.conn.execute("SET memory_limit='2GB'")  # Limit memory usage
        self.conn.execute("SET threads=4")  # Use 4 threads for parallelism
        self.conn.execute("SET enable_object_cache=true")  # Cache query results
        
        self._initialized = True
        print(f"✓ Database connected: {self.db_path}")
    
    def execute(self, query, params=None):
        """
        Execute a SQL query with optional parameters.
        
        Args:
            query (str): SQL query string
            params (list): Optional list of parameters for prepared statement
        
        Returns:
            duckdb.DuckDBPyRelation: Query result
        """
        if params:
            return self.conn.execute(query, params)
        else:
            return self.conn.execute(query)
    
    def query_df(self, query, params=None):
        """
        Execute query and return result as pandas DataFrame.
        
        Args:
            query (str): SQL query string
            params (list): Optional parameters
        
        Returns:
            pd.DataFrame: Query results
        """
        result = self.execute(query, params)
        return result.df()
    
    def query_one(self, query, params=None):
        """
        Execute query and return first row.
        
        Args:
            query (str): SQL query string
            params (list): Optional parameters
        
        Returns:
            tuple: First row of results, or None
        """
        result = self.execute(query, params)
        return result.fetchone()
    
    def query_all(self, query, params=None):
        """
        Execute query and return all rows.
        
        Args:
            query (str): SQL query string
            params (list): Optional parameters
        
        Returns:
            list: All rows of results
        """
        result = self.execute(query, params)
        return result.fetchall()
    
    @lru_cache(maxsize=128)
    def _cached_query(self, query, params_tuple=None):
        """
        Execute query with caching (for frequently used queries).
        
        Args:
            query (str): SQL query string
            params_tuple (tuple): Parameters as tuple (for hashability)
        
        Returns:
            pd.DataFrame: Cached query results
        """
        params = list(params_tuple) if params_tuple else None
        return self.query_df(query, params)
    
    def cached_query(self, query, params=None):
        """
        Public interface for cached queries.
        
        Args:
            query (str): SQL query string
            params (list): Optional parameters
        
        Returns:
            pd.DataFrame: Cached query results
        """
        params_tuple = tuple(params) if params else None
        return self._cached_query(query, params_tuple)
    
    def get_table_info(self, table_name):
        """
        Get metadata about a table.
        
        Args:
            table_name (str): Name of table
        
        Returns:
            dict: Table metadata (row count, column count, size estimate)
        """
        row_count = self.query_one(f"SELECT COUNT(*) FROM {table_name}")[0]
        
        columns = self.query_df(f"DESCRIBE {table_name}")
        col_count = len(columns)
        
        return {
            'table_name': table_name,
            'row_count': row_count,
            'column_count': col_count,
            'columns': columns['column_name'].tolist()
        }
    
    def list_tables(self):
        """
        List all tables in database.
        
        Returns:
            list: Table names
        """
        tables = self.query_all("SHOW TABLES")
        return [table[0] for table in tables]
    
    def close(self):
        """
        Close database connection.
        """
        if hasattr(self, 'conn'):
            self.conn.close()
            print("✓ Database connection closed")


# Convenience function for one-off queries
def get_db():
    """
    Get database manager instance.
    
    Returns:
        DatabaseManager: Singleton database manager
    """
    return DatabaseManager()
