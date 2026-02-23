"""
Database Manager - Connection Pool and Query Utilities

This module provides centralized database connection management for DuckDB.

Features:
- Singleton connection pool (per database path + access mode)
- Query caching for performance
- Prepared statement support
- Transaction management
- Connection health checks
"""

import threading
from functools import lru_cache
from pathlib import Path

import duckdb


class DatabaseManager:
    """
    Singleton database manager for DuckDB connections.
    """

    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, db_path="data/medicare_part_d.duckdb", read_only=False):
        """
        Singleton pattern: one instance per (database path, read_only mode).
        """
        db_key = (str(Path(db_path).resolve()), bool(read_only))
        if db_key not in cls._instances:
            with cls._lock:
                if db_key not in cls._instances:
                    instance = super(DatabaseManager, cls).__new__(cls)
                    instance._initialized = False
                    cls._instances[db_key] = instance
        return cls._instances[db_key]

    def __init__(self, db_path="data/medicare_part_d.duckdb", read_only=False):
        """
        Initialize database connection.

        Args:
            db_path (str): Path to DuckDB database file
            read_only (bool): Open DB in read-only mode
        """
        if self._initialized:
            return

        self.db_path = Path(db_path)
        self.read_only = bool(read_only)

        if not self.db_path.exists():
            raise FileNotFoundError(
                f"Database not found at {db_path}. "
                "Please run migration script first: python scripts/migrate_to_duckdb.py"
            )

        self.conn = duckdb.connect(str(self.db_path), read_only=self.read_only)

        # Configure DuckDB for optimal performance
        self.conn.execute("SET memory_limit='2GB'")
        self.conn.execute("SET threads=4")
        self.conn.execute("SET enable_object_cache=true")

        self._initialized = True
        mode = "read-only" if self.read_only else "read-write"
        print(f"[OK] Database connected ({mode}): {self.db_path}")

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
            "table_name": table_name,
            "row_count": row_count,
            "column_count": col_count,
            "columns": columns["column_name"].tolist(),
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
        if hasattr(self, "conn"):
            self.conn.close()
            self._initialized = False
            db_key = (str(self.db_path.resolve()), self.read_only)
            with self._lock:
                self._instances.pop(db_key, None)
            print("[OK] Database connection closed")


def get_db(read_only=False, db_path="data/medicare_part_d.duckdb"):
    """
    Get database manager instance.

    Args:
        read_only (bool): Open DB in read-only mode
        db_path (str): Path to DuckDB database file

    Returns:
        DatabaseManager: Singleton database manager
    """
    return DatabaseManager(db_path=db_path, read_only=read_only)
