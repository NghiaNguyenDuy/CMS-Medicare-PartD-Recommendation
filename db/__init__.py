"""
Database Package

Provides data access layer for Medicare Part D recommendation system.

Components:
- db_manager: Connection management and query utilities
- run_full_pipeline: Layer orchestration entrypoint
- bronze/gold/ml: Layer-specific table build scripts
"""

__version__ = '1.0.0'

from .db_manager import DatabaseManager, get_db

__all__ = ['DatabaseManager', 'get_db']
