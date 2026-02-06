"""
Database Package

Provides data access layer for Medicare Part D recommendation system.

Components:
- db_manager: Connection management and query utilities
- plan_repository: Plan data access methods
- formulary_repository: Formulary and coverage data access
- beneficiary_repository: Beneficiary profile data access
"""

__version__ = '1.0.0'

from .db_manager import DatabaseManager, get_db

__all__ = ['DatabaseManager', 'get_db']
