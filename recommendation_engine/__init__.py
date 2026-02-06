"""
Recommendation Engine Package

This package contains the core logic for Medicare Part D plan recommendations:
- plan_filter: County-level plan availability filtering
- coverage_checker: Formulary lookup and restriction checking  
- cost_estimator: Annual cost calculation with insulin cap handling
"""

__version__ = '1.0.0'

from .plan_filter import PlanFilter
from .coverage_checker import CoverageChecker
from .cost_estimator import CostEstimator

__all__ = ['PlanFilter', 'CoverageChecker', 'CostEstimator']
