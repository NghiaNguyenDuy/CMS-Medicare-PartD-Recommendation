"""
ML Model Package for Medicare Part D Plan Ranking

This package contains modules for training and using machine learning models
to rank Medicare Part D plans for beneficiaries.

Components:
- feature_engineering: Generate training pairs with features
- train_ranking_model: Train LightGBM ranking model with NDCG evaluation
"""

__version__ = '1.0.0'

from pathlib import Path

__all__ = []
