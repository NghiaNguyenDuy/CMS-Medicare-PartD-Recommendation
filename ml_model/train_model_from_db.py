"""
Train LightGBM Ranking Model using ML Schema Database

This script trains a ranking model using the pre-processed ml.training_plan_pairs table.
The model learns to rank plans by objective cost for each beneficiary:
annual premium + out-of-pocket + distance penalty.

Data Source: ml.training_plan_pairs (created by db/ml/05_training_pairs.py)
Output: models/plan_ranker.pkl (model artifact for streamlit)

Usage:
    python ml_model/train_model_from_db.py
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import GroupShuffleSplit
import pickle
from pathlib import Path
import sys
import logging
from datetime import datetime
import json
import traceback

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.db_manager import get_db
from ml_model.ranking_utils import create_ranking_labels_from_cost, groups_are_contiguous


# ===== Logging Setup =====
def setup_logging():
    """
    Setup logging configuration for training.
    Creates both console and file handlers.
    """
    # Create logs directory
    log_dir = Path('logs/ml_training')
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate timestamp for log filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'training_{timestamp}.log'
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also print to console
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    return logger, log_file


# Initialize logger
logger, current_log_file = setup_logging()


class PlanRankingModel:
    """
    LightGBM-based ranking model using ml.training_plan_pairs.
    """
    
    def __init__(self):
        """Initialize ranking model."""
        self.model = None
        self.feature_names = None
        self.training_stats = {}
    
    def load_training_data_from_db(self):
        """
        Load training data from ml.training_plan_pairs.
        
        Returns:
            pd.DataFrame: Training pairs with features
        """
        logger.info("="*60)
        logger.info("Loading Training Data from ML Database")
        logger.info("="*60)
        
        db = get_db(read_only=True)
        
        # Check if training pairs exist
        try:
            count = db.query_one("SELECT COUNT(*) FROM ml.training_plan_pairs")[0]
            logger.info(f"Found {count:,} training pairs in database")
        except Exception as e:
            logger.error("ml.training_plan_pairs table not found!")
            logger.error(f"Error: {str(e)}")
            logger.error("Please run db/ml/05_training_pairs.py first.")
            sys.exit(1)
        
        # Load training pairs
        query = """
            SELECT
                -- Identifiers
                bene_synth_id,
                PLAN_KEY as plan_key,  -- UPPERCASE in schema
                
                -- Plan features
                CAST(plan_premium AS DOUBLE) as premium,
                CAST(plan_deductible AS DOUBLE) as deductible,
                CAST(CASE WHEN contract_type = 'MA' THEN 1 ELSE 0 END AS INTEGER) as is_ma_pd,
                CAST(CASE WHEN contract_type = 'PDP' THEN 1 ELSE 0 END AS INTEGER) as is_pdp,
                
                -- Beneficiary features
                CAST(unique_drugs AS INTEGER) as num_drugs,
                CAST(bene_insulin_user AS INTEGER) as is_insulin_user,
                CAST(fills_target AS DOUBLE) as avg_fills_per_year,
                
                -- Formulary features (actual schema columns)
                CAST(COALESCE(formulary_generic_pct, 0) AS DOUBLE) as formulary_generic_pct,
                CAST(COALESCE(formulary_specialty_pct, 0) AS DOUBLE) as formulary_specialty_pct,
                CAST(COALESCE(formulary_pa_rate, 0) AS DOUBLE) as formulary_pa_rate,
                CAST(COALESCE(formulary_st_rate, 0) AS DOUBLE) as formulary_st_rate,
                CAST(COALESCE(formulary_ql_rate, 0) AS DOUBLE) as formulary_ql_rate,
                CAST(formulary_restrictiveness AS INTEGER) as formulary_restrictiveness,
                
                -- Network features (actual schema columns)
                CAST(COALESCE(network_preferred_pharmacies, 0) AS INTEGER) as network_preferred_pharmacies,
                CAST(COALESCE(network_total_pharmacies, 0) AS INTEGER) as network_total_pharmacies,
                CAST(COALESCE(network_adequacy_flag, 0) AS INTEGER) as network_adequacy_flag,
                
                -- Distance features
                CAST(COALESCE(distance_miles, 10) AS DOUBLE) as distance_miles,
                distance_category,
                CAST(has_distance_tradeoff AS INTEGER) as has_distance_tradeoff,
                
                -- Cost features (TARGET)
                CAST(estimated_annual_oop AS DOUBLE) as total_drug_oop,
                CAST(
                    COALESCE(total_annual_cost, plan_premium * 12 + estimated_annual_oop) AS DOUBLE
                ) as total_annual_cost,
                CAST(
                    COALESCE(
                        total_cost_with_distance,
                        plan_premium * 12 + estimated_annual_oop + COALESCE(distance_penalty, 0)
                    ) AS DOUBLE
                ) as ranking_cost_objective
                
            FROM ml.training_plan_pairs
            WHERE plan_premium IS NOT NULL
                AND estimated_annual_oop IS NOT NULL
        """
        
        try:
            training_df = db.query_df(query)
            
            logger.info(f"Loaded {len(training_df):,} training pairs")
            logger.info(f"  - Unique beneficiaries: {training_df['bene_synth_id'].nunique():,}")
            logger.info(f"  - Unique plans: {training_df['plan_key'].nunique():,}")
            logger.info(f"  - Avg pairs per beneficiary: {len(training_df) / training_df['bene_synth_id'].nunique():.1f}")
            
            # Log data quality checks
            null_counts = training_df.isnull().sum()
            if null_counts.sum() > 0:
                logger.warning(f"Null values found in data:")
                for col, count in null_counts[null_counts > 0].items():
                    logger.warning(f"  - {col}: {count} nulls")
            
            return training_df
            
        except Exception as e:
            logger.error(f"Error loading training data: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    def prepare_training_data(self, training_df):
        """
        Prepare data for LightGBM ranker.
        
        Args:
            training_df (pd.DataFrame): Training pairs from database
        
        Returns:
            tuple: (X, y, groups) for LightGBM
        """
        print("\nPreparing features for ranking model...")
        
        # Deterministic ordering keeps group boundaries stable for reproducible training.
        ordered_df = training_df.sort_values(
            ["bene_synth_id", "plan_key"]
        ).reset_index(drop=True)

        # Feature columns (aligned with actual schema)
        feature_cols = [
            # Plan features
            'premium', 'deductible', 'is_ma_pd', 'is_pdp',
            # Beneficiary features
            'num_drugs', 'is_insulin_user', 'avg_fills_per_year',
            # Formulary features
            'formulary_generic_pct', 'formulary_specialty_pct',
            'formulary_pa_rate', 'formulary_st_rate', 'formulary_ql_rate',
            'formulary_restrictiveness',
            # Network features
            'network_preferred_pharmacies', 'network_total_pharmacies', 'network_adequacy_flag',
            # Distance features
            'distance_miles', 'has_distance_tradeoff',
            # Cost component
            'total_drug_oop'
        ]
        
        # Extract features
        X = ordered_df[feature_cols].copy()
        
        # Add interaction features
        X['annual_premium'] = X['premium'] * 12
        X['cost_per_drug'] = X['total_drug_oop'] / np.maximum(X['num_drugs'], 1)
        X['premium_to_oop_ratio'] = X['annual_premium'] / np.maximum(X['total_drug_oop'], 1)
        
        feature_cols.extend(['annual_premium', 'cost_per_drug', 'premium_to_oop_ratio'])
        
        # Target: objective cost = annual premium + OOP + distance penalty.
        y_parts = []
        for _, group in ordered_df.groupby("bene_synth_id", sort=False):
            y_parts.append(create_ranking_labels_from_cost(group["ranking_cost_objective"]))
        y = np.concatenate(y_parts).astype(int)

        # Groups: counts in row order expected by LightGBM ranker.
        groups = ordered_df.groupby("bene_synth_id", sort=False).size().to_numpy()
        
        self.feature_names = feature_cols
        
        logger.info(f"Feature preparation complete:")
        logger.info(f"  - Features: {len(feature_cols)}")
        logger.info(f"  - Samples: {len(X):,}")
        logger.info(f"  - Groups (beneficiaries): {len(groups):,}")
        logger.info(f"  - Target range: [{y.min():.2f}, {y.max():.2f}]")
        
        # Log feature statistics
        logger.debug("Feature statistics:")
        for col in feature_cols:
            logger.debug(f"  - {col}: mean={X[col].mean():.2f}, std={X[col].std():.2f}")
        
        return X, y, groups, ordered_df
    
    def train(self, training_df, test_size=0.2, random_state=42):
        """
        Train LightGBM ranking model.
        
        Args:
            training_df (pd.DataFrame): Training pairs
            test_size (float): Fraction for testing
            random_state (int): Random seed
        
        Returns:
            dict: Training statistics
        """
        logger.info("="*60)
        logger.info("Training LightGBM Ranking Model")
        logger.info("="*60)
        
        # Prepare data
        X, y, _, ordered_df = self.prepare_training_data(training_df)
        
        # Split by beneficiary (group-based split)
        splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
        train_idx, test_idx = next(splitter.split(X, y, groups=ordered_df["bene_synth_id"]))

        # Preserve original sorted order so each beneficiary remains contiguous.
        train_idx = np.sort(train_idx)
        test_idx = np.sort(test_idx)

        X_train = X.iloc[train_idx].reset_index(drop=True)
        X_test = X.iloc[test_idx].reset_index(drop=True)
        y_train = y[train_idx]
        y_test = y[test_idx]

        train_bene_ids = ordered_df.iloc[train_idx]["bene_synth_id"].to_numpy()
        test_bene_ids = ordered_df.iloc[test_idx]["bene_synth_id"].to_numpy()
        train_groups = (
            ordered_df.iloc[train_idx]
            .groupby("bene_synth_id", sort=False)
            .size()
            .to_numpy()
        )
        test_groups = (
            ordered_df.iloc[test_idx]
            .groupby("bene_synth_id", sort=False)
            .size()
            .to_numpy()
        )

        if not groups_are_contiguous(train_bene_ids) or not groups_are_contiguous(test_bene_ids):
            raise ValueError("Non-contiguous groups detected after split.")
        
        logger.info("Train/Test Split:")
        logger.info(f"  Train: {len(X_train):,} samples, {len(train_groups):,} beneficiaries")
        logger.info(f"  Test: {len(X_test):,} samples, {len(test_groups):,} beneficiaries")
        logger.info(f"  Test size: {test_size*100:.1f}%")
        
        # Create LightGBM datasets
        train_data = lgb.Dataset(
            X_train,
            label=y_train,
            group=train_groups,
            feature_name=self.feature_names
        )
        
        test_data = lgb.Dataset(
            X_test,
            label=y_test,
            group=test_groups,
            feature_name=self.feature_names,
            reference=train_data
        )
        
        # LightGBM parameters
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'ndcg_eval_at': [3, 5, 10],
            'learning_rate': 0.05,
            'num_leaves': 31,
            'max_depth': -1,
            'min_data_in_leaf': 20,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'feature_fraction': 0.8,
            'verbose': -1
        }
        
        # Train model
        logger.info("Starting model training...")
        logger.info(f"Parameters: {json.dumps(params, indent=2)}")
        
        evals_result = {}
        try:
            self.model = lgb.train(
                params,
                train_data,
                num_boost_round=500,
                valid_sets=[train_data, test_data],
                valid_names=['train', 'test'],
                callbacks=[
                    lgb.early_stopping(stopping_rounds=50),
                    lgb.log_evaluation(period=50),
                    lgb.record_evaluation(evals_result)
                ]
            )
            
            logger.info("Training complete!")
            logger.info(f"  Best iteration: {self.model.best_iteration}")
            logger.info(f"  Best NDCG@3: {evals_result['test']['ndcg@3'][self.model.best_iteration-1]:.4f}")
            logger.info(f"  Best NDCG@5: {evals_result['test']['ndcg@5'][self.model.best_iteration-1]:.4f}")
            logger.info(f"  Best NDCG@10: {evals_result['test']['ndcg@10'][self.model.best_iteration-1]:.4f}")
            
        except Exception as e:
            logger.error(f"Error during training: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        
        # Store training stats
        self.training_stats = {
            'best_iteration': self.model.best_iteration,
            'train_ndcg@3': evals_result['train']['ndcg@3'][self.model.best_iteration-1],
            'test_ndcg@3': evals_result['test']['ndcg@3'][self.model.best_iteration-1],
            'train_ndcg@5': evals_result['train']['ndcg@5'][self.model.best_iteration-1],
            'test_ndcg@5': evals_result['test']['ndcg@5'][self.model.best_iteration-1],
            'feature_importance': dict(zip(
                self.feature_names,
                self.model.feature_importance(importance_type='gain')
            ))
        }
        
        return self.training_stats
    
    def get_feature_importance(self, top_n=15):
        """Get top N most important features."""
        if self.model is None:
            raise ValueError("Model not trained yet!")
        
        importance = pd.DataFrame({
            'feature': self.feature_names,
            'importance': self.model.feature_importance(importance_type='gain')
        })
        importance = importance.sort_values('importance', ascending=False).head(top_n)
        
        return importance
    
    def save_model(self, output_path='models/plan_ranker.pkl'):
        """
        Save trained model to disk.
        
        Args:
            output_path (str): Output file path
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        model_data = {
            'model': self.model,
            'feature_names': self.feature_names,
            'training_stats': self.training_stats
        }
        
        try:
            with open(output_file, 'wb') as f:
                pickle.dump(model_data, f)
            
            logger.info(f"Model saved to {output_path}")
            logger.info(f"Model size: {output_file.stat().st_size / 1024:.2f} KB")
            
        except Exception as e:
            logger.error(f"Error saving model: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    @classmethod
    def load_model(cls, model_path='models/plan_ranker.pkl'):
        """
        Load trained model from disk.
        
        Args:
            model_path (str): Path to saved model
        
        Returns:
            PlanRankingModel: Loaded model
        """
        with open(model_path, 'rb') as f:
            model_data = pickle.load(f)
        
        model_obj = cls()
        model_obj.model = model_data['model']
        model_obj.feature_names = model_data['feature_names']
        model_obj.training_stats = model_data.get('training_stats', {})
        
        logger.info(f"Model loaded from {model_path}")
        return model_obj


def save_training_results(stats, importance_df, log_file):
    """
    Save training results to a summary file.
    
    Args:
        stats (dict): Training statistics
        importance_df (pd.DataFrame): Feature importance
        log_file (Path): Path to log file
    """
    results_dir = Path('logs/ml_training/results')
    results_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    results_file = results_dir / f'training_results_{timestamp}.json'
    
    # Prepare results dictionary
    results = {
        'timestamp': timestamp,
        'log_file': str(log_file),
        'training_stats': {
            'best_iteration': int(stats['best_iteration']),
            'train_ndcg@3': float(stats['train_ndcg@3']),
            'test_ndcg@3': float(stats['test_ndcg@3']),
            'train_ndcg@5': float(stats['train_ndcg@5']),
            'test_ndcg@5': float(stats['test_ndcg@5'])
        },
        'feature_importance': importance_df.to_dict('records'),
        'model_path': 'models/plan_ranker.pkl'
    }
    
    # Save to JSON
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Training results saved to {results_file}")
    return results_file


def main():
    """
    Main execution: Train and save ranking model.
    """
    start_time = datetime.now()
    logger.info("Starting ML model training pipeline")
    logger.info(f"Start time: {start_time}")
    
    try:
        # Initialize model
        model = PlanRankingModel()
        
        # Load training data from database
        training_df = model.load_training_data_from_db()
        
        # Train model
        stats = model.train(training_df, test_size=0.2)
        
        # Display feature importance
        logger.info("="*60)
        logger.info("Top 15 Feature Importance")
        logger.info("="*60)
        importance_df = model.get_feature_importance(top_n=15)
        logger.info("\n" + importance_df.to_string(index=False))
        
        # Save model
        model.save_model()
        
        # Save training results
        results_file = save_training_results(stats, importance_df, current_log_file)
        
        # Final summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info("✓ Training Complete!")
        logger.info("="*60)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Model saved to: models/plan_ranker.pkl")
        logger.info(f"Test NDCG@3: {stats['test_ndcg@3']:.4f}")
        logger.info(f"Test NDCG@5: {stats['test_ndcg@5']:.4f}")
        logger.info(f"Log file: {current_log_file}")
        logger.info(f"Results file: {results_file}")
        
        return 0
        
    except Exception as e:
        logger.error("="*60)
        logger.error("Training FAILED!")
        logger.error("="*60)
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error(f"Log file: {current_log_file}")
        
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
