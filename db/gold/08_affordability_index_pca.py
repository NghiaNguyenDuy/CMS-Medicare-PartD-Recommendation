"""
Gold Layer: Affordability Index using PCA

Calculate affordability index using Principal Component Analysis
instead of hardcoded weights. This captures the natural variance in cost features.

Creates affordability_index column with quartile-based classification.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


def calculate_affordability_index():
    """
    Calculate affordability index using PCA on cost features.
    
    Updates:
        gold.agg_plan_cost_metrics - Adds affordability_index using PCA
    """
    db = get_db()
    
    print("="  * 60)
    print("Gold Layer: Affordability Index (PCA-based)")
    print("=" * 60)
    
    # Check if cost metrics table exists
    try:
        cost_count = db.query_one("SELECT COUNT(*) FROM gold.agg_plan_cost_metrics")[0]
        print(f"\n1. Found {cost_count:,} plans with cost metrics")
    except:
        print("\nERROR: gold.agg_plan_cost_metrics not found!")
        print("Please run cost aggregation first.")
        return False
    
    print("\n2. Extracting cost features for PCA...")
    
    # Get cost features - join cost metrics with plan info for premium/deductible
    cost_features_df = db.query_df("""
        SELECT
            cm.plan_key,
            -- From plan_info
            CAST(COALESCE(p.PREMIUM, 0) AS DOUBLE) AS premium,
            CAST(COALESCE(p.DEDUCTIBLE, 0) AS DOUBLE) AS deductible,
            -- From cost metrics
            CAST(COALESCE(cm.pref_avg_copay_amt, 0) AS DOUBLE) AS avg_copay_preferred,
            CAST(COALESCE(cm.pref_median_copay_amt, 0) AS DOUBLE) AS median_copay_preferred,
            CAST(COALESCE(cm.pref_avg_coins_rate, 0) AS DOUBLE) AS avg_coinsurance_preferred,
            CAST(COALESCE(cm.nonpref_avg_copay_amt, 0) AS DOUBLE) AS avg_copay_nonpreferred,
            CAST(COALESCE(cm.ded_applies_rate, 0) AS DOUBLE) AS ded_applies_rate,
            CAST(COALESCE(cm.specialty_row_share, 0) AS DOUBLE) AS specialty_share
        FROM gold.agg_plan_cost_metrics cm
        LEFT JOIN bronze.brz_plan_info p ON cm.plan_key = p.PLAN_KEY
        WHERE cm.plan_key IS NOT NULL
    """)
    
    print(f"   ✓ Extracted {len(cost_features_df):,} plans")
    
    # Prepare features for PCA
    feature_cols = [
        'premium', 
        'deductible', 
        'avg_copay_preferred',
        'median_copay_preferred',
        'avg_coinsurance_preferred',
        'avg_copay_nonpreferred',
        'ded_applies_rate',
        'specialty_share'
    ]
    
    X = cost_features_df[feature_cols].fillna(0).values
    
    print("\n3. Standardizing features...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    print("\n4. Applying PCA (1 component for affordability)...")
    pca = PCA(n_components=1)
    pca_scores = pca.fit_transform(X_scaled)
    
    # Get explained variance
    explained_var = pca.explained_variance_ratio_[0]
    print(f"   ✓ Explained variance: {explained_var*100:.1f}%")
    
    # Print feature weights
    print(f"\n   Feature contributions:")
    for i, col in enumerate(feature_cols):
        weight = pca.components_[0][i]
        print(f"     - {col}: {weight:.3f}")
    
    # Add PCA scores to dataframe
    cost_features_df['affordability_index'] = pca_scores.flatten()
    
    # Calculate quartiles for classification
    q1, q2, q3 = cost_features_df['affordability_index'].quantile([0.25, 0.50, 0.75])
    
    print(f"\n   Affordability quartiles:")
    print(f"     - Q1 (Low cost): {q1:.3f}")
    print(f"     - Q2 (Medium-Low): {q2:.3f}")
    print(f"     - Q3 (Medium-High): {q3:.3f}")
    
    # Create affordability class
    def classify_affordability(score):
        if score <= q1:
            return 0  # Most affordable
        elif score <= q2:
            return 1  # Moderately affordable
        elif score <= q3:
            return 2  # Moderately expensive
        else:
            return 3  # Most expensive
    
    cost_features_df['affordability_class'] = cost_features_df['affordability_index'].apply(
        classify_affordability
    )
    
    print("\n5. Updating gold.agg_plan_cost_metrics...")
    
    # Register dataframe for SQL access
    db.conn.register('pca_results', cost_features_df[['plan_key', 'affordability_index', 'affordability_class']])
    
    # Add columns if they don't exist
    try:
        db.execute("ALTER TABLE gold.agg_plan_cost_metrics ADD COLUMN affordability_index DOUBLE;")
        db.execute("ALTER TABLE gold.agg_plan_cost_metrics ADD COLUMN affordability_class INTEGER;")
    except:
        pass  # Columns may already exist
    
    # Update with PCA results
    db.execute("""
        UPDATE gold.agg_plan_cost_metrics c
        SET affordability_index = p.affordability_index,
            affordability_class = p.affordability_class
        FROM pca_results p
        WHERE c.plan_key = p.plan_key;
    """)
    
    # Validate
    print("\n6. Validation...")
    stats = db.query_df("""
        SELECT
            cm.affordability_class,
            COUNT(*) AS plan_count,
            ROUND(AVG(p.PREMIUM), 2) AS avg_premium,
            ROUND(AVG(p.DEDUCTIBLE), 2) AS avg_deductible,
            ROUND(AVG(cm.pref_avg_copay_amt), 2) AS avg_copay,
            ROUND(AVG(cm.affordability_index), 3) AS avg_index
        FROM gold.agg_plan_cost_metrics cm
        LEFT JOIN bronze.brz_plan_info p ON cm.plan_key = p.PLAN_KEY
        WHERE cm.affordability_class IS NOT NULL
        GROUP BY cm.affordability_class
        ORDER BY cm.affordability_class;
    """)
    
    print(f"\n✓ Affordability index calculated:")
    print(f"\n  Distribution by class:")
    for _, row in stats.iterrows():
        class_label = ['Most Affordable', 'Moderately Affordable', 'Moderately Expensive', 'Most Expensive'][int(row['affordability_class'])]
        print(f"    Class {int(row['affordability_class'])} ({class_label}):")
        print(f"      - Plans: {int(row['plan_count']):,}")
        print(f"      - Avg Premium: ${row['avg_premium']:.2f}")
        print(f"      - Avg Deductible: ${row['avg_deductible']:.2f}")
        print(f"      - Avg Copay: ${row['avg_copay']:.2f}")
        print(f"      - Avg Index: {row['avg_index']:.3f}")
    
    return True


if __name__ == "__main__":
    success = calculate_affordability_index()
    sys.exit(0 if success else 1)
