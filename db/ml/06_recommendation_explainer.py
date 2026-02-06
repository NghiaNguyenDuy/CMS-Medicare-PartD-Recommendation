"""
ML Prep: Recommendation Explainer

Generate explainable recommendations with:
- Distance tradeoff warnings
- Insulin coverage alerts
- Network adequacy flags
- Cost breakdowns

Creates:
    ml.recommendation_explanations - Human-readable explanations
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def create_recommendation_explanations():
    """
    Create recommendation explanations.
    
    Creates:
        ml.recommendation_explanations - Explainable recommendation text
    """
    db = get_db()
    
    print("=" * 60)
    print("ML Prep: Recommendation Explanations")
    print("=" * 60)
    
    # Check prerequisites
    try:
        pairs_count = db.query_one("SELECT COUNT(*) FROM ml.training_plan_pairs")[0]
        print(f"\n1. Found {pairs_count:,} training pairs")
    except:
        print("\nERROR: ml.training_plan_pairs not found!")
        print("Please run 05_training_pairs.py first.")
        return False
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS ml.recommendation_explanations;")
    
    print("\n2. Generating explanations...")
    
    create_sql = """
        CREATE TABLE ml.recommendation_explanations AS
        WITH ranked_recommendations AS (
            SELECT
                tp.*,
                ROW_NUMBER() OVER (
                    PARTITION BY tp.bene_synth_id
                    ORDER BY tp.total_cost_with_distance ASC
                ) AS recommendation_rank
            FROM ml.training_plan_pairs tp
        )
        SELECT
            rr.bene_synth_id,
            rr.plan_key,
            rr.recommendation_rank,
            rr.estimated_annual_oop,
            rr.plan_premium,
            rr.plan_deductible,
            rr.distance_miles,
            rr.distance_category,
            rr.has_distance_tradeoff,
            rr.network_adequacy_flag,
            rr.bene_insulin_user,
            
            -- Cost explanation
            PRINTF('Annual cost: $%.2f (Premium: $%.2f/mo, Deductible: $%.2f, Est. OOP: $%.2f)',
                rr.plan_premium * 12 + rr.estimated_annual_oop,
                rr.plan_premium,
                rr.plan_deductible,
                rr.estimated_annual_oop
            ) AS cost_explanation,
            
            -- Distance explanation
            CASE
                WHEN rr.distance_category = 'very_close'
                THEN 'Excellent pharmacy access - preferred pharmacies within 2 miles'
                
                WHEN rr.distance_category = 'nearby' AND rr.has_distance_tradeoff = FALSE
                THEN 'Good pharmacy access - preferred pharmacies 2-8 miles away'
                
                WHEN rr.has_distance_tradeoff = TRUE
                THEN PRINTF(
                    'This plan may save money but requires traveling %.1f miles to preferred pharmacies. Consider mail order for convenience.',
                    rr.distance_miles
                )
                
                WHEN rr.distance_category = 'moderate'
                THEN PRINTF(
                    'Moderate pharmacy access - nearest preferred pharmacy ~%.1f miles away. Mail order recommended.',
                    rr.distance_miles
                )
                
                WHEN rr.distance_category = 'far'
                THEN PRINTF(
                    'Limited pharmacy network - nearest preferred pharmacy %.1f miles away. Strongly recommend mail order pharmacy.',
                    rr.distance_miles
                )
                
                ELSE 'Pharmacy access information not available'
            END AS distance_explanation,
            
            -- Network warning
            CASE
                WHEN rr.network_adequacy_flag = 1
                THEN '⚠️  WARNING: This plan has limited pharmacy network. Verify your preferred pharmacy is included.'
                ELSE NULL
            END AS network_warning,
            
            -- Insulin warning
            CASE
                WHEN rr.bene_insulin_user = 1
                THEN '💉 Insulin user: Verify this plan complies with $35 insulin cost cap and covers your specific insulin products.'
                ELSE NULL
            END AS insulin_warning,
            
            -- Formulary warning
            CASE
                WHEN rr.formulary_restrictiveness = 2
                THEN '⚠️  This plan has high restrictions (prior auth, step therapy). Verify your medications are covered without barriers.'
                WHEN rr.formulary_restrictiveness = 1
                THEN 'ℹ️  This plan has moderate restrictions. Check if your medications require prior authorization.'
                ELSE NULL
            END AS formulary_warning,
            
            -- Overall recommendation
            CASE
                WHEN rr.recommendation_rank = 1
                THEN '⭐ RECOMMENDED: Best overall value based on your profile'
                WHEN rr.recommendation_rank <= 3 AND rr.has_distance_tradeoff = FALSE
                THEN '✓ Good option: Competitive cost with good pharmacy access'
                WHEN rr.recommendation_rank <= 3 AND rr.has_distance_tradeoff = TRUE
                THEN '💰 Budget option: Low cost but consider pharmacy distance'
                ELSE 'Alternative option'
            END AS recommendation_label,
            
            CURRENT_TIMESTAMP AS ml_ts
            
        FROM ranked_recommendations rr
        WHERE rr.recommendation_rank <= 5;  -- Top 5 recommendations per beneficiary
    """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n3. Creating indexes...")
    db.execute("CREATE INDEX idx_expl_bene ON ml.recommendation_explanations(bene_synth_id);")
    db.execute("CREATE INDEX idx_expl_plan ON ml.recommendation_explanations(plan_key);")
    db.execute("CREATE INDEX idx_expl_rank ON ml.recommendation_explanations(recommendation_rank);")
    
    # Validate
    print("\n4. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_recommendations,
            COUNT(DISTINCT bene_synth_id) AS unique_benes,
            SUM(CASE WHEN network_warning IS NOT NULL THEN 1 ELSE 0 END) AS network_warnings,
            SUM(CASE WHEN insulin_warning IS NOT NULL THEN 1 ELSE 0 END) AS insulin_warnings,
            SUM(CASE WHEN formulary_warning IS NOT NULL THEN 1 ELSE 0 END) AS formulary_warnings,
            SUM(CASE WHEN has_distance_tradeoff THEN 1 ELSE 0 END) AS distance_tradeoffs
        FROM ml.recommendation_explanations;
    """)
    
    print(f"\n✓ Recommendation explanations generated:")
    print(f"  - Total recommendations: {stats['total_recommendations'][0]:,}")
    print(f"  - Unique beneficiaries: {stats['unique_benes'][0]:,}")
    print(f"\n  Warnings/Flags:")
    print(f"  - Network adequacy warnings: {stats['network_warnings'][0]:,}")
    print(f"  - Insulin warnings: {stats['insulin_warnings'][0]:,}")
    print(f"  - Formulary warnings: {stats['formulary_warnings'][0]:,}")
    print(f"  - Distance tradeoffs: {stats['distance_tradeoffs'][0]:,}")
    
    # Sample recommendations
    print("\n5. Sample recommendations:")
    sample = db.query_df("""
        SELECT
            bene_synth_id,
            plan_key,
            recommendation_rank,
            cost_explanation,
            distance_explanation,
            recommendation_label
        FROM ml.recommendation_explanations
        WHERE bene_synth_id = (SELECT bene_synth_id FROM ml.recommendation_explanations LIMIT 1)
        ORDER BY recommendation_rank
        LIMIT 3;
    """)
    
    if len(sample) > 0:
        print(f"\n   Beneficiary: {sample['bene_synth_id'][0]}")
        for _, row in sample.iterrows():
            print(f"\n   Rank {row['recommendation_rank']}: {row['PLAN_KEY']}")
            print(f"   {row['recommendation_label']}")
            print(f"   {row['cost_explanation']}")
            print(f"   {row['distance_explanation']}")
    
    return True


if __name__ == "__main__":
    success = create_recommendation_explanations()
    sys.exit(0 if success else 1)
