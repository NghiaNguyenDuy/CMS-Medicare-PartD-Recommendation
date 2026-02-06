"""
ML Prep: Training Plan Pairs Generation

Generate beneficiary-plan training pairs with features:
- Plan characteristics (premium, deductible, formulary)
- Geographic compatibility (county matching)
- Distance features (pharmacy accessibility)
- Network adequacy
- Estimated annual out-of-pocket cost (TARGET)

Creates:
    ml.training_plan_pairs - Training data for recommender model
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def generate_training_pairs():
    """
    Generate training pairs for ML model.
    
    Creates:
        ml.training_plan_pairs - Beneficiary-plan pairs with features
    """
    db = get_db()
    
    print("=" * 60)
    print("ML Prep: Training Pairs Generation")
    print("=" * 60)
    
    # Check prerequisites
    try:
        bene_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
        print(f"\n1. Prerequisites check:")
        print(f"   ✓ Found {bene_count:,} synthetic beneficiaries")
    except:
        print("\nERROR: synthetic.syn_beneficiary not found!")
        return False
    
    # Create ML schema
    db.execute("CREATE SCHEMA IF NOT EXISTS ml;")
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS ml.training_plan_pairs;")
    
    print("\n2. Generating beneficiary-plan pairs...")
    print("   - Enforcing geographic constraints (county-level)")
    print("   - Including distance features")
    print("   - Adding plan characteristics")
    
    create_sql = """
        CREATE TABLE ml.training_plan_pairs AS
        SELECT
            -- Beneficiary identifiers
            b.bene_synth_id,
            b.state AS bene_state,
            b.county_code AS bene_county,
            b.zip_code AS bene_zip,
            b.density AS bene_density,
            
            b.state,
            b.county_code,
            b.zip_code,
            COALESCE(b.risk_segment, 'MED') AS risk_segment,
            COALESCE(b.unique_drugs, 3) AS unique_drugs,
            COALESCE(b.fills_target, 12) AS fills_target,
            COALESCE(b.total_rx_cost_est, 5000) AS total_rx_cost_est,
            COALESCE(b.insulin_user_flag, 0) AS bene_insulin_user,
            
            -- Plan attributes
            p.plan_key,
            -- Derive contract_type from IS_MA_PD and IS_PDP flags
            CASE 
                WHEN p.IS_MA_PD THEN 'MA'
                WHEN p.IS_PDP THEN 'PDP'
                ELSE NULL 
            END AS contract_type,
            CAST(p.PREMIUM AS DOUBLE) AS plan_premium,
            CAST(p.DEDUCTIBLE AS DOUBLE) AS plan_deductible,
            COALESCE(p.snp, 0) AS plan_snp,
            
            -- Cost estimate (simplified - from bene profile)
            COALESCE(b.total_rx_cost_est, 5000) AS estimated_annual_oop,
            
            -- Formulary metrics
            COALESCE(fm.total_drugs, 0) AS formulary_total_drugs,
            COALESCE(fm.generic_tier_pct, 0) AS formulary_generic_pct,
            COALESCE(fm.specialty_tier_pct, 0) AS formulary_specialty_pct,
            COALESCE(fm.pa_rate, 0) AS formulary_pa_rate,
            COALESCE(fm.st_rate, 0) AS formulary_st_rate,
            COALESCE(fm.ql_rate, 0) AS formulary_ql_rate,
            COALESCE(fm.restrictiveness_class, 0) AS formulary_restrictiveness,
            
            -- Network metrics
            COALESCE(nm.total_pharmacies, 0) AS network_total_pharmacies,
            COALESCE(nm.preferred_pharmacies, 0) AS network_preferred_pharmacies,
            COALESCE(nm.network_adequacy_flag, 0) AS network_adequacy_flag,
            
            -- Distance features
            COALESCE(df.simulated_distance_miles, 10.0) AS distance_miles,
            COALESCE(df.distance_category, 'nearby') AS distance_category,
            
            -- Distance inconvenience penalty
            CASE
                WHEN COALESCE(df.simulated_distance_miles, 10) > 15 THEN 200.0
                WHEN COALESCE(df.simulated_distance_miles, 10) > 8 THEN 100.0
                ELSE 0.0
            END AS distance_penalty,
            
            -- In real scenario, this would use detailed cost calculation
            LEAST(b.total_rx_cost_est * 0.25, 
                  p.deductible + (b.fills_target * 20.0)) AS estimated_annual_oop,
            
            CURRENT_TIMESTAMP AS ml_ts
            
        FROM synthetic.syn_beneficiary b
        
        -- Join plans available in beneficiary's county (GEOGRAPHIC CONSTRAINT)
        JOIN bronze.brz_plan_info p 
            ON b.state = p.state 
            AND b.county_code = p.county_code
            AND (p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y')
        
        -- Join formulary metrics
        LEFT JOIN gold.agg_plan_formulary_metrics fm
            ON p.plan_key = fm.plan_key
        
        -- Join network metrics
        LEFT JOIN gold.agg_plan_network_metrics nm
            ON p.plan_key = nm.plan_key
        
        -- Join distance features
        LEFT JOIN ml.plan_distance_features df
            ON p.plan_key = df.plan_key
            AND b.county_code = df.county_code;
    """
    
    db.execute(create_sql)
    
    # Add derived features
    print("\n3. Adding derived features...")
    db.execute("""
        ALTER TABLE ml.training_plan_pairs
        ADD COLUMN total_cost_with_distance DECIMAL(10,2);
        
        UPDATE ml.training_plan_pairs
        SET total_cost_with_distance = estimated_annual_oop + distance_penalty;
        
        -- Add tradeoff flag
        ALTER TABLE ml.training_plan_pairs
        ADD COLUMN has_distance_tradeoff BOOLEAN;
        
        UPDATE ml.training_plan_pairs
        SET has_distance_tradeoff = CASE
            WHEN estimated_annual_oop < 2000 AND distance_miles > 12 THEN TRUE
            ELSE FALSE
        END;
    """)
    
    # Create indexes
    print("\n4. Creating indexes...")
    db.execute("CREATE INDEX idx_train_bene ON ml.training_plan_pairs(bene_synth_id);")
    db.execute("CREATE INDEX idx_train_plan ON ml.training_plan_pairs(plan_key);")
    db.execute("CREATE INDEX idx_train_county ON ml.training_plan_pairs(bene_county);")
    
    # Validate
    print("\n5. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_pairs,
            COUNT(DISTINCT bene_synth_id) AS unique_benes,
            COUNT(DISTINCT plan_key) AS unique_plans,
            ROUND(AVG(estimated_annual_oop), 2) AS avg_oop,
            ROUND(AVG(distance_miles), 2) AS avg_distance,
            SUM(CASE WHEN has_distance_tradeoff THEN 1 ELSE 0 END) AS tradeoff_pairs,
            ROUND(100.0 * SUM(CASE WHEN has_distance_tradeoff THEN 1 ELSE 0 END) / COUNT(*), 2) AS tradeoff_pct
        FROM ml.training_plan_pairs;
    """)
    
    pairs_per_bene = stats['total_pairs'][0] / stats['unique_benes'][0] if stats['unique_benes'][0] > 0 else 0
    
    print(f"\n✓ Training pairs generated:")
    print(f"  - Total pairs: {stats['total_pairs'][0]:,}")
    print(f"  - Unique beneficiaries: {stats['unique_benes'][0]:,}")
    print(f"  - Unique plans: {stats['unique_plans'][0]:,}")
    print(f"  - Avg plans per beneficiary: {pairs_per_bene:.1f}")
    print(f"\n  Cost & Distance:")
    print(f"  - Avg estimated OOP: ${stats['avg_oop'][0]:,.2f}")
    print(f"  - Avg distance: {stats['avg_distance'][0]} miles")
    print(f"  - Pairs with tradeoff: {stats['tradeoff_pairs'][0]:,} ({stats['tradeoff_pct'][0]}%)")
    
    return True


if __name__ == "__main__":
    success = generate_training_pairs()
    sys.exit(0 if success else 1)
