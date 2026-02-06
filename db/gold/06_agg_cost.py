"""
Gold Layer: Plan Cost Structure Metrics Aggregation

Calculate comprehensive cost metrics per plan from beneficiary_cost table:
- Cost type distribution (copay vs coinsurance)
- Average, median, and P90 values for copays and coinsurance rates
- Tier-level breakdowns
- Deductible application rates
- Mail order vs retail comparisons

Creates:
    gold.agg_plan_cost_metrics - Cost structure per plan
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def create_cost_metrics():
    """
    Create plan cost structure metrics aggregation.
    
    Creates:
        gold.agg_plan_cost_metrics - Comprehensive cost stats per plan
    """
    db = get_db()
    
    print("=" * 60)
    print("Gold Layer: Plan Cost Structure Metrics")
    print("=" * 60)
    
    # Create gold schema
    db.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS gold.agg_plan_cost_metrics;")
    
    print("\n1. Aggregating cost structure metrics...")
    
    create_sql = """
        CREATE TABLE gold.agg_plan_cost_metrics AS
        WITH base_cost AS (
            -- Load from bronze beneficiary_cost with type conversions
            SELECT
                PLAN_KEY as plan_key,
                CAST(TIER AS INTEGER) AS tier,
                DAYS_SUPPLY_LABEL as days_supply_label,
                
                -- Preferred retail
                CAST(COST_TYPE_PREF AS INTEGER) AS cost_type_pref,
                CAST(COST_AMT_PREF AS DOUBLE) AS cost_amt_pref,
                
                -- Nonpreferred retail
                CAST(COST_TYPE_NONPREF AS INTEGER) AS cost_type_nonpref,
                CAST(COST_AMT_NONPREF AS DOUBLE) AS cost_amt_nonpref,
                
                -- Preferred mail
                CAST(COST_TYPE_MAIL_PREF AS INTEGER) AS cost_type_mail_pref,
                CAST(COST_AMT_MAIL_PREF AS DOUBLE) AS cost_amt_mail_pref,
                
                -- Nonpreferred mail
                CAST(COST_TYPE_MAIL_NONPREF AS INTEGER) AS cost_type_mail_nonpref,
                CAST(COST_AMT_MAIL_NONPREF AS DOUBLE) AS cost_amt_mail_nonpref,
                
                -- Flags
                TIER_SPECIALTY_YN as tier_specialty_yn,
                DED_APPLIES_YN as ded_applies_yn
            FROM bronze.brz_beneficiary_cost
            WHERE PLAN_KEY IS NOT NULL
        ),
        plan_level AS (
            -- Plan-level overall metrics
            SELECT
                plan_key,
                
                -- Deductible applies rate
                AVG(CASE WHEN UPPER(ded_applies_yn) = 'Y' THEN 1.0 ELSE 0.0 END) AS ded_applies_rate,
                AVG(CASE WHEN UPPER(tier_specialty_yn) = 'Y' THEN 1.0 ELSE 0.0 END) AS specialty_row_share,
                
                -- =============== PREFERRED RETAIL ===============
                AVG(CASE WHEN cost_type_pref = 0 THEN 1.0 ELSE 0.0 END) AS pref_not_offered_share,
                AVG(CASE WHEN cost_type_pref = 1 THEN 1.0 ELSE 0.0 END) AS pref_copay_share,
                AVG(CASE WHEN cost_type_pref = 2 THEN 1.0 ELSE 0.0 END) AS pref_coinsurance_share,
                
                -- Copay amounts ($)
                AVG(CASE WHEN cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_amt,
                MEDIAN(CASE WHEN cost_type_pref = 1 THEN cost_amt_pref END) AS pref_median_copay_amt,
                QUANTILE_CONT(CASE WHEN cost_type_pref = 1 THEN cost_amt_pref END, 0.9) AS pref_p90_copay_amt,
                
                -- Coinsurance rates (0-1)
                AVG(CASE WHEN cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate,
                MEDIAN(CASE WHEN cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_median_coins_rate,
                QUANTILE_CONT(CASE WHEN cost_type_pref = 2 THEN cost_amt_pref / 100.0 END, 0.9) AS pref_p90_coins_rate,
                
                -- =============== NONPREFERRED RETAIL ===============
                AVG(CASE WHEN cost_type_nonpref = 0 THEN 1.0 ELSE 0.0 END) AS nonpref_not_offered_share,
                AVG(CASE WHEN cost_type_nonpref = 1 THEN 1.0 ELSE 0.0 END) AS nonpref_copay_share,
                AVG(CASE WHEN cost_type_nonpref = 2 THEN 1.0 ELSE 0.0 END) AS nonpref_coinsurance_share,
                
                AVG(CASE WHEN cost_type_nonpref = 1 THEN cost_amt_nonpref END) AS nonpref_avg_copay_amt,
                MEDIAN(CASE WHEN cost_type_nonpref = 1 THEN cost_amt_nonpref END) AS nonpref_median_copay_amt,
                QUANTILE_CONT(CASE WHEN cost_type_nonpref = 1 THEN cost_amt_nonpref END, 0.9) AS nonpref_p90_copay_amt,
                
                AVG(CASE WHEN cost_type_nonpref = 2 THEN cost_amt_nonpref / 100.0 END) AS nonpref_avg_coins_rate,
                MEDIAN(CASE WHEN cost_type_nonpref = 2 THEN cost_amt_nonpref / 100.0 END) AS nonpref_median_coins_rate,
                QUANTILE_CONT(CASE WHEN cost_type_nonpref = 2 THEN cost_amt_nonpref / 100.0 END, 0.9) AS nonpref_p90_coins_rate,
                
                -- =============== MAIL PREFERRED ===============
                AVG(CASE WHEN cost_type_mail_pref = 0 THEN 1.0 ELSE 0.0 END) AS mail_pref_not_offered_share,
                AVG(CASE WHEN cost_type_mail_pref = 1 THEN 1.0 ELSE 0.0 END) AS mail_pref_copay_share,
                AVG(CASE WHEN cost_type_mail_pref = 2 THEN 1.0 ELSE 0.0 END) AS mail_pref_coinsurance_share,
                
                AVG(CASE WHEN cost_type_mail_pref = 1 THEN cost_amt_mail_pref END) AS mail_pref_avg_copay_amt,
                MEDIAN(CASE WHEN cost_type_mail_pref = 1 THEN cost_amt_mail_pref END) AS mail_pref_median_copay_amt,
                QUANTILE_CONT(CASE WHEN cost_type_mail_pref = 1 THEN cost_amt_mail_pref END, 0.9) AS mail_pref_p90_copay_amt,
                
                AVG(CASE WHEN cost_type_mail_pref = 2 THEN cost_amt_mail_pref / 100.0 END) AS mail_pref_avg_coins_rate,
                MEDIAN(CASE WHEN cost_type_mail_pref = 2 THEN cost_amt_mail_pref / 100.0 END) AS mail_pref_median_coins_rate,
                QUANTILE_CONT(CASE WHEN cost_type_mail_pref = 2 THEN cost_amt_mail_pref / 100.0 END, 0.9) AS mail_pref_p90_coins_rate,
                
                -- =============== MAIL NONPREFERRED ===============
                AVG(CASE WHEN cost_type_mail_nonpref = 0 THEN 1.0 ELSE 0.0 END) AS mail_nonpref_not_offered_share,
                AVG(CASE WHEN cost_type_mail_nonpref = 1 THEN 1.0 ELSE 0.0 END) AS mail_nonpref_copay_share,
                AVG(CASE WHEN cost_type_mail_nonpref = 2 THEN 1.0 ELSE 0.0 END) AS mail_nonpref_coinsurance_share,
                
                AVG(CASE WHEN cost_type_mail_nonpref = 1 THEN cost_amt_mail_nonpref END) AS mail_nonpref_avg_copay_amt,
                MEDIAN(CASE WHEN cost_type_mail_nonpref = 1 THEN cost_amt_mail_nonpref END) AS mail_nonpref_median_copay_amt,
                QUANTILE_CONT(CASE WHEN cost_type_mail_nonpref = 1 THEN cost_amt_mail_nonpref END, 0.9) AS mail_nonpref_p90_copay_amt,
                
                AVG(CASE WHEN cost_type_mail_nonpref = 2 THEN cost_amt_mail_nonpref / 100.0 END) AS mail_nonpref_avg_coins_rate,
                MEDIAN(CASE WHEN cost_type_mail_nonpref = 2 THEN cost_amt_mail_nonpref / 100.0 END) AS mail_nonpref_median_coins_rate,
                QUANTILE_CONT(CASE WHEN cost_type_mail_nonpref = 2 THEN cost_amt_mail_nonpref / 100.0 END, 0.9) AS mail_nonpref_p90_coins_rate
                
            FROM base_cost
            GROUP BY plan_key
        ),
        pref_tier AS (
            -- Tier-level metrics for preferred retail (tiers 1-7)
            SELECT
                plan_key,
                
                -- Copay amounts by tier
                AVG(CASE WHEN tier = 1 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_1,
                AVG(CASE WHEN tier = 2 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_2,
                AVG(CASE WHEN tier = 3 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_3,
                AVG(CASE WHEN tier = 4 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_4,
                AVG(CASE WHEN tier = 5 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_5,
                AVG(CASE WHEN tier = 6 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_6,
                AVG(CASE WHEN tier = 7 AND cost_type_pref = 1 THEN cost_amt_pref END) AS pref_avg_copay_tier_7,
                
                -- Coinsurance rates by tier (0-1)
                AVG(CASE WHEN tier = 1 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_1,
                AVG(CASE WHEN tier = 2 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_2,
                AVG(CASE WHEN tier = 3 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_3,
                AVG(CASE WHEN tier = 4 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_4,
                AVG(CASE WHEN tier = 5 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_5,
                AVG(CASE WHEN tier = 6 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_6,
                AVG(CASE WHEN tier = 7 AND cost_type_pref = 2 THEN cost_amt_pref / 100.0 END) AS pref_avg_coins_rate_tier_7
                
            FROM base_cost
            GROUP BY plan_key
        )
        -- Final join
        SELECT
            pl.*,
            pt.pref_avg_copay_tier_1,
            pt.pref_avg_copay_tier_2,
            pt.pref_avg_copay_tier_3,
            pt.pref_avg_copay_tier_4,
            pt.pref_avg_copay_tier_5,
            pt.pref_avg_copay_tier_6,
            pt.pref_avg_copay_tier_7,
            pt.pref_avg_coins_rate_tier_1,
            pt.pref_avg_coins_rate_tier_2,
            pt.pref_avg_coins_rate_tier_3,
            pt.pref_avg_coins_rate_tier_4,
            pt.pref_avg_coins_rate_tier_5,
            pt.pref_avg_coins_rate_tier_6,
            pt.pref_avg_coins_rate_tier_7,
            CURRENT_TIMESTAMP AS gold_ts
        FROM plan_level pl
        LEFT JOIN pref_tier pt ON pl.plan_key = pt.plan_key;
    """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n2. Creating indexes...")
    db.execute("CREATE INDEX idx_agg_cost_plan ON gold.agg_plan_cost_metrics(plan_key);")
    
    # Validate
    print("\n3. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_plans,
            ROUND(AVG(pref_avg_copay_amt), 2) AS avg_pref_copay,
            ROUND(AVG(pref_median_copay_amt), 2) AS median_pref_copay,
            ROUND(AVG(pref_avg_coins_rate) * 100, 1) AS avg_pref_coins_pct,
            ROUND(AVG(ded_applies_rate) * 100, 1) AS ded_applies_pct,
            ROUND(AVG(specialty_row_share) * 100, 1) AS specialty_pct,
            ROUND(AVG(mail_pref_avg_copay_amt), 2) AS avg_mail_copay
        FROM gold.agg_plan_cost_metrics;
    """)
    
    print(f"\n✓ Cost structure metrics aggregation complete:")
    print(f"  - Total plans: {stats['total_plans'][0]:,}")
    print(f"\n  Preferred Retail:")
    print(f"  - Avg copay: ${stats['avg_pref_copay'][0]:.2f}")
    print(f"  - Median copay: ${stats['median_pref_copay'][0]:.2f}")
    print(f"  - Avg coinsurance: {stats['avg_pref_coins_pct'][0]}%")
    print(f"\n  Plan characteristics:")
    print(f"  - Deductible applies: {stats['ded_applies_pct'][0]}%")
    print(f"  - Specialty tiers: {stats['specialty_pct'][0]}%")
    print(f"  - Avg mail copay: ${stats['avg_mail_copay'][0]:.2f}")
    
    return True


if __name__ == "__main__":
    success = create_cost_metrics()
    sys.exit(0 if success else 1)
