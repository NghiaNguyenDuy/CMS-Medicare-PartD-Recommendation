"""
Gold Layer: Plan Formulary Metrics Aggregation

Calculate formulary characteristics per plan:
- Total drugs
- Formulary breadth
- Tier distribution
- Restriction rates (PA/ST/QL)
- Insulin coverage

Creates:
    gold.agg_plan_formulary_metrics - Formulary statistics per plan
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def create_formulary_metrics():
    """
    Create plan formulary metrics aggregation.
    
    Creates:
        gold.agg_plan_formulary_metrics - Formulary stats per plan
    """
    db = get_db()
    
    print("=" * 60)
    print("Gold Layer: Plan Formulary Metrics")
    print("=" * 60)
    
    # Create gold schema
    db.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS gold.agg_plan_formulary_metrics;")
    
    print("\n1. Aggregating formulary metrics...")
    
    create_sql = """
        CREATE TABLE gold.agg_plan_formulary_metrics AS
        WITH plan_formularies AS (
            -- Join plans with their formularies
            SELECT DISTINCT
                p.plan_key,
                p.formulary_id
            FROM bronze.brz_plan_info p
            WHERE p.formulary_id IS NOT NULL
                AND (p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y')
        ),
        formulary_stats AS (
            SELECT
                pf.plan_key,
                COUNT(DISTINCT f.ndc) AS total_drugs,
                
                -- Tier distribution
                ROUND(AVG(f.tier_level_value), 2) AS avg_tier,
                SUM(CASE WHEN f.tier_level_value <= 2 THEN 1 ELSE 0 END) AS generic_tier_count,
                SUM(CASE WHEN f.tier_level_value >= 3 AND f.tier_level_value < 5 THEN 1 ELSE 0 END) AS brand_tier_count,
                SUM(CASE WHEN f.tier_level_value >= 5 THEN 1 ELSE 0 END) AS specialty_tier_count,
                
                -- Restriction rates
                ROUND(100.0 * SUM(CASE WHEN UPPER(f.prior_authorization_yn) = 'Y' THEN 1 ELSE 0 END) / 
                    NULLIF(COUNT(*), 0), 2) AS pa_rate,
                ROUND(100.0 * SUM(CASE WHEN UPPER(f.step_therapy_yn) = 'Y' THEN 1 ELSE 0 END) / 
                    NULLIF(COUNT(*), 0), 2) AS st_rate,
                ROUND(100.0 * SUM(CASE WHEN UPPER(f.quantity_limit_yn) = 'Y' THEN 1 ELSE 0 END) / 
                    NULLIF(COUNT(*), 0), 2) AS ql_rate,
                
                -- Average restrictions per drug
                ROUND(AVG(
                    CAST(CASE WHEN UPPER(f.prior_authorization_yn) = 'Y' THEN 1 ELSE 0 END AS DOUBLE) +
                    CAST(CASE WHEN UPPER(f.step_therapy_yn) = 'Y' THEN 1 ELSE 0 END AS DOUBLE) +
                    CAST(CASE WHEN UPPER(f.quantity_limit_yn) = 'Y' THEN 1 ELSE 0 END AS DOUBLE)
                ), 2) AS avg_restriction_count,
                
                -- Insulin coverage using insulin_ref lookup
                COUNT(DISTINCT CASE 
                    WHEN ir.ndc IS NOT NULL THEN f.ndc
                    WHEN ir.rxcui IS NOT NULL AND f.rxcui IS NOT NULL THEN f.rxcui
                    ELSE NULL 
                END) AS insulin_drug_count
                
            FROM plan_formularies pf
            JOIN bronze.brz_basic_formulary f ON pf.formulary_id = f.formulary_id
            LEFT JOIN bronze.brz_insulin_ref ir 
                ON f.ndc = ir.ndc 
                OR f.rxcui = ir.rxcui
            GROUP BY pf.plan_key
        )
        SELECT
            fs.plan_key,
            fs.total_drugs,
            -- Formulary breadth (percentage of common drugs covered - placeholder)
            ROUND(LEAST(100.0, fs.total_drugs / 20.0), 2) AS formulary_breadth_pct,
            fs.generic_tier_count,
            fs.brand_tier_count,
            fs.specialty_tier_count,
            ROUND(100.0 * fs.generic_tier_count / NULLIF(fs.total_drugs, 0), 2) AS generic_tier_pct,
            ROUND(100.0 * fs.specialty_tier_count / NULLIF(fs.total_drugs, 0), 2) AS specialty_tier_pct,
            fs.avg_tier,
            fs.pa_rate,
            fs.st_rate,
            fs.ql_rate,
            fs.avg_restriction_count,
            -- Restrictiveness class: 0 (low), 1 (medium), 2 (high) - QUANTILE-BASED
            -- Use NTILE to divide plans into 3 equal buckets based on avg_restriction_count
            (NTILE(3) OVER (ORDER BY fs.avg_restriction_count ASC) - 1) AS restrictiveness_class,
            fs.insulin_drug_count,
            ROUND(100.0 * fs.insulin_drug_count / NULLIF(fs.total_drugs, 0), 2) AS insulin_coverage_pct,
            CURRENT_TIMESTAMP AS gold_ts
        FROM formulary_stats fs;
    """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n2. Creating indexes...")
    db.execute("CREATE INDEX idx_agg_form_plan ON gold.agg_plan_formulary_metrics(plan_key);")
    db.execute("CREATE INDEX idx_agg_form_restrictiveness ON gold.agg_plan_formulary_metrics(restrictiveness_class);")
    
    # Validate
    print("\n3. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_plans,
            ROUND(AVG(total_drugs), 0) AS avg_drugs,
            ROUND(AVG(generic_tier_pct), 1) AS avg_generic_pct,
            ROUND(AVG(specialty_tier_pct), 1) AS avg_specialty_pct,
            ROUND(AVG(pa_rate), 1) AS avg_pa_rate,
            ROUND(AVG(st_rate), 1) AS avg_st_rate,
            ROUND(AVG(ql_rate), 1) AS avg_ql_rate,
            ROUND(AVG(avg_restriction_count), 2) AS avg_restrictions,
            SUM(CASE WHEN restrictiveness_class = 0 THEN 1 ELSE 0 END) AS low_restrictive,
            SUM(CASE WHEN restrictiveness_class = 1 THEN 1 ELSE 0 END) AS med_restrictive,
            SUM(CASE WHEN restrictiveness_class = 2 THEN 1 ELSE 0 END) AS high_restrictive
        FROM gold.agg_plan_formulary_metrics;
    """)
    
    print(f"\n✓ Formulary metrics aggregation complete:")
    print(f"  - Total plans: {stats['total_plans'][0]:,}")
    print(f"  - Avg drugs per plan: {stats['avg_drugs'][0]:,}")
    print(f"  - Avg generic tier %: {stats['avg_generic_pct'][0]}%")
    print(f"  - Avg specialty tier %: {stats['avg_specialty_pct'][0]}%")
    print(f"\n  Restriction rates:")
    print(f"  - Prior auth: {stats['avg_pa_rate'][0]}%")
    print(f"  - Step therapy: {stats['avg_st_rate'][0]}%")
    print(f"  - Quantity limits: {stats['avg_ql_rate'][0]}%")
    print(f"  - Avg restrictions/drug: {stats['avg_restrictions'][0]}")
    print(f"\n  Restrictiveness classes:")
    print(f"  - Low (0): {stats['low_restrictive'][0]:,}")
    print(f"  - Medium (1): {stats['med_restrictive'][0]:,}")
    print(f"  - High (2): {stats['high_restrictive'][0]:,}")
    
    return True


if __name__ == "__main__":
    success = create_formulary_metrics()
    sys.exit(0 if success else 1)
