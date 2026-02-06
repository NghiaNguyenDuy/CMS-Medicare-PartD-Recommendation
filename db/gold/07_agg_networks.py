"""
Gold Layer: Plan Network Metrics Aggregation

Calculate network adequacy metrics per plan:
- Total pharmacies
- Preferred pharmacies
- Network adequacy flags

Creates:
    gold.agg_plan_network_metrics - Network statistics per plan
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def create_network_metrics():
    """
    Create plan network metrics aggregation.
    
    Creates:
        gold.agg_plan_network_metrics - Network adequacy per plan
    """
    db = get_db()
    
    print("=" * 60)
    print("Gold Layer: Plan Network Metrics")
    print("=" * 60)
    
    # Create gold schema
    db.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS gold.agg_plan_network_metrics;")
    
    print("\n1. Aggregating network metrics...")
    
    # Check if pharmacy_networks table exists
    tables = db.list_tables()
    # has_network_table = any('pharmacy' in t.lower() and 'network' in t.lower() for t in tables)
    
    # if has_network_table:
    create_sql = """
        CREATE TABLE gold.agg_plan_network_metrics AS
        SELECT
            pn.PLAN_KEY as plan_key,
            
            -- Total pharmacies in network
            COUNT(DISTINCT pn.PHARMACY_NUMBER) AS total_pharmacies,
            
            -- Preferred pharmacies (retail + mail)
            SUM(CASE WHEN pn.IS_PREFERRED_RETAIL = 1 THEN 1 ELSE 0 END) AS preferred_pharmacies_retail,
            SUM(CASE WHEN pn.IS_PREFERRED_MAIL = 1 THEN 1 ELSE 0 END) AS preferred_pharmacies_mail,
            SUM(CASE WHEN pn.IS_PREFERRED_RETAIL = 1 OR pn.IS_PREFERRED_MAIL = 1 THEN 1 ELSE 0 END) AS preferred_pharmacies,
            
            -- Percentage preferred
            ROUND(100.0 * SUM(CASE WHEN pn.IS_PREFERRED_RETAIL = 1 OR pn.IS_PREFERRED_MAIL = 1 THEN 1 ELSE 0 END) / 
                NULLIF(COUNT(DISTINCT pn.PHARMACY_NUMBER), 0), 2) AS pref_pharmacy_pct,
            
            -- Retail vs Mail availability
            SUM(CASE WHEN pn.OFFERS_RETAIL = 1 THEN 1 ELSE 0 END) AS retail_pharmacies,
            SUM(CASE WHEN pn.OFFERS_MAIL = 1 THEN 1 ELSE 0 END) AS mail_pharmacies,
            
            -- In-area pharmacies
            SUM(CASE WHEN pn.IS_IN_AREA = 1 THEN 1 ELSE 0 END) AS in_area_pharmacies,
            ROUND(100.0 * SUM(CASE WHEN pn.IS_IN_AREA = 1 THEN 1 ELSE 0 END) / 
                NULLIF(COUNT(DISTINCT pn.PHARMACY_NUMBER), 0), 2) AS in_area_pct,
            
            -- Network adequacy flag: 1 if poor network (<10 preferred)
            CASE 
                WHEN SUM(CASE WHEN pn.IS_PREFERRED_RETAIL = 1 OR pn.IS_PREFERRED_MAIL = 1 THEN 1 ELSE 0 END) < 10 
                THEN 1 
                ELSE 0 
            END AS network_adequacy_flag,
            
            -- Mail order availability
            MAX(CASE WHEN pn.OFFERS_MAIL = 1 THEN 1 ELSE 0 END) AS mail_order_available,
            
            -- Average fees
            AVG(pn.AVG_BRAND_FEE) AS avg_brand_dispensing_fee,
            AVG(pn.AVG_GENERIC_FEE) AS avg_generic_dispensing_fee,
            AVG(pn.FLOOR_PRICE) AS avg_floor_price,
            
            CURRENT_TIMESTAMP AS gold_ts
        FROM bronze.brz_pharmacy_network pn
        GROUP BY pn.PLAN_KEY;
    """
    # else:
    #     # Create placeholder table with estimated values
    #     print("   ⚠️  Pharmacy network table not found, using estimates...")
    #     create_sql = """
    #         CREATE TABLE gold.agg_plan_network_metrics AS
    #         SELECT
    #             p.plan_key,
    #             -- Estimate based on plan type
    #             CASE
    #                 WHEN p.contract_type LIKE '%PDP%' THEN 50
    #                 WHEN p.contract_type LIKE '%MA%' THEN 30
    #                 ELSE 40
    #             END AS total_pharmacies,
    #             CASE
    #                 WHEN p.contract_type LIKE '%PDP%' THEN 25
    #                 WHEN p.contract_type LIKE '%MA%' THEN 15
    #                 ELSE 20
    #             END AS preferred_pharmacies,
    #             50.0 AS pref_pharmacy_pct,
    #             0 AS network_adequacy_flag,
    #             1 AS mail_order_available,
    #             CURRENT_TIMESTAMP AS gold_ts
    #         FROM bronze.brz_plan_info p
    #         WHERE p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y'
    #         GROUP BY p.plan_key, p.contract_type;
    #     """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n2. Creating indexes...")
    db.execute("CREATE INDEX idx_agg_net_plan ON gold.agg_plan_network_metrics(plan_key);")
    
    # Validate
    print("\n3. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_plans,
            ROUND(AVG(total_pharmacies), 1) AS avg_total_pharmacies,
            ROUND(AVG(preferred_pharmacies), 1) AS avg_preferred_pharmacies,
            ROUND(AVG(retail_pharmacies), 1) AS avg_retail_pharmacies,
            ROUND(AVG(mail_pharmacies), 1) AS avg_mail_pharmacies,
            ROUND(AVG(pref_pharmacy_pct), 1) AS avg_pref_pct,
            ROUND(AVG(in_area_pct), 1) AS avg_in_area_pct,
            SUM(network_adequacy_flag) AS poor_network_plans,
            SUM(mail_order_available) AS mail_order_plans,
            ROUND(AVG(avg_brand_dispensing_fee), 2) AS avg_brand_fee,
            ROUND(AVG(avg_generic_dispensing_fee), 2) AS avg_generic_fee
        FROM gold.agg_plan_network_metrics;
    """)
    
    print(f"\n✓ Network metrics aggregation complete:")
    print(f"  - Total plans: {stats['total_plans'][0]:,}")
    print(f"\n  Pharmacy counts:")
    print(f"  - Avg total pharmacies: {stats['avg_total_pharmacies'][0]}")
    print(f"  - Avg preferred pharmacies: {stats['avg_preferred_pharmacies'][0]}")
    print(f"  - Avg retail pharmacies: {stats['avg_retail_pharmacies'][0]}")
    print(f"  - Avg mail pharmacies: {stats['avg_mail_pharmacies'][0]}")
    print(f"\n  Coverage:")
    print(f"  - Avg preferred %: {stats['avg_pref_pct'][0]}%")
    print(f"  - Avg in-area %: {stats['avg_in_area_pct'][0]}%")
    print(f"\n  Quality flags:")
    print(f"  - Plans with poor network: {stats['poor_network_plans'][0]:,}")
    print(f"  - Plans with mail order: {stats['mail_order_plans'][0]:,}")
    print(f"\n  Dispensing fees:")
    print(f"  - Avg brand fee: ${stats['avg_brand_fee'][0]:.2f}")
    print(f"  - Avg generic fee: ${stats['avg_generic_fee'][0]:.2f}")
    
    return True


if __name__ == "__main__":
    success = create_network_metrics()
    sys.exit(0 if success else 1)
