"""
ML Prep: Distance Proxy Calculation

Calculate simulated distance to nearest preferred pharmacy based on:
- Population density (from bronze.brz_zipcode)
- Network adequacy from gold.agg_plan_network_metrics (actual # of preferred pharmacies)
- Plan type (PDP vs MA-PD) as fallback

Creates:
    ml.plan_distance_features - Distance metrics per plan-county
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def calculate_distance_proxy():
    """
    Calculate distance proxy for plan-county combinations.
    
    Creates:
        ml.plan_distance_features - Simulated distance to pharmacies
    """
    db = get_db()
    
    print("=" * 60)
    print("ML Prep: Distance Proxy Calculation")
    print("=" * 60)
    
    # Create ML schema
    db.execute("CREATE SCHEMA IF NOT EXISTS ml;")
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS ml.plan_distance_features;")
    
    print("\n1. Calculating distance proxy...")
    print("   - Based on density + network adequacy + plan type")
    
    create_sql = """
        CREATE TABLE ml.plan_distance_features AS
        WITH plan_counties AS (
            -- Get unique plan-county combinations
            SELECT DISTINCT
                p.plan_key,
                case 
                    when p.IS_MA_PD then 'MA'
                    when p.IS_PDP then 'PDP'
                    else null end as contract_type,
                p.state,
                p.county_code
            FROM bronze.brz_plan_info p
            WHERE p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y'
        ),
        county_density AS (
            -- Average density per county from zipcode data
            SELECT
                z.county_code,
                AVG(COALESCE(z.density, 0)) AS avg_density,
                COUNT(*) AS zip_count,
                MAX(z.population) AS max_pop
            FROM bronze.brz_zipcode z
            GROUP BY z.county_code
        ),
        network_adequacy AS (
            -- Use actual network metrics from gold layer
            SELECT
                p.plan_key,
                p.county_code,
                COALESCE(nm.preferred_pharmacies, 
                    -- Fallback estimate if no network data
                    CASE
                        WHEN p.contract_type = 'PDP' THEN 25
                        WHEN p.contract_type = 'MA' THEN 15
                        ELSE 20
                    END
                ) AS preferred_pharmacies,
                COALESCE(nm.total_pharmacies, 30) AS total_pharmacies,
                COALESCE(nm.pref_pharmacy_pct, 50.0) AS pref_pharmacy_pct,
                COALESCE(nm.in_area_pharmacies, 20) AS in_area_pharmacies
            FROM plan_counties p
            LEFT JOIN gold.agg_plan_network_metrics nm ON p.plan_key = nm.plan_key
        )
        SELECT
            pc.plan_key,
            pc.contract_type,
            pc.state,
            pc.county_code,
            COALESCE(cd.avg_density, 100) AS county_avg_density,
            COALESCE(cd.zip_count, 1) AS county_zip_count,
            na.preferred_pharmacies,
            na.total_pharmacies,
            na.pref_pharmacy_pct,
            na.in_area_pharmacies,
            
            -- Distance proxy calculation using ACTUAL network data
            CASE
                -- Urban + high network adequacy = very close (0.5 - 2 miles)
                WHEN cd.avg_density >= 1000 AND na.preferred_pharmacies >= 20 
                THEN 0.5 + (RANDOM() * 1.5)
                
                -- Suburban + medium network = moderate (2 - 8 miles)
                WHEN cd.avg_density >= 250 AND cd.avg_density < 1000 
                    AND na.preferred_pharmacies >= 10
                THEN 2.0 + (RANDOM() * 6.0)
                
                -- Rural + low network = far (8 - 25 miles)
                WHEN cd.avg_density < 250 OR na.preferred_pharmacies < 10
                THEN 8.0 + (RANDOM() * 17.0)
                
                ELSE 5.0 + (RANDOM() * 10.0)
            END AS simulated_distance_miles,
            
            CURRENT_TIMESTAMP AS ml_ts
        FROM plan_counties pc
        LEFT JOIN county_density cd ON pc.county_code = cd.county_code
        LEFT JOIN network_adequacy na ON pc.plan_key = na.plan_key 
            AND pc.county_code = na.county_code;
    """
    
    db.execute(create_sql)
    
    # Add distance category
    print("\n2. Categorizing distances...")
    db.execute("""
        ALTER TABLE ml.plan_distance_features
        ADD COLUMN distance_category VARCHAR;
        
        UPDATE ml.plan_distance_features
        SET distance_category = CASE
            WHEN simulated_distance_miles < 3 THEN 'very_close'
            WHEN simulated_distance_miles < 8 THEN 'nearby'
            WHEN simulated_distance_miles < 15 THEN 'moderate'
            ELSE 'far'
        END;
    """)
    
    # Create indexes
    print("\n3. Creating indexes...")
    db.execute("CREATE INDEX idx_dist_plan ON ml.plan_distance_features(plan_key);")
    db.execute("CREATE INDEX idx_dist_county ON ml.plan_distance_features(county_code);")
    db.execute("CREATE INDEX idx_dist_category ON ml.plan_distance_features(distance_category);")
    
    # Validate
    print("\n4. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_plan_counties,
            COUNT(DISTINCT plan_key) AS unique_plans,
            COUNT(DISTINCT county_code) AS unique_counties,
            ROUND(AVG(simulated_distance_miles), 2) AS avg_distance,
            ROUND(MIN(simulated_distance_miles), 2) AS min_distance,
            ROUND(MAX(simulated_distance_miles), 2) AS max_distance,
            ROUND(AVG(preferred_pharmacies), 1) AS avg_pref_pharmacies,
            ROUND(AVG(total_pharmacies), 1) AS avg_total_pharmacies,
            SUM(CASE WHEN distance_category = 'very_close' THEN 1 ELSE 0 END) AS very_close_count,
            SUM(CASE WHEN distance_category = 'nearby' THEN 1 ELSE 0 END) AS nearby_count,
            SUM(CASE WHEN distance_category = 'moderate' THEN 1 ELSE 0 END) AS moderate_count,
            SUM(CASE WHEN distance_category = 'far' THEN 1 ELSE 0 END) AS far_count
        FROM ml.plan_distance_features;
    """)
    
    print(f"\n✓ Distance proxy calculation complete:")
    print(f"  - Total plan-county pairs: {stats['total_plan_counties'][0]:,}")
    print(f"  - Unique plans: {stats['unique_plans'][0]:,}")
    print(f"  - Unique counties: {stats['unique_counties'][0]:,}")
    print(f"\n  Network adequacy:")
    print(f"  - Avg preferred pharmacies: {stats['avg_pref_pharmacies'][0]}")
    print(f"  - Avg total pharmacies: {stats['avg_total_pharmacies'][0]}")
    print(f"\n  Distance metrics:")
    print(f"  - Avg distance: {stats['avg_distance'][0]} miles")
    print(f"  - Range: {stats['min_distance'][0]} - {stats['max_distance'][0]} miles")
    print(f"\n  Distance categories:")
    print(f"  - Very close (<3mi): {stats['very_close_count'][0]:,}")
    print(f"  - Nearby (3-8mi): {stats['nearby_count'][0]:,}")
    print(f"  - Moderate (8-15mi): {stats['moderate_count'][0]:,}")
    print(f"  - Far (>15mi): {stats['far_count'][0]:,}")
    
    return True


if __name__ == "__main__":
    success = calculate_distance_proxy()
    sys.exit(0 if success else 1)
