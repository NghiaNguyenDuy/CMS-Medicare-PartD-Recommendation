"""
Gold Layer: Zipcode Dimension

Create dim_zipcode dimension table from bronze zipcode data
with county code mapping and density categorization.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
from datetime import datetime


def create_dim_zipcode():
    """
    Create zipcode dimension table.
    
    Creates:
        gold.dim_zipcode - Zip code master with county codes and density categories
    """
    db = get_db()
    
    print("=" * 60)
    print("Gold Layer: Zipcode Dimension")
    print("=" * 60)
    
    # Create gold schema
    db.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    
    # Drop if exists
    db.execute("DROP TABLE IF EXISTS gold.dim_zipcode;")
    
    print("\n1. Creating dim_zipcode from bronze.brz_zipcode...")
    
    create_sql = """
        CREATE TABLE gold.dim_zipcode AS
            SELECT
            zip_code,
            city,
            state,
            county,
            county_code,
            lat,
            lng,
            COALESCE(population, 0) AS population,
            COALESCE(density, 0) AS density,
            -- Density categorization
            CASE
                WHEN COALESCE(density, 0) >= 1000 THEN 'urban'
                WHEN COALESCE(density, 0) >= 250 THEN 'suburban'
                ELSE 'rural'
            END AS density_category,
            CURRENT_TIMESTAMP AS gold_ts
        FROM bronze.brz_zipcode
    """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n2. Creating indexes...")
    db.execute("CREATE INDEX idx_dim_zip_county ON gold.dim_zipcode(county_code);")
    db.execute("CREATE INDEX idx_dim_zip_state ON gold.dim_zipcode(state);")
    db.execute("CREATE INDEX idx_dim_zip_density_cat ON gold.dim_zipcode(density_category);")
    
    # Validate
    print("\n3. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_zipcodes,
            COUNT(DISTINCT county_code) AS unique_counties,
            COUNT(DISTINCT state) AS unique_states,
            SUM(CASE WHEN density_category = 'urban' THEN 1 ELSE 0 END) AS urban_zips,
            SUM(CASE WHEN density_category = 'suburban' THEN 1 ELSE 0 END) AS suburban_zips,
            SUM(CASE WHEN density_category = 'rural' THEN 1 ELSE 0 END) AS rural_zips
        FROM gold.dim_zipcode;
    """)
    
    print(f"\n✓ dim_zipcode created successfully:")
    print(f"  - Total zip codes: {stats['total_zipcodes'][0]:,}")
    print(f"  - Unique counties: {stats['unique_counties'][0]:,}")
    print(f"  - Unique states: {stats['unique_states'][0]:,}")
    print(f"  - Urban: {stats['urban_zips'][0]:,}")
    print(f"  - Suburban: {stats['suburban_zips'][0]:,}")
    print(f"  - Rural: {stats['rural_zips'][0]:,}")
    
    return True


if __name__ == "__main__":
    success = create_dim_zipcode()
    sys.exit(0 if success else 1)
