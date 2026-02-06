"""
Bronze Layer: Geography Ingestion

Ingest dim_zipcode_geo.csv into bronze.brz_zipcode table.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
import pandas as pd
from datetime import datetime


def ingest_zipcode_geo():
    """
    Ingest zipcode geography data from CSV.
    
    Creates:
        bronze.brz_zipcode - Zip code master with population and density
    """
    db = get_db()
    
    print("=" * 60)
    print("Bronze Ingestion: Zipcode Geography")
    print("=" * 60)
    
    # Read CSV
    zip_file = Path("data/dim_zipcode_geo.csv")
    if not zip_file.exists():
        print(f"ERROR: {zip_file} not found!")
        return False
    
    print(f"\n1. Loading {zip_file}...")
    df = pd.read_csv(zip_file)
    print(f"   ✓ Loaded {len(df):,} zip codes")
    
    # Add metadata
    df['source_filename'] = 'dim_zipcode_geo.csv'
    df['ingestion_ts'] = datetime.now()
    df['updated_at'] = datetime.now()
    
    # Derive county code from county name if needed
    # (For now, we'll create a placeholder - this should be joined with dim_geography)
    print("\n2. Creating bronze.brz_zipcode table...")
    
    # Create schema
    db.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    
    # Drop if exists (for fresh load)
    db.execute("DROP TABLE IF EXISTS bronze.brz_zipcode;")
    
    # Create table from DataFrame
    db.conn.register('zip_df', df)
    
    create_sql = """
        CREATE TABLE bronze.brz_zipcode AS
        SELECT
            zip_code,
            city,
            state,
            county,
            CAST(NULL AS VARCHAR) AS county_code,  -- Will be populated in Silver
            lat,
            lng,
            population,
            density,
            source_filename,
            ingestion_ts,
            updated_at
        FROM zip_df;
    """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n3. Creating indexes...")
    db.execute("CREATE INDEX idx_brz_zip_state ON bronze.brz_zipcode(state);")
    db.execute("CREATE INDEX idx_brz_zip_county_name ON bronze.brz_zipcode(county);")
    
    # Validate
    row_count = db.query_one("SELECT COUNT(*) FROM bronze.brz_zipcode")[0]
    print(f"\n✓ Ingestion complete: {row_count:,} rows in bronze.brz_zipcode")
    
    return True


if __name__ == "__main__":
    success = ingest_zipcode_geo()
    sys.exit(0 if success else 1)
