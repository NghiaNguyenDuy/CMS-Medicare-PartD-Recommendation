"""
Bronze Layer: Insulin Reference Data Ingestion

Ingest insulin_ref.csv containing official insulin NDC/RXCUI lists
from CMS Part D Senior Savings Model.

Creates:
    bronze.brz_insulin_ref - Insulin drug reference lookup
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
import pandas as pd
from datetime import datetime


def ingest_insulin_reference():
    """
    Ingest insulin reference data from CSV.
    
    Creates:
        bronze.brz_insulin_ref - Insulin NDC/RXCUI reference
    """
    db = get_db()
    
    print("=" * 60)
    print("Bronze Ingestion: Insulin Reference Data")
    print("=" * 60)
    
    # Read CSV (support current and legacy locations)
    candidate_files = [
        Path("data/insulin_ref.csv"),
        Path("data/temp/insulin_ref.csv"),
    ]
    insulin_file = next((path for path in candidate_files if path.exists()), None)
    if insulin_file is None:
        print("ERROR: insulin_ref.csv not found in data/ or data/temp/")
        return False
    
    print(f"\n1. Loading {insulin_file}...")
    df = pd.read_csv(insulin_file, dtype={"ndc": str, "rxcui": str})
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    print(f"   ✓ Loaded {len(df):,} insulin NDCs")
    
    # Add metadata
    df['ingestion_ts'] = datetime.now()
    
    # Create schema
    print("\n2. Creating bronze.brz_insulin_ref table...")
    db.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    
    # Drop if exists (for fresh load)
    db.execute("DROP TABLE IF EXISTS bronze.brz_insulin_ref;")
    
    # Create table from DataFrame
    db.conn.register('insulin_df', df)
    
    create_sql = """
        CREATE TABLE bronze.brz_insulin_ref AS
        SELECT
            ndc,
            rxcui,
            source,
            source_year,
            is_insulin,
            ref_ts,
            ingestion_ts
        FROM insulin_df
        WHERE is_insulin = 1;  -- Only insulin drugs
    """
    
    db.execute(create_sql)
    
    # Create indexes
    print("\n3. Creating indexes...")
    db.execute("CREATE INDEX idx_brz_insulin_ndc ON bronze.brz_insulin_ref(ndc);")
    db.execute("CREATE INDEX idx_brz_insulin_rxcui ON bronze.brz_insulin_ref(rxcui);")
    
    # Validate
    row_count = db.query_one("SELECT COUNT(*) FROM bronze.brz_insulin_ref")[0]
    unique_ndcs = db.query_one("SELECT COUNT(DISTINCT ndc) FROM bronze.brz_insulin_ref")[0]
    unique_rxcuis = db.query_one("SELECT COUNT(DISTINCT rxcui) FROM bronze.brz_insulin_ref WHERE rxcui IS NOT NULL")[0]
    
    print(f"\n✓ Ingestion complete:")
    print(f"  - Total records: {row_count:,}")
    print(f"  - Unique NDCs: {unique_ndcs:,}")
    print(f"  - Unique RXCUIs: {unique_rxcuis:,}")
    
    return True


if __name__ == "__main__":
    success = ingest_insulin_reference()
    sys.exit(0 if success else 1)
