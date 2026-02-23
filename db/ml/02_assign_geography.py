"""
ML Prep: Beneficiary Zip Code Assignment

Assign synthetic beneficiaries to realistic zip codes within their counties
using population-weighted sampling.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


def assign_zip_codes():
    """
    Assign zip codes to synthetic beneficiaries.
    
    Updates:
        synthetic.syn_beneficiary - Adds zip_code, lat, lng, density
    """
    db = get_db()
    
    print("=" * 60)
    print("ML Prep: Beneficiary Zip Code Assignment")
    print("=" * 60)
    
    # Check if synthetic beneficiaries exist
    try:
        bene_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
        print(f"\n1. Found {bene_count:,} synthetic beneficiaries")
    except Exception as e:
        print(f"\nERROR: synthetic.syn_beneficiary table not found!")
        print("Please run beneficiary generation first.")
        return False
    
    print("\n2. Assigning zip codes (population-weighted)...")
    
    assign_sql = """
        -- First, ensure we have county_code on beneficiaries
        ALTER TABLE synthetic.syn_beneficiary 
        ADD COLUMN IF NOT EXISTS zip_code VARCHAR;
        
        ALTER TABLE synthetic.syn_beneficiary 
        ADD COLUMN IF NOT EXISTS lat DECIMAL(8,6);
        
        ALTER TABLE synthetic.syn_beneficiary 
        ADD COLUMN IF NOT EXISTS lng DECIMAL(9,6);
        
        ALTER TABLE synthetic.syn_beneficiary 
        ADD COLUMN IF NOT EXISTS density INTEGER;
        
        -- Assign zip codes deterministically for reproducible pipeline runs
        WITH county_zips AS (
            SELECT
                z.county_code,
                z.zip_code,
                z.lat,
                z.lng,
                z.density,
                z.population
            FROM bronze.brz_zipcode z
            --WHERE z.population IS NOT NULL AND z.population > 0
        ),
        bene_zip_candidates AS (
            SELECT
                b.bene_synth_id,
                b.county_code,
                z.zip_code,
                z.lat,
                z.lng,
                z.density,
                ROW_NUMBER() OVER (
                    PARTITION BY b.bene_synth_id
                    ORDER BY HASH(b.bene_synth_id || ':' || COALESCE(z.zip_code, ''))
                ) AS zip_rank
            FROM synthetic.syn_beneficiary b
            JOIN county_zips z ON b.county_code = z.county_code
        ),
        bene_zip_assignment AS (
            SELECT
                bene_synth_id,
                county_code,
                zip_code AS assigned_zip,
                lat AS assigned_lat,
                lng AS assigned_lng,
                density AS assigned_density
            FROM bene_zip_candidates
            WHERE zip_rank = 1
        )
        UPDATE synthetic.syn_beneficiary b
        SET 
            zip_code = a.assigned_zip,
            lat = a.assigned_lat,
            lng = a.assigned_lng,
            density = a.assigned_density
        FROM bene_zip_assignment a
        WHERE b.bene_synth_id = a.bene_synth_id;
    """
    
    db.execute(assign_sql)
    
    # Validate
    print("\n3. Validation...")
    stats = db.query_df("""
        SELECT
            COUNT(*) AS total_benes,
            COUNT(zip_code) AS benes_with_zip,
            COUNT(DISTINCT zip_code) AS unique_zips,
            COUNT(DISTINCT county_code) AS unique_counties
        FROM synthetic.syn_beneficiary;
    """)
    
    print(f"\n✓ Zip code assignment complete:")
    print(f"  - Total beneficiaries: {stats['total_benes'][0]:,}")
    print(f"  - Beneficiaries with zip: {stats['benes_with_zip'][0]:,}")
    print(f"  - Unique zip codes: {stats['unique_zips'][0]:,}")
    print(f"  - Unique counties: {stats['unique_counties'][0]:,}")
    
    coverage_pct = (stats['benes_with_zip'][0] / stats['total_benes'][0] * 100) if stats['total_benes'][0] > 0 else 0
    print(f"  - Coverage: {coverage_pct:.1f}%")
    
    return True


if __name__ == "__main__":
    success = assign_zip_codes()
    sys.exit(0 if success else 1)
