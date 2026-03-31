"""
Schema Validation and Workflow Review

Validates synthetic-layer schemas and checks
alignment with downstream ML scripts.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
import pandas as pd


def validate_schema():
    """Check synthetic-layer schemas."""
    db = get_db()
    
    print("=" * 70)
    print("SCHEMA VALIDATION: synthetic layer")
    print("=" * 70)
    
    # 1. Check table exists
    print("\n1. Table Existence Check")
    tables = db.list_tables()
    if 'synthetic.syn_beneficiary' not in tables:
        print("   ❌ synthetic.syn_beneficiary NOT FOUND!")
        print("\n   Run: python scripts/generate_beneficiary_profiles.py")
        return False
    
    print("   ✓ synthetic.syn_beneficiary exists")
    if 'synthetic.syn_beneficiary_prescriptions' in tables:
        print("   ✓ synthetic.syn_beneficiary_prescriptions exists")
    else:
        print("   ⚠ synthetic.syn_beneficiary_prescriptions NOT FOUND")
        print("     Re-run: python scripts/generate_beneficiary_profiles.py")
    
    # 2. Show schema
    print("\n2. Table Schema")
    schema = db.query_df("DESCRIBE synthetic.syn_beneficiary")
    print(schema.to_string(index=False))
    
    # 3. Row count
    row_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
    print(f"\n3. syn_beneficiary Row Count: {row_count:,}")
    try:
        rx_row_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary_prescriptions")[0]
        print(f"   syn_beneficiary_prescriptions Row Count: {rx_row_count:,}")
    except:
        pass
    
    # 4. Sample data
    print("\n4. Sample Data (first 3 rows)")
    sample = db.query_df("SELECT * FROM synthetic.syn_beneficiary LIMIT 3")
    print(sample.to_string(index=False))
    
    # 5. Column name validation for downstream scripts
    print("\n5. Column Name Validation")
    
    expected_columns = {
        'bene_synth_id': 'Beneficiary ID',
        'state': 'State (2-char)',
        'county_code': 'County code (5-char)',
        'zip_code': 'Zip code (initially NULL)',
        'lat': 'Latitude (initially NULL)',
        'lng': 'Longitude (initially NULL)',
        'density': 'Population density (initially NULL)',
        'risk_segment': 'Risk level (LOW/MED/HIGH)',
        'unique_drugs': 'Number of unique drugs',
        'fills_target': 'Annual fills',
        'total_rx_cost_est': 'Estimated annual Rx cost',
        'insulin_user_flag': 'Insulin user (0/1)',
        'created_at': 'Timestamp'
    }
    
    actual_columns = schema['column_name'].tolist()
    
    missing = []
    for col in expected_columns.keys():
        if col.lower() in [c.lower() for c in actual_columns]:
            print(f"   ✓ {col}")
        else:
            print(f"   ❌ MISSING: {col}")
            missing.append(col)
    
    if missing:
        print(f"\n   ⚠️  Missing columns: {', '.join(missing)}")
        print("   These may cause downstream script failures!")
    
    # 6. Check for NULL geography (expected initially)
    print("\n6. Geographic Assignment Status")
    geo_stats = db.query_df("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN zip_code IS NULL THEN 1 ELSE 0 END) AS missing_zip,
            SUM(CASE WHEN lat IS NULL THEN 1 ELSE 0 END) AS missing_lat,
            SUM(CASE WHEN county_code IS NOT NULL THEN 1 ELSE 0 END) AS has_county
        FROM synthetic.syn_beneficiary
    """)
    
    print(f"   Total beneficiaries: {geo_stats['total'][0]:,}")
    print(f"   Has county_code: {geo_stats['has_county'][0]:,}")
    print(f"   Missing zip_code: {geo_stats['missing_zip'][0]:,}")
    print(f"   Missing lat/lng: {geo_stats['missing_lat'][0]:,}")
    
    if geo_stats['missing_zip'][0] > 0:
        print("\n   ℹ️  Zip codes not yet assigned.")
        print("      Run: python db/ml/02_assign_geography.py")
    
    # 7. Data quality check
    print("\n7. Data Quality Checks")
    quality = db.query_df("""
        SELECT
            COUNT(DISTINCT bene_synth_id) AS unique_benes,
            COUNT(DISTINCT county_code) AS unique_counties,
            COUNT(DISTINCT state) AS unique_states,
            ROUND(AVG(unique_drugs), 1) AS avg_drugs,
            ROUND(AVG(fills_target), 1) AS avg_fills,
            ROUND(AVG(total_rx_cost_est), 2) AS avg_cost,
            SUM(insulin_user_flag) AS insulin_users,
            ROUND(100.0 * SUM(insulin_user_flag) / COUNT(*), 1) AS insulin_pct
        FROM synthetic.syn_beneficiary
    """)
    
    print(f"   Unique beneficiaries: {quality['unique_benes'][0]:,}")
    print(f"   Counties: {quality['unique_counties'][0]:,}")
    print(f"   States: {quality['unique_states'][0]:,}")
    print(f"   Avg drugs/bene: {quality['avg_drugs'][0]}")
    print(f"   Avg fills/year: {quality['avg_fills'][0]}")
    print(f"   Avg annual cost: ${quality['avg_cost'][0]:,.2f}")
    print(f"   Insulin users: {quality['insulin_users'][0]:,} ({quality['insulin_pct'][0]}%)")
    
    print("\n" + "=" * 70)
    return True


def check_workflow_alignment():
    """Check column alignment with downstream ML scripts."""
    db = get_db()
    
    print("\n" + "=" * 70)
    print("WORKFLOW ALIGNMENT CHECK")
    print("=" * 70)
    
    # Scripts that depend on synthetic.syn_beneficiary
    deps = {
        'db/ml/02_assign_geography.py': [
            'bene_synth_id', 'county_code', 'state'
        ],
        'db/ml/05_training_pairs.py': [
            'bene_synth_id', 'state', 'county_code', 'zip_code',
            'density', 'risk_segment', 'fills_target', 'unique_drugs',
            'total_rx_cost_est', 'insulin_user_flag'
        ]
    }
    
    # Get actual columns
    schema = db.query_df("DESCRIBE synthetic.syn_beneficiary")
    actual_cols_lower = [c.lower() for c in schema['column_name'].tolist()]
    
    print("\n1. Downstream Script Dependencies\n")
    
    all_aligned = True
    for script, required_cols in deps.items():
        print(f"   {Path(script).name}:")
        missing = []
        for col in required_cols:
            if col.lower() in actual_cols_lower:
                print(f"      ✓ {col}")
            else:
                print(f"      ❌ MISSING: {col}")
                missing.append(col)
                all_aligned = False
        
        if missing:
            print(f"      ⚠️  WARNING: {script} may fail!")
        print()
    
    if all_aligned:
        print("   ✅ All scripts aligned!")
    else:
        print("   ⚠️  Some misalignments found. Review column names.")
    
    return all_aligned


def suggest_fixes():
    """Suggest fixes for common issues."""
    db = get_db()
    
    print("\n" + "=" * 70)
    print("SUGGESTED WORKFLOW")
    print("=" * 70)
    
    print("""
STEP 1: Generate Beneficiaries
   Command: python scripts/generate_beneficiary_profiles.py
   Creates: synthetic.syn_beneficiary and synthetic.syn_beneficiary_prescriptions

STEP 2: Assign Zip Codes  
   Command: python db/ml/02_assign_geography.py
   Updates: zip_code, lat, lng, density columns

STEP 3: Calculate Distance Features
   Command: python db/ml/03_calculate_distance.py
   Creates: ml.plan_distance_features

STEP 4: Generate Training Pairs
   Command: python db/ml/05_training_pairs.py
   Creates: ml.training_plan_pairs
   Requires: All Gold tables + Distance features

STEP 5: Generate Recommendations
   Command: python db/ml/06_recommendation_explainer.py
   Creates: ml.recommendation_explanations

FULL PIPELINE:
   python -m db.run_full_pipeline --layers ml
""")
    
    # Check current status
    print("\n" + "=" * 70)
    print("CURRENT STATUS")
    print("=" * 70 + "\n")
    
    checks = [
        ('synthetic.syn_beneficiary', 'Beneficiary table'),
        ('synthetic.syn_beneficiary_prescriptions', 'Beneficiary drug rows'),
        ('bronze.brz_plan_info', 'Bronze plan data'),
        ('bronze.brz_insulin_ref', 'Insulin reference'),
        ('gold.agg_plan_formulary_metrics', 'Formulary metrics'),
        ('gold.agg_plan_network_metrics', 'Network metrics'),
        ('ml.plan_distance_features', 'Distance features'),
        ('ml.training_plan_pairs', 'Training pairs')
    ]
    
    for table, description in checks:
        try:
            count = db.query_one(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"   ✓ {table}: {count:,} rows")
        except:
            print(f"   ❌ {table}: NOT FOUND")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    try:
        # Run validations
        schema_ok = validate_schema()
        
        if schema_ok:
            alignment_ok = check_workflow_alignment()
        
        # Show suggestions
        suggest_fixes()
        
        print("\n✓ Validation complete!\n")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
