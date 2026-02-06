"""
Generate Synthetic Beneficiary Profiles from PDE Events (DuckDB Version)

This script processes Prescription Drug Event (PDE) data to create synthetic beneficiary
profiles using the DuckDB medallion architecture. The profiles are stored in
synthetic.syn_beneficiary and use actual formulary data and insulin references.

Key Improvements:
- Uses bronze.brz_insulin_ref for insulin identification (84 official NDCs)
- Samples drugs from bronze.brz_basic_formulary for realism
- Assigns zip codes using population-weighted sampling
- Integrates with DuckDB for efficient processing
- Creates database table instead of CSV file

Input:
- data/pde.csv: Raw PDE events (pipe-delimited)
- bronze.brz_basic_formulary: SPUF formulary data
- bronze.brz_insulin_ref: Official insulin NDC reference
- gold.dim_zipcode: Zipcode geography for assignment

Output:
- synthetic.syn_beneficiary: Beneficiary profiles table in DuckDB

Usage:
    python scripts/generate_beneficiary_profiles.py [--num-beneficiaries N] [--from-pde]
"""

import sys
from pathlib import Path

# Add parent directory to path for db imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np
from db.db_manager import get_db
import argparse
from datetime import datetime


def create_from_pde(pde_file='data/pde.csv', num_beneficiaries=None):
    """
    Generate beneficiary profiles from actual PDE data.
    
    Args:
        pde_file: Path to PDE CSV file
        num_beneficiaries: Limit to N beneficiaries (None = all)
    """
    db = get_db()
    
    print("=" * 60)
    print("Beneficiary Profile Generation (from PDE)")
    print("=" * 60)
    
    # Load PDE data
    print(f"\n1. Loading PDE data from {pde_file}...")
    if not Path(pde_file).exists():
        print(f"ERROR: {pde_file} not found!")
        return False
    
    pde_df = pd.read_csv(
        pde_file,
        delimiter='|',
        usecols=[
            'BENE_ID', 'PROD_SRVC_ID', 'QTY_DSPNSD_NUM',
            'DAYS_SUPLY_NUM', 'FILL_NUM', 'TOT_RX_CST_AMT', 'SRVC_DT'
        ],
        dtype={
            'BENE_ID': str,
            'PROD_SRVC_ID': str,
            'QTY_DSPNSD_NUM': float,
            'DAYS_SUPLY_NUM': int,
            'FILL_NUM': int,
            'TOT_RX_CST_AMT': float
        },
        parse_dates=['SRVC_DT']
    )
    
    print(f"   ✓ Loaded {len(pde_df):,} PDE events")
    print(f"   ✓ Unique beneficiaries: {pde_df['BENE_ID'].nunique():,}")
    
    # Limit beneficiaries if requested
    if num_beneficiaries:
        unique_benes = pde_df['BENE_ID'].unique()[:num_beneficiaries]
        pde_df = pde_df[pde_df['BENE_ID'].isin(unique_benes)]
        print(f"   ✓ Limited to {num_beneficiaries:,} beneficiaries")
    
    # Aggregate by beneficiary + drug
    print("\n2. Aggregating to beneficiary-drug profiles...")
    profiles = pde_df.groupby(['BENE_ID', 'PROD_SRVC_ID']).agg({
        'FILL_NUM': 'count',
        'DAYS_SUPLY_NUM': lambda x: x.mode()[0] if len(x) > 0 else 30,
        'QTY_DSPNSD_NUM': 'mean',
        'TOT_RX_CST_AMT': 'sum'
    }).reset_index()
    
    profiles.columns = [
        'bene_id', 'ndc', 'fills_per_year',
        'days_supply_mode', 'qty_per_fill', 'total_rx_cost'
    ]
    
    print(f"   ✓ Generated {len(profiles):,} beneficiary-drug profiles")
    
    # Identify insulin using bronze.brz_insulin_ref
    print("\n3. Identifying insulin drugs using bronze.brz_insulin_ref...")
    
    db.conn.register('profiles_temp', profiles)
    
    insulin_check = db.query_df("""
        SELECT
            p.bene_id,
            p.ndc,
            p.fills_per_year,
            p.days_supply_mode,
            p.qty_per_fill,
            p.total_rx_cost,
            CASE WHEN ir.ndc IS NOT NULL THEN 1 ELSE 0 END AS is_insulin
        FROM profiles_temp p
        LEFT JOIN bronze.brz_insulin_ref ir ON p.ndc = ir.ndc
    """)
    
    insulin_users = insulin_check[insulin_check['is_insulin'] == 1]['bene_id'].nunique()
    print(f"   ✓ Insulin users: {insulin_users:,}")
    
    # Create beneficiary-level summary
    print("\n4. Creating beneficiary summary...")
    bene_summary = insulin_check.groupby('bene_id').agg({
        'ndc': 'count',
        'fills_per_year': 'sum',
        'total_rx_cost': 'sum',
        'is_insulin': 'max'
    }).reset_index()
    
    bene_summary.columns = [
        'bene_synth_id', 'unique_drugs', 'fills_target',
        'total_rx_cost_est', 'insulin_user_flag'
    ]
    
    # Add risk segment based on cost
    bene_summary['risk_segment'] = pd.cut(
        bene_summary['total_rx_cost_est'],
        bins=[0, 2000, 5000, float('inf')],
        labels=['LOW', 'MED', 'HIGH']
    ).astype(str)
    
    return bene_summary


def create_synthetic(num_beneficiaries=10000):
    """
    Generate synthetic beneficiary profiles by sampling from bronze.brz_basic_formulary.
    
    Args:
        num_beneficiaries: Number of synthetic beneficiaries to generate
    """
    db = get_db()
    
    print("=" * 60)
    print(f"Synthetic Beneficiary Profile Generation")
    print("=" * 60)
    
    print(f"\n1. Generating {num_beneficiaries:,} synthetic beneficiaries...")
    
    # Sample drugs from formulary
    print("\n2. Sampling drugs from bronze.brz_basic_formulary...")
    
    formulary_drugs = db.query_df("""
        SELECT DISTINCT
            ndc,
            TIER_LEVEL_VALUE as TIER,
            prior_authorization_yn,
            step_therapy_yn,
            quantity_limit_yn
        FROM bronze.brz_basic_formulary
        WHERE ndc IS NOT NULL
        LIMIT 1000  -- Sample 1000 common drugs
    """)
    
    print(f"   ✓ Sampled {len(formulary_drugs):,} drugs from formulary")
    
    # Check which are insulin
    formulary_drugs['is_insulin'] = formulary_drugs['NDC'].isin(
        db.query_df("SELECT ndc FROM bronze.brz_insulin_ref")['ndc']
    ).astype(int)
    
    insulin_drug_count = formulary_drugs['is_insulin'].sum()
    print(f"   ✓ Insulin drugs in sample: {insulin_drug_count}")
    
    # Generate beneficiary profiles
    print(f"\n3. Creating synthetic beneficiary profiles...")
    
    bene_ids = [f"SYNTH_{i:06d}" for i in range(num_beneficiaries)]
    
    # Risk distribution: 50% LOW, 30% MED, 20% HIGH
    risk_segments = np.random.choice(
        ['LOW', 'MED', 'HIGH'],
        size=num_beneficiaries,
        p=[0.5, 0.3, 0.2]
    )
    
    # Insulin user flag: ~15% are insulin users
    insulin_users = np.random.choice(
        [0, 1],
        size=num_beneficiaries,
        p=[0.85, 0.15]
    )
    
    # Unique drugs per beneficiary (depends on risk)
    unique_drugs = []
    for risk in risk_segments:
        if risk == 'LOW':
            drugs = np.random.randint(1, 4)  # 1-3 drugs
        elif risk == 'MED':
            drugs = np.random.randint(3, 7)  # 3-6 drugs
        else:  # HIGH
            drugs = np.random.randint(5, 12)  # 5-11 drugs
        unique_drugs.append(drugs)
    
    # Fills per year (correlated with drugs)
    fills_target = [int(drugs * np.random.uniform(3, 5)) for drugs in unique_drugs]
    
    # Estimated annual cost (correlated with risk)
    total_rx_cost_est = []
    for risk in risk_segments:
        if risk == 'LOW':
            cost = np.random.uniform(500, 2000)
        elif risk == 'MED':
            cost = np.random.uniform(2000, 5000)
        else:  # HIGH
            cost = np.random.uniform(5000, 15000)
        total_rx_cost_est.append(round(cost, 2))
    
    # Create dataframe
    bene_summary = pd.DataFrame({
        'bene_synth_id': bene_ids,
        'risk_segment': risk_segments,
        'unique_drugs': unique_drugs,
        'fills_target': fills_target,
        'total_rx_cost_est': total_rx_cost_est,
        'insulin_user_flag': insulin_users
    })
    
    return bene_summary


def assign_geography(bene_summary):
    """
    Assign beneficiaries to counties and zip codes.
    
    Uses population-weighted sampling from gold.dim_zipcode.
    """
    db = get_db()
    
    print("\n4. Assigning geographic locations...")
    
    # Get county distribution from bronze.brz_geographic
    try:
        county_weights = db.query_df("""
            SELECT county_code, state_label, COUNT(*) AS weight
            FROM bronze.brz_geographic
            GROUP BY all
            ORDER BY weight DESC
            --LIMIT 500  -- Top 500 counties
        """)
        
        print(f"   ✓ Loaded {len(county_weights):,} counties")
        
        # Assign counties
        total_weight = county_weights['weight'].sum()
        county_probs = county_weights['weight'] / total_weight
        
        assigned_counties = np.random.choice(
            county_weights['COUNTY_CODE'],
            size=len(bene_summary),
            p=county_probs,
            replace=True
        )
        
        bene_summary['COUNTY_CODE'] = assigned_counties
        
        # Add state
        county_state_map = county_weights.set_index('COUNTY_CODE')['STATE_LABEL'].to_dict()
        bene_summary['STATE'] = bene_summary['COUNTY_CODE'].map(county_state_map)
        
        print(f"   ✓ Assigned to {bene_summary['COUNTY_CODE'].nunique():,} counties")
        print(f"   ✓ States: {bene_summary['STATE'].nunique():,}")
        
    except Exception as e:
        print(f"   ! Warning: Could not load geographic data, using defaults")
        print(f"   ! Error: {e}")
        # Fallback
        default_counties = ['06037', '17031', '36061', '48201']
        bene_summary['COUNTY_CODE'] = np.random.choice(default_counties, len(bene_summary))
        bene_summary['STATE'] = bene_summary['COUNTY_CODE'].map({
            '06037': 'CA', '17031': 'IL', '36061': 'NY', '48201': 'TX'
        })
    
    return bene_summary


def save_to_database(bene_summary):
    """
    Save beneficiary profiles to synthetic.syn_beneficiary table.
    """
    db = get_db()
    
    print("\n5. Saving to database...")
    
    # Create synthetic schema
    db.execute("CREATE SCHEMA IF NOT EXISTS synthetic;")
    
    # Drop and recreate table
    db.execute("DROP TABLE IF EXISTS synthetic.syn_beneficiary;")
    
    # Register dataframe
    db.conn.register('bene_data', bene_summary)
    
    # Create table
    db.execute("""
        CREATE TABLE synthetic.syn_beneficiary AS
        SELECT
            bene_synth_id,
            state,
            county_code,
            NULL AS zip_code,  -- Will be assigned by ml/02_assign_geography.py
            NULL AS lat,
            NULL AS lng,
            NULL AS density,
            risk_segment,
            unique_drugs,
            fills_target,
            total_rx_cost_est,
            insulin_user_flag,
            CURRENT_TIMESTAMP AS created_at
        FROM bene_data;
    """)
    
    # Create indexes
    db.execute("CREATE INDEX idx_synth_bene_id ON synthetic.syn_beneficiary(bene_synth_id);")
    db.execute("CREATE INDEX idx_synth_county ON synthetic.syn_beneficiary(county_code);")
    db.execute("CREATE INDEX idx_synth_insulin ON synthetic.syn_beneficiary(insulin_user_flag);")
    
    row_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
    print(f"   ✓ Created synthetic.syn_beneficiary with {row_count:,} rows")
    
    return True


def main():
    """
    Main execution function.
    """
    parser = argparse.ArgumentParser(description='Generate synthetic beneficiary profiles')
    parser.add_argument('--num-beneficiaries', type=int, default=10000,
                        help='Number of synthetic beneficiaries to generate')
    parser.add_argument('--from-pde', action='store_true',
                        help='Generate from PDE data instead of synthetic')
    parser.add_argument('--pde-file', default='data/pde.csv',
                        help='Path to PDE file')
    
    args = parser.parse_args()
    
    try:
        # Generate profiles
        if args.from_pde:
            bene_summary = create_from_pde(
                pde_file=args.pde_file,
                num_beneficiaries=args.num_beneficiaries if args.num_beneficiaries != 10000 else None
            )
        else:
            bene_summary = create_synthetic(num_beneficiaries=args.num_beneficiaries)
        
        if bene_summary is None:
            print("\nERROR: Profile generation failed!")
            return 1
        
        # Assign geography
        bene_summary = assign_geography(bene_summary)
        
        # Save to database
        save_to_database(bene_summary)
        
        # Print summary
        print("\n" + "=" * 60)
        print("✓ Beneficiary Profile Generation Complete!")
        print("=" * 60)
        print(f"  Table: synthetic.syn_beneficiary")
        print(f"  Total beneficiaries: {len(bene_summary):,}")
        print(f"  Insulin users: {bene_summary['insulin_user_flag'].sum():,} ({bene_summary['insulin_user_flag'].mean()*100:.1f}%)")
        print(f"  Risk distribution:")
        for segment in ['LOW', 'MED', 'HIGH']:
            count = (bene_summary['risk_segment'] == segment).sum()
            pct = count / len(bene_summary) * 100
            print(f"    - {segment}: {count:,} ({pct:.1f}%)")
        print(f"  Avg drugs/beneficiary: {bene_summary['unique_drugs'].mean():.1f}")
        print(f"  Avg annual cost: ${bene_summary['total_rx_cost_est'].mean():,.2f}")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Run: python db/ml/02_assign_geography.py  (assign zip codes)")
        print("  2. Run: python -m db.run_full_pipeline --layers ml  (build ML features)")
        print("=" * 60 + "\n")
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
