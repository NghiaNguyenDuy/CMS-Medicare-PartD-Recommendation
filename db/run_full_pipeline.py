"""
Full Pipeline Orchestrator

Runs the complete DuckDB medallion pipeline:
1. Bronze: Raw ingestion
2. Silver: Transformations
3. Gold: Dimensions & aggregations
4. ML: Feature engineering & training data
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.db_manager import get_db


def run_pipeline(layers=None, force=False):
    """
    Run full medallion pipeline.
    
    Args:
        layers (list): Specific layers to run, e.g., ['bronze', 'silver']
                      If None, runs all layers
        force (bool): Force recreation of tables
    """
    all_layers = ['bronze', 'silver', 'gold', 'ml']
    layers_to_run = layers if layers else all_layers
    
    print("=" * 70)
    print("DuckDB Medallion Pipeline")
    print("=" * 70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Layers: {', '.join(layers_to_run)}")
    print(f"Force: {force}")
    print("=" * 70)
    
    results = {}
    
    # 1. Bronze Layer
    if 'bronze' in layers_to_run:
        print("\n\n" + "=" * 70)
        print("BRONZE LAYER: Raw Ingestion")
        print("=" * 70)
        
        # Check if migrate_to_duckdb has been run
        db = get_db()
        tables = db.list_tables()
        
        if not any('brz_plan_info' in t for t in tables):
            print("\n⚠️  Bronze tables not found!")
            print("Please run: python scripts/migrate_to_duckdb.py --force")
            return False
        
        # Run bronze scripts
        try:
            print("\n▶ Running bronze.05_ingest_geography...")
            from db.bronze import ingest_geography
            results['bronze_geography'] = ingest_geography.ingest_zipcode_geo()
        except Exception as e:
            print(f"❌ Bronze layer failed: {e}")
            results['bronze'] = False
    
    # 2. Silver Layer
    if 'silver' in layers_to_run:
        print("\n\n" + "=" * 70)
        print("SILVER LAYER: Transformations")
        print("=" * 70)
        print("\n⚠️  Silver layer scripts not yet implemented")
        print("Using existing bronze tables directly for now...")
        results['silver'] = True
    
    # 3. Gold Layer
    if 'gold' in layers_to_run:
        print("\n\n" + "=" * 70)
        print("GOLD LAYER: Dimensions & Aggregations")
        print("=" * 70)
        
        try:
            print("\n▶ Running gold.03_dim_zipcode...")
            from db.gold import dim_zipcode
            results['gold_zipcode'] = dim_zipcode.create_dim_zipcode()
            
            print("\n▶ Running gold.05_agg_formulary...")
            from db.gold import agg_formulary
            results['gold_formulary'] = agg_formulary.create_formulary_metrics()
            
            print("\n▶ Running gold.07_agg_networks...")
            from db.gold import agg_networks
            results['gold_networks'] = agg_networks.create_network_metrics()
            
        except Exception as e:
            print(f"❌ Gold layer failed: {e}")
            import traceback
            traceback.print_exc()
            results['gold'] = False
    
    # 4. ML Layer
    if 'ml' in layers_to_run:
        print("\n\n" + "=" * 70)
        print("ML LAYER: Feature Engineering")
        print("=" * 70)
        
        try:
            # Check if synthetic beneficiaries exist
            db = get_db()
            try:
                bene_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
                print(f"\n✓ Found {bene_count:,} synthetic beneficiaries")
                
                print("\n▶ Running ml.02_assign_geography...")
                from db.ml import assign_geography
                results['ml_geography'] = assign_geography.assign_zip_codes()
                
                print("\n▶ Running ml.03_calculate_distance...")
                from db.ml import calculate_distance
                results['ml_distance'] = calculate_distance.calculate_distance_proxy()
                
                print("\n▶ Running ml.05_training_pairs...")
                from db.ml import training_pairs
                results['ml_training'] = training_pairs.generate_training_pairs()
                
                print("\n▶ Running ml.06_recommendation_explainer...")
                from db.ml import recommendation_explainer
                results['ml_explainer'] = recommendation_explainer.create_recommendation_explanations()
                
            except:
                print("\n⚠️  synthetic.syn_beneficiary table not found")
                print("Please generate synthetic beneficiaries first.")
                results['ml'] = False
        except Exception as e:
            print(f"❌ ML layer failed: {e}")
            import traceback
            traceback.print_exc()
            results['ml'] = False
    
    # Summary
    print("\n\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    
    for layer, success in results.items():
        status = "✓" if success else "❌"
        print(f"{status} {layer}")
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    return all(results.values())


def main():
    parser = argparse.ArgumentParser(description='Run DuckDB medallion pipeline')
    parser.add_argument(
        '--layers',
        nargs='+',
        choices=['bronze', 'silver', 'gold', 'ml'],
        help='Specific layers to run (default: all)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force recreation of tables'
    )
    
    args = parser.parse_args()
    
    success = run_pipeline(layers=args.layers, force=args.force)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
