"""
Full Pipeline Orchestrator

Runs the DuckDB medallion pipeline:
1. Bronze: Reference ingestion
2. Silver: Placeholder (not implemented)
3. Gold: Aggregations
4. ML: Feature engineering and recommendation outputs
"""

import argparse
import importlib
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.db_manager import get_db


def run_step(module_path, function_name):
    """Import module dynamically and run a target function."""
    module = importlib.import_module(module_path)
    fn = getattr(module, function_name)
    result = fn()
    return result is not False


def run_pipeline(layers=None, force=False):
    """
    Run full medallion pipeline.

    Args:
        layers (list): Specific layers to run, e.g., ['bronze', 'gold']
        force (bool): Reserved for future forced rebuild behavior
    """
    all_layers = ["bronze", "silver", "gold", "ml"]
    layers_to_run = layers if layers else all_layers

    print("=" * 70)
    print("DuckDB Medallion Pipeline")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Layers: {', '.join(layers_to_run)}")
    print(f"Force: {force}")
    print("=" * 70)

    results = {}

    if "bronze" in layers_to_run:
        print("\n" + "=" * 70)
        print("BRONZE LAYER: Reference Ingestion")
        print("=" * 70)

        db = get_db()
        if not db.has_table("bronze.brz_plan_info"):
            print("[ERROR] bronze.brz_plan_info not found.")
            print("Run one-time bootstrap first: python scripts/migrate_to_duckdb.py --force")
            return False

        try:
            print("Running db.bronze.05_ingest_geography.ingest_zipcode_geo ...")
            results["bronze_geography"] = run_step(
                "db.bronze.05_ingest_geography",
                "ingest_zipcode_geo",
            )
        except Exception as exc:
            print(f"[ERROR] Bronze layer failed: {exc}")
            results["bronze"] = False

    if "silver" in layers_to_run:
        print("\n" + "=" * 70)
        print("SILVER LAYER: Transformations")
        print("=" * 70)
        print("[INFO] Silver layer scripts are not implemented in this repository.")
        results["silver"] = True

    if "gold" in layers_to_run:
        print("\n" + "=" * 70)
        print("GOLD LAYER: Dimensions and Aggregations")
        print("=" * 70)
        gold_steps = [
            ("db.gold.03_dim_zipcode", "create_dim_zipcode", "gold_dim_zipcode"),
            ("db.gold.05_agg_formulary", "create_formulary_metrics", "gold_formulary"),
            ("db.gold.07_agg_networks", "create_network_metrics", "gold_networks"),
            ("db.gold.06_agg_cost", "create_cost_metrics", "gold_cost"),
            ("db.gold.08_affordability_index_pca", "calculate_affordability_index", "gold_affordability"),
        ]
        for module_path, function_name, result_key in gold_steps:
            try:
                print(f"Running {module_path}.{function_name} ...")
                results[result_key] = run_step(module_path, function_name)
            except Exception as exc:
                print(f"[ERROR] Gold step failed ({module_path}): {exc}")
                results[result_key] = False

    if "ml" in layers_to_run:
        print("\n" + "=" * 70)
        print("ML LAYER: Feature Engineering")
        print("=" * 70)

        db = get_db()
        try:
            bene_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
            print(f"Found {bene_count:,} synthetic beneficiaries")
            try:
                rx_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary_prescriptions")[0]
                print(f"Found {rx_count:,} synthetic beneficiary prescriptions")
            except Exception:
                print("[WARN] synthetic.syn_beneficiary_prescriptions not found.")
                print("       Re-run: python scripts/generate_beneficiary_profiles.py")
        except Exception:
            print("[ERROR] synthetic.syn_beneficiary not found.")
            print("Run: python scripts/generate_beneficiary_profiles.py")
            results["ml"] = False
            bene_count = 0

        if bene_count > 0:
            ml_steps = [
                ("db.ml.02_assign_geography", "assign_zip_codes", "ml_geography"),
                ("db.ml.03_calculate_distance", "calculate_distance_proxy", "ml_distance"),
                ("db.ml.05_training_pairs", "generate_training_pairs", "ml_training"),
                ("db.ml.06_recommendation_explainer", "create_recommendation_explanations", "ml_explainer"),
            ]
            for module_path, function_name, result_key in ml_steps:
                try:
                    print(f"Running {module_path}.{function_name} ...")
                    results[result_key] = run_step(module_path, function_name)
                except Exception as exc:
                    print(f"[ERROR] ML step failed ({module_path}): {exc}")
                    results[result_key] = False

    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    for step_name, success in results.items():
        status = "[OK]" if success else "[FAIL]"
        print(f"{status} {step_name}")

    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    return bool(results) and all(results.values())


def main():
    parser = argparse.ArgumentParser(description="Run DuckDB medallion pipeline")
    parser.add_argument(
        "--layers",
        nargs="+",
        choices=["bronze", "silver", "gold", "ml"],
        help="Specific layers to run (default: all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reserved for future force rebuild behavior",
    )
    args = parser.parse_args()

    success = run_pipeline(layers=args.layers, force=args.force)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
