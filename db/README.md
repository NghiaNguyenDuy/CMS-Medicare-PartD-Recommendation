# DuckDB Pipeline (`db/`)

This repository currently uses:
- `bronze` scripts for reference-table ingestion
- `gold` scripts for plan-level aggregations
- `ml` scripts for feature engineering and recommendation explanations
- `run_full_pipeline.py` as the executable orchestrator

## Actual executable modules

### Bronze
- `db/bronze/05_ingest_geography.py`
- `db/bronze/06_ingest_insulin_ref.py`

### Gold
- `db/gold/03_dim_zipcode.py`
- `db/gold/05_agg_formulary.py`
- `db/gold/06_agg_cost.py`
- `db/gold/07_agg_networks.py`
- `db/gold/08_affordability_index_pca.py`

### ML
- `db/ml/02_assign_geography.py`
- `db/ml/03_calculate_distance.py`
- `db/ml/05_training_pairs.py`
- `db/ml/06_recommendation_explainer.py`

## Standard execution flow
```bash
# one-time bootstrap only if database file does not exist
python scripts/migrate_to_duckdb.py --force

python db/bronze/06_ingest_insulin_ref.py
python db/bronze/05_ingest_geography.py
python scripts/generate_beneficiary_profiles.py
python -m db.run_full_pipeline --layers gold ml
```

## Notes
- `scripts/migrate_to_duckdb.py` is treated as one-time bootstrap for existing historical data.
- App/inference should use read-only DB access (`get_db(read_only=True)`).
- Legacy repository-style modules were moved under `archive/`.
