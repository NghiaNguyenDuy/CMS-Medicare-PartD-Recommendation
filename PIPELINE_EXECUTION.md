# Pipeline Execution

## 0) Prerequisites
```bash
pip install -r requirements.txt
```

## 1) One-time bootstrap (skip if DB already exists)
```bash
python scripts/migrate_to_duckdb.py --force
```

## 2) Build reference tables
```bash
python db/bronze/06_ingest_insulin_ref.py
python db/bronze/05_ingest_geography.py
python scripts/generate_beneficiary_profiles.py
```

## 3) Run Gold + ML pipeline
```bash
python -m db.run_full_pipeline --layers gold ml
```

## 4) Validate outputs
```bash
python -c "from db.db_manager import get_db; db=get_db(read_only=True); print(db.query_one('SELECT COUNT(*) FROM gold.dim_zipcode')[0])"
python -c "from db.db_manager import get_db; db=get_db(read_only=True); print(db.query_one('SELECT COUNT(*) FROM ml.training_plan_pairs')[0])"
python -c "from db.db_manager import get_db; db=get_db(read_only=True); print(db.query_one('SELECT COUNT(*) FROM ml.recommendation_explanations')[0])"
python -c "from db.db_manager import get_db; db=get_db(read_only=True); print(db.query_df('SELECT bene_synth_id, recommendation_rank, plan_key, total_cost_with_distance FROM ml.recommendation_explanations ORDER BY bene_synth_id, recommendation_rank LIMIT 10'))"
```

## 5) Train model
```bash
python ml_model/train_model_from_db.py
```

## 6) Run app
```bash
streamlit run app/streamlit_app_interactive.py
```
