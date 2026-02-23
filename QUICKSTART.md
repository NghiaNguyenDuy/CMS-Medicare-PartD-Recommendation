# Quick Start

## 1) Install dependencies
```bash
pip install -r requirements.txt
```

## 2) One-time bootstrap (only if `data/medicare_part_d.duckdb` is missing)
```bash
python scripts/migrate_to_duckdb.py --force
```

## 3) Build reference + feature layers
```bash
python db/bronze/06_ingest_insulin_ref.py
python db/bronze/05_ingest_geography.py
python scripts/generate_beneficiary_profiles.py
python -m db.run_full_pipeline --layers gold ml
```

## 4) Train ranker
```bash
python ml_model/train_model_from_db.py
```

## 5) Launch app
```bash
streamlit run app/streamlit_app_interactive.py
```

## 6) Verify core tables
```python
from db.db_manager import get_db

db = get_db(read_only=True)
print("Plans:", db.query_one("SELECT COUNT(*) FROM bronze.brz_plan_info")[0])
print("Zipcodes:", db.query_one("SELECT COUNT(*) FROM bronze.brz_zipcode")[0])
print("Training pairs:", db.query_one("SELECT COUNT(*) FROM ml.training_plan_pairs")[0])
print("Recommendations:", db.query_one("SELECT COUNT(*) FROM ml.recommendation_explanations")[0])
```
