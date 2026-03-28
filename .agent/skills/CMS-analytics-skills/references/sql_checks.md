# SQL Checks

Use these checks before and after SQL changes.

## 1. Inspect Tables From DuckDB

```python
from db.db_manager import get_db
db = get_db(read_only=True)
print(db.list_tables())
```

## 2. Inspect Columns for a Specific Table

```python
from db.db_manager import get_db
db = get_db(read_only=True)
print(db.query_df("DESCRIBE ml.training_plan_pairs")[["column_name", "column_type"]])
```

## 3. Check Row Counts by Layer

```python
from db.db_manager import get_db
db = get_db(read_only=True)
tables = [
    "bronze.brz_plan_info",
    "bronze.brz_basic_formulary",
    "bronze.brz_pharmacy_network",
    "gold.agg_plan_formulary_metrics",
    "gold.agg_plan_network_metrics",
    "gold.agg_plan_cost_metrics",
    "gold.dim_zipcode",
    "ml.plan_distance_features",
    "ml.training_plan_pairs",
    "ml.recommendation_explanations",
    "synthetic.syn_beneficiary",
    "synthetic.syn_beneficiary_prescriptions",
]
for t in tables:
    try:
        c = db.query_one(f"SELECT COUNT(*) FROM {t}")[0]
        print(f"{t}: {c:,}")
    except Exception as e:
        print(f"{t}: MISSING ({e})")
```

## 4. Validate Key Join Columns

```python
from db.db_manager import get_db
db = get_db(read_only=True)
print(db.query_df("DESCRIBE bronze.brz_plan_info")[["column_name","column_type"]])
print(db.query_df("DESCRIBE gold.agg_plan_network_metrics")[["column_name","column_type"]])
print(db.query_df("DESCRIBE gold.agg_plan_formulary_metrics")[["column_name","column_type"]])
print(db.query_df("DESCRIBE gold.dim_zipcode")[["column_name","column_type"]])
```

## 5. Contract Type Derivation Pattern

```sql
CASE
  WHEN p.IS_MA_PD THEN 'MA'
  WHEN p.IS_PDP THEN 'PDP'
  ELSE NULL
END AS contract_type
```

## 6. Verify Recommendation Click-Path Tables

```python
from db.db_manager import get_db
db = get_db(read_only=True)
checks = {
    "location_zip_lookup": "SELECT COUNT(*) FROM gold.dim_zipcode",
    "candidate_plans": "SELECT COUNT(*) FROM bronze.brz_plan_info WHERE PREMIUM IS NOT NULL",
    "drug_coverage_formulary": "SELECT COUNT(*) FROM bronze.brz_basic_formulary",
    "drug_coverage_exclusions": "SELECT COUNT(*) FROM bronze.brz_excluded_drugs",
    "pharmacy_access": "SELECT COUNT(*) FROM bronze.brz_pharmacy_network",
}
for name, sql in checks.items():
    print(name, db.query_one(sql)[0])
```
