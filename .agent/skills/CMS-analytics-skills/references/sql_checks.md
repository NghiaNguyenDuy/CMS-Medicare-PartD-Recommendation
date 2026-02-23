# SQL Checks

Use these checks before and after SQL changes.

## 1. Inspect Metadata CSV Quickly

```powershell
Import-Csv data/meta_db.csv |
  Where-Object { $_.schema -eq 'ml' -and $_.name -eq 'training_plan_pairs' } |
  Select-Object schema, name, column_names, column_types
```

## 2. Verify Table Availability in DuckDB

```python
from db.db_manager import get_db
db = get_db()
print(db.list_tables())
```

## 3. Check Row Counts by Layer

```python
from db.db_manager import get_db
db = get_db()
tables = [
    "bronze.brz_plan_info",
    "gold.agg_plan_formulary_metrics",
    "gold.agg_plan_network_metrics",
    "gold.agg_plan_cost_metrics",
    "ml.plan_distance_features",
    "ml.training_plan_pairs",
    "ml.recommendation_explanations",
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
db = get_db()
print(db.query_df("DESCRIBE bronze.brz_plan_info")[["column_name","column_type"]])
print(db.query_df("DESCRIBE gold.agg_plan_network_metrics")[["column_name","column_type"]])
```

## 5. Contract Type Derivation Pattern

```sql
CASE
  WHEN p.IS_MA_PD THEN 'MA'
  WHEN p.IS_PDP THEN 'PDP'
  ELSE NULL
END AS contract_type
```
