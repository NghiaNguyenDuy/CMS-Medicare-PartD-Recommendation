# Quick Start Guide - DuckDB Medallion Architecture

## Fastest Path from Zero to Working Pipeline

### Prerequisites
```bash
pip install duckdb pandas
```

### Step 1: Initial Setup (One-Time)
```bash
# Navigate to project directory
cd "c:\Users\nghia.n\OneDrive - COLLECTIUS SYSTEMS PTE. LTD\Documents\1.Personal\1.Learning\1. Practice\master\thesis\agent-code"

# Verify data files exist
# Required: data/dim_zipcode_geo.csv
# Required: SPUF parquet files in data/
```

### Step 2: Load Data into DuckDB (5 minutes)

```bash
# Load SPUF data → Bronze tables
python scripts/migrate_to_duckdb.py --force

# Load zipcode geography
python db/bronze/05_ingest_geography.py

# Generate synthetic beneficiaries (if needed)
python scripts/generate_beneficiary_profiles.py
```

### Step 3: Run Complete Pipeline (2 minutes)

```bash
# Build Gold + ML layers
python -m db.run_full_pipeline
```

### Step 4: Verify Success

```python
from db.db_manager import get_db

db = get_db()

# Check completion
print("✓ Plans:", db.query_one("SELECT COUNT(*) FROM plan_info")[0])
print("✓ Zipcodes:", db.query_one("SELECT COUNT(*) FROM gold.dim_zipcode")[0])
print("✓ Training data:", db.query_one("SELECT COUNT(*) FROM ml.training_plan_pairs")[0])
print("✓ Recommendations:", db.query_one("SELECT COUNT(*) FROM ml.recommendation_explanations")[0])
```

### Step 5: Get Recommendations

```python
# View sample recommendations
recs = db.query_df("""
    SELECT
        plan_key,
        recommendation_rank,
        cost_explanation,
        distance_explanation,
        recommendation_label
    FROM ml.recommendation_explanations
    WHERE bene_synth_id = (SELECT bene_synth_id FROM ml.recommendation_explanations LIMIT 1)
    ORDER BY recommendation_rank
    LIMIT 5;
""")

for _, rec in recs.iterrows():
    print(f"\n{rec['recommendation_label']}")
    print(f"Plan: {rec['plan_key']}")
    print(f"{rec['cost_explanation']}")
    print(f"{rec['distance_explanation']}")
```

## Done! 🎉

Your DuckDB medallion architecture is now running with:
- ✅ Geographic intelligence (county-level matching, zip codes)
- ✅ Distance-based recommendations
- ✅ Formulary and network metrics
- ✅ Explainable recommendations with warnings

## Troubleshooting

**Error: "Database not found"**
→ Run: `python scripts/migrate_to_duckdb.py --force`

**Error: "Table not found"**
→ Run layers in order: Bronze → Gold → ML

**Error: "synthetic.syn_beneficiary not found"**
→ Run: `python scripts/generate_beneficiary_profiles.py`

## Next Steps

- See `db/PROCESSING_GUIDE.md` for detailed documentation
- See `walkthrough.md` for complete feature list
- See `db/README.md` for architecture overview
