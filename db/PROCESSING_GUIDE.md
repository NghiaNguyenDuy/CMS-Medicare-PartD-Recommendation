# Complete Guide: Processing Data into DuckDB

## Overview

This guide provides step-by-step instructions for processing Medicare Part D SPUF data into DuckDB using the medallion architecture.

## Prerequisites

### 1. Data Files Required

Ensure you have the following SPUF data files in the `data/` directory:

**Plan Information:**
- `data/PlanInfo_PPUF_2024.parquet` (or CSV)

**Formulary:**
- `data/BasicDrugsFormulary_PPUF_2024.parquet`
- `data/ExcludedDrugs_PPUF_2024.parquet` (optional)
- `data/IndicationBasedCoverage_PPUF_2024.parquet` (optional)

**Costs:**
- `data/BeneficiaryCost_PPUF_2024.parquet`
- `data/InsulinCost_PPUF_2024.parquet`
- `data/Pricing_PPUF_2024.parquet` (optional)

**Networks:**
- `data/PharmacyNetworks_PPUF_2024.parquet` (optional)

**Geography:**
- `data/Geographic_PPUF_2024.parquet`
- `data/dim_zipcode_geo.csv` ✅ **Required for distance features**

### 2. Python Environment

```bash
# Install required packages
pip install duckdb pandas
```

### 3. Verify Database Manager

```python
# Test database connection
python -c "from db.db_manager import get_db; print('✓ DB manager working')"
```

## Step-by-Step Processing Guide

### Step 1: Bronze Layer - Initial Data Ingestion

This step loads raw SPUF data from parquet files into DuckDB.

```bash
# Navigate to project root
cd "c:\Users\nghia.n\OneDrive - COLLECTIUS SYSTEMS PTE. LTD\Documents\1.Personal\1.Learning\1. Practice\master\thesis\agent-code"

# Run initial bronze ingestion
python scripts/migrate_to_duckdb.py --force
```

**What this does:**
- Creates `data/medicare_part_d.duckdb` database
- Loads tables: plan_info, basic_drugs_formulary, beneficiary_cost, insulin_cost, geographic
- Adds PLAN_KEY and FORMULARY_ID columns
- Creates indexes for performance

**Expected output:**
```
Starting DuckDB migration...
✓ Database created: data/medicare_part_d.duckdb
Migrating plan_info...
  ✓ Loaded X rows
Migrating basic_drugs_formulary...
  ✓ Loaded X rows
[...]
✓ Migration complete!
```

**Validation:**
```python
from db.db_manager import get_db

db = get_db()
tables = db.list_tables()
print("Tables created:", tables)

# Check row counts
print("Plans:", db.query_one("SELECT COUNT(*) FROM plan_info")[0])
print("Formulary:", db.query_one("SELECT COUNT(*) FROM basic_drugs_formulary")[0])
```

### Step 2: Bronze Layer - Zipcode Geography

Load additional geographic data for distance calculations.

```bash
# Ingest zipcode data
python db/bronze/05_ingest_geography.py
```

**What this does:**
- Loads `data/dim_zipcode_geo.csv` (29K+ zip codes)
- Creates `bronze.brz_zipcode` table
- Adds population and density metadata

**Expected output:**
```
========================================
Bronze Ingestion: Zipcode Geography
========================================

1. Loading data/dim_zipcode_geo.csv...
   ✓ Loaded 29,932 zip codes

2. Creating bronze.brz_zipcode table...

3. Creating indexes...

✓ Ingestion complete: 29,932 rows in bronze.brz_zipcode
```

**Validation:**
```python
db = get_db()
zip_count = db.query_one("SELECT COUNT(*) FROM bronze.brz_zipcode")[0]
print(f"✓ Loaded {zip_count:,} zip codes")
```

### Step 3: Gold Layer - Create Dimensions and Metrics

Transform bronze data into analysis-ready dimensions and aggregations.

```bash
# Run all Gold layer scripts
python db/gold/03_dim_zipcode.py
python db/gold/05_agg_formulary.py
python db/gold/07_agg_networks.py
```

**Or run all at once:**
```bash
python -m db.run_full_pipeline --layers gold
```

**What this creates:**

**dim_zipcode:**
- Zipcode master with county codes
- Density categories (urban/suburban/rural)
- Lat/lng coordinates

**agg_plan_formulary_metrics:**
- Total drugs per plan
- Tier distribution
- Restriction rates (PA/ST/QL)
- Restrictiveness classification

**agg_plan_network_metrics:**
- Pharmacy counts
- Preferred pharmacy percentages
- Network adequacy flags

**Expected output:**
```
========================================
Gold Layer: Zipcode Dimension
========================================
✓ dim_zipcode created successfully:
  - Total zip codes: 29,932
  - Urban: 5,234
  - Suburban: 8,123
  - Rural: 16,575

========================================
Gold Layer: Plan Formulary Metrics
========================================
✓ Formulary metrics aggregation complete:
  - Total plans: 3,456
  - Avg drugs per plan: 2,834
  - Avg generic tier %: 68.5%
  - Restrictiveness: Low(1,234), Med(1,567), High(655)

========================================
Gold Layer: Plan Network Metrics
========================================
✓ Network metrics aggregation complete:
  - Total plans: 3,456
  - Avg preferred pharmacies: 23
```

**Validation:**
```python
db = get_db()

# Check Gold tables
print("Zipcode dimension:", db.query_one("SELECT COUNT(*) FROM gold.dim_zipcode")[0])
print("Formulary metrics:", db.query_one("SELECT COUNT(*) FROM gold.agg_plan_formulary_metrics")[0])
print("Network metrics:", db.query_one("SELECT COUNT(*) FROM gold.agg_plan_network_metrics")[0])

# Sample query
sample = db.query_df("""
    SELECT 
        plan_key,
        total_drugs,
        restrictiveness_class,
        preferred_pharmacies
    FROM gold.agg_plan_formulary_metrics fm
    JOIN gold.agg_plan_network_metrics nm USING (plan_key)
    LIMIT 5;
""")
print(sample)
```

### Step 4: Generate Synthetic Beneficiaries (Optional)

If you need synthetic beneficiary data for testing:

```bash
# Use existing script or create synthetic data
python scripts/generate_beneficiary_profiles.py
```

**Note:** This creates `synthetic.syn_beneficiary` table which is required for ML layer.

### Step 5: ML Layer - Feature Engineering and Training Data

Generate ML-ready features and training data.

```bash
# Run all ML layer scripts
python db/ml/02_assign_geography.py
python db/ml/03_calculate_distance.py
python db/ml/05_training_pairs.py
python db/ml/06_recommendation_explainer.py
```

**Or run all at once:**
```bash
python -m db.run_full_pipeline --layers ml
```

**What this creates:**

**Beneficiary zip assignment:**
- Assigns realistic zip codes to synthetic beneficiaries
- Population-weighted distribution

**Distance features:**
- Simulated pharmacy distances (0.5-25 miles)
- Distance categories (very_close/nearby/moderate/far)

**Training pairs:**
- Beneficiary-plan combinations with county constraints
- Plan features + distance + OOP estimates

**Recommendation explanations:**
- Top 5 recommendations per beneficiary
- Human-readable cost/distance explanations
- Warning flags (network, insulin, formulary)

**Expected output:**
```
========================================
ML Prep: Beneficiary Zip Code Assignment
========================================
✓ Zip code assignment complete:
  - Total beneficiaries: 10,000
  - Beneficiaries with zip: 10,000
  - Coverage: 100.0%

========================================
ML Prep: Distance Proxy Calculation
========================================
✓ Distance proxy calculation complete:
  - Total plan-county pairs: 125,456
  - Avg distance: 7.3 miles
  - Very close: 18,234
  - Nearby: 45,678
  - Moderate: 38,912
  - Far: 22,632

========================================
ML Prep: Training Pairs Generation
========================================
✓ Training pairs generated:
  - Total pairs: 456,789
  - Unique beneficiaries: 10,000
  - Avg plans per beneficiary: 45.7
  - Pairs with tradeoff: 23,456 (5.1%)

========================================
ML Prep: Recommendation Explanations
========================================
✓ Recommendation explanations generated:
  - Total recommendations: 50,000
  - Network warnings: 4,567
  - Insulin warnings: 2,345
  - Distance tradeoffs: 3,456
```

**Validation:**
```python
db = get_db()

# Check ML tables
print("Distance features:", db.query_one("SELECT COUNT(*) FROM ml.plan_distance_features")[0])
print("Training pairs:", db.query_one("SELECT COUNT(*) FROM ml.training_plan_pairs")[0])
print("Explanations:", db.query_one("SELECT COUNT(*) FROM ml.recommendation_explanations")[0])

# Sample recommendation
rec = db.query_df("""
    SELECT
        bene_synth_id,
        plan_key,
        recommendation_rank,
        cost_explanation,
        distance_explanation,
        recommendation_label
    FROM ml.recommendation_explanations
    WHERE bene_synth_id = (SELECT bene_synth_id FROM ml.recommendation_explanations LIMIT 1)
    ORDER BY recommendation_rank
    LIMIT 3;
""")
print(rec)
```

### Step 6: Run Full Pipeline (All Layers)

For convenience, run all layers at once:

```bash
# Complete pipeline: Bronze → Silver → Gold → ML
python -m db.run_full_pipeline
```

This executes all steps sequentially with progress reporting.

## Quick Reference Commands

### Check Database Status

```python
from db.db_manager import get_db

db = get_db()

# List all tables
print("Tables:", db.list_tables())

# Check each layer
layers = {
    'Bronze': ['bronze.brz_zipcode'],
    'Gold': ['gold.dim_zipcode', 'gold.agg_plan_formulary_metrics', 'gold.agg_plan_network_metrics'],
    'ML': ['ml.plan_distance_features', 'ml.training_plan_pairs', 'ml.recommendation_explanations']
}

for layer, tables in layers.items():
    print(f"\n{layer} Layer:")
    for table in tables:
        try:
            count = db.query_one(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"  ✓ {table}: {count:,} rows")
        except:
            print(f"  ✗ {table}: NOT FOUND")
```

### Query Examples

**Get recommendations for a beneficiary:**
```python
recs = db.query_df("""
    SELECT *
    FROM ml.recommendation_explanations
    WHERE bene_synth_id = 'BENE_0001'
    ORDER BY recommendation_rank;
""")
```

**Find plans with distance tradeoffs:**
```python
tradeoffs = db.query_df("""
    SELECT
        plan_key,
        COUNT(DISTINCT bene_synth_id) AS affected_benes,
        AVG(estimated_annual_oop) AS avg_oop,
        AVG(distance_miles) AS avg_distance
    FROM ml.training_plan_pairs
    WHERE has_distance_tradeoff = TRUE
    GROUP BY plan_key
    ORDER BY affected_benes DESC
    LIMIT 10;
""")
```

**Analyze formulary restrictiveness:**
```python
formulary_stats = db.query_df("""
    SELECT
        restrictiveness_class,
        COUNT(*) AS plan_count,
        AVG(pa_rate) AS avg_pa_rate,
        AVG(st_rate) AS avg_st_rate
    FROM gold.agg_plan_formulary_metrics
    GROUP BY restrictiveness_class
    ORDER BY restrictiveness_class;
""")
```

## Troubleshooting

### Issue: "Database not found"

**Solution:**
```bash
# Run initial migration
python scripts/migrate_to_duckdb.py --force
```

### Issue: "Table not found" errors

**Solution:** Run layers in order:
```bash
# 1. Bronze first
python scripts/migrate_to_duckdb.py --force
python db/bronze/05_ingest_geography.py

# 2. Then Gold
python -m db.run_full_pipeline --layers gold

# 3. Then ML (requires synthetic beneficiaries)
python scripts/generate_beneficiary_profiles.py
python -m db.run_full_pipeline --layers ml
```

### Issue: "synthetic.syn_beneficiary not found"

**Solution:**
```bash
# Generate synthetic beneficiaries first
python scripts/generate_beneficiary_profiles.py
```

### Issue: Import errors

**Solution:**
```bash
# Ensure you're in project root
cd "c:\Users\nghia.n\OneDrive - COLLECTIUS SYSTEMS PTE. LTD\Documents\1.Personal\1.Learning\1. Practice\master\thesis\agent-code"

# Verify Python path
python -c "import sys; print(sys.path)"
```

## Performance Tips

1. **Memory:** DuckDB is configured for 2GB memory. For larger datasets, increase in `db_manager.py`:
   ```python
   self.conn.execute("SET memory_limit='4GB'")
   ```

2. **Parallel Processing:** Adjust thread count:
   ```python
   self.conn.execute("SET threads=8")
   ```

3. **Indexes:** All tables have indexes on key columns for fast lookups.

4. **Caching:** Use `db.cached_query()` for frequently-run queries.

## Summary

**Complete processing sequence:**

1. ✅ **Bronze ingestion:** `python scripts/migrate_to_duckdb.py --force`
2. ✅ **Zipcode data:** `python db/bronze/05_ingest_geography.py`
3. ✅ **Gold dimensions:** `python -m db.run_full_pipeline --layers gold`
4. ✅ **Synthetic benes:** `python scripts/generate_beneficiary_profiles.py`
5. ✅ **ML features:** `python -m db.run_full_pipeline --layers ml`

**Or all at once:**
```bash
python scripts/migrate_to_duckdb.py --force
python db/bronze/05_ingest_geography.py
python scripts/generate_beneficiary_profiles.py
python -m db.run_full_pipeline
```

**Validation:**
```bash
python -c "
from db.db_manager import get_db
db = get_db()
print('Plans:', db.query_one('SELECT COUNT(*) FROM plan_info')[0])
print('Zipcodes:', db.query_one('SELECT COUNT(*) FROM gold.dim_zipcode')[0])
print('Training pairs:', db.query_one('SELECT COUNT(*) FROM ml.training_plan_pairs')[0])
print('Recommendations:', db.query_one('SELECT COUNT(*) FROM ml.recommendation_explanations')[0])
print('✓ Pipeline complete!')
"
```

Your data is now ready for ML training and recommendation generation! 🎉
