# Pipeline Execution Guide

## Prerequisites Check

✅ **SPUF Files Found** (9 parquet files in data/SPUF/):
- basic_formulary.parquet
- beneficiary_cost.parquet
- excluded_drugs.parquet
- geographic.parquet
- indication_coverage.parquet
- insulin_cost.parquet
- pharmacy_network.parquet
- plan_info.parquet
- pricing.parquet

✅ **Python Virtual Environment**: `.env` (activate with `.env\Scripts\activate`)

---

## Execution Steps

### Step 1: Bronze Layer - SPUF Data Migration

**Close any open database connections first!**

```bash
# Activate venv
.env\Scripts\activate

# Run migration (--force to recreate)
python scripts\migrate_to_duckdb.py --force
```

**Expected Output**:
- 9 bronze.brz_* tables created
- Indexes created
- Summary with row counts

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); print('Bronze tables:', [t for t in db.list_tables() if 'bronze' in t])"
```

---

### Step 2: Bronze Layer - Reference Data

#### 2a. Insulin Reference (84 NDCs)

```bash
python db\bronze\06_ingest_insulin_ref.py
```

**Expected**: `bronze.brz_insulin_ref` with 84 rows

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); print('Insulin NDCs:', db.query_one('SELECT COUNT(*) FROM bronze.brz_insulin_ref')[0])"
```

#### 2b. Zipcode Geography

```bash
python db\bronze\05_ingest_geography.py
```

**Expected**: `bronze.brz_zipcode` with ~29K rows

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); print('Zipcodes:', db.query_one('SELECT COUNT(*) FROM bronze.brz_zipcode')[0])"
```

---

### Step 3: Gold Layer - Aggregations

#### 3a. Zipcode Dimension

```bash
python db\gold\03_dim_zipcode.py
```

**Expected**: `gold.dim_zipcode` with density categories

#### 3b. Formulary Metrics (with Quantile-Based Restrictiveness ✨)

```bash
python db\gold\05_agg_formulary.py
```

**Expected**: `gold.agg_plan_formulary_metrics`
- Restrictiveness class uses `NTILE(3)` (not hardcoded)
- Insulin coverage from bronze.brz_insulin_ref

**Validation - Check Restrictiveness Distribution**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); stats = db.query_df('SELECT restrictiveness_class, COUNT(*) as cnt FROM gold.agg_plan_formulary_metrics GROUP BY restrictiveness_class ORDER BY restrictiveness_class'); print(stats)"
```

Should show roughly equal distribution (0, 1, 2).

#### 3c. Network Adequacy Metrics

```bash
python db\gold\07_agg_networks.py
```

**Expected**: `gold.agg_plan_network_metrics`

#### 3d. PCA Affordability Index (Optional but Recommended)

```bash
python db\gold\08_affordability_index_pca.py
```

**Expected**: Updates `gold.agg_plan_cost_metrics` with:
- `affordability_index` (continuous)
- `affordability_class` (0-3 quartiles)

**Note**: This script requires `gold.agg_plan_cost_metrics` to exist first. If it doesn't, you may need to create it or skip this step.

---

### Step 4: Synthetic Beneficiaries

```bash
# Default: 10K synthetic beneficiaries
python scripts\generate_beneficiary_profiles.py

# OR from PDE data:
# python scripts\generate_beneficiary_profiles.py --from-pde --pde-file data\pde.csv
```

**Expected**: `synthetic.syn_beneficiary` with:
- 10,000 rows (or as specified)
- County assignments
- NULL zip_code (filled in next step)
- ~15% insulin users

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); stats = db.query_df('SELECT COUNT(*) as total, SUM(insulin_user_flag) as insulin_users, COUNT(DISTINCT county_code) as counties FROM synthetic.syn_beneficiary'); print(stats)"
```

---

### Step 5: ML Layer - Feature Engineering

#### 5a. Assign Zip Codes

```bash
python db\ml\02_assign_geography.py
```

**Expected**: Updates `synthetic.syn_beneficiary`:
- Fills `zip_code`, `lat`, `lng`, `density`
- Population-weighted within county

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); filled = db.query_one('SELECT COUNT(*) FROM synthetic.syn_beneficiary WHERE zip_code IS NOT NULL')[0]; print(f'Zip codes assigned: {filled:,}')"
```

#### 5b. Calculate Distance Features

```bash
python db\ml\03_calculate_distance.py
```

**Expected**: `ml.plan_distance_features`
- Simulated distance based on density + network
- Distance categories

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); stats = db.query_df('SELECT AVG(simulated_distance_miles) as avg_dist, distance_category, COUNT(*) as cnt FROM ml.plan_distance_features GROUP BY distance_category'); print(stats)"
```

#### 5c. Generate Training Pairs

```bash
python db\ml\05_training_pairs.py
```

**Expected**: `ml.training_plan_pairs`
- Beneficiary-plan pairs
- County-level geographic constraints
- Distance features included

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); stats = db.query_df('SELECT COUNT(*) as total_pairs, COUNT(DISTINCT bene_synth_id) as benes, COUNT(DISTINCT plan_key) as plans FROM ml.training_plan_pairs'); print(stats)"
```

#### 5d. Generate Recommendations

```bash
python db\ml\06_recommendation_explainer.py
```

**Expected**: `ml.recommendation_explanations`
- Top 5 recommendations per beneficiary
- Warning flags
- Human-readable explanations

**Validation**:
```bash
python -c "from db.db_manager import get_db; db = get_db(); sample = db.query_df('SELECT bene_synth_id, rank, plan_key, total_oop_estimate, distance_explanation FROM ml.recommendation_explanations WHERE bene_synth_id = (SELECT MIN(bene_synth_id) FROM ml.recommendation_explanations) ORDER BY rank'); print(sample)"
```

---

### Step 6: Full Validation

```bash
# Run comprehensive validation
.env\Scripts\python.exe db\utils\validate_schema.py
```

**Expected**: Report on all tables, row counts, data quality

---

## Quick Pipeline (All Steps)

```bash
# Activate venv
.env\Scripts\activate

# Full pipeline from scratch
python scripts\migrate_to_duckdb.py --force
python db\bronze\06_ingest_insulin_ref.py
python db\bronze\05_ingest_geography.py
python db\gold\03_dim_zipcode.py
python db\gold\05_agg_formulary.py
python db\gold\07_agg_networks.py
python scripts\generate_beneficiary_profiles.py
python db\ml\02_assign_geography.py
python db\ml\03_calculate_distance.py
python db\ml\05_training_pairs.py
python db\ml\06_recommendation_explainer.py

# Validate
python db\utils\validate_schema.py
```

**OR use orchestration**:
```bash
python -m db.run_full_pipeline --layers gold ml
```

---

## Troubleshooting

### Database Lock Error
```
IO Error: Cannot open file ... already open in another process
```

**Solution**: Close DBeaver, DuckDB CLI, or any other database connections.

### Missing Table Error
```
Catalog Error: Table with name X does not exist
```

**Solution**: Check which step failed and rerun previous steps. Use validation commands to verify each step.

### Import Error
```
ModuleNotFoundError: No module named 'duckdb'
```

**Solution**: 
```bash
.env\Scripts\activate
pip install duckdb pandas numpy scikit-learn python-dotenv
```

---

## Expected Final State

**Bronze Tables** (11 total):
- bronze.brz_plan_info
- bronze.brz_basic_formulary
- bronze.brz_beneficiary_cost
- bronze.brz_insulin_cost
- bronze.brz_geographic
- bronze.brz_excluded_drugs
- bronze.brz_ibc
- bronze.brz_pharmacy_networks
- bronze.brz_pricing
- bronze.brz_insulin_ref ✨
- bronze.brz_zipcode ✨

**Gold Tables** (3-4):
- gold.dim_zipcode
- gold.agg_plan_formulary_metrics
- gold.agg_plan_network_metrics
- gold.agg_plan_cost_metrics (if created)

**Synthetic Tables** (1):
- synthetic.syn_beneficiary

**ML Tables** (3):
- ml.plan_distance_features
- ml.training_plan_pairs
- ml.recommendation_explanations

---

## Next Steps After Execution

1. **Review Recommendations**: Query `ml.recommendation_explanations` for sample beneficiaries
2. **Analyze Metrics**: Check formulary restrictiveness distribution, network adequacy
3. **Export for ML**: Use `ml.training_plan_pairs` for model training
4. **Build UI**: Connect recommendation engine to front-end application
