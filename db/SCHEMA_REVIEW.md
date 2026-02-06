# Schema Review and Workflow Validation

## synthetic.syn_beneficiary Table Schema

### Expected Schema

```sql
CREATE TABLE synthetic.syn_beneficiary (
    bene_synth_id VARCHAR,         -- Beneficiary ID (e.g., "SYNTH_000001")
    state VARCHAR,                 -- 2-char state code (e.g., "CA")
    county_code VARCHAR,           -- 5-char SSA county code (e.g., "06037")
    zip_code VARCHAR,              -- 5-char zip code (NULL initially, filled by step 2)
    lat DOUBLE,                    -- Latitude (NULL initially, filled by step 2)
    lng DOUBLE,                    -- Longitude (NULL initially, filled by step 2)
    density INTEGER,               -- Population density (NULL initially, filled by step 2)
    risk_segment VARCHAR,          -- Risk level: "LOW", "MED", or "HIGH"
    unique_drugs INTEGER,          -- Number of unique drugs beneficiary takes
    fills_target INTEGER,          -- Target annual fills
    total_rx_cost_est DECIMAL,     -- Estimated annual Rx cost
    insulin_user_flag INTEGER,     -- 1 if insulin user, 0 otherwise
    created_at TIMESTAMP           -- Creation timestamp
);
```

### Indexes

- `idx_synth_bene_id` on `bene_synth_id`
- `idx_synth_county` on `county_code`
- `idx_synth_insulin` on `insulin_user_flag`

---

## Workflow Dependencies

### Step 1: Generate Beneficiaries
**Script**: `scripts/generate_beneficiary_profiles.py`

**Creates**: `synthetic.syn_beneficiary` with:
- ✓ bene_synth_id
- ✓ state (lowercase)
- ✓ county_code
- ⚠️ zip_code (NULL - assigned in step 2)
- ⚠️ lat/lng/density (NULL - assigned in step 2)
- ✓ risk_segment
- ✓ unique_drugs
- ✓ fills_target
- ✓ total_rx_cost_est
- ✓ insulin_user_flag

**Dependencies**:
- `bronze.brz_geographic` - For county sampling
- `bronze.brz_insulin_ref` - For insulin identification
- `bronze.brz_basic_formulary` - For drug sampling (synthetic mode)

---

### Step 2: Assign Geography
**Script**: `db/ml/02_assign_geography.py`

**Updates**: `synthetic.syn_beneficiary` columns:
- zip_code
- lat
- lng
- density

**SQL Columns Used**:
```sql
SELECT 
    b.bene_synth_id,
    b.county_code,
    b.state
FROM synthetic.syn_beneficiary b
```

**Dependencies**:
- `bronze.brz_zipcode` - Zipcode reference with lat/lng/population

**Potential Issue**: User changed from `gold.dim_zipcode` to `bronze.brz_zipcode` ✓

---

###Step 3: Calculate Distance Features
**Script**: `db/ml/03_calculate_distance.py`

**Creates**: `ml.plan_distance_features`

**SQL Columns Used**:
```sql
SELECT plan_key, county_code
FROM bronze.brz_plan_info
```

**Dependencies**:
- `bronze.brz_plan_info`
- `gold.agg_plan_network_metrics`

**Alignment**: ✓ Uses bronze.brz_plan_info (user updated)

---

### Step 4: Generate Training Pairs
**Script**: `db/ml/05_training_pairs.py`

**Creates**: `ml.training_plan_pairs`

**SQL Columns Used from synthetic.syn_beneficiary**:
```sql
SELECT
    b.bene_synth_id,
    b.state AS bene_state,
    b.county_code AS bene_county,
    b.zip_code AS bene_zip,
    b.density AS bene_density,
    b.risk_segment,
    b.fills_target,
    b.unique_drugs,
    b.total_rx_cost_est,
    b.insulin_user_flag
FROM synthetic.syn_beneficiary b
```

**Critical Columns**:
- ✓ bene_synth_id
- ✓ state (lowercase after fix)
- ✓ county_code
- ✓ zip_code (must be filled by step 2)
- ✓ density (must be filled by step 2)
- ✓ risk_segment
- ✓ fills_target
- ✓ unique_drugs
- ✓ total_rx_cost_est
- ✓ insulin_user_flag

**Dependencies**:
- `bronze.brz_plan_info` (user updated ✓)
- `gold.agg_plan_formulary_metrics`
- `gold.agg_plan_network_metrics`
- `ml.plan_distance_features`

---

## Column Name Alignment Issues Found & Fixed

### Issue 1: Case Sensitivity
**Problem**: DataFrame had `COUNTY_CODE` and `STATE` (uppercase), but SQL expects lowercase

**Fixed in**: `generate_beneficiary_profiles.py::save_to_database()`

**Solution**:
```python
# OLD - causes mismatch
'STATE': bene_summary['STATE'],  # uppercase
'COUNTY_CODE': bene_summary['COUNTY_CODE']

# NEW - consistent with SQL
'state': LOWER(STATE),  # lowercase in CREATE TABLE
'county_code': COUNTY_CODE
```

### Issue 2: NDC Column Name
**Problem**: Query returns uppercase `NDC` but code expected lowercase `ndc`

**Fixed by user in**: Line 182 of generate_beneficiary_profiles.py
```python
# OLD
formulary_drugs['is_insulin'] = formulary_drugs['ndc'].isin(...)

# NEW (user fix)
formulary_drugs['is_insulin'] = formulary_drugs['NDC'].isin(...)
```

### Issue 3: Geographic Column Names
**Problem**: Query returns `STATE_LABEL` but code expected `state`

**Fixed by user in**: Lines 262-280 of generate_beneficiary_profiles.py
```python
# OLD
SELECT county_code, state FROM bronze.brz_geographic

# NEW (user fix)
SELECT county_code, state_label FROM bronze.brz_geographic
```

---

## Validation Checklist

### Before Running Pipeline

- [ ] Run: `python scripts/generate_beneficiary_profiles.py`
- [ ] Check: `SELECT COUNT(*) FROM synthetic.syn_beneficiary` → Should return 10,000 (default)
- [ ] Check: `SELECT COUNT(*) FROM bronze.brz_insulin_ref` → Should return 84
- [ ] Check: `SELECT COUNT(*) FROM bronze.brz_plan_info` → Should have plans
- [ ] Check: `SELECT COUNT(*) FROM bronze.brz_basic_formulary` → Should have formulary

### After Step 1 (Generate)

```sql
-- Should have rows
SELECT COUNT(*) FROM synthetic.syn_beneficiary;

-- Should have county assignments
SELECT COUNT(DISTINCT county_code), COUNT(DISTINCT state) 
FROM synthetic.syn_beneficiary;

-- Zip codes should be NULL (assigned in step 2)
SELECT SUM(CASE WHEN zip_code IS NULL THEN 1 ELSE 0 END) AS missing_zip
FROM synthetic.syn_beneficiary;
-- Should equal total row count

-- Check insulin users (~15%)
SELECT 
    SUM(insulin_user_flag) AS insulin_users,
    COUNT(*) AS total,
    ROUND(100.0 * SUM(insulin_user_flag) / COUNT(*), 1) AS insulin_pct
FROM synthetic.syn_beneficiary;
```

### After Step 2 (Assign Geography)

```sql
-- Zip codes should now be filled
SELECT SUM(CASE WHEN zip_code IS NOT NULL THEN 1 ELSE 0 END) AS has_zip
FROM synthetic.syn_beneficiary;

-- Lat/lng should be filled
SELECT COUNT(*) FROM synthetic.syn_beneficiary
WHERE lat IS NOT NULL AND lng IS NOT NULL;
```

### After Step 4 (Training Pairs)

```sql
-- Should have many bene-plan pairs
SELECT COUNT(*) AS total_pairs,
       COUNT(DISTINCT bene_synth_id) AS unique_benes,
       COUNT(DISTINCT plan_key) AS unique_plans
FROM ml.training_plan_pairs;

-- Should have geographic matches
SELECT COUNT(*) FROM ml.training_plan_pairs
WHERE bene_county = (
    SELECT county_code FROM bronze.brz_plan_info p 
    WHERE p.plan_key = ml.training_plan_pairs.plan_key
);
```

---

## Common Issues and Solutions

### Issue: "table synthetic.syn_beneficiary not found"
**Solution**: Run `python scripts/generate_beneficiary_profiles.py`

### Issue: "column zip_code is NULL in ml.training_plan_pairs"
**Solution**: Must run `python db/ml/02_assign_geography.py` first

### Issue: "no plans matched for beneficiaries"
**Cause**: County codes don't match between beneficiaries and plans
**Solution**: Check `bronze.brz_geographic` has correct county mappings

### Issue: "insulin_ref table not found"
**Solution**: Run `python db/bronze/06_ingest_insulin_ref.py`

### Issue: ModuleNotFoundError: No module named 'duckdb'
**Solution**: Install dependencies: `pip install duckdb pandas numpy scikit-learn`

---

## Recommended Execution Order

```bash
# 0. Prerequisites (one-time setup)
pip install duckdb pandas numpy scikit-learn

# 1. Migrate SPUF data to bronze
python scripts/migrate_to_duckdb.py --force

# 2. Load insulin reference
python db/bronze/06_ingest_insulin_ref.py

# 3. Load zipcode geography
python db/bronze/05_ingest_geography.py

# 4. Build Gold layer
python -m db.run_full_pipeline --layers gold

# 5. Generate synthetic beneficiaries
python scripts/generate_beneficiary_profiles.py

# 6. Assign zip codes
python db/ml/02_assign_geography.py

# 7. Calculate distance features
python db/ml/03_calculate_distance.py

# 8. Generate training pairs
python db/ml/05_training_pairs.py

# 9. Generate recommendations
python db/ml/06_recommendation_explainer.py

# OR run steps 6-9 together:
python -m db.run_full_pipeline --layers ml
```

---

## Schema Alignment Summary

| Column | generate_beneficiary_profiles.py | 02_assign_geography.py | 05_training_pairs.py | Status |
|--------|----------------------------------|------------------------|----------------------|--------|
| bene_synth_id | ✓ Creates | ✓ Uses | ✓ Uses | ✅ Aligned |
| state | ✓ Creates (lowercase) | ✓ Uses | ✓ Uses | ✅ Fixed |
| county_code | ✓ Creates | ✓ Uses | ✓ Uses | ✅ Aligned |
| zip_code | ✓ Creates (NULL) | ✓ Fills | ✓ Uses | ✅ Aligned |
| lat | ✓ Creates (NULL) | ✓ Fills | - | ✅ Aligned |
| lng | ✓ Creates (NULL) | ✓ Fills | - | ✅ Aligned |
| density | ✓ Creates (NULL) | ✓ Fills | ✓ Uses | ✅ Aligned |
| risk_segment | ✓ Creates | - | ✓ Uses | ✅ Aligned |
| fills_target | ✓ Creates | - | ✓ Uses | ✅ Aligned |
| unique_drugs | ✓ Creates | - | ✓ Uses | ✅ Aligned |
| total_rx_cost_est | ✓ Creates | - | ✓ Uses | ✅ Aligned |
| insulin_user_flag | ✓ Creates | - | ✓ Uses | ✅ Aligned |

**All columns aligned! ✅**
