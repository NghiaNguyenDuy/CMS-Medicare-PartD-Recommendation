# Affordability Index PCA Update Summary

## Changes Made to `db/gold/08_affordability_index_pca.py`

### ✅ Updated to Use Actual Cost Metrics Columns

## Problem Identified

The script was trying to use columns that **don't exist** in `gold.agg_plan_cost_metrics`:
- ❌ `premium` (not in cost metrics)
- ❌ `deductible` (not in cost metrics)
- ❌ `avg_copay_preferred` (wrong name)
- ❌ `avg_coinsurance_preferred` (wrong name)

### Actual Schema

`gold.agg_plan_cost_metrics` contains:
- `pref_avg_copay_amt`, `pref_median_copay_amt`, `pref_p90_copay_amt`
- `pref_avg_coins_rate`, `pref_median_coins_rate`, `pref_p90_coins_rate`
- `nonpref_avg_copay_amt`, `nonpref_median_copay_amt`, etc.
- `mail_pref_avg_copay_amt`, `mail_nonpref_avg_copay_amt`, etc.
- `ded_applies_rate`, `specialty_row_share`
- `pref_avg_copay_tier_1` through `tier_7`
- `pref_avg_coins_rate_tier_1` through `tier_7`

**Missing**: `premium`, `deductible` (these are in `bronze.brz_plan_info`)

---

## Updated Implementation

### 1. Feature Extraction Query (Lines 48-64)

**NEW - Join with bronze.brz_plan_info**:
```sql
SELECT
    cm.plan_key,
    -- From plan_info (for premium/deductible)
    CAST(COALESCE(p.PREMIUM, 0) AS DOUBLE) AS premium,
    CAST(COALESCE(p.DEDUCTIBLE, 0) AS DOUBLE) AS deductible,
    -- From cost metrics (actual column names)
    CAST(COALESCE(cm.pref_avg_copay_amt, 0) AS DOUBLE) AS avg_copay_preferred,
    CAST(COALESCE(cm.pref_median_copay_amt, 0) AS DOUBLE) AS median_copay_preferred,
    CAST(COALESCE(cm.pref_avg_coins_rate, 0) AS DOUBLE) AS avg_coinsurance_preferred,
    CAST(COALESCE(cm.nonpref_avg_copay_amt, 0) AS DOUBLE) AS avg_copay_nonpreferred,
    CAST(COALESCE(cm.ded_applies_rate, 0) AS DOUBLE) AS ded_applies_rate,
    CAST(COALESCE(cm.specialty_row_share, 0) AS DOUBLE) AS specialty_share
FROM gold.agg_plan_cost_metrics cm
LEFT JOIN bronze.brz_plan_info p ON cm.plan_key = p.PLAN_KEY
```

**Key Changes**:
- ✅ JOIN with `bronze.brz_plan_info` to get `premium` and `deductible`
- ✅ Use `pref_avg_copay_amt` instead of non-existent `avg_copay_preferred`
- ✅ Use `pref_avg_coins_rate` instead of `avg_coinsurance_preferred`
- ✅ Added `median_copay_preferred` (from `pref_median_copay_amt`)
- ✅ Added `avg_copay_nonpreferred` (from `nonpref_avg_copay_amt`)
- ✅ Added `specialty_share` (from `specialty_row_share`)

### 2. PCA Feature List (Lines 66-78)

**NEW - 8 Features** (was 5):
```python
feature_cols = [
    'premium',                      # From brz_plan_info
    'deductible',                   # From brz_plan_info
    'avg_copay_preferred',          # From pref_avg_copay_amt
    'median_copay_preferred',       # From pref_median_copay_amt (NEW)
    'avg_coinsurance_preferred',    # From pref_avg_coins_rate
    'avg_copay_nonpreferred',       # From nonpref_avg_copay_amt (NEW)
    'ded_applies_rate',             # From ded_applies_rate
    'specialty_share'               # From specialty_row_share (NEW)
]
```

**Benefit**: More comprehensive affordability assessment including:
- Median copay (more robust than mean)
- Non-preferred pharmacy costs
- Specialty tier usage

### 3. Validation Query (Lines 148-162)

**NEW - Join for Premium/Deductible**:
```sql
SELECT
    cm.affordability_class,
    COUNT(*) AS plan_count,
    ROUND(AVG(p.PREMIUM), 2) AS avg_premium,
    ROUND(AVG(p.DEDUCTIBLE), 2) AS avg_deductible,
    ROUND(AVG(cm.pref_avg_copay_amt), 2) AS avg_copay,
    ROUND(AVG(cm.affordability_index), 3) AS avg_index
FROM gold.agg_plan_cost_metrics cm
LEFT JOIN bronze.brz_plan_info p ON cm.plan_key = p.PLAN_KEY
WHERE cm.affordability_class IS NOT NULL
GROUP BY cm.affordability_class
```

**Added**: `avg_copay` to validation output

---

## PCA Feature Interpretation

With the updated features, the PCA will capture:

| Feature | Source | Interpretation |
|---------|--------|----------------|
| `premium` | `brz_plan_info.PREMIUM` | Monthly cost |
| `deductible` | `brz_plan_info.DEDUCTIBLE` | Initial out-of-pocket threshold |
| `avg_copay_preferred` | `agg_plan_cost_metrics.pref_avg_copay_amt` | Typical copay at preferred pharmacy |
| `median_copay_preferred` | `agg_plan_cost_metrics.pref_median_copay_amt` | Robust central copay value |
| `avg_coinsurance_preferred` | `agg_plan_cost_metrics.pref_avg_coins_rate` | % coinsurance rate |
| `avg_copay_nonpreferred` | `agg_plan_cost_metrics.nonpref_avg_copay_amt` | Cost penalty for non-preferred |
| `ded_applies_rate` | `agg_plan_cost_metrics.ded_applies_rate` | How often deductible applies |
| `specialty_share` | `agg_plan_cost_metrics.specialty_row_share` | Specialty tier prevalence |

**PCA First Component**: Will capture the primary axis of cost variation across all these dimensions.

---

## Execution Order

```bash
# 1. Run cost aggregation (creates gold.agg_plan_cost_metrics)
python db\gold\06_agg_cost.py

# 2. Run affordability PCA (adds affordability_index to cost metrics)
python db\gold\08_affordability_index_pca.py
```

**Dependencies**:
- **Required Input**: 
  - `gold.agg_plan_cost_metrics` (from `06_agg_cost.py`)
  - `bronze.brz_plan_info` (from migration)
- **Output**: Adds `affordability_index` and `affordability_class` columns to `gold.agg_plan_cost_metrics`

---

## Dependent Processes

### ✅ No External Dependencies Found

The `affordability_class` and `affordability_index` columns are:
- Added to `gold.agg_plan_cost_metrics`
- Can be used by downstream ML scripts for feature engineering
- Currently not referenced by other scripts (available for future use)

### Potential ML Features

Once calculated, these can be joined in `ml.training_plan_pairs`:
```sql
LEFT JOIN gold.agg_plan_cost_metrics cm
    ON p.plan_key = cm.plan_key
```

Then use:
- `cm.affordability_index` - Continuous cost score
- `cm.affordability_class` - Categorical (0=most affordable, 3=most expensive)

---

## Validation

After running the updated script, expect:

**PCA Output**:
```
Feature contributions:
  - premium: 0.XXX
  - deductible: 0.XXX
  - avg_copay_preferred: 0.XXX
  - median_copay_preferred: 0.XXX
  - avg_coinsurance_preferred: 0.XXX
  - avg_copay_nonpreferred: 0.XXX
  - ded_applies_rate: 0.XXX
  - specialty_share: 0.XXX
  
Explained variance: XX.X%
```

**Affordability Distribution**:
```
Class 0 (Most Affordable):
  - Plans: XXX
  - Avg Premium: $XX.XX
  - Avg Deductible: $XXX.XX
  - Avg Copay: $XX.XX
  - Avg Index: -X.XXX (negative = below mean cost)

Class 3 (Most Expensive):
  - Plans: XXX
  - Avg Premium: $XX.XX
  - Avg Deductible: $XXX.XX
  - Avg Copay: $XX.XX
  - Avg Index: X.XXX (positive = above mean cost)
```

**Expected Pattern**: Higher class → Higher premium, deductible, copays, index

---

## Summary

**Corrected Data Flow**:
```
bronze.brz_beneficiary_cost
    ↓
gold.agg_plan_cost_metrics (06_agg_cost.py)
    ↓ + JOIN bronze.brz_plan_info
PCA Feature Matrix (08_affordability_index_pca.py)
    ↓
gold.agg_plan_cost_metrics.affordability_index
```

**Status**: ✅ Aligned with actual schema

---

**Updated**: 2026-02-04  
**Script**: `db/gold/08_affordability_index_pca.py`
