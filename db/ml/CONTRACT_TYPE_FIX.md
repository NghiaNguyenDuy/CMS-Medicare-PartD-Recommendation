# Contract Type Derivation Fix

## Issue

Multiple scripts were referencing `contract_type` column directly from `bronze.brz_plan_info`, but this column **does not exist**.

### Actual Schema

`bronze.brz_plan_info` has:
- `IS_MA_PD` - Boolean flag for MA-PD plans
- `IS_PDP` - Boolean flag for PDP plans  
- No `contract_type` column

## Solution

Derive `contract_type` using CASE statement:

```sql
CASE 
    WHEN p.IS_MA_PD THEN 'MA'
    WHEN p.IS_PDP THEN 'PDP'
    ELSE NULL 
END AS contract_type
```

## Files Updated

### ✅ `db/ml/05_training_pairs.py` (Line 84-89)

**Before**:
```sql
p.contract_type,
```

**After**:
```sql
-- Derive contract_type from IS_MA_PD and IS_PDP flags
CASE 
    WHEN p.IS_MA_PD THEN 'MA'
    WHEN p.IS_PDP THEN 'PDP'
    ELSE NULL 
END AS contract_type,
```

### ✅ `db/ml/03_calculate_distance.py` (Lines 50-53)

**Status**: Already correct - derives contract_type properly

```sql
case 
    when p.IS_MA_PD then 'MA'
    when p.IS_PDP then 'PDP'
    else null end as contract_type,
```

### ✅ `db/ml/06_recommendation_explainer.py`

**Status**: No changes needed - uses `contract_type` from `ml.training_plan_pairs` table which derives it correctly

## Dependent Tables

### `ml.plan_distance_features`
- Contains derived `contract_type` column
- Used in downstream ML scripts

### `ml.training_plan_pairs`  
- Contains derived `contract_type` column
- Used by recommendation explainer

## Validation

After running updated scripts, verify:

```sql
-- Check contract_type distribution
SELECT 
    contract_type,
    COUNT(*) as count
FROM ml.training_plan_pairs
GROUP BY contract_type;
```

**Expected**:
```
contract_type | count
--------------|-------
MA            | XXXX
PDP           | XXXX
```

---

**Updated**: 2026-02-05  
**Status**: ✅ Fixed
