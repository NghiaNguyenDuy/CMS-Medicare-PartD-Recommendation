# Skill: Medicare Part D Plan Recommendation - DuckDB Implementation

---
name: CMS Medicare Part D Analytics
description: Comprehensive knowledge of Medicare Part D data schemas, pipeline processes, and ML model implementation using DuckDB database
---

## Overview

This skill provides comprehensive knowledge of the Medicare Part D plan recommendation system built using CMS SPUF (Standard Part D PUF) data, processed into a structured DuckDB database with Bronze/Gold/ML layers.

**Database:** `medicare_part_d.duckdb`  
**Schemas:** Bronze (raw), Gold (aggregated), ML (features), Synthetic (test data)

---

## Database Architecture

### Layer Structure

```
Bronze Layer (brz_*) 
    ↓ Transformation
Gold Layer (gold.*)
    ↓ Feature Engineering  
ML Layer (ml.*)
    ↓ Training/Inference
Synthetic Layer (synthetic.*) - Test data
```

---

## 1. Bronze Layer - Raw CMS Data

### 1.1 `bronze.brz_plan_info`

**Purpose:** Medicare Part D plan metadata and pricing

**Key Columns:**
- `CONTRACT_ID`, `PLAN_ID`, `SEGMENT_ID` → Composite key
- `PLAN_KEY` = `{CONTRACT_ID}-{PLAN_ID}-{SEGMENT_ID}` (derived)
- `FORMULARY_ID` → Links to formulary tables
- `PREMIUM` (DOUBLE) → Monthly premium in dollars
- `DEDUCTIBLE` (DOUBLE) → Annual deductible in dollars
- `IS_MA_PD` (BOOLEAN) → Medicare Advantage PD plan flag
- `IS_PDP` (BOOLEAN) → Standalone Prescription Drug Plan flag
- `STATE`, `COUNTY_CODE` → Geographic service area
- `MA_REGION_CODE`, `PDP_REGION_CODE` → Regional identifiers

**Important Notes:**
- No `contract_type` column exists - must derive from `IS_MA_PD`/`IS_PDP`:
  ```sql
  CASE 
      WHEN IS_MA_PD THEN 'MA'
      WHEN IS_PDP THEN 'PDP' 
      ELSE NULL 
  END AS contract_type
  ```

### 1.2 `bronze.brz_basic_formulary`

**Purpose:** Drug coverage by formulary (which drugs are covered)

**Key Columns:**
- `FORMULARY_ID`, `NDC` → Composite key
- `RXCUI` (VARCHAR) → RxNorm concept unique identifier
- `TIER_LEVEL_VALUE` (DOUBLE) → Formulary tier (1-7, generics typically 1-2, brands 3-4, specialty 5-7)
- `QUANTITY_LIMIT_YN`, `PRIOR_AUTHORIZATION_YN`, `STEP_THERAPY_YN` → Restriction flags
- `RESTRICTION_COUNT` (BIGINT) → Total # of restrictions (derived: QL + PA + ST)
- `IS_GENERIC_TIER`, `IS_BRAND_TIER`, `IS_SPECIALTY_TIER` → Tier category flags (derived)

**Formulary Breadth Calculation:**
```sql
coverage_rate = COUNT(DISTINCT covered_ndc) / COUNT(DISTINCT all_ndc)
```

### 1.3 `bronze.brz_beneficiary_cost`

**Purpose:** Cost-sharing rules by tier, days supply, and pharmacy type

**Key Columns:**
- `CONTRACT_ID`, `PLAN_ID`, `SEGMENT_ID`, `COVERAGE_LEVEL`, `TIER`, `DAYS_SUPPLY` → Composite key
- `COVERAGE_LEVEL` (BIGINT):
  - 0 = Pre-deductible
  - 1 = Initial coverage
  - 3 = Catastrophic
- `TIER` (DOUBLE) → Formulary tier (1-7)
- `DAYS_SUPPLY` (BIGINT) → 30, 60, 90 days
- `COST_TYPE_PREF` (BIGINT) → 1=Copay, 2=Coinsurance, 3=Not offered
- `COST_AMT_PREF` (DOUBLE) → Dollar amount (copay) or % rate (coinsurance)
- `COST_TYPE_NONPREF`, `COST_AMT_NONPREF` → Non-preferred pharmacy costs
- `COST_TYPE_MAIL_PREF`, `COST_AMT_MAIL_PREF` → Mail order costs
- `DED_APPLIES_YN` (VARCHAR) → 'Y' if deductible applies to this tier

**Cost Calculation:**
- If `COST_TYPE = 1` (Copay): OOP = `COST_AMT`
- If `COST_TYPE = 2` (Coinsurance): OOP = `COST_AMT * drug_allowed_cost`

### 1.4 `bronze.brz_insulin_cost`

**Purpose:** Special insulin cost-sharing (IRA $35 cap compliance)

**Key Columns:**
- `CONTRACT_ID`, `PLAN_ID`, `SEGMENT_ID`, `TIER`, `DAYS_SUPPLY` → Composite key
- `copay_amt_pref_insln` (DOUBLE) → Insulin copay at preferred pharmacy
- `copay_amt_nonpref_insln` (DOUBLE) → Insulin copay at non-preferred pharmacy
- `copay_amt_mail_pref_insln`, `copay_amt_mail_nonpref_insln` → Mail order insulin copays
- `EXCEEDS_CAP` (BOOLEAN) → True if copay > $35 (derived flag)

**Usage:** Always check this table first for insulin users before using brz_beneficiary_cost

### 1.5 `bronze.brz_pharmacy_network`

**Purpose:** Pharmacy network adequacy per plan

**Key Columns:**
- `CONTRACT_ID`, `PLAN_ID`, `SEGMENT_ID`, `PHARMACY_NUMBER` → Composite key
- `NETWORK_KEY` = `{PLAN_KEY}-{PHARMACY_NUMBER}` (derived)
- `IS_PREFERRED_RETAIL`, `IS_PREFERRED_MAIL` (BOOLEAN) → Preferred status flags
- `OFFERS_RETAIL`, `OFFERS_MAIL` (BOOLEAN) → Channel availability
- `IS_IN_AREA` (BOOLEAN) → In service area flag
- `BRAND_DISPENSING_FEE_30/60/90`, `GENERIC_DISPENSING_FEE_30/60/90` → Dispensing fees
- `AVG_BRAND_FEE`, `AVG_GENERIC_FEE` (DOUBLE) → Average fees (derived)
- `PHARMACY_TYPE` (VARCHAR) → 'RETAIL', 'MAIL', or 'BOTH' (derived)

### 1.6 `bronze.brz_excluded_drugs`

**Purpose:** Drugs explicitly not covered by plan

**Key Columns:**
- `CONTRACT_ID`, `PLAN_ID`, `RXCUI` → Composite key
- `TIER`, `QUANTITY_LIMIT_YN`, `PRIOR_AUTH_YN`, `STEP_THERAPY_YN`, `CAPPED_BENEFIT_YN` → Restriction flags
- `COMPLETELY_EXCLUDED` (BOOLEAN) → No coverage at all (derived)
- `CONDITIONALLY_COVERED` (BOOLEAN) → Coverage with heavy restrictions (derived)

**Coverage Check Order:**
1. Check excluded_drugs first (if found → not covered)
2. Then check basic_formulary (if found → covered with tier/restrictions)
3. If neither → off-formulary/unknown

### 1.7 `bronze.brz_zipcode`

**Purpose:** Geographic data for distance calculations

**Key Columns:**
- `zip_code` (VARCHAR) → 5-digit ZIP code (PK)
- `county_code` (INTEGER) → SSA county code
- `lat`, `lng` (DOUBLE) → Coordinates
- `population`, `density` (DOUBLE) → Population density (people/sq mi)
- `state`, `county`, `city` (VARCHAR) → Location names

**Usage:** Used to calculate simulated distance to nearest pharmacy in `ml.plan_distance_features`

### 1.8 `bronze.brz_geographic`

**Purpose:** County-to-region mapping

**Key Columns:**
- `COUNTY_CODE` (VARCHAR) → SSA county code (PK)
- `STATENAME`, `COUNTY` → Human-readable names
- `MA_REGION_CODE`, `PDP_REGION_CODE` → Regional plan service areas
- `STATE_CODE` → 2-letter state code

**Plan Availability Logic:**
- MA-PD plans: Filter by `STATE` + `COUNTY_CODE`
- PDP plans: Filter by `PDP_REGION_CODE` (mapped from county)

### 1.9 `bronze.brz_insulin_ref`

**Purpose:** Reference table mapping NDCs to insulin flag

**Key Columns:**
- `ndc` (VARCHAR) → NDC code (PK)
- `rxcui` (VARCHAR) → RxNorm code
- `is_insulin` (BIGINT) → 1 if insulin product
- `source`, `source_year` → Data provenance

**Usage:** Join with beneficiary drug list to identify insulin users

---

## 2. Gold Layer - Aggregated Metrics

### 2.1 `gold.agg_plan_formulary_metrics`

**Purpose:** Formulary breadth and restrictiveness per plan

**Key Columns:**
- `PLAN_KEY` (VARCHAR) → Plan identifier (PK)
- `total_drugs` (BIGINT) → # of drugs on formulary
- `formulary_breadth_pct` (DOUBLE) → % of all NDCs covered
- `generic_tier_pct`, `specialty_tier_pct` (DOUBLE) → Tier distribution
- `avg_tier` (DOUBLE) → Average tier level (lower = better generics)
- `pa_rate`, `st_rate`, `ql_rate` (DOUBLE) → % drugs with restrictions
- `avg_restriction_count` (DOUBLE) → Avg # restrictions per drug
- `restrictiveness_class` (BIGINT) → 0=Low, 1=Medium, 2=High (quantile-based)
- `insulin_drug_count` (BIGINT) → # of insulin products covered
- `insulin_coverage_pct` (DOUBLE) → % of insulin NDCs covered

**Created by:** `db/gold/05_agg_formulary.py`

### 2.2 `gold.agg_plan_network_metrics`

**Purpose:** Pharmacy network adequacy per plan

**Key Columns:**
- `plan_key` (VARCHAR) → Plan identifier (PK)
- `total_pharmacies` (BIGINT) → Total pharmacies in network
- `preferred_pharmacies` (BIGINT) → # preferred pharmacies (retail OR mail)
- `preferred_pharmacies_retail`, `preferred_pharmacies_mail` (BIGINT) → Channel breakdown
- `pref_pharmacy_pct` (DOUBLE) → % of network that's preferred
- `retail_pharmacies`, `mail_pharmacies` (BIGINT) → Channel availability counts
- `in_area_pharmacies` (BIGINT) → # pharmacies in service area
- `in_area_pct` (DOUBLE) → % network in-area
- `network_adequacy_flag` (INTEGER) → 1 if poor network (<10 preferred pharmacies)
- `mail_order_available` (INTEGER) → 1 if mail order exists
- `avg_brand_dispensing_fee`, `avg_generic_dispensing_fee`, `avg_floor_price` (DOUBLE) → Fee averages

**Created by:** `db/gold/07_agg_networks.py`

**Usage in ML:**
- Used in `db/ml/03_calculate_distance.py` to determine simulated distance to pharmacy
- Plans with `preferred_pharmacies < 10` → Longer simulated distances

### 2.3 `gold.agg_plan_cost_metrics`

**Purpose:** Cost structure aggregation by pharmacy type and tier

**Key Columns:**
- `plan_key` (VARCHAR) → Plan identifier (PK)
- `ded_applies_rate` (DOUBLE) → % of benefit rows where deductible applies
- `specialty_row_share` (DOUBLE) → % of rows for specialty tiers

**Preferred Pharmacy Metrics:**
- `pref_not_offered_share`, `pref_copay_share`, `pref_coinsurance_share` → Cost type distribution
- `pref_avg_copay_amt`, `pref_median_copay_amt`, `pref_p90_copay_amt` → Copay $ amounts
- `pref_avg_coins_rate`, `pref_median_coins_rate`, `pref_p90_coins_rate` → Coinsurance % rates

**Non-Preferred, Mail Pref, Mail Non-Pref:** Same structure as preferred (`nonpref_*`, `mail_pref_*`, `mail_nonpref_*`)

**Tier-Level Metrics:**
- `pref_avg_copay_tier_1` through `tier_7` → Average copay per tier
- `pref_avg_coins_rate_tier_1` through `tier_7` → Average coinsurance per tier

**PCA Features (added by `db/gold/08_affordability_index_pca.py`):**
- `affordability_index` (DOUBLE) → PCA-based cost score (negative = more affordable)
- `affordability_class` (INTEGER) → 0-3 quartile (0 = most affordable)

**Created by:** 
- `db/gold/06_agg_cost.py` (cost metrics)
- `db/gold/08_affordability_index_pca.py` (adds affordability index)

**Affordability PCA Inputs:**
```python
features = [
    'premium', 'deductible',  # From brz_plan_info
    'pref_avg_copay_amt', 'pref_median_copay_amt',  # From cost metrics
    'pref_avg_coins_rate', 'nonpref_avg_copay_amt',
    'ded_applies_rate', 'specialty_row_share'
]
```

### 2.4 `gold.dim_zipcode`

**Purpose:** ZIP code dimension with density categories

**Key Columns:**
- `zip_code` (VARCHAR) → ZIP code (PK)
- `city`, `state`, `county`, `county_code` → Location
- `lat`, `lng`, `population`, `density` → Geographic data
- `density_category` (VARCHAR) → 'URBAN' (≥1000), 'SUBURBAN' (250-1000), 'RURAL' (<250)

**Created by:** `db/gold/03_dim_zipcode.py`

**Usage:** Used in distance calculation to estimate pharmacy proximity based on population density

---

## 3. ML Layer - Feature Engineering

### 3.1 `ml.plan_distance_features`

**Purpose:** Simulated distance to nearest preferred pharmacy

**Key Columns:**
- `PLAN_KEY`, `STATE`, `COUNTY_CODE` → Plan-county identifier (composite PK)
- `contract_type` (VARCHAR) → 'MA' or 'PDP' (derived from IS_MA_PD/IS_PDP)
- `county_avg_density` (DOUBLE) → Avg population density in county
- `preferred_pharmacies`, `total_pharmacies` (BIGINT) → From gold.agg_plan_network_metrics
- `pref_pharmacy_pct`, `in_area_pharmacies` (DOUBLE) → Network metrics
- `simulated_distance_miles` (DOUBLE) → Estimated distance using density + network size
- `distance_category` (VARCHAR) → 'very_close' (<3mi), 'nearby' (3-8mi), 'moderate' (8-15mi), 'far' (>15mi)

**Distance Calculation Logic:**
```sql
CASE
    WHEN density >= 1000 AND preferred_pharmacies >= 20 THEN 0.5 + RANDOM() * 1.5  -- Urban, good network
    WHEN density >= 250 AND preferred_pharmacies >= 10 THEN 2.0 + RANDOM() * 6.0   -- Suburban
    WHEN density < 250 OR preferred_pharmacies < 10 THEN 8.0 + RANDOM() * 17.0     -- Rural/poor network
    ELSE 5.0 + RANDOM() * 10.0
END
```

**Created by:** `db/ml/03_calculate_distance.py`

**Important Fix Applied:** Now uses actual `gold.agg_plan_network_metrics.preferred_pharmacies` instead of placeholder estimates

### 3.2 `ml.training_plan_pairs`

**Purpose:** Beneficiary-plan pairs with features for training LightGBM ranking model

**Key Columns:**

**Identifiers:**
- `bene_synth_id` (VARCHAR) → Synthetic beneficiary ID
- `PLAN_KEY` (VARCHAR) → Plan key

**Beneficiary Features:**
- `bene_state`, `bene_county`, `bene_zip`, `bene_density` → Location
- `risk_segment` (VARCHAR) → 'LOW', 'MED', 'HIGH' (risk profile)
- `unique_drugs` (BIGINT) → # of different medications
- `fills_target` (BIGINT) → Annual prescription fills
- `total_rx_cost_est` (DOUBLE) → Estimated annual Rx cost
- `bene_insulin_user` (BIGINT) → 1 if insulin user

**Plan Features:**
- `contract_type` (VARCHAR) → Derived from IS_MA_PD/IS_PDP
- `plan_premium`, `plan_deductible` (DOUBLE) → From brz_plan_info
- `plan_snp` (BIGINT) → Special Needs Plan flag

**Formulary Features (from gold.agg_plan_formulary_metrics):**
- `formulary_total_drugs`, `formulary_generic_pct`, `formulary_specialty_pct`
- `formulary_pa_rate`, `formulary_st_rate`, `formulary_ql_rate`
- `formulary_restrictiveness` → 0-2 class

**Network Features (from gold.agg_plan_network_metrics):**
- `network_total_pharmacies`, `network_preferred_pharmacies`
- `network_adequacy_flag` → 1 if poor network

**Distance Features (from ml.plan_distance_features):**
- `distance_miles` (DOUBLE) → Simulated distance
- `distance_category` (VARCHAR) → Category label
- `distance_penalty` (DECIMAL) → Cost penalty for distance
- `has_distance_tradeoff` (BOOLEAN) → True if far but cheap

**Cost Features (TARGET):**
- `estimated_annual_oop` (DOUBLE) → Estimated out-of-pocket drug costs
- `total_cost_with_distance` (DECIMAL) → `premium*12 + OOP + distance_penalty`

**Created by:** `db/ml/05_training_pairs.py`

**Service Area Constraint:** Only includes plan-beneficiary pairs where plan is available in beneficiary's county

### 3.3 `ml.recommendation_explanations`

**Purpose:** Human-readable explanations for top 5 plan recommendations per beneficiary

**Key Columns:**
- `bene_synth_id` (VARCHAR) → Beneficiary ID
- `PLAN_KEY` (VARCHAR) → Plan key
- `recommendation_rank` (BIGINT) → 1-5 (1 = best)
- `estimated_annual_oop`, `plan_premium`, `plan_deductible` (DOUBLE) → Cost components
- `distance_miles`, `distance_category` (DOUBLE, VARCHAR) → Pharmacy access
- `has_distance_tradeoff` (BOOLEAN) → True if saving money but far pharmacy
- `network_adequacy_flag` (INTEGER) → 1 if poor network
- `bene_insulin_user` (BIGINT) → 1 if insulin user

**Explanation Text Fields:**
- `cost_explanation` (VARCHAR) → "Annual cost: $X (Premium: $Y/mo, Deductible: $Z, Est. OOP: $W)"
- `distance_explanation` (VARCHAR) → Context-aware pharmacy access explanation
- `network_warning` (VARCHAR) → "⚠️ WARNING: Limited pharmacy network..." (if applicable)
- `insulin_warning` (VARCHAR) → "💉 Insulin user: Verify $35 cap..." (if applicable)
- `formulary_warning` (VARCHAR) → Restriction warnings (PA/ST)
- `recommendation_label` (VARCHAR) → "⭐ RECOMMENDED" / "✓ Good option" / "💰 Budget option"

**Created by:** `db/ml/06_recommendation_explainer.py`

**Used in:** Streamlit app (`app/streamlit_app_ml.py`) to display recommendations without ML inference

---

## 4. Synthetic Layer - Test Data

### 4.1 `synthetic.syn_beneficiary`

**Purpose:** Synthetic beneficiary profiles for testing

**Key Columns:**
- `bene_synth_id` (VARCHAR) → Unique ID (PK)
- `STATE`, `COUNTY_CODE`, `zip_code` → Location
- `lat`, `lng`, `density` → Geographic coordinates
- `risk_segment` (VARCHAR) → 'LOW', 'MED', 'HIGH'
- `unique_drugs` (BIGINT) → # medications
- `fills_target` (BIGINT) → Annual fills
- `total_rx_cost_est` (DOUBLE) → Estimated annual Rx cost
- `insulin_user_flag` (BIGINT) → 1 if insulin user

**Created by:** `db/ml/04_synthetic_beneficiaries.py`

**Generation Logic:**
- Samples real counties with plans available
- Assigns risk segments based on distribution
- Estimates drug utilization and costs

---

## 5. Data Pipeline Execution Order

### 5.1 Bronze Layer (Migration)
```bash
python scripts\migrate_to_duckdb.py
```

**Creates:**
- `brz_plan_info`, `brz_basic_formulary`, `brz_excluded_drugs`
- `brz_beneficiary_cost`, `brz_insulin_cost`
- `brz_pharmacy_network`, `brz_geographic`, `brz_zipcode`

### 5.2 Gold Layer (Aggregations)
```bash
python db\gold\03_dim_zipcode.py               # Density categories
python db\gold\05_agg_formulary.py             # Formulary metrics + restrictiveness
python db\gold\06_agg_cost.py                  # Cost structure per plan
python db\gold\07_agg_networks.py              # Network adequacy
python db\gold\08_affordability_index_pca.py   # PCA affordability index
```

### 5.3 ML Layer (Feature Engineering)
```bash
# Note: Scripts 01 and 04 are referenced in documentation but not yet created
# Current implementation uses these scripts:
python db\ml\02_assign_geography.py          # Assign geography to entities
python db\ml\03_calculate_distance.py        # Distance proxies (uses gold.agg_plan_network_metrics)
python db\ml\05_training_pairs.py            # Beneficiary-plan pairs (uses synthetic.syn_beneficiary)
python db\ml\06_recommendation_explainer.py  # Explanations (creates ml.recommendation_explanations)
```

**Missing Scripts (to be created):**
- `db/ml/01_prepare_plan_features.py` - Plan-level feature aggregation (currently handled by Gold layer)
- `db/ml/04_synthetic_beneficiaries.py` - Test beneficiary generation (data may already exist in synthetic.syn_beneficiary)

**Note:** The pipeline currently works without these scripts because:
- Plan features are computed in Gold layer (gold.agg_plan_formulary_metrics, gold.agg_plan_network_metrics)
- Synthetic beneficiaries may have been created through another process

### 5.4 Model Training (Offline)
```bash
python ml_model\train_model_from_db.py
```

**Output:** `models/plan_ranker.pkl` (LightGBM model artifact)

### 5.5 Streamlit App (Inference)
```bash
streamlit run app\streamlit_app_ml.py
```

**Uses:** Pre-computed `ml.recommendation_explanations` (zero latency)

---

## 6. Key Implementation Fixes

### 6.1 Contract Type Derivation

**Problem:** `bronze.brz_plan_info` has NO `contract_type` column

**Solution:** Derive in queries:
```sql
CASE 
    WHEN IS_MA_PD THEN 'MA'
    WHEN IS_PDP THEN 'PDP'
    ELSE NULL 
END AS contract_type
```

**Files Updated:**
- `db/ml/03_calculate_distance.py`
- `db/ml/05_training_pairs.py`

### 6.2 Network Metrics Integration

**Problem:** `db/ml/03_calculate_distance.py` used hardcoded estimates (PDP=25, MA=15)

**Solution:** JOIN with `gold.agg_plan_network_metrics`:
```sql
LEFT JOIN gold.agg_plan_network_metrics nm ON p.plan_key = nm.plan_key
```

Now uses actual `preferred_pharmacies` count for distance calculation

### 6.3 Affordability Index PCA

**Problem:** Cost metrics table doesn't have `premium`/`deductible` columns

**Solution:** JOIN with `bronze.brz_plan_info`:
```sql
LEFT JOIN bronze.brz_plan_info p ON cm.plan_key = p.PLAN_KEY
```

PCA Features: `premium`, `deductible`, `pref_avg_copay_amt`, `pref_median_copay_amt`, `pref_avg_coins_rate`, `nonpref_avg_copay_amt`, `ded_applies_rate`, `specialty_row_share`

---

## 7. Common Queries

### 7.1 Get Plans Available in County
```sql
SELECT p.PLAN_KEY, p.PLAN_NAME, p.PREMIUM, p.DEDUCTIBLE,
       CASE WHEN p.IS_MA_PD THEN 'MA' WHEN p.IS_PDP THEN 'PDP' END as contract_type
FROM bronze.brz_plan_info p
WHERE p.COUNTY_CODE = '06037'  -- Los Angeles County
   OR p.STATE = 'CA';  -- PDP plans serve entire state/region
```

### 7.2 Check Drug Coverage in Plan
```sql
-- Step 1: Check if excluded
SELECT * FROM bronze.brz_excluded_drugs 
WHERE PLAN_KEY = 'H1234-001-000' AND RXCUI = '123456';

-- Step 2: If not excluded, check formulary
SELECT f.TIER_LEVEL_VALUE, f.RESTRICTION_COUNT, f.PRIOR_AUTHORIZATION_YN
FROM bronze.brz_plan_info p
JOIN bronze.brz_basic_formulary f ON p.FORMULARY_ID = f.FORMULARY_ID
WHERE p.PLAN_KEY = 'H1234-001-000' AND f.NDC = '00002871501';
```

### 7.3 Get Network Adequacy for Plan
```sql
SELECT nm.total_pharmacies, nm.preferred_pharmacies, nm.pref_pharmacy_pct,
       nm.network_adequacy_flag
FROM gold.agg_plan_network_metrics nm
WHERE nm.plan_key = 'H1234-001-000';
```

### 7.4 Get Recommendations for Beneficiary
```sql
SELECT recommendation_rank, PLAN_KEY, 
       cost_explanation, distance_explanation, recommendation_label,
       network_warning, insulin_warning, formulary_warning
FROM ml.recommendation_explanations
WHERE bene_synth_id = 'BENE_12345'
ORDER BY recommendation_rank;
```

---

## 8. Data Quality Notes

### 8.1 Key Uniqueness
- `brz_plan_info`: (CONTRACT_ID, PLAN_ID, SEGMENT_ID) → Unique
- `brz_basic_formulary`: (FORMULARY_ID, NDC) → Unique
- `brz_beneficiary_cost`: (CONTRACT_ID, PLAN_ID, SEGMENT_ID, COVERAGE_LEVEL, TIER, DAYS_SUPPLY, pharmacy_type) → Unique
- `brz_pharmacy_network`: (CONTRACT_ID, PLAN_ID, SEGMENT_ID, PHARMACY_NUMBER) → Unique

### 8.2 Missing Data Handling
- `PREMIUM`/`DEDUCTIBLE` nulls → COALESCE to 0
- `preferred_pharmacies` null → Use fallback estimate based on contract_type
- Missing coverage → Flag as off-formulary, not excluded

### 8.3 Derived Flags
- All `IS_*`, `HAS_*` boolean flags are derived during migration
- `*_KEY` composite keys are derived for join optimization
- `*_LABEL` human-readable labels are derived for reporting

---

## 9. Testing & Validation

### 9.1 Schema Validation
```python
# Run schema validator
python db\utils\validate_schema.py
```

### 9.2 Data Completeness
```sql
-- Check Bronze layer counts
SELECT 'brz_plan_info' as table_name, COUNT(*) FROM bronze.brz_plan_info
UNION ALL
SELECT 'brz_basic_formulary', COUNT(*) FROM bronze.brz_basic_formulary
UNION ALL
SELECT 'brz_pharmacy_network', COUNT(*) FROM bronze.brz_pharmacy_network;

-- Check Gold layer aggregations
SELECT 'agg_plan_formulary_metrics', COUNT(*) FROM gold.agg_plan_formulary_metrics
UNION ALL
SELECT 'agg_plan_network_metrics', COUNT(*) FROM gold.agg_plan_network_metrics
UNION ALL
SELECT 'agg_plan_cost_metrics', COUNT(*) FROM gold.agg_plan_cost_metrics;

-- Check ML layer features
SELECT 'training_plan_pairs', COUNT(*) FROM ml.training_plan_pairs
UNION ALL
SELECT 'recommendation_explanations', COUNT(*) FROM ml.recommendation_explanations;
```

### 9.3 ML Model Validation
- Training NDCG@3 should be > 0.85
- Test NDCG@3 should be > 0.80
- Top-1 recommendation should have lowest `total_cost_with_distance`

---

## 10. File Locations

**Data:** `data/medicare_part_d.duckdb`  
**Metadata:** `data/meta_db.csv`  
**Migration Script:** `scripts/migrate_to_duckdb.py`  
**Bronze Scripts:** `db/bronze/` (Note: Bronze layer created via migration)  
**Gold Scripts:** `db/gold/` (03_dim_zipcode.py, 05_agg_formulary.py, 06_agg_cost.py, 07_agg_networks.py, 08_affordability_index_pca.py)  
**ML Scripts:** `db/ml/` (02_assign_geography.py, 03_calculate_distance.py, 05_training_pairs.py, 06_recommendation_explainer.py)  
**Note:** ML scripts 01 (plan features) and 04 (synthetic beneficiaries) are missing from current implementation  
**Model Training:** `ml_model/train_model_from_db.py`  
**Streamlit Apps:** `app/streamlit_app.py` (old), `app/streamlit_app_ml.py` (ML database version)  
**Documentation:** `ML_STREAMLIT_WORKFLOW.md`, `PIPELINE_EXECUTION.md`, `db/PROCESSING_GUIDE.md`, `db/SCHEMA_REVIEW.md`  
**Utilities:** `db/db_manager.py`, `db/plan_repository.py`, `db/formulary_repository.py`, `db/utils/`

---

**Last Updated:** 2026-02-05  
**Database Version:** medicare_part_d.duckdb (SPUF 2025 Q3 data)
