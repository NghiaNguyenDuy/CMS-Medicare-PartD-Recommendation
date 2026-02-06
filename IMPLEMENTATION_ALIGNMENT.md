# Implementation Plan Alignment - Source and Target Mapping

## Overview

This document maps the implementation plan to actual source code, justifying all sources and targets in the data processing pipeline.

---

## Bronze Layer: SPUF Data Ingestion

### Source-to-Target Mapping

| SPUF Source File | Bronze Table | Script | Justification |
|------------------|--------------|--------|---------------|
| `plan_information_PPUF.parquet` | `bronze.brz_plan_info` | `migrate_to_duckdb.py:175` | Core plan characteristics (premium, ded, geography) |
| `basic_drugs_formulary.parquet` | `bronze.brz_basic_formulary` | `migrate_to_duckdb.py:189` | Drug tier and restriction data |
| `beneficiary_cost.parquet` | `bronze.brz_beneficiary_cost` | `migrate_to_duckdb.py:199` | Copay/coinsurance by tier/coverage/days |
| `insulin_cost.parquet` | `bronze.brz_insulin_cost` | `migrate_to_duckdb.py:209` | $35 cap compliance data |
| `geographic.parquet` | `bronze.brz_geographic` | `migrate_to_duckdb.py:220` | State/county/PDP region mapping |
| `excluded_drugs.parquet` | `bronze.brz_excluded_drugs` | `migrate_to_duckdb.py:230` | Part B drugs, partial coverage |
| `ibc.parquet` | `bronze.brz_ibc` | `migrate_to_duckdb.py:242` | Indication-based coverage constraints |
| `pharmacy_networks.parquet` | `bronze.brz_pharmacy_networks` | `migrate_to_duckdb.py:253` | Preferred/standard pharmacy lists |

### Reference Data Ingestion

| Reference Source | Bronze Table | Script | Justification |
|----------------|--------------|--------|---------------|
| `data/insulin_ref.csv` | `bronze.brz_insulin_ref` | `db/bronze/06_ingest_insulin_ref.py` | Official CMS insulin NDC list (84 products) - replaces hardcoded values |
| `data/dim_zipcode_geo.csv` | `bronze.brz_zipcode` | `db/bronze/05_ingest_geography.py` | Zip-level lat/lng/population for beneficiary assignment |

**Key Improvement**: Insulin reference lookup eliminates hardcoded NDC/RXCUI lists, using official CMS Part D Senior Savings Model data.

---

## Silver Layer: Normalization (Skipped - Using Bronze Directly)

**Design Decision**: The implementation uses `bronze.*` tables directly in Gold aggregations instead of creating intermediate Silver tables. This simplifies the architecture for this use case.

**Justification**:
- Bronze tables already have correct types from Parquet schema
- Business rules (type labels, flags) can be applied in Gold CTEs
- Reduces data duplication and query complexity

---

## Gold Layer: Analytics-Ready Aggregations

### Dimension Tables

#### 1. dim_zipcode

| Source | Target | Script | Transformation |
|--------|--------|--------|----------------|
| `bronze.brz_zipcode` | `gold.dim_zipcode` | `db/gold/03_dim_zipcode.py` | Add density_category (urban/suburban/rural) based on density thresholds |

**Justification**: Enriches raw zipcode data with derived categories for geographic analysis.

#### 2. Plan Formulary Metrics

| Source | Target | Script | Key Metrics |
|--------|--------|--------|-------------|
| `bronze.brz_plan_info` + `bronze.brz_basic_formulary` + `bronze.brz_insulin_ref` | `gold.agg_plan_formulary_metrics` | `db/gold/05_agg_formulary.py` | total_drugs, tier distribution, restriction rates (PA/ST/QL), insulin coverage, restrictiveness_class |

**Key Transformations**:
1. **Insulin Identification**: `LEFT JOIN bronze.brz_insulin_ref ON f.ndc = ir.ndc OR f.rxcui = ir.rxcui`
   - **Improvement**: Uses bronze.brz_insulin_ref instead of hardcoded list
   - **Result**: Accurate 84-product coverage

2. **Restrictiveness Classification** ✨ **IMPROVED**:
   - **Old (Hardcoded)**: 
     ```sql
     CASE 
       WHEN avg_restriction_count >= 1.5 THEN 2
       WHEN avg_restriction_count >= 0.75 THEN 1
       ELSE 0
     END
     ```
   - **New (Quantile-Based)**:
     ```sql
     (NTILE(3) OVER (ORDER BY avg_restriction_count ASC) - 1) AS restrictiveness_class
     ```
   - **Justification**: Data-driven tertiles instead of arbitrary thresholds. Automatically adapts to data distribution.

3. **Restriction Rate Calculation**:
   - PA rate: `% drugs with prior_authorization_yn = 'Y'`
   - ST rate: `% drugs with step_therapy_yn = 'Y'`
   - QL rate: `% drugs with quantity_limit_yn = 'Y'`
   - Avg restrictions: Mean of (PA + ST + QL) flags per drug

#### 3. Network Adequacy Metrics

| Source | Target | Script | Key Metrics |
|--------|--------|--------|-------------|
| `bronze.brz_pharmacy_networks` OR `bronze.brz_plan_info` (fallback) | `gold.agg_plan_network_metrics` | `db/gold/07_agg_networks.py` | total_pharmacies, preferred_pharmacies, network_adequacy_flag, mail_order_available |

**Transformation Logic**:
- If pharmacy_networks table exists: Aggregate by plan_key
- Else: Create placeholder metrics from plan_info (contract type-based inference)

**Justification**: Handles optional pharmacy network data gracefully.

#### 4. Affordability Index (PCA-Based) ✨ **NEW**

| Source | Target | Script | Method |
|--------|--------|--------|--------|
| `gold.agg_plan_cost_metrics` | Updates `affordability_index` + `affordability_class` columns | `db/gold/08_affordability_index_pca.py` | PCA on 5 cost features |

**Key Features**:
1. **Input Features**: premium, deductible, avg_copay_preferred, avg_coinsurance_preferred, ded_applies_rate
2. **Preprocessing**: StandardScaler normalization
3. **Method**: sklearn PCA with n_components=1
4. **Output**: 
   - `affordability_index`: Continuous PC1 score
   - `affordability_class`: Quartile (0-3)

**Justification**: 
- Replaces hardcoded affordability formulas (e.g., `0.4 * premium + 0.3 * deductible`)
- Data-driven weights based on variance explained
- Interpretable via feature contributions

---

## ML Layer: Feature Engineering

### 1. Geographic Assignment

| Source | Target | Script | Purpose |
|--------|--------|--------|---------|
| `synthetic.syn_beneficiary` + `bronze.brz_zipcode` | Updates `zip_code`, `lat`, `lng`, `density` | `db/ml/02_assign_geography.py` | Assign realistic zip codes within county using population weights |

**Process**:
```sql
1. Get zip weight within county: population / SUM(population) OVER (PARTITION BY county_code)
2. Assign zip using row_number + modulo for deterministic assignment
3. Update beneficiary with zip, lat, lng, density
```

**Justification**: Enables distance calculations and realistic geographic distribution.

### 2. Distance Proxy Calculation

| Source | Target | Script | Method |
|--------|--------|--------|--------|
| `bronze.brz_plan_info` + `gold.agg_plan_network_metrics` + `bronze.brz_zipcode` | `ml.plan_distance_features` | `db/ml/03_calculate_distance.py` | Simulated distance based on density + network adequacy |

**Distance Formula**:
```sql
CASE
  -- Urban + high network = 0.5-2 miles
  WHEN density >= 1000 AND preferred_pharmacies >= 20 
    THEN 0.5 + RANDOM() * 1.5
  
  -- Suburban + medium = 2-8 miles  
  WHEN density BETWEEN 250 AND 1000 AND preferred_pharmacies >= 10
    THEN 2.0 + RANDOM() * 6.0
  
  -- Rural + low = 8-25 miles
  ELSE 8.0 + RANDOM() * 17.0
END
```

**Justification**: No actual pharmacy locations available, so proxy based on observable characteristics (population density, network size).

### 3. Training Pairs Generation

| Source | Target | Script | Purpose |
|--------|--------|--------|---------|
| `synthetic.syn_beneficiary` + `bronze.brz_plan_info` + Gold aggregations + `ml.plan_distance_features` | `ml.training_plan_pairs` | `db/ml/05_training_pairs.py` | Beneficiary-plan pairs with geographic constraints |

**Critical JOIN Logic**:
```sql
FROM synthetic.syn_beneficiary b
JOIN bronze.brz_plan_info p 
  ON b.state = p.state 
  AND b.county_code = p.county_code  -- ENFORCE SERVICE AREA
```

**Features Generated**:
- Beneficiary characteristics: risk_segment, fills_target, unique_drugs, insulin_user_flag
- Plan features: premium, deductible, formulary metrics, network metrics
- Distance features: simulated_distance_miles, distance_category
- Target: estimated_annual_oop + distance_penalty

**Justification**: Only recommend plans available in beneficiary's county (geographic constraint from implementation plan).

### 4. Recommendation Explanations

| Source | Target | Script | Purpose |
|--------|--------|--------|---------|
| `ml.training_plan_pairs` | `ml.recommendation_explanations` | `db/ml/06_recommendation_explainer.py` | Human-readable recommendations with warnings |

**Warning Flags**:
- Network adequacy: `preferred_pharmacies < 10`
- Insulin coverage: `insulin_user_flag = 1` with explanation
- Formulary restrictiveness: `restrictiveness_class = 2`
- Distance tradeoff: `estimated_oop < 2000 AND distance_miles > 12`

**Justification**: Enables consultation-focused recommendations with clear explanations.

---

## Synthetic Data Generation

| Source | Target | Script | Method |
|--------|--------|--------|--------|
| `data/pde.csv` (optional) OR generated | `synthetic.syn_beneficiary` | `scripts/generate_beneficiary_profiles.py` | PDE aggregation or synthetic generation |

**Two Modes**:

1. **PDE Mode** (`--from-pde`):
   - Aggregate fills by bene_id + ndc
   - Join with `bronze.brz_insulin_ref` for insulin flagging
   - Calculate risk from total_rx_cost

2. **Synthetic Mode (Default)**:
   - Sample drugs from `bronze.brz_basic_formulary`
   - Assign risk_segment (50% LOW, 30% MED, 20% HIGH)
   - Set insulin_user_flag (~15%)
   - Generate fills/cost based on risk

**Justification**: Flexible approach - use real PDE if available, else generate realistic synthetic data from actual formulary.

---

## Schema Consistency: bronze.* Naming

**Implementation Plan Specification** (Line 36-44):
```
Bronze Layer Tables:
- brz_plan_info
- brz_formulary
- brz_beneficiary_cost
...
```

**Actual Implementation**:
All Bronze tables use `bronze.brz_*` prefix consistently:
- `bronze.brz_plan_info` ✓
- `bronze.brz_basic_formulary` ✓
- `bronze.brz_beneficiary_cost` ✓
- `bronze.brz_insulin_ref` ✓ (additional)
- `bronze.brz_zipcode` ✓ (additional)

**Justification**: Clear layer boundaries, follows medallion architecture best practices.

---

## Key Improvements vs. Implementation Plan

### 1. Insulin Reference Lookup
- **Plan**: "Expand NDC list (currently ~3 NDCs, should be 50+)"
- **Implementation**: 84 official NDCs from CMS Part D Senior Savings Model via `bronze.brz_insulin_ref`
- **Status**: ✅ EXCEEDED

### 2. Quantile-Based Classification
- **Plan**: Restrictiveness class based on avg_restriction_count
- **Old**: Hardcoded thresholds (>=1.5, >=0.75)
- **New**: `NTILE(3) OVER (ORDER BY avg_restriction_count ASC)`
- **Status**: ✅ IMPROVED (data-driven)

### 3. PCA-Based Affordability
- **Plan**: "Affordability Index (PCA-based): Components: premium, deductible, avg copay/coins"
- **Implementation**: `db/gold/08_affordability_index_pca.py` with sklearn PCA
- **Status**: ✅ IMPLEMENTED

### 4. Geographic Intelligence
- **Plan**: "Distance Proxy Calculation" with density-based logic
- **Implementation**: `db/ml/03_calculate_distance.py` with exact formula from plan
- **Status**: ✅ IMPLEMENTED

### 5. County-Level Service Area
- **Plan**: "Only pair beneficiaries with plans available in their county"
- **Implementation**: `JOIN ON b.county_code = p.county_code` in `05_training_pairs.py`
- **Status**: ✅ IMPLEMENTED

---

## Verification Checklist

| Requirement | Implementation | Status |
|-------------|----------------|--------|
| Bronze tables from SPUF | `migrate_to_duckdb.py` creates 8 bronze tables | ✅ |
| Insulin reference ingestion | `06_ingest_insulin_ref.py` loads 84 NDCs | ✅ |
| Quantile-based restrictiveness | `NTILE(3)` in `05_agg_formulary.py` | ✅ |
| PCA affordability index | `08_affordability_index_pca.py` with sklearn | ✅ |
| Zipcode dimension | `03_dim_zipcode.py` with density categories | ✅ |
| Geographic assignment | `ml/02_assign_geography.py` with population weights | ✅ |
| Distance proxy | `ml/03_calculate_distance.py` with density formula | ✅ |
| County-level constraints | JOIN on county_code in `05_training_pairs.py` | ✅ |
| Explanation generation | `ml/06_recommendation_explainer.py` with warnings | ✅ |

**All requirements from implementation plan: IMPLEMENTED ✅**

---

## Environment Configuration

**Database Path**: Configured in two places:
1. `db/db_manager.py:35` - Default: `'data/medicare_part_d.duckdb'`
2. `scripts/migrate_to_duckdb.py:32` - Default: `'data/medicare_part_d.duckdb'`

**Recommendation**: Create `.env` file for configuration:
```bash
# .env
DB_PATH=data/medicare_part_d.duckdb
DATA_DIR=data
SPUF_DIR=data/SPUF
```

Then update scripts to use `python-dotenv`:
```python
from dotenv import load_dotenv
import os

load_dotenv()
DB_PATH = os.getenv('DB_PATH', 'data/medicare_part_d.duckdb')
```

**Status**: Currently hardcoded, but easily extractable to .env if needed.

---

## Data Flow Summary

```
SPUF Parquet Files
    ↓ (migrate_to_duckdb.py)
Bronze Tables (bronze.brz_*)
    ↓ (05_agg_formulary.py, 07_agg_networks.py)
Gold Aggregations (gold.agg_*)
    ↓ (08_affordability_index_pca.py)
Enhanced Gold Features
    ↓ (generate_beneficiary_profiles.py)
Synthetic Beneficiaries
    ↓ (ml/02_assign_geography.py)
Geo-Assigned Beneficiaries
    ↓ (ml/03_calculate_distance.py)
Distance Features
    ↓ (ml/05_training_pairs.py)
Training Pairs (county-constrained)
    ↓ (ml/06_recommendation_explainer.py)
Recommendations with Explanations
```

**Each arrow represents a verified source → target transformation in the actual code.**
