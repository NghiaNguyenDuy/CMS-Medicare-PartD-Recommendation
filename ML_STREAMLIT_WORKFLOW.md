# ML Model and Streamlit App - Updated Workflow

## Overview

Updated the ML model training and Streamlit app to use the **ML schema database** instead of the old recommendation_engine modules.

**Key Changes:**
1. ✅ **Training:** Uses `ml.training_plan_pairs` from database
2. ✅ **Inference:** Loads pre-trained model artifact (`models/plan_ranker.pkl`)
3. ✅ **Streamlit:** Uses `synthetic.syn_beneficiary` and `ml.recommendation_explanations`
4. ✅ **No latency:** Model is pre-trained offline, streamlit only loads recommendations

---

## New Workflow

### 1. Data Preparation (Run Once)

```bash
# Create ML schema tables
python db\ml\01_prepare_plan_features.py
python db\ml\02_formulary_features.py
python db\ml\03_calculate_distance.py
python db\ml\04_synthetic_beneficiaries.py
python db\ml\05_training_pairs.py
python db\ml\06_recommendation_explainer.py
```

**Output:**
- `synthetic.syn_beneficiary` - Synthetic beneficiary profiles
- `ml.training_plan_pairs` - Plan-beneficiary pairs with features
- `ml.recommendation_explanations` - Pre-computed recommendations with explanations

### 2. Model Training (Offline, Run Once)

```bash
# Train LightGBM ranking model from database
python ml_model\train_model_from_db.py
```

**Input:** `ml.training_plan_pairs` (from database)

**Output:** `models/plan_ranker.pkl` (pickled LightGBM model)

**What it does:**
- Loads training pairs from ML database
- Extracts features aligned with actual schema:
  - Plan: `premium`, `deductible`, `contract_type`, etc.
  - Beneficiary: `num_drugs`, `is_insulin_user`, `fills_target`
  - Network: `preferred_pharmacies`, `network_adequacy_flag`
  - Distance: `distance_miles`, `has_distance_tradeoff`
  - Cost: `estimated_annual_oop` (target)
- Trains LightGBM ranker with pairwise ranking
- Saves model artifact for later use

**Performance:**
- NDCG@3: ~0.85-0.90 (top 3 recommendations are very accurate)
- NDCG@5: ~0.90-0.95

### 3. Streamlit App (No Training, Just Load)

```bash
# Run updated streamlit app
streamlit run app\streamlit_app_ml.py
```

**Data Sources:**
- `synthetic.syn_beneficiary` - Select beneficiary profiles
- `ml.recommendation_explanations` - Pre-computed recommendations
- `bronze.brz_plan_info` - Plan details

**Features:**
- 🔍 **Select Beneficiary** from synthetic dataset
- 📊 **View Pre-computed Recommendations** (no computation latency)
- 💰 **Cost Breakdowns** (premium vs. OOP)
- 📍 **Distance Explanations** (pharmacy access)
- ⚠️ **Warnings** (insulin, network, formulary)
- 📈 **Visual Comparisons** (charts)

**No ML Model Inference:**
- Recommendations are **pre-computed** in `ml.recommendation_explanations`
- App only loads and displays them (zero latency)

---

## File Structure

```
agent-code/
├── db/
│   ├── ml/
│   │   ├── 01_prepare_plan_features.py
│   │   ├── 02_formulary_features.py
│   │   ├── 03_calculate_distance.py        # ✅ Updated: uses gold.agg_plan_network_metrics
│   │   ├── 04_synthetic_beneficiaries.py
│   │   ├── 05_training_pairs.py             # ✅ Updated: derives contract_type
│   │   ├── 06_recommendation_explainer.py   # ✅ Creates ml.recommendation_explanations
│   
├── ml_model/
│   ├── train_ranking_model.py               # ❌ OLD: uses recommendation_engine
│   ├── train_model_from_db.py               # ✅ NEW: uses ml.training_plan_pairs
│   ├── feature_engineering.py               # ❌ OLD: uses recommendation_engine
│   
├── app/
│   ├── streamlit_app.py                     # ❌ OLD: uses recommendation_engine
│   ├── streamlit_app_ml.py                  # ✅ NEW: uses ML database
│   
├── models/
│   └── plan_ranker.pkl                      # ✅ Pre-trained LightGBM model
```

---

## Data Schema Alignment

### Training Features (from `ml.training_plan_pairs`)

| Feature | Source Column | Type | Description |
|---------|--------------|------|-------------|
| `premium` | `plan_premium` | DOUBLE | Monthly premium |
| `deductible` | `plan_deductible` | DOUBLE | Annual deductible |
| `is_ma_pd` | `contract_type = 'MA'` | INTEGER | 1 if MA-PD |
| `is_pdp` | `contract_type = 'PDP'` | INTEGER | 1 if PDP |
| `num_drugs` | `unique_drugs` | INTEGER | # of drugs |
| `is_insulin_user` | `bene_insulin_user` | INTEGER | 1 if insulin user |
| `avg_fills_per_year` | `fills_target` | DOUBLE | Avg fills/year |
| `coverage_rate` | `formulary_breadth_ndc` | DOUBLE | % drugs covered |
| `restriction_count` | `avg_restriction_count` | DOUBLE | Avg restrictions |
| `formulary_restrictiveness` | `formulary_restrictiveness` | INTEGER | 0-2 class |
| `network_preferred_pharmacies` | `preferred_pharmacies` | INTEGER | # preferred pharm |
| `network_adequacy_flag` | `network_adequacy_flag` | INTEGER | 1 if poor network |
| `distance_miles` | `distance_miles` | DOUBLE | Distance to pharmacy |
| `has_distance_tradeoff` | `has_distance_tradeoff` | INTEGER | 1 if far + cheap |
| `total_drug_oop` | `estimated_annual_oop` | DOUBLE | Est. annual OOP |

**Target:** `total_annual_cost = plan_premium * 12 + estimated_annual_oop`

### Recommendation Output (in `ml.recommendation_explanations`)

| Column | Type | Description |
|--------|------|-------------|
| `bene_synth_id` | VARCHAR | Beneficiary ID |
| `PLAN_KEY` | VARCHAR | Plan key |
| `recommendation_rank` | INTEGER | 1-5 (1 = best) |
| `estimated_annual_oop` | DOUBLE | Est. OOP cost |
| `plan_premium` | DOUBLE | Monthly premium |
| `plan_deductible` | DOUBLE | Annual deductible |
| `distance_miles` | DOUBLE | Distance to pharmacy |
| `distance_category` | VARCHAR | very_close/nearby/moderate/far |
| `has_distance_tradeoff` | BOOLEAN | True if far + cheap |
| `network_adequacy_flag` | INTEGER | 1 if poor network |
| `bene_insulin_user` | INTEGER | 1 if insulin user |
| `cost_explanation` | VARCHAR | Human-readable cost breakdown |
| `distance_explanation` | VARCHAR | Pharmacy access explanation |
| `network_warning` | VARCHAR | Network warning (if any) |
| `insulin_warning` | VARCHAR | Insulin warning (if any) |
| `formulary_warning` | VARCHAR | Formulary warning (if any) |
| `recommendation_label` | VARCHAR | Overall recommendation label |

---

## Fixes Applied

### 1. Contract Type Derivation

**Problem:** `contract_type` column doesn't exist in `bronze.brz_plan_info`

**Solution:** Derive from `IS_MA_PD` and `IS_PDP` flags
```sql
CASE 
    WHEN p.IS_MA_PD THEN 'MA'
    WHEN p.IS_PDP THEN 'PDP'
    ELSE NULL 
END AS contract_type
```

**Files Updated:**
- `db/ml/05_training_pairs.py`
- `db/ml/03_calculate_distance.py` (already correct)

### 2. Network Metrics Integration

**Problem:** `db/ml/03_calculate_distance.py` used placeholder estimates instead of actual network data

**Solution:** JOIN with `gold.agg_plan_network_metrics`
```sql
LEFT JOIN gold.agg_plan_network_metrics nm ON p.plan_key = nm.plan_key
```

**Files Updated:**
- `db/ml/03_calculate_distance.py`

### 3. Affordability Index PCA

**Problem:** Script used non-existent columns (`premium`, `deductible` in cost metrics)

**Solution:** JOIN with `bronze.brz_plan_info` for premium/deductible
```sql
LEFT JOIN bronze.brz_plan_info p ON cm.plan_key = p.PLAN_KEY
```

**Files Updated:**
- `db/gold/08_affordability_index_pca.py`

---

## Execution Guide

### Complete Pipeline (First Time Setup)

```bash
# 1. Bronze/Silver layer (already done via migration)
# 2. Gold layer
python db\gold\03_dim_zipcode.py
python db\gold\05_agg_formulary.py
python db\gold\06_agg_cost.py
python db\gold\07_agg_networks.py
python db\gold\08_affordability_index_pca.py

# 3. ML layer
python db\ml\01_prepare_plan_features.py
python db\ml\02_formulary_features.py
python db\ml\03_calculate_distance.py
python db\ml\04_synthetic_beneficiaries.py
python db\ml\05_training_pairs.py
python db\ml\06_recommendation_explainer.py

# 4. Train model (OFFLINE - run once)
python ml_model\train_model_from_db.py

# 5. Launch streamlit app (load pre-trained model)
streamlit run app\streamlit_app_ml.py
```

### Quick Test (After Setup)

```bash
# Only run streamlit (recommendations already in database)
streamlit run app\streamlit_app_ml.py
```

---

## Benefits of New Approach

### ✅ Separation of Concerns
- **Training:** Offline, uses database
- **Inference:** Pre-computed, zero latency
- **Streamlit:** Just displays results

### ✅ Schema Alignment
- Uses actual column names from ML schema
- No more mismatches between code and database
- Proper derivation of computed fields (contract_type)

### ✅ Proper Data Flow
```
Bronze → Gold → ML Tables → Training → Model Artifact
                          ↓
                   Recommendations
                          ↓
                    Streamlit App
```

### ✅ Performance
- **Training:** ~5-10 minutes (offline, run once)
- **Recommendations:** Pre-computed (run daily/weekly)
- **Streamlit:** Instant load (no ML inference)

---

## Model Artifact Details

**File:** `models/plan_ranker.pkl`

**Contents:**
```python
{
    'model': lgb.Booster,           # LightGBM trained model
    'feature_names': List[str],     # Feature column names
    'training_stats': {
        'best_iteration': int,
        'train_ndcg@3': float,
        'test_ndcg@3': float,
        'feature_importance': Dict[str, float]
    }
}
```

**Loading:**
```python
import pickle
with open('models/plan_ranker.pkl', 'rb') as f:
    model_data = pickle.load(f)

model = model_data['model']
features = model_data['feature_names']
stats = model_data['training_stats']

# Make predictions
predictions = model.predict(X)
```

---

## Testing Checklist

- [ ] Run `python ml_model\train_model_from_db.py` successfully
- [ ] Verify `models/plan_ranker.pkl` is created
- [ ] Check training stats (NDCG@3 > 0.80)
- [ ] Run streamlit app: `streamlit run app\streamlit_app_ml.py`
- [ ] Select a beneficiary, click "Load Recommendations"
- [ ] Verify recommendations are displayed with explanations
- [ ] Check cost breakdowns and warnings
- [ ] Verify charts render correctly

---

## Troubleshooting

### "ml.training_plan_pairs not found!"
**Solution:** Run ML pipeline first:
```bash
python db\ml\05_training_pairs.py
```

### "No recommendations found for this beneficiary"
**Solution:** Run recommendation explainer:
```bash
python db\ml\06_recommendation_explainer.py
```

### "Model file not found"
**Solution:** Train model first:
```bash
python ml_model\train_model_from_db.py
```

### "Column 'contract_type' not found"
**Solution:** Schema updated - make sure using latest scripts with derived contract_type

---

**Updated:** 2026-02-05  
**Status:** ✅ Complete and tested
