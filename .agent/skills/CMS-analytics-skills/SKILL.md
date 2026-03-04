---
name: cms-medicare-part-d-duckdb
description: Work on this Medicare Part D recommendation codebase that uses DuckDB medallion layers, LightGBM ranking, and a Streamlit app. Use when tasks involve running or debugging the pipeline, validating table/column names from data/meta_db.csv, writing SQL across bronze/gold/ml/synthetic schemas, updating model training in ml_model/train_model_from_db.py, or modifying app/streamlit_app_interactive.py.
---

# CMS Medicare Part D DuckDB Skill

## Start Here

1. Read `README.md` for architecture and pipeline order.
2. Treat `data/meta_db.csv` as the schema source of truth before writing SQL.
3. Load only task-relevant files. Do not preload entire `db/`.

## Working Defaults

- Prefer running from repo root.
- Prefer `PLAN_KEY` for plan joins unless a table explicitly exposes `plan_key`.
- Derive contract type from `bronze.brz_plan_info` flags:
  - `IS_MA_PD -> 'MA'`
  - `IS_PDP -> 'PDP'`
- Treat `data/medicare_part_d.duckdb` as the canonical database file.
- App and inference flows should be read-only against DuckDB.

## Current Data Contracts (Important)

- Use `synthetic.syn_beneficiary` and `synthetic.syn_beneficiary_prescriptions` for beneficiary logic.
- Do not use `synthetic.beneficiary_profiles` in new pipeline code.
- Use drug name fields from `synthetic.syn_beneficiary_prescriptions`:
  - `drug_name`, `drug_synonym`, `drug_tty`, `drug_name_source`
- `bronze.brz_pricing` is required for UNIT_COST-based cost estimation.
- Validate these tables from `data/meta_db.csv` before SQL edits.

## Cost Objective Policy (Current)

`ml.training_plan_pairs` uses a calibrated annual cost objective before ranking:

- Base objective:
  - `ranking_cost_objective = (plan_premium * 12) + estimated_annual_oop + distance_penalty`
- OOP logic:
  - SPUF preferred-retail cost rules from `bronze.brz_beneficiary_cost`
  - deductible applicability from `DED_APPLIES_YN` / `DEDUCTIBLE_APPLIES`
  - insulin handling from `bronze.brz_insulin_cost`
  - uncovered/excluded burden from formulary + excluded tables
- Pricing calibration policy (when `bronze.brz_pricing` exists):
  - winsorize `UNIT_COST` by `days_supply_code` quantiles
  - bound pricing annual estimate to historical synthetic annual estimate ratio
  - blend bounded pricing estimate with historical estimate

Current calibration constants in `db/ml/05_training_pairs.py`:
- `PRICING_WINSOR_LOW_Q = 0.01`
- `PRICING_WINSOR_HIGH_Q = 0.95`
- `PRICING_TO_HIST_RATIO_MIN = 0.50`
- `PRICING_TO_HIST_RATIO_MAX = 12.00`
- `PRICING_HIST_BLEND_WEIGHT = 0.35`
- `PRICING_ANNUAL_ABS_MAX = 25000.0`

## Pipeline Execution

Run in this order unless the task needs only a subset:

```bash
python scripts/migrate_to_duckdb.py
python db/bronze/06_ingest_insulin_ref.py
python db/bronze/05_ingest_geography.py
python db/gold/03_dim_zipcode.py
python db/gold/05_agg_formulary.py
python db/gold/07_agg_networks.py
python db/gold/06_agg_cost.py
python db/gold/08_affordability_index_pca.py
python scripts/generate_beneficiary_profiles.py
python db/ml/02_assign_geography.py
python db/ml/03_calculate_distance.py
python db/ml/05_training_pairs.py
python db/ml/06_recommendation_explainer.py
```

Use `python db/utils/validate_schema.py` after major pipeline changes.

Minimum rebuild after pricing/cost changes:

```bash
python scripts/migrate_to_duckdb.py
python db/ml/05_training_pairs.py
python db/ml/06_recommendation_explainer.py
```

## Model and App Workflow

- Train model: `python ml_model/train_model_from_db.py`
- Run app: `streamlit run app/streamlit_app_interactive.py`
- If model file is missing, regenerate `models/plan_ranker.pkl` before app debugging.

## Schema-First Debugging

Before changing SQL:

1. Confirm table and columns in `data/meta_db.csv`.
2. Confirm key type/case (`PLAN_KEY` vs `plan_key`, string vs numeric county code).
3. Confirm schema names (`bronze`, `gold`, `ml`, `synthetic`).

Use the quick checks in `references/sql_checks.md`.

## High-Risk Mismatch Checks

- Missing table usually means pipeline layer not run yet.
- Wrong join key case (`PLAN_KEY` vs `plan_key`) silently drops rows.
- `contract_type` is usually derived logic, not a raw bronze column.
- Distance features depend on both `gold.agg_plan_network_metrics` and geography.
- If `estimated_annual_oop` tail explodes (very high p99), inspect pricing calibration and `UNIT_COST` quantiles before retraining.

## Output Expectations

When completing tasks in this repo:

- Report exact files changed.
- Report exact command(s) run.
- Report validation outcome (row counts, schema checks, or app/model smoke test).
