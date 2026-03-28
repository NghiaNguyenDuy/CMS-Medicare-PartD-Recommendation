---
name: cms-medicare-part-d-duckdb
description: Work on this Medicare Part D recommendation codebase that uses DuckDB medallion layers, synthetic beneficiary generation, LightGBM ranking, and a Streamlit app. Use when tasks involve the bronze/gold/ml/synthetic schemas, pipeline execution, DuckDB SQL validation, model training in ml_model/train_model_from_db.py, or real-time recommendation logic in app/streamlit_app_interactive.py.
---

# CMS Medicare Part D DuckDB Skill

## Start Here

1. Read `README.md` for architecture and pipeline order.
2. Treat the live DuckDB schema as the source of truth before writing SQL.
3. Load only task-relevant files. Do not preload entire `db/`.

## Working Defaults

- Prefer running from repo root.
- Canonical database file: `data/medicare_part_d.duckdb`
- Shared DB entrypoint: `db/db_manager.py`
- Prefer `PLAN_KEY` for plan joins unless a table explicitly exposes `plan_key`.
- Derive contract type from `bronze.brz_plan_info` flags:
  - `IS_MA_PD -> 'MA'`
  - `IS_PDP -> 'PDP'`
- App and inference flows should be read-only against DuckDB.

## Current Data Contracts (Important)

- Use `synthetic.syn_beneficiary` and `synthetic.syn_beneficiary_prescriptions` for beneficiary logic.
- Do not use `synthetic.beneficiary_profiles` in new pipeline code.
- Use drug name fields from `synthetic.syn_beneficiary_prescriptions`:
  - `drug_name`, `drug_synonym`, `drug_tty`, `drug_name_source`
- `bronze.brz_pricing` is required for UNIT_COST-based cost estimation.
- Validate table/column names from DuckDB with `DESCRIBE`, `information_schema`, or the checks in `references/sql_checks.md`.

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
python scripts/migrate_to_duckdb.py --force
python db/bronze/06_ingest_insulin_ref.py
python db/bronze/05_ingest_geography.py
python scripts/generate_beneficiary_profiles.py
python -m db.run_full_pipeline --layers gold ml
```

Use `python db/utils/validate_schema.py` after major pipeline changes.

Minimum rebuild after pricing/cost changes:

```bash
python scripts/migrate_to_duckdb.py --force
python scripts/generate_beneficiary_profiles.py
python -m db.run_full_pipeline --layers gold ml
```

## Model and App Workflow

- Train model: `python ml_model/train_model_from_db.py`
- Run app: `streamlit run app/streamlit_app_interactive.py`
- If model file is missing, regenerate `models/plan_ranker.pkl` before app debugging.
- App inference entrypoint: `app/streamlit_app_interactive.py`
- Primary click path for recommendations:
  1. sidebar input + medication normalization via `utils/drug_input.py`
  2. resolve county/state codes from `gold.dim_zipcode`
  3. load candidate plans from `bronze.brz_plan_info` + gold metrics
  4. optional nearby-county fallback if no local plans
  5. optional requested-drug coverage check from formulary + excluded tables
  6. pharmacy distance and access metrics from `bronze.brz_pharmacy_network`
  7. `rank_plans(...)` computes OOP, distance penalty, model features, and final decision score

## Schema-First Debugging

Before changing SQL:

1. Confirm the table exists in DuckDB.
2. Confirm key type/case (`PLAN_KEY` vs `plan_key`, string vs numeric county code).
3. Confirm schema names (`bronze`, `gold`, `ml`, `synthetic`).
4. Confirm whether the app uses the field at training time, inference time, or both.

Use the quick checks in `references/sql_checks.md`.

## High-Risk Mismatch Checks

- Missing table usually means pipeline layer not run yet.
- Wrong join key case (`PLAN_KEY` vs `plan_key`) silently drops rows.
- `contract_type` is usually derived logic, not a raw bronze column.
- Distance features depend on both `gold.agg_plan_network_metrics` and geography.
- App ranking now uses pharmacy-level distance when ZIP is available, not only the precomputed ML distance proxy.
- Requested-drug coverage in the app depends on `bronze.brz_basic_formulary` plus `bronze.brz_excluded_drugs`.
- If `estimated_annual_oop` tail explodes (very high p99), inspect pricing calibration and `UNIT_COST` quantiles before retraining.

## Output Expectations

When completing tasks in this repo:

- Report exact files changed.
- Report exact command(s) run.
- Report validation outcome (row counts, schema checks, or app/model smoke test).
