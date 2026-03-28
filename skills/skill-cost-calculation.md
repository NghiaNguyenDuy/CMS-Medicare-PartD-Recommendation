# Skill: Medicare Part D Cost Calculation

## Purpose

This skill describes how the project computes Medicare Part D plan recommendations from CMS SPUF 2025 Q3 data using DuckDB, plus the assumptions that make the output understandable and auditable for counselor workflows.

Use this skill when you need to:

- explain how annual beneficiary OOP is estimated
- trace a ranking result back to SPUF source fields
- extend the recommendation logic or Streamlit UI
- validate whether a new cost rule belongs in `silver` or `gold`

## System Context

The implementation lives in:

- `src/medicare_partd/extract.py`
- `src/medicare_partd/pipeline.py`
- `src/medicare_partd/recommend.py`
- `streamlit_app.py`

The old pandas prototype at `scripts/load_cms_raw.py` is reference-only. Production flow uses the DuckDB pipeline.

## Runtime Flow

The pipeline runs in four stages:

1. `extract`
   - Unzip the local CMS Q3 2025 archives into `data/staging/2025-Q3/raw/`.
   - Preserve the original raw text shape.
2. `bronze`
   - Load raw CMS, RXCUI, ZIP, insulin, and PDE files into DuckDB with source metadata.
3. `silver`
   - Normalize plan keys, ZIP and county mappings, service areas, drug reference mappings, PDE-based utilization defaults, formulary coverage facts, pharmacy facts, and cost-rule tables.
4. `gold`
   - Materialize serving tables for ZIP-to-plan filtering, plan drug cost basis, network summaries, plan summaries, and recommendation features.

## Cost Calculation Model

The recommendation engine is rules-first and transparent.

### Inputs

- `BeneficiaryInput`
  - `zipcode`
  - `age_band`
  - `lis_status`
  - `chronic_condition_flags`
  - `pharmacy_preference`
  - `top_n`
- `MedicationInput`
  - `drug_name` or `rxcui` or `ndc`
  - `tier_family`
  - `day_supply`
  - optional `quantity_override`
  - optional `fills_per_year_override`

### Drug Resolution

- Drug names resolve through `silver.dim_drug_reference`.
- If quantity or annual fills are missing, defaults come from `gold.drug_input_defaults`, which is derived from PDE sample data.
- If `tier_family` is not supplied, the engine infers it from `gold.plan_drug_cost_basis`.

### Per-Fill Price Approximation

For each candidate plan and drug:

1. Look up `UNIT_COST` from pricing.
2. Choose dispensing fee by:
   - channel: preferred retail, nonpreferred retail, preferred mail, nonpreferred mail
   - fee family: generic or brand
3. Compute negotiated price approximation:

`negotiated_price = max(unit_cost * quantity + dispensing_fee, floor_price)`

4. Apply the channel-specific SPUF beneficiary cost rule.
5. Apply deductible logic when `DED_APPLIES_YN` indicates the tier is deductible-applicable.
6. Apply insulin override from insulin beneficiary cost rules when applicable.
7. Cap annual beneficiary OOP at `$2,000`.
8. For `lis_status='full'`, cap per-fill OOP at:
   - generic: `$4.90`
   - brand/specialty: `$12.15`

### Annual Cost

For each plan:

- `annual_drug_oop = sum(per_fill_oop across all simulated fills)`
- `annual_premium = monthly_premium * 12`
- `annual_total_cost = annual_premium + annual_drug_oop`

### Ranking

Plans are sorted by:

1. full coverage before partial coverage
2. lower `annual_total_cost`
3. fewer uncovered drugs
4. lower network risk

### Explanations

The engine produces short explanation strings from factual flags:

- uncovered drugs
- insulin channel risk
- preferred retail network limitation
- estimated preferred pharmacy distance
- mail-order dependency
- prior authorization
- step therapy
- quantity limit
- projected deductible exposure

## Data Model Guidance

Put data in `silver` when it is normalized and reusable across multiple downstream consumers.

Examples:

- `silver.dim_plan`
- `silver.dim_zipcode`
- `silver.dim_drug_reference`
- `silver.fact_plan_drug_coverage`
- `silver.fact_plan_pharmacy`
- `silver.plan_beneficiary_cost_rules`

Put data in `gold` when it is counselor-facing or recommendation-serving.

Examples:

- `gold.plan_service_area`
- `gold.plan_channel_summary`
- `gold.plan_network_summary`
- `gold.plan_drug_cost_basis`
- `gold.plan_summary`
- `gold.drug_input_defaults`
- `gold.recommendation_features`

## Key Assumptions

- CMS source snapshot is local SPUF 2025 Q3 only.
- CMS text files are loaded as Latin-1.
- SPUF `UNIT_COST` is a plan-level average, not a pharmacy-specific posted price.
- PDE sample data is used only to infer default quantity and fill frequency.
- Brand vs generic dispensing fee selection is driven by `tier_family`.
- Distance is ZIP-centroid based, not route-time based.
- Partial LIS is not modeled.
- Suppressed plans are excluded from recommendation because downstream SPUF files are absent.

## Extension Rules

When modifying the logic:

- keep raw CMS column names intact in `bronze`
- prefer new normalized dimensions or facts in `silver` before adding serving shortcuts in `gold`
- keep recommendation explanations traceable to explicit flags or metrics
- document every new assumption in the README and example docs
- do not wire new logic into `scripts/load_cms_raw.py`

## Related Notes

- `skills/cost-calculation-examples.md`
- `skills/cost-calculation-logics.md`
- `README.md`
