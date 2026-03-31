# Improvement Version Summary

Date: March 28, 2026

## Purpose

This document summarizes the trust-first rebuild of the Medicare Part D recommendation system.
The goal of this version is to make the application more meaningful for beneficiary decision support
and more valuable for research by improving:

- recommendation trustworthiness
- reproducibility of data preparation and model evaluation
- consistency between offline training and online inference
- transparency of ranking, warnings, and evidence quality
- usability through a clearer dual-track Streamlit experience

This version keeps the existing stack:

- DuckDB
- LightGBM
- Streamlit

It adds a dual-track application:

- Beneficiary Decision Support
- Research / Evaluation

## What Changed

### Product-level changes

- The app now uses a staged recommendation pipeline instead of a single-pass rank-and-display flow.
- Service-area eligibility is enforced before recommendation scoring.
- Requested-drug coverage is treated as a first-class outcome.
- Weak-evidence cases are downgraded to comparison-and-warning mode.
- Research mode now compares the trained model against a transparent heuristic baseline.

### Data and ML changes

- ZIP assignment is now deterministic and population-weighted.
- Distance features are now deterministic for a given snapshot and seed.
- Suppressed plans are removed from service-area candidates.
- Candidate plans are deduplicated before scoring.
- Each recommendation run now produces a stable audit record.

### Main implementation files

- `app/streamlit_app_interactive.py`
- `recommendation_engine/plan_data.py`
- `recommendation_engine/decision_support.py`
- `recommendation_engine/research_eval.py`
- `utils/deterministic.py`
- `scripts/generate_beneficiary_profiles.py`
- `db/ml/02_assign_geography.py`
- `db/ml/03_calculate_distance.py`
- `db/run_full_pipeline.py`
- `db/db_manager.py`

## End-to-End Processing Flow

### Offline data and model pipeline

```text
Bronze CMS/reference tables
    ->
Gold plan/formulary/network aggregates
    ->
Synthetic beneficiary generation
    ->
Deterministic county repair and county assignment weights
    ->
Deterministic population-weighted ZIP assignment
    ->
Deterministic plan-county distance feature generation
    ->
Beneficiary x plan training pair generation
    ->
LightGBM LambdaRank training
    ->
Saved model artifact + training statistics
```

### Online beneficiary decision-support pipeline

```text
Profile input
    ->
Medication list input
    ->
Preference weights input
    ->
Service-area plan retrieval
    ->
Suppressed-plan filter + candidate deduplication
    ->
Requested-drug coverage enrichment
    ->
Eligibility gate
    ->
OOP and access estimation
    ->
ML scoring + decision score
    ->
Confidence / warning / evidence-gap generation
    ->
Recommendation report + audit record
```

### Research and evaluation pipeline

```text
Deterministic sample from ml.training_plan_pairs
    ->
ML score
    ->
Transparent baseline score
    ->
Top-1 and top-3 evaluation
    ->
Cost regret, coverage, access, stability, and subgroup analysis
    ->
Downloadable summary and detail outputs
```

## Application UX Flow

The Streamlit application is now organized into four beneficiary-facing steps:

1. Profile
2. Medications
3. Preferences
4. Results

The results page is built for decision support rather than metric dumping. It includes:

- one featured recommendation card
- side-by-side comparison for the top plans
- warnings and evidence gaps
- trust signals for service area, medication fit, and access evidence
- expandable detail sections:
  - why this plan
  - what to verify
  - what changes if assumptions change
- downloadable recommendation CSV and audit JSON

Research mode is separate from beneficiary mode and exposes:

- deterministic cohort sampling
- model vs baseline comparison
- data coverage diagnostics
- recommendation stability analysis
- subgroup fairness slices
- downloadable research outputs

## Data Transformation Logic

### 1. County repair and beneficiary generation

The synthetic beneficiary process now explicitly repairs geography before downstream ML preparation.

Key logic:

- `bronze.brz_geographic.state_label` is treated as the preferred state source.
- Missing state labels are repaired from `bronze.brz_zipcode.state`.
- Counties with no valid ZIP support are removed before assignment weighting.
- Silent `state='NA'` fallthrough behavior was removed.
- Downstream geography exclusions are auditable instead of silent.

Main function:

- `build_county_assignment_weights()` in `scripts/generate_beneficiary_profiles.py`

### 2. Deterministic population-weighted ZIP assignment

ZIP assignment is no longer pseudo-random without population use.

Current logic:

- candidate ZIP rows are grouped by county
- ZIP population is converted into normalized weights
- a stable hash-based draw is created from `county_code | bene_synth_id | seed`
- the draw is applied against the county ZIP cumulative distribution
- the selected ZIP, state, density, and coordinates are written back to the beneficiary table
- exclusions are written to `ml.beneficiary_geography_audit`

Main functions:

- `stable_fraction()` in `utils/deterministic.py`
- `assign_population_weighted_zips()` in `db/ml/02_assign_geography.py`

Outputs:

- updated `synthetic.syn_beneficiary`
- `ml.beneficiary_geography_audit`

### 3. Deterministic distance feature generation

Distance features are now reproducible for a fixed seed and data snapshot.

Current logic:

- plan-county rows are built from active, non-suppressed plans
- county density and plan network metrics are joined
- a low/high distance range is assigned based on:
  - county density
  - preferred pharmacy count
  - contract type defaults when network coverage is sparse
- a stable uniform draw is produced with:
  - `plan_key`
  - `county_code`
  - `seed`
- the result is converted into:
  - `simulated_distance_miles`
  - `distance_category`

Distance categories:

- `very_close`
- `nearby`
- `moderate`
- `far`

Main functions:

- `stable_uniform()` in `utils/deterministic.py`
- `compute_deterministic_distance_features()` in `db/ml/03_calculate_distance.py`

Output table:

- `ml.plan_distance_features`

### 4. Service-area plan retrieval and candidate cleaning

The app now retrieves plans in a stricter way.

Current logic:

- plans are retrieved by exact `state` and `county_code`
- suppressed plans are excluded
- missing-premium plans are excluded
- plan rows are enriched with formulary and network aggregates
- candidate rows are deduplicated with preference for:
  - service-area eligible rows
  - local rows
  - non-comparison rows
  - higher requested-drug coverage
  - lower distance
  - lower premium

Main functions:

- `fetch_plans_for_service_area()` in `recommendation_engine/plan_data.py`
- `deduplicate_plan_candidates()` in `recommendation_engine/plan_data.py`

Important behavior:

- nearby county plans can be shown only as comparison-only rows
- they are not treated as eligible recommendations for the selected county

### 5. Requested-drug coverage enrichment

Requested-drug coverage is computed at the plan level using the requested NDC list.

Current logic:

- the beneficiary’s requested NDCs are cross-joined against candidate plans
- formulary membership is checked through `bronze.brz_basic_formulary`
- excluded-drug logic is applied through `bronze.brz_excluded_drugs`
- outputs include:
  - `requested_drugs`
  - `covered_drugs`
  - `in_formulary_drugs`
  - `excluded_drugs`
  - `uncovered_ndcs`
  - `drug_coverage_pct`

Coverage buckets:

- `Good fit`: coverage >= 90%
- `Partial fit`: coverage >= 50% and < 90%
- `Poor fit`: coverage < 50%
- `Not evaluated`: no medication list supplied

Main logic:

- `fetch_plan_drug_coverage()` in `recommendation_engine/plan_data.py`
- `classify_coverage_status()` in `recommendation_engine/decision_support.py`

### 6. Training pair generation

The training table remains centered on beneficiary-plan pairs, but this version emphasizes consistency with app-side decision support.

Training-pair logic includes:

- geographic service-area match between beneficiary and plan
- suppressed-plan exclusion
- formulary metrics join
- network metrics join
- deterministic distance feature join
- annual OOP approximation by requested prescription rows
- premium + OOP + distance objective construction

Cost logic components in `ml.training_plan_pairs`:

- annual premium
- estimated annual out-of-pocket drug spending
- deductible component
- insulin override proxy
- uncovered-drug burden
- distance penalty

Target objective:

- `ranking_cost_objective = total_annual_cost + distance_penalty`

Main file:

- `db/ml/05_training_pairs.py`

## Recommendation and Scoring Logic

### Stage 1. Eligibility gate

The recommendation pipeline first determines whether a plan is eligible for the selected service area.

Outputs:

- `service_area_eligible`
- `comparison_only`
- `eligibility_status`

### Stage 2. Coverage gate

If a medication list is provided, the plan is evaluated for requested-drug coverage before recommendation ranking.

Behavior:

- when local plans meet the minimum requested-drug threshold, only those are recommended
- when no local plan meets the threshold, the app falls back to comparison-and-warning mode instead of silently pretending high confidence

### Stage 3. App-side OOP estimation

The app estimates annual OOP with an exposed component breakdown.

Main drivers:

- beneficiary medication burden
- generic share
- specialty share
- prior authorization / step therapy / quantity limit rates
- deductible
- insulin-user adjustment
- uncovered requested-drug burden
- network adequacy penalty

Returned breakdown fields:

- `covered_component`
- `uncovered_component`
- `deductible_component`
- `network_component`

Main function:

- `estimate_plan_oop_with_breakdown()` in `recommendation_engine/decision_support.py`

### Stage 4. Access estimation

If a ZIP code is available, pharmacy-level distance metrics are computed live from `bronze.brz_pharmacy_network`.
If not, or if pharmacy evidence is missing, the app falls back to safer defaults and records the evidence gap.

Access fields used in scoring:

- `nearest_preferred_miles`
- `preferred_within_10mi`
- `network_accessibility_score`
- `distance_penalty`

### Stage 5. ML score

The online scoring step recreates the training feature contract and sends it into the saved LightGBM model.

Feature families:

- plan features
- beneficiary features
- formulary features
- network features
- distance features
- cost features
- interaction features

### Stage 6. Decision score

The beneficiary-facing ranking uses a transparent blended decision score rather than the raw ML score alone.

Decision score combines normalized components:

- ML suitability
- total cost
- access
- coverage

User-controlled weights are applied through:

- `build_decision_weights()`
- `compute_decision_support_scores()`

### Stage 7. Confidence and warning logic

Each recommendation is accompanied by:

- `warning_flags`
- `evidence_gaps`
- `confidence_band`

Confidence bands:

- `High`
- `Medium`
- `Low`
- `Exploratory`

Confidence is reduced when:

- the plan is outside the selected service area
- requested-drug coverage is weak
- formulary metrics are missing
- network metrics are missing
- pharmacy-distance evidence is missing

### Stable recommendation output schema

The app and research paths now share a stable recommendation contract:

- `eligibility_status`
- `coverage_status`
- `coverage_pct_requested`
- `estimated_total_annual_cost`
- `cost_breakdown`
- `access_summary`
- `warning_flags`
- `decision_score`
- `ml_score`
- `heuristic_score`
- `confidence_band`
- `evidence_gaps`

Main functions:

- `build_recommendation_schema()` in `recommendation_engine/decision_support.py`
- `create_run_audit()` in `recommendation_engine/decision_support.py`

## Model Algorithms

### Primary training algorithm

Model:

- LightGBM ranker

Objective:

- `lambdarank`

Evaluation metrics:

- NDCG@3
- NDCG@5
- NDCG@10

Training split strategy:

- group-based split by `bene_synth_id`
- deterministic row ordering by `bene_synth_id`, `plan_key`
- group contiguity preserved before LightGBM training

Core training parameters:

- learning rate: `0.05`
- num leaves: `31`
- min data in leaf: `20`
- bagging fraction: `0.8`
- bagging frequency: `5`
- feature fraction: `0.8`
- early stopping: `50` rounds
- max boosting rounds: `500`

Main file:

- `ml_model/train_model_from_db.py`

### Target label construction

The model does not directly regress cost.
Instead, within each beneficiary group it learns a ranking preference derived from cost ordering.

Label creation process:

- compute `ranking_cost_objective`
- sort candidate plans within each beneficiary
- convert lower objective cost into higher ranking labels
- train LambdaRank to prefer lower-cost, better-access, better-fit plans within each beneficiary group

Supporting files:

- `ml_model/train_model_from_db.py`
- `ml_model/ranking_utils.py`

### Online ranking algorithm

At inference time the app:

1. rebuilds the trained feature vector for each candidate plan
2. gets the LightGBM prediction as `ml_score`
3. computes a transparent `decision_score`
4. orders plans by:
   - `decision_score`
   - `ml_score`

### Transparent baseline algorithm

Research mode compares the ML model against a heuristic baseline.

Baseline formula:

- 55% normalized total cost
- 20% normalized access
- 25% normalized coverage

Main function:

- `compute_heuristic_score()` in `recommendation_engine/decision_support.py`

## Research Evaluation Logic

Research mode is designed to make results analyzable rather than only viewable.

### Deterministic sample construction

Samples are pulled from `ml.training_plan_pairs` using:

- beneficiary-level filtering
- deterministic ordering with a seed
- optional filters:
  - risk segment
  - insulin user slice
  - state

### Evaluation outputs

Current research outputs include:

- average top-1 cost regret
- median top-1 cost regret
- average top-3 cost regret
- average requested-drug coverage
- average access burden in miles
- best-plan hit rate
- recommendation stability under alternate preferences
- subgroup tables by:
  - insulin use
  - risk segment
  - density bucket
  - top states
  - top counties

Main functions:

- `load_research_sample()`
- `score_research_sample()`
- `evaluate_model_vs_baseline()`
- `compute_preference_stability()`
- `build_fairness_tables()`
- `build_data_coverage_diagnostics()`

## Reproducibility and Auditability

This version explicitly improves reproducibility.

Reproducibility controls:

- fixed default seed: `42`
- deterministic geography assignment
- deterministic distance feature generation
- deterministic training row ordering
- deterministic research sampling

Audit artifacts:

- `run_id`
- model version fingerprint
- data snapshot fingerprint
- seed
- structured user input summary
- feature coverage summary
- top-k recommendation outputs

## Validation Added

The improvement version also adds regression coverage for the new trust and reproducibility logic.

New tests cover:

- coverage bucket logic
- candidate-plan deduplication
- heuristic score behavior
- recommendation schema and audit fields
- county repair logic
- deterministic ZIP assignment
- deterministic distance feature generation
- service-area table discovery
- suppressed-plan exclusion in app candidates
- suppressed-plan exclusion in training pairs

Relevant test files:

- `tests/test_decision_support_logic.py`
- `tests/test_deterministic_pipeline_logic.py`
- `tests/test_service_area_integrity.py`

## Practical Meaning of This Version

For users:

- recommendations are less likely to show invalid or misleading plans
- medication fit is visible instead of hidden
- weak-evidence cases are clearly labeled
- results are easier to compare and verify

For research:

- core synthetic geography and distance generation are reproducible
- evaluation is no longer ML-only; it includes a transparent baseline
- subgroup and stability analysis are available directly in the app
- recommendation outputs are structured enough for export and audit

## Recommended Next Documentation Follow-Up

To keep the repo consistent, the next documentation update should align:

- `README.md`
- `ML_STREAMLIT_WORKFLOW.md`
- `PIPELINE_EXECUTION.md`

with the new dual-track app flow and trust-first recommendation contract.
