"""Decision-support helpers shared by the app and tests."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import uuid

import numpy as np
import pandas as pd


DEFAULT_RESEARCH_SEED = 42
RECOMMENDATION_SCHEMA_COLUMNS = [
    "run_id",
    "PLAN_KEY",
    "PLAN_NAME",
    "contract_type",
    "eligibility_status",
    "coverage_status",
    "coverage_pct_requested",
    "estimated_total_annual_cost",
    "cost_breakdown",
    "access_summary",
    "warning_flags",
    "decision_score",
    "ml_score",
    "heuristic_score",
    "confidence_band",
    "evidence_gaps",
]


@dataclass(frozen=True)
class ProfileInput:
    """Structured beneficiary profile input."""

    state: str
    county: str
    state_code: str
    county_code: str
    zip_code: str | None
    risk_segment: str
    num_drugs: int
    avg_fills_per_year: float
    is_insulin_user: int
    total_rx_cost_est: float


@dataclass(frozen=True)
class MedicationListItem:
    """Structured medication input used for coverage and cost estimation."""

    drug_name: str
    ndc: str
    fills_per_year: float
    days_supply_mode: int
    tier_level: int
    is_insulin: bool
    annual_cost_est: float


@dataclass(frozen=True)
class PreferenceWeights:
    """Structured decision-weight input."""

    ml_weight: float
    cost_weight: float
    access_weight: float
    coverage_weight: float
    distance_penalty_rate: float
    minimum_coverage_pct: float
    local_only: bool


def normalize_series(series, higher_is_better: bool = True) -> pd.Series:
    """Normalize a numeric series to [0, 1]."""
    s = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)

    if s.isna().all():
        norm = pd.Series(0.5, index=series.index)
    else:
        min_val = s.min()
        max_val = s.max()
        if pd.isna(min_val) or pd.isna(max_val) or np.isclose(min_val, max_val):
            norm = pd.Series(0.5, index=series.index)
        else:
            norm = (s - min_val) / (max_val - min_val)
        fill_value = float(norm.median()) if not pd.isna(norm.median()) else 0.5
        norm = norm.fillna(fill_value)

    return norm if higher_is_better else (1.0 - norm)


def compute_heuristic_score(plans_df: pd.DataFrame) -> pd.Series:
    """Transparent baseline score used for comparisons and research mode."""
    ranked = plans_df.copy()
    cost_component = normalize_series(ranked["total_cost_with_distance"], higher_is_better=False)
    distance_source = ranked["nearest_preferred_miles"] if "nearest_preferred_miles" in ranked.columns else ranked["distance_miles"]
    access_component = normalize_series(distance_source, higher_is_better=False)

    if "drug_coverage_pct" in ranked.columns:
        coverage_component = normalize_series(ranked["drug_coverage_pct"], higher_is_better=True)
    else:
        coverage_component = normalize_series(ranked["formulary_generic_pct"], higher_is_better=True)

    return 100.0 * (
        (0.55 * cost_component)
        + (0.20 * access_component)
        + (0.25 * coverage_component)
    )


def classify_coverage_status(coverage_pct_requested: float, requested_drugs: int = 0) -> str:
    """Bucket requested-drug coverage into user-facing fit labels."""
    if int(requested_drugs or 0) <= 0:
        return "Not evaluated"
    pct = float(coverage_pct_requested or 0.0)
    if pct >= 90.0:
        return "Good fit"
    if pct >= 50.0:
        return "Partial fit"
    return "Poor fit"


def estimate_plan_oop_with_breakdown(
    beneficiary_profile: dict,
    plan,
    medication_rows=None,
):
    """
    Estimate annual out-of-pocket cost and expose a stable breakdown.

    This is an app-side approximation aligned to the training objective:
    annual premium + OOP + distance burden, with uncovered requested drugs
    treated as a strong penalty.
    """
    medication_rows = medication_rows or []
    plan_data = plan.to_dict() if hasattr(plan, "to_dict") else dict(plan)

    base_rx_cost = float(beneficiary_profile.get("total_rx_cost_est", 0.0) or 0.0)
    medication_cost_map = {
        str(row.get("ndc", "")): float(row.get("annual_cost_est", 0.0) or 0.0)
        for row in medication_rows
        if str(row.get("ndc", "")).strip()
    }
    if medication_cost_map:
        base_rx_cost = max(base_rx_cost, float(sum(medication_cost_map.values())))

    num_drugs = max(int(beneficiary_profile.get("num_drugs", 1) or 1), 1)
    fills_per_year = max(float(beneficiary_profile.get("avg_fills_per_year", 12.0) or 12.0), 1.0)
    is_insulin_user = bool(int(beneficiary_profile.get("is_insulin_user", 0) or 0))

    generic_pct = float(plan_data.get("formulary_generic_pct", 0) or 0)
    specialty_pct = float(plan_data.get("formulary_specialty_pct", 0) or 0)
    pa_rate = float(plan_data.get("formulary_pa_rate", 0) or 0)
    st_rate = float(plan_data.get("formulary_st_rate", 0) or 0)
    ql_rate = float(plan_data.get("formulary_ql_rate", 0) or 0)
    deductible = float(plan_data.get("deductible", 0) or 0)
    network_adequacy_flag = int(plan_data.get("network_adequacy_flag", 0) or 0)

    generic_ratio = np.clip(generic_pct / 100.0 if generic_pct > 1 else generic_pct, 0.0, 1.0)
    specialty_ratio = np.clip(specialty_pct / 100.0 if specialty_pct > 1 else specialty_pct, 0.0, 1.0)
    pa_ratio = np.clip(pa_rate / 100.0 if pa_rate > 1 else pa_rate, 0.0, 1.0)
    st_ratio = np.clip(st_rate / 100.0 if st_rate > 1 else st_rate, 0.0, 1.0)
    ql_ratio = np.clip(ql_rate / 100.0 if ql_rate > 1 else ql_rate, 0.0, 1.0)

    uncovered_ndcs = {
        token.strip()
        for token in str(plan_data.get("uncovered_ndcs", "") or "").split(",")
        if token.strip()
    }
    uncovered_requested_cost = float(
        sum(cost for ndc, cost in medication_cost_map.items() if ndc in uncovered_ndcs)
    )
    requested_drugs = int(plan_data.get("requested_drugs", len(medication_cost_map)) or 0)
    uncovered_drugs = int(plan_data.get("requested_drugs", 0) or 0) - int(plan_data.get("covered_drugs", 0) or 0)

    if uncovered_requested_cost <= 0 and requested_drugs > 0 and uncovered_drugs > 0:
        uncovered_requested_cost = base_rx_cost * (float(uncovered_drugs) / float(requested_drugs))

    covered_cost_base = max(base_rx_cost - uncovered_requested_cost, 0.0)
    drug_intensity = 0.20 + min(0.25, 0.01 * (num_drugs - 1)) + min(0.15, 0.002 * max(fills_per_year - 12, 0))
    generic_discount = 1.0 - (0.22 * generic_ratio)
    specialty_penalty = 1.0 + (0.25 * specialty_ratio)
    restriction_penalty = 1.0 + (0.18 * pa_ratio) + (0.12 * st_ratio) + (0.08 * ql_ratio)
    insulin_adjustment = 0.92 if is_insulin_user else 1.0

    covered_component = (
        covered_cost_base
        * drug_intensity
        * generic_discount
        * specialty_penalty
        * restriction_penalty
        * insulin_adjustment
    )
    uncovered_component = uncovered_requested_cost
    deductible_component = min(deductible, 800.0) * 0.35
    network_component = covered_component * (0.08 if network_adequacy_flag == 1 else 0.0)

    estimated_oop = covered_component + uncovered_component + deductible_component + network_component
    estimated_oop = float(max(50.0, estimated_oop))

    return estimated_oop, {
        "covered_component": round(float(covered_component), 2),
        "uncovered_component": round(float(uncovered_component), 2),
        "deductible_component": round(float(deductible_component), 2),
        "network_component": round(float(network_component), 2),
    }


def build_warning_flags(plan_row, requested_drugs: int = 0):
    """Create user-facing warning labels for a recommendation row."""
    warnings = []
    if not bool(plan_row.get("service_area_eligible", True)):
        warnings.append("Outside the selected service area")
    if classify_coverage_status(plan_row.get("drug_coverage_pct", 0.0), requested_drugs) == "Poor fit":
        warnings.append("Low requested-drug coverage")
    elif classify_coverage_status(plan_row.get("drug_coverage_pct", 0.0), requested_drugs) == "Partial fit":
        warnings.append("Only part of the requested medication list is covered")
    if bool(plan_row.get("network_adequacy_flag", 0)):
        warnings.append("Limited preferred pharmacy network")
    nearest_pref = float(plan_row.get("nearest_preferred_miles", plan_row.get("distance_miles", 0.0)) or 0.0)
    if nearest_pref > 10:
        warnings.append("Nearest preferred pharmacy is over 10 miles away")
    if float(plan_row.get("deductible", 0.0) or 0.0) >= 400.0:
        warnings.append("Higher deductible than many alternatives")
    if int(plan_row.get("formulary_restrictiveness", 0) or 0) >= 2:
        warnings.append("Restrictive formulary may require extra approvals")
    return warnings


def build_evidence_gaps(plan_row, requested_drugs: int = 0):
    """List evidence gaps so the app can lower confidence visibly."""
    gaps = []
    if not bool(plan_row.get("has_formulary_metrics", 0)):
        gaps.append("No plan-level formulary aggregate available")
    if not bool(plan_row.get("has_network_metrics", 0)):
        gaps.append("No plan-level pharmacy network aggregate available")
    if not bool(plan_row.get("has_pharmacy_distance_data", 0)):
        gaps.append("No pharmacy-level distance evidence for this run")
    if int(requested_drugs or 0) <= 0:
        gaps.append("No requested medication list supplied")
    if not bool(plan_row.get("service_area_eligible", True)):
        gaps.append("Shown for comparison only outside the selected service area")
    return gaps


def classify_confidence_band(plan_row, requested_drugs: int = 0) -> str:
    """Translate fit + evidence quality into a confidence label."""
    if not bool(plan_row.get("service_area_eligible", True)):
        return "Exploratory"

    evidence_gaps = build_evidence_gaps(plan_row, requested_drugs=requested_drugs)
    coverage_status = classify_coverage_status(
        plan_row.get("drug_coverage_pct", 0.0),
        requested_drugs=requested_drugs,
    )
    if coverage_status == "Poor fit" or len(evidence_gaps) >= 3:
        return "Low"
    if coverage_status == "Partial fit" or len(evidence_gaps) >= 1:
        return "Medium"
    return "High"


def compute_feature_coverage(plans_df: pd.DataFrame) -> dict:
    """Summarize evidence coverage across a candidate set."""
    total = int(len(plans_df))
    if total == 0:
        return {
            "candidate_plans": 0,
            "eligible_plans": 0,
            "plans_with_network_metrics": 0,
            "plans_with_pharmacy_distance_data": 0,
            "plans_with_formulary_metrics": 0,
        }

    return {
        "candidate_plans": total,
        "eligible_plans": int(pd.Series(plans_df.get("service_area_eligible", False)).fillna(False).astype(bool).sum()),
        "plans_with_network_metrics": int(pd.Series(plans_df.get("has_network_metrics", 0)).fillna(0).astype(int).sum()),
        "plans_with_pharmacy_distance_data": int(pd.Series(plans_df.get("has_pharmacy_distance_data", 0)).fillna(0).astype(int).sum()),
        "plans_with_formulary_metrics": int(pd.Series(plans_df.get("has_formulary_metrics", 0)).fillna(0).astype(int).sum()),
    }


def build_recommendation_schema(
    plans_df: pd.DataFrame,
    run_id: str,
    requested_drugs: int = 0,
) -> pd.DataFrame:
    """Materialize a stable recommendation output contract for the UI and exports."""
    if len(plans_df) == 0:
        return pd.DataFrame(columns=RECOMMENDATION_SCHEMA_COLUMNS)

    records = plans_df.copy()
    records["run_id"] = run_id
    records["eligibility_status"] = records.get("eligibility_status", "Eligible")
    records["coverage_pct_requested"] = pd.to_numeric(records.get("drug_coverage_pct", 0.0), errors="coerce").fillna(0.0)
    records["coverage_status"] = records["coverage_pct_requested"].apply(
        lambda pct: classify_coverage_status(pct, requested_drugs=requested_drugs)
    )
    records["estimated_total_annual_cost"] = pd.to_numeric(records.get("total_cost_with_distance", 0.0), errors="coerce").fillna(0.0)
    records["ml_score"] = pd.to_numeric(records.get("score", 0.0), errors="coerce").fillna(0.0)
    records["heuristic_score"] = pd.to_numeric(records.get("heuristic_score", 0.0), errors="coerce").fillna(0.0)

    cost_breakdowns = []
    access_summaries = []
    warning_flags = []
    evidence_gaps = []
    confidence_bands = []
    for _, row in records.iterrows():
        cost_breakdowns.append(
            {
                "annual_premium": round(float(row.get("premium", 0.0) or 0.0) * 12.0, 2),
                "estimated_oop": round(float(row.get("estimated_annual_oop", 0.0) or 0.0), 2),
                "distance_penalty": round(float(row.get("distance_penalty", 0.0) or 0.0), 2),
                "estimated_total_annual_cost": round(float(row.get("total_cost_with_distance", 0.0) or 0.0), 2),
                "oop_components": row.get("oop_breakdown", {}),
            }
        )
        access_summaries.append(
            {
                "local_plan": bool(row.get("is_local", False)),
                "comparison_only": bool(row.get("comparison_only", False)),
                "distance_miles": round(float(row.get("distance_miles", 0.0) or 0.0), 2),
                "nearest_preferred_miles": round(float(row.get("nearest_preferred_miles", row.get("distance_miles", 0.0)) or 0.0), 2),
                "preferred_within_10mi": int(float(row.get("preferred_within_10mi", 0) or 0)),
                "network_accessibility_score": round(float(row.get("network_accessibility_score", 0.0) or 0.0), 1),
                "location_county": str(row.get("location_county", "")),
            }
        )
        warnings = build_warning_flags(row, requested_drugs=requested_drugs)
        warning_flags.append(warnings)
        gaps = build_evidence_gaps(row, requested_drugs=requested_drugs)
        evidence_gaps.append(gaps)
        confidence_bands.append(classify_confidence_band(row, requested_drugs=requested_drugs))

    records["cost_breakdown"] = cost_breakdowns
    records["access_summary"] = access_summaries
    records["warning_flags"] = warning_flags
    records["evidence_gaps"] = evidence_gaps
    records["confidence_band"] = confidence_bands

    missing_cols = [col for col in RECOMMENDATION_SCHEMA_COLUMNS if col not in records.columns]
    for col in missing_cols:
        records[col] = None
    return records[RECOMMENDATION_SCHEMA_COLUMNS + [col for col in records.columns if col not in RECOMMENDATION_SCHEMA_COLUMNS]]


def serialize_nested_columns(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    """Convert nested dict/list columns to JSON for CSV export."""
    columns = columns or ["cost_breakdown", "access_summary", "warning_flags", "evidence_gaps"]
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = out[col].apply(
                lambda value: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
            )
    return out


def create_run_audit(
    user_input_summary: dict,
    model_version: str,
    data_snapshot: str,
    seed: int,
    feature_coverage: dict,
    recommendations: pd.DataFrame,
    run_id: str | None = None,
) -> dict:
    """Create a stable audit record for one recommendation run."""
    run_id = run_id or uuid.uuid4().hex[:12]
    top_k = []
    if len(recommendations) > 0:
        export_cols = [
            "PLAN_KEY",
            "PLAN_NAME",
            "eligibility_status",
            "coverage_status",
            "coverage_pct_requested",
            "estimated_total_annual_cost",
            "decision_score",
            "ml_score",
            "confidence_band",
        ]
        top_k = serialize_nested_columns(recommendations[export_cols].head(5)).to_dict("records")

    return {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_version": model_version,
        "data_snapshot": data_snapshot,
        "seed": int(seed),
        "user_input_summary": user_input_summary,
        "feature_coverage": feature_coverage,
        "top_k_outputs": top_k,
    }


def as_public_types(profile: ProfileInput, medications, preferences: PreferenceWeights) -> dict:
    """Bundle structured public interfaces for debugging and audit use."""
    return {
        "Profile": asdict(profile),
        "MedicationList": [asdict(item) for item in medications],
        "PreferenceWeights": asdict(preferences),
    }
