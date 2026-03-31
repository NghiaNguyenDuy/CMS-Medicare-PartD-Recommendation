import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recommendation_engine.decision_support import (
    build_recommendation_schema,
    classify_coverage_status,
    compute_heuristic_score,
    create_run_audit,
)
from recommendation_engine.plan_data import deduplicate_plan_candidates


def test_classify_coverage_status_buckets():
    assert classify_coverage_status(0.0, requested_drugs=0) == "Not evaluated"
    assert classify_coverage_status(95.0, requested_drugs=4) == "Good fit"
    assert classify_coverage_status(60.0, requested_drugs=4) == "Partial fit"
    assert classify_coverage_status(25.0, requested_drugs=4) == "Poor fit"


def test_deduplicate_plan_candidates_prefers_eligible_local_and_higher_coverage():
    plans_df = pd.DataFrame(
        [
            {
                "PLAN_KEY": "PLAN_A",
                "premium": 50.0,
                "service_area_eligible": False,
                "is_local": False,
                "comparison_only": True,
                "drug_coverage_pct": 100.0,
                "distance_miles": 5.0,
            },
            {
                "PLAN_KEY": "PLAN_A",
                "premium": 45.0,
                "service_area_eligible": True,
                "is_local": True,
                "comparison_only": False,
                "drug_coverage_pct": 80.0,
                "distance_miles": 0.0,
            },
            {
                "PLAN_KEY": "PLAN_A",
                "premium": 40.0,
                "service_area_eligible": True,
                "is_local": True,
                "comparison_only": False,
                "drug_coverage_pct": 95.0,
                "distance_miles": 1.0,
            },
        ]
    )

    deduped = deduplicate_plan_candidates(plans_df)

    assert len(deduped) == 1
    assert bool(deduped.loc[0, "service_area_eligible"]) is True
    assert bool(deduped.loc[0, "is_local"]) is True
    assert float(deduped.loc[0, "drug_coverage_pct"]) == 95.0


def test_compute_heuristic_score_rewards_lower_cost_and_better_coverage():
    plans_df = pd.DataFrame(
        [
            {
                "total_cost_with_distance": 900.0,
                "distance_miles": 2.0,
                "drug_coverage_pct": 100.0,
            },
            {
                "total_cost_with_distance": 2200.0,
                "distance_miles": 12.0,
                "drug_coverage_pct": 40.0,
            },
        ]
    )

    scores = compute_heuristic_score(plans_df)

    assert float(scores.iloc[0]) > float(scores.iloc[1])


def test_build_recommendation_schema_and_audit_include_trust_fields():
    ranked_df = pd.DataFrame(
        [
            {
                "PLAN_KEY": "PLAN_1",
                "PLAN_NAME": "Plan One",
                "contract_type": "PDP",
                "premium": 40.0,
                "estimated_annual_oop": 800.0,
                "distance_penalty": 0.0,
                "total_cost_with_distance": 1280.0,
                "score": 0.91,
                "heuristic_score": 88.0,
                "decision_score": 92.5,
                "service_area_eligible": True,
                "comparison_only": False,
                "is_local": True,
                "eligibility_status": "Eligible",
                "drug_coverage_pct": 100.0,
                "has_formulary_metrics": 1,
                "has_network_metrics": 1,
                "has_pharmacy_distance_data": 1,
                "distance_miles": 0.0,
                "nearest_preferred_miles": 1.5,
                "preferred_within_10mi": 12,
                "network_accessibility_score": 90.0,
                "location_county": "Autauga",
                "oop_breakdown": {
                    "covered_component": 650.0,
                    "uncovered_component": 0.0,
                    "deductible_component": 100.0,
                    "network_component": 50.0,
                },
                "network_adequacy_flag": 0,
                "deductible": 300.0,
                "formulary_restrictiveness": 1,
                "decision_reason": "strong total cost",
            },
            {
                "PLAN_KEY": "PLAN_2",
                "PLAN_NAME": "Plan Two",
                "contract_type": "PDP",
                "premium": 60.0,
                "estimated_annual_oop": 1300.0,
                "distance_penalty": 200.0,
                "total_cost_with_distance": 2220.0,
                "score": 0.52,
                "heuristic_score": 40.0,
                "decision_score": 44.0,
                "service_area_eligible": False,
                "comparison_only": True,
                "is_local": False,
                "eligibility_status": "Comparison only - outside selected service area",
                "drug_coverage_pct": 40.0,
                "has_formulary_metrics": 0,
                "has_network_metrics": 0,
                "has_pharmacy_distance_data": 0,
                "distance_miles": 22.0,
                "nearest_preferred_miles": 22.0,
                "preferred_within_10mi": 0,
                "network_accessibility_score": 20.0,
                "location_county": "Elmore",
                "oop_breakdown": {
                    "covered_component": 500.0,
                    "uncovered_component": 600.0,
                    "deductible_component": 100.0,
                    "network_component": 100.0,
                },
                "network_adequacy_flag": 1,
                "deductible": 500.0,
                "formulary_restrictiveness": 3,
                "decision_reason": "comparison only",
            },
        ]
    )

    recommendations = build_recommendation_schema(ranked_df, run_id="run123", requested_drugs=3)
    audit = create_run_audit(
        user_input_summary={"Profile": {"state": "AL"}},
        model_version="model-v1",
        data_snapshot="snapshot-v1",
        seed=42,
        feature_coverage={"candidate_plans": 2},
        recommendations=recommendations,
        run_id="run123",
    )

    expected_cols = {
        "run_id",
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
    }

    assert expected_cols.issubset(set(recommendations.columns))
    assert recommendations.loc[0, "coverage_status"] == "Good fit"
    assert recommendations.loc[1, "confidence_band"] == "Exploratory"
    assert isinstance(recommendations.loc[0, "warning_flags"], list)
    assert isinstance(recommendations.loc[0, "cost_breakdown"], dict)
    assert audit["run_id"] == "run123"
    assert audit["seed"] == 42
    assert len(audit["top_k_outputs"]) == 2
