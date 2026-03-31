import importlib.util
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.generate_beneficiary_profiles import build_county_assignment_weights


ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name, relative_path):
    spec = importlib.util.spec_from_file_location(module_name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


assign_geography = load_module("assign_geography_module", "db/ml/02_assign_geography.py")
distance_proxy = load_module("distance_proxy_module", "db/ml/03_calculate_distance.py")


def test_build_county_assignment_weights_repairs_missing_state_and_filters_unmatched_counties():
    geo_df = pd.DataFrame(
        [
            {"county_code": "1001", "state_label": None},
            {"county_code": "01003", "state_label": "AL"},
            {"county_code": "99999", "state_label": None},
        ]
    )
    zip_df = pd.DataFrame(
        [
            {"county_code": "01001", "state": "AL"},
            {"county_code": "01003", "state": "AL"},
        ]
    )

    weights = build_county_assignment_weights(geo_df, zip_df)

    assert set(weights["county_code"]) == {"01001", "01003"}
    assert set(weights["state"]) == {"AL"}


def test_assign_population_weighted_zips_is_deterministic_and_logs_exclusions():
    bene_df = pd.DataFrame(
        [
            {"bene_synth_id": "BENE_1", "county_code": "01001"},
            {"bene_synth_id": "BENE_2", "county_code": "01001"},
            {"bene_synth_id": "BENE_3", "county_code": "01001"},
            {"bene_synth_id": "BENE_4", "county_code": "99999"},
        ]
    )
    zip_candidates_df = pd.DataFrame(
        [
            {
                "county_code": "01001",
                "state": "AL",
                "zip_code": "36003",
                "population": 1000,
                "lat": 32.53,
                "lng": -86.64,
                "density": 90,
            },
            {
                "county_code": "01001",
                "state": "AL",
                "zip_code": "36066",
                "population": 200,
                "lat": 32.48,
                "lng": -86.47,
                "density": 140,
            },
        ]
    )

    assignments_a, audit_a = assign_geography.assign_population_weighted_zips(bene_df, zip_candidates_df, seed=17)
    assignments_b, audit_b = assign_geography.assign_population_weighted_zips(bene_df, zip_candidates_df, seed=17)

    pd.testing.assert_frame_equal(
        assignments_a.sort_values("bene_synth_id").reset_index(drop=True),
        assignments_b.sort_values("bene_synth_id").reset_index(drop=True),
    )
    pd.testing.assert_frame_equal(
        audit_a.sort_values("bene_synth_id").reset_index(drop=True),
        audit_b.sort_values("bene_synth_id").reset_index(drop=True),
    )

    excluded = audit_a[audit_a["assignment_status"] == "excluded"].reset_index(drop=True)
    assert len(excluded) == 1
    assert excluded.loc[0, "bene_synth_id"] == "BENE_4"
    assert excluded.loc[0, "exclusion_reason"] == "No ZIP candidates available for county"


def test_compute_deterministic_distance_features_repeats_for_same_seed():
    plan_counties_df = pd.DataFrame(
        [
            {"plan_key": "PLAN_1", "contract_type": "PDP", "state": "AL", "county_code": "01001"},
            {"plan_key": "PLAN_2", "contract_type": "MA", "state": "AL", "county_code": "01003"},
        ]
    )
    county_density_df = pd.DataFrame(
        [
            {"county_code": "01001", "avg_density": 1200.0, "zip_count": 4},
            {"county_code": "01003", "avg_density": 80.0, "zip_count": 2},
        ]
    )
    network_metrics_df = pd.DataFrame(
        [
            {"plan_key": "PLAN_1", "preferred_pharmacies": 30, "total_pharmacies": 45, "pref_pharmacy_pct": 66.7, "in_area_pharmacies": 25},
            {"plan_key": "PLAN_2", "preferred_pharmacies": 6, "total_pharmacies": 18, "pref_pharmacy_pct": 33.3, "in_area_pharmacies": 10},
        ]
    )

    distance_a = distance_proxy.compute_deterministic_distance_features(
        plan_counties_df,
        county_density_df,
        network_metrics_df,
        seed=11,
    )
    distance_b = distance_proxy.compute_deterministic_distance_features(
        plan_counties_df,
        county_density_df,
        network_metrics_df,
        seed=11,
    )

    pd.testing.assert_frame_equal(
        distance_a.sort_values(["plan_key", "county_code"]).reset_index(drop=True),
        distance_b.sort_values(["plan_key", "county_code"]).reset_index(drop=True),
    )
    assert set(distance_a["distance_category"]) <= {"very_close", "nearby", "moderate", "far"}
