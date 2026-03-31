import numpy as np
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ml_model.ranking_utils import create_ranking_labels_from_cost, groups_are_contiguous
from ml_model.train_model_from_db import PlanRankingModel


def test_create_ranking_labels_prefers_lower_cost():
    costs = pd.Series([500.0, 1200.0, 1200.0, 2500.0])
    labels = create_ranking_labels_from_cost(costs)

    assert labels[0] > labels[1]
    assert labels[1] == labels[2]
    assert labels[2] > labels[3]
    assert labels.min() >= 0
    assert labels.max() <= 30


def test_prepare_training_data_uses_ranking_cost_and_contiguous_groups():
    df = pd.DataFrame(
        [
            # Intentionally unsorted rows
            {
                "bene_synth_id": "BENE_B",
                "plan_key": "PLAN_2",
                "premium": 30.0,
                "deductible": 200.0,
                "is_ma_pd": 0,
                "is_pdp": 1,
                "num_drugs": 4,
                "is_insulin_user": 0,
                "avg_fills_per_year": 12.0,
                "formulary_generic_pct": 0.7,
                "formulary_specialty_pct": 0.1,
                "formulary_pa_rate": 0.2,
                "formulary_st_rate": 0.1,
                "formulary_ql_rate": 0.1,
                "formulary_restrictiveness": 1,
                "network_preferred_pharmacies": 20,
                "network_total_pharmacies": 40,
                "network_adequacy_flag": 0,
                "distance_miles": 6.0,
                "has_distance_tradeoff": 0,
                "total_drug_oop": 1200.0,
                "total_annual_cost": 1500.0,
                "ranking_cost_objective": 1500.0,
            },
            {
                "bene_synth_id": "BENE_A",
                "plan_key": "PLAN_2",
                "premium": 45.0,
                "deductible": 250.0,
                "is_ma_pd": 1,
                "is_pdp": 0,
                "num_drugs": 5,
                "is_insulin_user": 1,
                "avg_fills_per_year": 14.0,
                "formulary_generic_pct": 0.6,
                "formulary_specialty_pct": 0.2,
                "formulary_pa_rate": 0.3,
                "formulary_st_rate": 0.2,
                "formulary_ql_rate": 0.1,
                "formulary_restrictiveness": 2,
                "network_preferred_pharmacies": 10,
                "network_total_pharmacies": 30,
                "network_adequacy_flag": 1,
                "distance_miles": 12.0,
                "has_distance_tradeoff": 1,
                "total_drug_oop": 900.0,
                # Deliberately lower annual cost, but higher objective
                "total_annual_cost": 900.0,
                "ranking_cost_objective": 1400.0,
            },
            {
                "bene_synth_id": "BENE_A",
                "plan_key": "PLAN_1",
                "premium": 40.0,
                "deductible": 250.0,
                "is_ma_pd": 1,
                "is_pdp": 0,
                "num_drugs": 5,
                "is_insulin_user": 1,
                "avg_fills_per_year": 14.0,
                "formulary_generic_pct": 0.6,
                "formulary_specialty_pct": 0.2,
                "formulary_pa_rate": 0.3,
                "formulary_st_rate": 0.2,
                "formulary_ql_rate": 0.1,
                "formulary_restrictiveness": 2,
                "network_preferred_pharmacies": 12,
                "network_total_pharmacies": 32,
                "network_adequacy_flag": 0,
                "distance_miles": 4.0,
                "has_distance_tradeoff": 0,
                "total_drug_oop": 1000.0,
                # Deliberately higher annual cost, but lower objective
                "total_annual_cost": 2000.0,
                "ranking_cost_objective": 700.0,
            },
            {
                "bene_synth_id": "BENE_B",
                "plan_key": "PLAN_1",
                "premium": 35.0,
                "deductible": 200.0,
                "is_ma_pd": 0,
                "is_pdp": 1,
                "num_drugs": 4,
                "is_insulin_user": 0,
                "avg_fills_per_year": 12.0,
                "formulary_generic_pct": 0.7,
                "formulary_specialty_pct": 0.1,
                "formulary_pa_rate": 0.2,
                "formulary_st_rate": 0.1,
                "formulary_ql_rate": 0.1,
                "formulary_restrictiveness": 1,
                "network_preferred_pharmacies": 18,
                "network_total_pharmacies": 38,
                "network_adequacy_flag": 0,
                "distance_miles": 8.0,
                "has_distance_tradeoff": 0,
                "total_drug_oop": 1000.0,
                "total_annual_cost": 1700.0,
                "ranking_cost_objective": 1700.0,
            },
        ]
    )

    model = PlanRankingModel()
    _, y, groups, ordered_df = model.prepare_training_data(df)

    assert groups.tolist() == [2, 2]
    assert groups_are_contiguous(ordered_df["bene_synth_id"].to_numpy())
    assert ordered_df["bene_synth_id"].tolist() == ["BENE_A", "BENE_A", "BENE_B", "BENE_B"]
    assert ordered_df["plan_key"].tolist() == ["PLAN_1", "PLAN_2", "PLAN_1", "PLAN_2"]

    # For BENE_A, PLAN_1 has lower ranking_cost_objective than PLAN_2.
    bene_a_labels = y[:2]
    assert bene_a_labels[0] > bene_a_labels[1]

    # Each beneficiary group has exactly two plans -> labels should be {0, 1}.
    assert set(np.unique(y[:2])) == {0, 1}
    assert set(np.unique(y[2:])) == {0, 1}
