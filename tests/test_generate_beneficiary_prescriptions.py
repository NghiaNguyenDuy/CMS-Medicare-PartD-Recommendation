import numpy as np
import pandas as pd
import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "generate_beneficiary_profiles.py"
SPEC = importlib.util.spec_from_file_location("generate_beneficiary_profiles", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

align_beneficiary_summary = MODULE.align_beneficiary_summary
allocate_fills_across_drugs = MODULE.allocate_fills_across_drugs
generate_prescriptions_from_beneficiaries = MODULE.generate_prescriptions_from_beneficiaries
build_rxcui_name_maps = MODULE.build_rxcui_name_maps


def test_allocate_fills_across_drugs_respects_target_and_minimum():
    rng = np.random.default_rng(7)
    fills = allocate_fills_across_drugs(fills_target=12, drug_count=4, rng=rng)
    assert fills.sum() == 12
    assert len(fills) == 4
    assert (fills >= 1).all()


def test_generate_prescriptions_enforces_insulin_when_available():
    bene_df = pd.DataFrame(
        {
            "bene_synth_id": ["B1", "B2"],
            "unique_drugs": [2, 3],
            "fills_target": [6, 9],
            "insulin_user_flag": [1, 0],
            "risk_segment": ["MED", "LOW"],
            "total_rx_cost_est": [2500.0, 1200.0],
        }
    )

    drug_pool = pd.DataFrame(
        {
            "ndc": ["11111111111", "22222222222", "33333333333", "44444444444"],
            "rxcui": ["111", "222", "333", "444"],
            "tier_level": [2, 3, 1, 4],
            "has_prior_auth": [0, 1, 0, 1],
            "has_step_therapy": [0, 0, 0, 1],
            "has_quantity_limit": [0, 0, 1, 1],
            "formulary_coverage": [10, 8, 7, 6],
            "is_insulin": [1, 0, 0, 0],
        }
    )

    rxcui_ref = pd.DataFrame(
        {
            "rxcui": ["111", "222"],
            "drug_name": ["Insulin Lispro", "Metformin"],
            "drug_synonym": ["Humalog", ""],
            "tty": ["SCD", "SCD"],
        }
    )
    name_map, synonym_map, tty_map = build_rxcui_name_maps(rxcui_ref)

    rx_df = generate_prescriptions_from_beneficiaries(
        bene_df=bene_df,
        drug_pool=drug_pool,
        seed=11,
        rxcui_name_map=name_map,
        rxcui_synonym_map=synonym_map,
        rxcui_tty_map=tty_map,
    )
    assert not rx_df.empty
    assert set(rx_df["bene_synth_id"].unique()) == {"B1", "B2"}

    insulin_rows_b1 = rx_df[(rx_df["bene_synth_id"] == "B1") & (rx_df["is_insulin"] == 1)]
    assert len(insulin_rows_b1) >= 1
    assert "drug_name" in rx_df.columns
    assert "drug_name_source" in rx_df.columns

    # Drug rows are from CMS pool only.
    assert set(rx_df["ndc"].unique()).issubset(set(drug_pool["ndc"].unique()))


def test_align_beneficiary_summary_matches_prescription_aggregates():
    bene_df = pd.DataFrame(
        {
            "bene_synth_id": ["B1", "B2"],
            "state": ["CA", "TX"],
            "county_code": ["06037", "48201"],
            "unique_drugs": [1, 1],
            "fills_target": [1, 1],
            "total_rx_cost_est": [1.0, 1.0],
            "insulin_user_flag": [0, 0],
            "risk_segment": ["LOW", "LOW"],
        }
    )

    rx_df = pd.DataFrame(
        {
            "bene_synth_id": ["B1", "B1", "B2"],
            "ndc": ["11111111111", "22222222222", "33333333333"],
            "fills_per_year": [3, 5, 4],
            "estimated_annual_drug_cost": [1500.0, 2000.0, 7000.0],
            "is_insulin": [0, 1, 1],
        }
    )

    aligned = align_beneficiary_summary(bene_df, rx_df)
    assert len(aligned) == 2

    b1 = aligned[aligned["bene_synth_id"] == "B1"].iloc[0]
    assert b1["unique_drugs"] == 2
    assert b1["fills_target"] == 8
    assert float(b1["total_rx_cost_est"]) == 3500.0
    assert b1["insulin_user_flag"] == 1
    assert b1["risk_segment"] == "MED"

    b2 = aligned[aligned["bene_synth_id"] == "B2"].iloc[0]
    assert b2["risk_segment"] == "HIGH"
