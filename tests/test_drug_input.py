import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.drug_input import (
    build_requested_ndcs,
    normalize_medication_rows,
    normalize_ndc_token,
    parse_ndc_text,
    summarize_medication_rows,
)


def test_normalize_ndc_token_variants():
    values = normalize_ndc_token("00002-8715-01")
    assert "00002871501" in values
    assert "00002871501" == values[-1]


def test_parse_ndc_text_dedupes_and_normalizes():
    parsed = parse_ndc_text("00002871501, 00002-8715-01\n68992301001")
    assert parsed[0] == "00002871501"
    assert parsed.count("00002871501") == 1
    assert "68992301001" in parsed


def test_build_requested_ndcs_merges_text_and_selected():
    ndcs = build_requested_ndcs(
        "00002871501",
        selected_ndcs=["68992301001", "00002-8715-01"],
    )
    assert "00002871501" in ndcs
    assert "68992301001" in ndcs
    assert len(ndcs) == 2


def test_normalize_medication_rows_coerces_and_filters_invalid():
    rows = normalize_medication_rows(
        [
            {
                "drug_name": "Drug A",
                "ndc": "00002-8715-01",
                "fills_per_year": "9",
                "days_supply_mode": "90",
                "tier_level": "3",
                "is_insulin": "1",
                "annual_cost_est": "1200.50",
            },
            {"drug_name": "Invalid", "ndc": "", "fills_per_year": 5},
        ]
    )
    assert len(rows) == 1
    assert rows[0]["ndc"] == "00002871501"
    assert rows[0]["days_supply_mode"] == 90
    assert rows[0]["tier_level"] == 3
    assert rows[0]["is_insulin"] is True
    assert rows[0]["annual_cost_est"] == 1200.5


def test_summarize_medication_rows_dedupes_ndc_and_aggregates():
    summary = summarize_medication_rows(
        [
            {
                "drug_name": "Drug A",
                "ndc": "00002871501",
                "fills_per_year": 10,
                "days_supply_mode": 30,
                "tier_level": 2,
                "is_insulin": 0,
                "annual_cost_est": 400,
            },
            {
                "drug_name": "Drug A alt",
                "ndc": "00002-8715-01",
                "fills_per_year": 12,
                "days_supply_mode": 90,
                "tier_level": 3,
                "is_insulin": 1,
                "annual_cost_est": 500,
            },
            {
                "drug_name": "Drug B",
                "ndc": "68992301001",
                "fills_per_year": 6,
                "days_supply_mode": 60,
                "tier_level": 4,
                "is_insulin": 0,
                "annual_cost_est": 800,
            },
        ]
    )
    assert summary["num_drugs"] == 2
    assert summary["requested_ndcs"] == ("00002871501", "68992301001")
    assert summary["is_insulin_user"] == 1
    assert round(summary["avg_fills_per_year"], 2) == 9.0
    assert summary["total_annual_drug_cost"] == 1300.0
