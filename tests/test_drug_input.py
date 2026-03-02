import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from utils.drug_input import build_requested_ndcs, normalize_ndc_token, parse_ndc_text


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
