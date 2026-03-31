import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.db_manager import get_db
from recommendation_engine.plan_data import fetch_plans_for_service_area


DB_PATH = Path(__file__).resolve().parents[1] / "data" / "medicare_part_d.duckdb"


@pytest.fixture(scope="module")
def db():
    if not DB_PATH.exists():
        pytest.skip("DuckDB snapshot not available for integration checks.")
    return get_db(read_only=True)


def test_db_manager_detects_schema_tables(db):
    assert db.has_table("bronze.brz_plan_info") is True
    assert "bronze.brz_plan_info" in db.list_tables()


def test_fetch_plans_for_service_area_excludes_suppressed_rows(db):
    sample = db.query_one(
        """
        SELECT state, CAST(county_code AS VARCHAR)
        FROM bronze.brz_plan_info
        WHERE plan_suppressed_yn = 'Y'
        LIMIT 1
        """
    )
    if sample is None:
        pytest.skip("No suppressed rows were found in the current snapshot.")

    state_code, county_code = sample
    plans_df = fetch_plans_for_service_area(db, state_code, county_code)
    suppressed_df = db.query_df(
        """
        SELECT DISTINCT plan_key
        FROM bronze.brz_plan_info
        WHERE state = ?
          AND CAST(county_code AS VARCHAR) = ?
          AND plan_suppressed_yn = 'Y'
        """,
        [state_code, county_code],
    )

    suppressed_plan_keys = set(suppressed_df["PLAN_KEY"].astype(str).tolist())
    returned_plan_keys = set(plans_df["PLAN_KEY"].astype(str).tolist())

    assert suppressed_plan_keys
    assert suppressed_plan_keys.isdisjoint(returned_plan_keys)


def test_training_pairs_exclude_suppressed_service_area_rows(db):
    suppressed_pairs = db.query_one(
        """
        SELECT COUNT(*)
        FROM ml.training_plan_pairs tp
        JOIN bronze.brz_plan_info p
          ON tp.plan_key = p.plan_key
         AND tp.state = p.state
         AND tp.county_code = CAST(p.county_code AS VARCHAR)
        WHERE p.plan_suppressed_yn = 'Y'
        """
    )[0]

    assert suppressed_pairs == 0


def test_get_nearby_counties_query_executes_without_parser_error(db):
    sample = db.query_one(
        """
        SELECT zip_code, state
        FROM gold.dim_zipcode
        WHERE zip_code IS NOT NULL
          AND state IS NOT NULL
        LIMIT 1
        """
    )
    if sample is None:
        pytest.skip("No ZIP/state sample was found in gold.dim_zipcode.")

    zip_code, state_name = sample

    from app.streamlit_app_interactive import get_nearby_counties

    nearby_df = get_nearby_counties(str(zip_code), str(state_name), 50)

    assert set(["county_code", "county", "state", "distance_miles"]).issubset(set(nearby_df.columns))
