"""Database-backed helpers for candidate plan retrieval and coverage checks."""

from __future__ import annotations

import pandas as pd
import numpy as np


SERVICE_AREA_PLAN_QUERY = """
    SELECT
        p.PLAN_KEY,
        p.PLAN_NAME,
        p.CONTRACT_NAME,
        p.FORMULARY_ID,
        CASE
            WHEN p.IS_MA_PD THEN 'MA'
            WHEN p.IS_PDP THEN 'PDP'
            ELSE 'Other'
        END AS contract_type,
        CAST(p.PREMIUM AS DOUBLE) AS premium,
        CAST(p.DEDUCTIBLE AS DOUBLE) AS deductible,
        CAST(COALESCE(fm.total_drugs, 0) AS BIGINT) AS formulary_total_drugs,
        CAST(COALESCE(fm.generic_tier_pct, 0) AS DOUBLE) AS formulary_generic_pct,
        CAST(COALESCE(fm.specialty_tier_pct, 0) AS DOUBLE) AS formulary_specialty_pct,
        CAST(COALESCE(fm.pa_rate, 0) AS DOUBLE) AS formulary_pa_rate,
        CAST(COALESCE(fm.st_rate, 0) AS DOUBLE) AS formulary_st_rate,
        CAST(COALESCE(fm.ql_rate, 0) AS DOUBLE) AS formulary_ql_rate,
        CAST(COALESCE(fm.restrictiveness_class, 0) AS INTEGER) AS formulary_restrictiveness,
        CAST(COALESCE(nm.preferred_pharmacies, 0) AS INTEGER) AS network_preferred_pharmacies,
        CAST(COALESCE(nm.total_pharmacies, 0) AS INTEGER) AS network_total_pharmacies,
        CAST(COALESCE(nm.network_adequacy_flag, 0) AS INTEGER) AS network_adequacy_flag,
        CAST(CASE WHEN fm.plan_key IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) AS has_formulary_metrics,
        CAST(CASE WHEN nm.plan_key IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) AS has_network_metrics,
        CAST(0.0 AS DOUBLE) AS distance_miles,
        CAST(0 AS INTEGER) AS has_distance_tradeoff
    FROM bronze.brz_plan_info p
    LEFT JOIN gold.agg_plan_formulary_metrics fm
      ON p.PLAN_KEY = fm.PLAN_KEY
    LEFT JOIN gold.agg_plan_network_metrics nm
      ON p.PLAN_KEY = nm.plan_key
    WHERE p.STATE = ?
      AND p.COUNTY_CODE = ?
      AND p.PREMIUM IS NOT NULL
      AND (p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y')
"""


def fetch_plans_for_service_area(db, state_code: str, county_code: str) -> pd.DataFrame:
    """Return eligible in-area plans for the selected service area."""
    plans_df = db.query_df(SERVICE_AREA_PLAN_QUERY, [state_code, county_code])
    if len(plans_df) == 0:
        return plans_df

    plans_df = plans_df.copy()
    plans_df["is_local"] = True
    plans_df["service_area_eligible"] = True
    plans_df["comparison_only"] = False
    plans_df["eligibility_status"] = "Eligible"
    plans_df["distance_category"] = "Local"
    return deduplicate_plan_candidates(plans_df)


def deduplicate_plan_candidates(plans_df: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per plan, preferring eligible/local/nearer rows."""
    if len(plans_df) == 0 or "PLAN_KEY" not in plans_df.columns:
        return plans_df

    ranked = plans_df.copy()
    if "service_area_eligible" not in ranked.columns:
        ranked["service_area_eligible"] = True
    if "is_local" not in ranked.columns:
        ranked["is_local"] = False
    if "comparison_only" not in ranked.columns:
        ranked["comparison_only"] = False
    if "distance_miles" not in ranked.columns:
        ranked["distance_miles"] = 9999.0
    if "premium" not in ranked.columns:
        ranked["premium"] = np.inf
    if "drug_coverage_pct" not in ranked.columns:
        ranked["drug_coverage_pct"] = 0.0

    ranked["service_area_eligible"] = ranked["service_area_eligible"].astype(bool)
    ranked["is_local"] = ranked["is_local"].astype(bool)
    ranked["comparison_only"] = ranked["comparison_only"].astype(bool)
    ranked["distance_miles"] = pd.to_numeric(ranked["distance_miles"], errors="coerce").fillna(9999.0)
    ranked["premium"] = pd.to_numeric(ranked["premium"], errors="coerce").fillna(np.inf)
    ranked["drug_coverage_pct"] = pd.to_numeric(ranked["drug_coverage_pct"], errors="coerce").fillna(0.0)

    ranked = ranked.sort_values(
        [
            "service_area_eligible",
            "is_local",
            "comparison_only",
            "drug_coverage_pct",
            "distance_miles",
            "premium",
            "PLAN_KEY",
        ],
        ascending=[False, False, True, False, True, True, True],
    )
    ranked = ranked.drop_duplicates(subset=["PLAN_KEY"], keep="first")
    return ranked.reset_index(drop=True)


def fetch_plan_drug_coverage(db, plan_keys, requested_ndcs) -> pd.DataFrame:
    """Compute requested-drug coverage for each candidate plan."""
    plan_keys = tuple(str(pk) for pk in (plan_keys or ()))
    requested_ndcs = tuple(str(ndc) for ndc in (requested_ndcs or ()))

    if len(plan_keys) == 0 or len(requested_ndcs) == 0:
        return pd.DataFrame(
            columns=[
                "PLAN_KEY",
                "requested_drugs",
                "covered_drugs",
                "in_formulary_drugs",
                "excluded_drugs",
                "uncovered_ndcs",
                "drug_coverage_pct",
            ]
        )

    plan_placeholders = ", ".join(["?"] * len(plan_keys))
    requested_union = " UNION ALL ".join(["SELECT ? AS ndc"] * len(requested_ndcs))

    sql = f"""
        WITH candidate_plans AS (
            SELECT DISTINCT
                PLAN_KEY,
                FORMULARY_ID
            FROM bronze.brz_plan_info
            WHERE PLAN_KEY IN ({plan_placeholders})
        ),
        requested AS (
            {requested_union}
        ),
        grid AS (
            SELECT
                cp.PLAN_KEY,
                cp.FORMULARY_ID,
                r.ndc
            FROM candidate_plans cp
            CROSS JOIN (SELECT DISTINCT ndc FROM requested) r
        ),
        coverage_raw AS (
            SELECT
                g.PLAN_KEY,
                g.FORMULARY_ID,
                g.ndc,
                bf.RXCUI,
                bf.NDC AS covered_ndc
            FROM grid g
            LEFT JOIN bronze.brz_basic_formulary bf
              ON bf.FORMULARY_ID = g.FORMULARY_ID
             AND bf.NDC = g.ndc
        ),
        coverage_eval AS (
            SELECT
                c.PLAN_KEY,
                c.ndc,
                CASE
                    WHEN c.covered_ndc IS NULL THEN 0
                    WHEN ex.RXCUI IS NOT NULL THEN 0
                    ELSE 1
                END AS is_covered,
                CASE WHEN c.covered_ndc IS NOT NULL THEN 1 ELSE 0 END AS in_formulary,
                CASE WHEN ex.RXCUI IS NOT NULL THEN 1 ELSE 0 END AS is_excluded
            FROM coverage_raw c
            LEFT JOIN bronze.brz_excluded_drugs ex
              ON ex.FORMULARY_ID = c.FORMULARY_ID
             AND ex.RXCUI = c.RXCUI
        )
        SELECT
            PLAN_KEY,
            COUNT(*) AS requested_drugs,
            SUM(is_covered) AS covered_drugs,
            SUM(in_formulary) AS in_formulary_drugs,
            SUM(is_excluded) AS excluded_drugs,
            STRING_AGG(
                CASE WHEN is_covered = 0 THEN ndc ELSE NULL END,
                ', ' ORDER BY ndc
            ) AS uncovered_ndcs
        FROM coverage_eval
        GROUP BY PLAN_KEY
    """

    params = list(plan_keys) + list(requested_ndcs)
    coverage_df = db.query_df(sql, params)

    if len(coverage_df) == 0:
        return coverage_df

    coverage_df["requested_drugs"] = pd.to_numeric(coverage_df["requested_drugs"], errors="coerce").fillna(0)
    coverage_df["covered_drugs"] = pd.to_numeric(coverage_df["covered_drugs"], errors="coerce").fillna(0)
    coverage_df["drug_coverage_pct"] = np.where(
        coverage_df["requested_drugs"] > 0,
        100.0 * coverage_df["covered_drugs"] / coverage_df["requested_drugs"],
        0.0,
    )
    return coverage_df
