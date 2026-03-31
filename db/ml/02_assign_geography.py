"""
ML Prep: Beneficiary Zip Code Assignment

Assign synthetic beneficiaries to realistic zip codes within their counties
using deterministic population-weighted sampling.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
from utils.deterministic import stable_fraction


DEFAULT_ASSIGNMENT_SEED = 42


def normalize_county_code(value) -> str:
    """Normalize county codes to zero-padded 5-character strings."""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text == "":
        return ""
    try:
        return str(int(float(text))).zfill(5)
    except (TypeError, ValueError):
        return text.zfill(5) if text.isdigit() else text


def assign_population_weighted_zips(
    bene_df: pd.DataFrame,
    zip_candidates_df: pd.DataFrame,
    seed: int = DEFAULT_ASSIGNMENT_SEED,
):
    """
    Deterministically assign beneficiaries to ZIPs using county-level populations.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            assignments and audit rows
    """
    bene = bene_df.copy()
    bene["county_code"] = bene["county_code"].map(normalize_county_code)
    bene["bene_synth_id"] = bene["bene_synth_id"].astype(str)

    candidates = zip_candidates_df.copy()
    candidates["county_code"] = candidates["county_code"].map(normalize_county_code)
    candidates["zip_code"] = candidates["zip_code"].astype(str).str.strip()
    candidates["population"] = pd.to_numeric(candidates["population"], errors="coerce").fillna(0.0).clip(lower=0.0)
    candidates["density"] = pd.to_numeric(candidates["density"], errors="coerce")
    candidates["lat"] = pd.to_numeric(candidates["lat"], errors="coerce")
    candidates["lng"] = pd.to_numeric(candidates["lng"], errors="coerce")
    candidates["state"] = candidates["state"].astype(str).str.strip().str.upper()
    candidates = candidates[
        (candidates["county_code"] != "")
        & (candidates["zip_code"] != "")
        & candidates["state"].ne("")
    ].copy()
    candidates = candidates.sort_values(
        ["county_code", "population", "zip_code"],
        ascending=[True, False, True],
    ).reset_index(drop=True)

    assignments = []
    audit_rows = []

    county_groups = {county: group.reset_index(drop=True) for county, group in candidates.groupby("county_code", sort=False)}

    for row in bene.itertuples(index=False):
        county_code = str(row.county_code)
        candidate_group = county_groups.get(county_code)
        if candidate_group is None or len(candidate_group) == 0:
            audit_rows.append(
                {
                    "bene_synth_id": str(row.bene_synth_id),
                    "county_code": county_code,
                    "assignment_status": "excluded",
                    "exclusion_reason": "No ZIP candidates available for county",
                    "assigned_zip": None,
                    "assigned_state": None,
                }
            )
            continue

        weights = candidate_group["population"].to_numpy(dtype=float)
        if float(weights.sum()) <= 0.0:
            weights = np.ones(len(candidate_group), dtype=float)
        weights = weights / weights.sum()
        cdf = weights.cumsum()
        draw = stable_fraction(f"{county_code}|{row.bene_synth_id}", seed=seed)
        chosen_idx = int(np.searchsorted(cdf, draw, side="right"))
        chosen_idx = min(chosen_idx, len(candidate_group) - 1)
        chosen = candidate_group.iloc[chosen_idx]

        assignments.append(
            {
                "bene_synth_id": str(row.bene_synth_id),
                "county_code": county_code,
                "state": str(chosen["state"]),
                "zip_code": str(chosen["zip_code"]),
                "lat": float(chosen["lat"]) if pd.notna(chosen["lat"]) else None,
                "lng": float(chosen["lng"]) if pd.notna(chosen["lng"]) else None,
                "density": float(chosen["density"]) if pd.notna(chosen["density"]) else None,
            }
        )
        audit_rows.append(
            {
                "bene_synth_id": str(row.bene_synth_id),
                "county_code": county_code,
                "assignment_status": "assigned",
                "exclusion_reason": None,
                "assigned_zip": str(chosen["zip_code"]),
                "assigned_state": str(chosen["state"]),
            }
        )

    return pd.DataFrame(assignments), pd.DataFrame(audit_rows)


def assign_zip_codes(seed: int = DEFAULT_ASSIGNMENT_SEED):
    """
    Assign zip codes to synthetic beneficiaries.

    Updates:
        synthetic.syn_beneficiary - Adds zip_code, lat, lng, density, and repaired state
        ml.beneficiary_geography_audit - Explicit assignment / exclusion audit
    """
    db = get_db()

    print("=" * 60)
    print("ML Prep: Beneficiary Zip Code Assignment")
    print("=" * 60)

    try:
        bene_df = db.query_df(
            """
            SELECT
                CAST(bene_synth_id AS VARCHAR) AS bene_synth_id,
                CAST(county_code AS VARCHAR) AS county_code
            FROM synthetic.syn_beneficiary
            """
        )
        print(f"\n1. Found {len(bene_df):,} synthetic beneficiaries")
    except Exception:
        print("\nERROR: synthetic.syn_beneficiary table not found!")
        print("Please run beneficiary generation first.")
        return False

    zip_candidates_df = db.query_df(
        """
        SELECT
            CAST(county_code AS VARCHAR) AS county_code,
            CAST(state AS VARCHAR) AS state,
            CAST(zip_code AS VARCHAR) AS zip_code,
            lat,
            lng,
            density,
            population
        FROM bronze.brz_zipcode
        WHERE county_code IS NOT NULL
          AND zip_code IS NOT NULL
        """
    )

    print("\n2. Assigning zip codes (deterministic population-weighted)...")
    assignments_df, audit_df = assign_population_weighted_zips(
        bene_df=bene_df,
        zip_candidates_df=zip_candidates_df,
        seed=int(seed),
    )

    db.execute("CREATE SCHEMA IF NOT EXISTS ml;")
    db.execute("DROP TABLE IF EXISTS ml.beneficiary_geography_audit;")

    if len(assignments_df) > 0:
        db.conn.register("zip_assignments_df", assignments_df)
    else:
        db.conn.register(
            "zip_assignments_df",
            pd.DataFrame(columns=["bene_synth_id", "county_code", "state", "zip_code", "lat", "lng", "density"]),
        )

    if len(audit_df) > 0:
        db.conn.register("zip_assignment_audit_df", audit_df)
    else:
        db.conn.register(
            "zip_assignment_audit_df",
            pd.DataFrame(columns=["bene_synth_id", "county_code", "assignment_status", "exclusion_reason", "assigned_zip", "assigned_state"]),
        )

    db.execute(
        """
        ALTER TABLE synthetic.syn_beneficiary
        ADD COLUMN IF NOT EXISTS zip_code VARCHAR;

        ALTER TABLE synthetic.syn_beneficiary
        ADD COLUMN IF NOT EXISTS lat DECIMAL(8,6);

        ALTER TABLE synthetic.syn_beneficiary
        ADD COLUMN IF NOT EXISTS lng DECIMAL(9,6);

        ALTER TABLE synthetic.syn_beneficiary
        ADD COLUMN IF NOT EXISTS density INTEGER;

        UPDATE synthetic.syn_beneficiary b
        SET
            state = a.state,
            zip_code = a.zip_code,
            lat = a.lat,
            lng = a.lng,
            density = CAST(a.density AS INTEGER)
        FROM zip_assignments_df a
        WHERE b.bene_synth_id = a.bene_synth_id;
        """
    )

    db.execute(
        """
        CREATE TABLE ml.beneficiary_geography_audit AS
        SELECT
            CAST(bene_synth_id AS VARCHAR) AS bene_synth_id,
            CAST(county_code AS VARCHAR) AS county_code,
            CAST(assignment_status AS VARCHAR) AS assignment_status,
            CAST(exclusion_reason AS VARCHAR) AS exclusion_reason,
            CAST(assigned_zip AS VARCHAR) AS assigned_zip,
            CAST(assigned_state AS VARCHAR) AS assigned_state,
            CURRENT_TIMESTAMP AS audit_ts
        FROM zip_assignment_audit_df
        """
    )

    print("\n3. Validation...")
    stats = db.query_df(
        """
        SELECT
            COUNT(*) AS total_benes,
            COUNT(zip_code) AS benes_with_zip,
            COUNT(DISTINCT zip_code) AS unique_zips,
            COUNT(DISTINCT county_code) AS unique_counties,
            SUM(CASE WHEN state IS NULL OR TRIM(CAST(state AS VARCHAR)) = '' THEN 1 ELSE 0 END) AS benes_without_state
        FROM synthetic.syn_beneficiary;
        """
    )
    audit_stats = db.query_df(
        """
        SELECT
            SUM(CASE WHEN assignment_status = 'assigned' THEN 1 ELSE 0 END) AS assigned_benes,
            SUM(CASE WHEN assignment_status = 'excluded' THEN 1 ELSE 0 END) AS excluded_benes
        FROM ml.beneficiary_geography_audit;
        """
    )

    print("\n[OK] Zip code assignment complete:")
    print(f"  - Total beneficiaries: {stats['total_benes'][0]:,}")
    print(f"  - Beneficiaries with zip: {stats['benes_with_zip'][0]:,}")
    print(f"  - Unique zip codes: {stats['unique_zips'][0]:,}")
    print(f"  - Unique counties: {stats['unique_counties'][0]:,}")
    print(f"  - Beneficiaries without repaired state: {int(stats['benes_without_state'][0] or 0):,}")
    print(f"  - Assigned via ZIP weighting: {int(audit_stats['assigned_benes'][0] or 0):,}")
    print(f"  - Explicitly excluded with audit reason: {int(audit_stats['excluded_benes'][0] or 0):,}")

    coverage_pct = (stats["benes_with_zip"][0] / stats["total_benes"][0] * 100) if stats["total_benes"][0] > 0 else 0
    print(f"  - ZIP coverage: {coverage_pct:.1f}%")

    return True


if __name__ == "__main__":
    success = assign_zip_codes()
    sys.exit(0 if success else 1)
