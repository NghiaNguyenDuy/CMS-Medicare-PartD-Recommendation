"""
ML Prep: Distance Proxy Calculation

Create deterministic distance features for plan-county combinations so the
same snapshot and seed always yield the same training data.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db
from utils.deterministic import stable_uniform


DEFAULT_DISTANCE_SEED = 42


def _normalize_county_code(series: pd.Series) -> pd.Series:
    """Normalize county codes to zero-padded strings."""
    def _normalize(value):
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if text == "":
            return ""
        try:
            return str(int(float(text))).zfill(5)
        except (TypeError, ValueError):
            return text.zfill(5) if text.isdigit() else text

    return series.map(_normalize)


def compute_deterministic_distance_features(
    plan_counties_df: pd.DataFrame,
    county_density_df: pd.DataFrame,
    network_metrics_df: pd.DataFrame,
    seed: int = DEFAULT_DISTANCE_SEED,
) -> pd.DataFrame:
    """Build deterministic distance features from plan, county, and network inputs."""
    plan_counties = plan_counties_df.copy()
    plan_counties["county_code"] = _normalize_county_code(plan_counties["county_code"])
    plan_counties = plan_counties[(plan_counties["state"].notna()) & (plan_counties["county_code"] != "")].copy()

    county_density = county_density_df.copy()
    county_density["county_code"] = _normalize_county_code(county_density["county_code"])
    county_density = county_density.drop_duplicates(subset=["county_code"])

    network_metrics = network_metrics_df.copy()
    if len(network_metrics) > 0:
        network_metrics = network_metrics.drop_duplicates(subset=["plan_key"])

    merged = plan_counties.merge(county_density, on="county_code", how="left")
    merged = merged.merge(network_metrics, on="plan_key", how="left")
    merged["county_avg_density"] = pd.to_numeric(merged.get("avg_density"), errors="coerce").fillna(100.0)
    merged["county_zip_count"] = pd.to_numeric(merged.get("zip_count"), errors="coerce").fillna(1).astype(int)

    default_pref = merged["contract_type"].map({"PDP": 25, "MA": 15}).fillna(20)
    merged["preferred_pharmacies"] = pd.to_numeric(
        merged.get("preferred_pharmacies"), errors="coerce"
    ).fillna(default_pref).clip(lower=0.0)
    merged["total_pharmacies"] = pd.to_numeric(
        merged.get("total_pharmacies"), errors="coerce"
    ).fillna(30.0).clip(lower=0.0)
    merged["pref_pharmacy_pct"] = pd.to_numeric(
        merged.get("pref_pharmacy_pct"), errors="coerce"
    ).fillna(50.0)
    merged["in_area_pharmacies"] = pd.to_numeric(
        merged.get("in_area_pharmacies"), errors="coerce"
    ).fillna(20.0)

    lower_bounds = []
    upper_bounds = []
    for row in merged.itertuples(index=False):
        density = float(row.county_avg_density)
        preferred = float(row.preferred_pharmacies)
        if density >= 1000.0 and preferred >= 20.0:
            low, high = 0.5, 2.0
        elif 250.0 <= density < 1000.0 and preferred >= 10.0:
            low, high = 2.0, 8.0
        elif density < 250.0 or preferred < 10.0:
            low, high = 8.0, 25.0
        else:
            low, high = 5.0, 15.0
        lower_bounds.append(low)
        upper_bounds.append(high)

    merged["distance_low"] = lower_bounds
    merged["distance_high"] = upper_bounds
    merged["simulated_distance_miles"] = merged.apply(
        lambda row: round(
            stable_uniform(
                f"{row['plan_key']}|{row['county_code']}",
                float(row["distance_low"]),
                float(row["distance_high"]),
                seed=seed,
            ),
            6,
        ),
        axis=1,
    )
    merged["distance_category"] = merged["simulated_distance_miles"].apply(
        lambda miles: "very_close" if miles < 3 else ("nearby" if miles < 8 else ("moderate" if miles < 15 else "far"))
    )

    return merged[
        [
            "plan_key",
            "contract_type",
            "state",
            "county_code",
            "county_avg_density",
            "county_zip_count",
            "preferred_pharmacies",
            "total_pharmacies",
            "pref_pharmacy_pct",
            "in_area_pharmacies",
            "simulated_distance_miles",
            "distance_category",
        ]
    ].copy()


def calculate_distance_proxy(seed: int = DEFAULT_DISTANCE_SEED):
    """
    Calculate deterministic distance proxies for plan-county combinations.
    """
    db = get_db()

    print("=" * 60)
    print("ML Prep: Distance Proxy Calculation")
    print("=" * 60)

    db.execute("CREATE SCHEMA IF NOT EXISTS ml;")
    db.execute("DROP TABLE IF EXISTS ml.plan_distance_features;")

    print("\n1. Calculating deterministic distance proxy...")
    print(f"   - Seed: {seed}")
    print("   - Inputs: county density + network adequacy + plan type")

    plan_counties_df = db.query_df(
        """
        SELECT DISTINCT
            p.plan_key,
            CASE
                WHEN p.IS_MA_PD THEN 'MA'
                WHEN p.IS_PDP THEN 'PDP'
                ELSE NULL
            END AS contract_type,
            CAST(p.state AS VARCHAR) AS state,
            CAST(p.county_code AS VARCHAR) AS county_code
        FROM bronze.brz_plan_info p
        WHERE (p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y')
          AND p.state IS NOT NULL
          AND p.county_code IS NOT NULL
        """
    )
    county_density_df = db.query_df(
        """
        SELECT
            CAST(county_code AS VARCHAR) AS county_code,
            AVG(COALESCE(density, 0)) AS avg_density,
            COUNT(*) AS zip_count
        FROM bronze.brz_zipcode
        WHERE county_code IS NOT NULL
        GROUP BY county_code
        """
    )
    network_metrics_df = db.query_df(
        """
        SELECT
            plan_key,
            preferred_pharmacies,
            total_pharmacies,
            pref_pharmacy_pct,
            in_area_pharmacies
        FROM gold.agg_plan_network_metrics
        """
    )

    distance_df = compute_deterministic_distance_features(
        plan_counties_df=plan_counties_df,
        county_density_df=county_density_df,
        network_metrics_df=network_metrics_df,
        seed=int(seed),
    )

    db.conn.register("distance_df", distance_df)
    db.execute(
        """
        CREATE TABLE ml.plan_distance_features AS
        SELECT
            CAST(plan_key AS VARCHAR) AS plan_key,
            CAST(contract_type AS VARCHAR) AS contract_type,
            CAST(state AS VARCHAR) AS state,
            CAST(county_code AS VARCHAR) AS county_code,
            CAST(county_avg_density AS DOUBLE) AS county_avg_density,
            CAST(county_zip_count AS INTEGER) AS county_zip_count,
            CAST(preferred_pharmacies AS DOUBLE) AS preferred_pharmacies,
            CAST(total_pharmacies AS DOUBLE) AS total_pharmacies,
            CAST(pref_pharmacy_pct AS DOUBLE) AS pref_pharmacy_pct,
            CAST(in_area_pharmacies AS DOUBLE) AS in_area_pharmacies,
            CAST(simulated_distance_miles AS DOUBLE) AS simulated_distance_miles,
            CAST(distance_category AS VARCHAR) AS distance_category,
            CURRENT_TIMESTAMP AS ml_ts
        FROM distance_df
        """
    )

    print("\n2. Creating indexes...")
    db.execute("CREATE INDEX idx_dist_plan ON ml.plan_distance_features(plan_key);")
    db.execute("CREATE INDEX idx_dist_county ON ml.plan_distance_features(county_code);")
    db.execute("CREATE INDEX idx_dist_category ON ml.plan_distance_features(distance_category);")

    print("\n3. Validation...")
    stats = db.query_df(
        """
        SELECT
            COUNT(*) AS total_plan_counties,
            COUNT(DISTINCT plan_key) AS unique_plans,
            COUNT(DISTINCT county_code) AS unique_counties,
            ROUND(AVG(simulated_distance_miles), 2) AS avg_distance,
            ROUND(MIN(simulated_distance_miles), 2) AS min_distance,
            ROUND(MAX(simulated_distance_miles), 2) AS max_distance,
            ROUND(AVG(preferred_pharmacies), 1) AS avg_pref_pharmacies,
            ROUND(AVG(total_pharmacies), 1) AS avg_total_pharmacies,
            SUM(CASE WHEN distance_category = 'very_close' THEN 1 ELSE 0 END) AS very_close_count,
            SUM(CASE WHEN distance_category = 'nearby' THEN 1 ELSE 0 END) AS nearby_count,
            SUM(CASE WHEN distance_category = 'moderate' THEN 1 ELSE 0 END) AS moderate_count,
            SUM(CASE WHEN distance_category = 'far' THEN 1 ELSE 0 END) AS far_count
        FROM ml.plan_distance_features
        """
    )

    print("\n[OK] Distance proxy calculation complete:")
    print(f"  - Total plan-county pairs: {stats['total_plan_counties'][0]:,}")
    print(f"  - Unique plans: {stats['unique_plans'][0]:,}")
    print(f"  - Unique counties: {stats['unique_counties'][0]:,}")
    print(f"  - Avg preferred pharmacies: {stats['avg_pref_pharmacies'][0]}")
    print(f"  - Avg total pharmacies: {stats['avg_total_pharmacies'][0]}")
    print(f"  - Avg distance: {stats['avg_distance'][0]} miles")
    print(f"  - Range: {stats['min_distance'][0]} - {stats['max_distance'][0]} miles")
    print(f"  - Very close (<3mi): {stats['very_close_count'][0]:,}")
    print(f"  - Nearby (3-8mi): {stats['nearby_count'][0]:,}")
    print(f"  - Moderate (8-15mi): {stats['moderate_count'][0]:,}")
    print(f"  - Far (>15mi): {stats['far_count'][0]:,}")

    return True


if __name__ == "__main__":
    success = calculate_distance_proxy()
    sys.exit(0 if success else 1)
