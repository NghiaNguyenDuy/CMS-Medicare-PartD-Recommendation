"""
Generate synthetic beneficiary data for research workflows.

This script creates:
1. synthetic.syn_beneficiary
2. synthetic.syn_beneficiary_prescriptions

Design goals:
- Keep beneficiary-level attributes in syn_beneficiary.
- Generate beneficiary-level prescription rows from CMS formulary coverage data.
- Enrich prescription drug names from local RXCUI reference files.
- Ensure insulin users have at least one insulin prescription when insulin NDCs exist.
- Keep app/inference read-only by materializing synthetic data during pipeline build.

Usage:
    python scripts/generate_beneficiary_profiles.py
    python scripts/generate_beneficiary_profiles.py --num-beneficiaries 20000
    python scripts/generate_beneficiary_profiles.py --from-pde --pde-file data/pde.csv
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Add parent directory to path for db imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.db_manager import get_db


DEFAULT_SEED = 42
DEFAULT_RXCUI_INFO_DIR = Path("data/rxcui_info")


def assign_risk_segment(cost_series: pd.Series) -> pd.Series:
    """Assign LOW/MED/HIGH risk bands from estimated annual cost."""
    risk = pd.cut(
        pd.to_numeric(cost_series, errors="coerce").fillna(0.0),
        bins=[-np.inf, 2000.0, 5000.0, np.inf],
        labels=["LOW", "MED", "HIGH"],
    )
    return risk.astype(str).replace({"nan": "MED"})


def load_rxcui_reference(rxcui_info_dir=DEFAULT_RXCUI_INFO_DIR):
    """
    Load RXCUI -> drug name reference from local CSV shards.

    Expected columns across files:
    - rxcui
    - name
    - synonym
    - tty
    - language
    - suppress
    """
    rxcui_info_dir = Path(rxcui_info_dir)
    files = sorted(rxcui_info_dir.glob("*.csv"))
    if len(files) == 0:
        print(f"[WARN] No RXCUI files found under {rxcui_info_dir}")
        return pd.DataFrame(columns=["rxcui", "drug_name", "drug_synonym", "tty"])

    frames = []
    wanted_cols = {"rxcui", "name", "synonym", "tty", "language", "suppress"}
    for file_path in files:
        try:
            df = pd.read_csv(
                file_path,
                dtype=str,
                usecols=lambda c: str(c).strip().lower() in wanted_cols,
            )
            df.columns = [str(c).strip().lower() for c in df.columns]
            if "rxcui" not in df.columns:
                continue
            frames.append(df)
        except Exception as exc:
            print(f"[WARN] Skipped RXCUI file {file_path.name}: {exc}")

    if len(frames) == 0:
        return pd.DataFrame(columns=["rxcui", "drug_name", "drug_synonym", "tty"])

    ref = pd.concat(frames, ignore_index=True)
    for col in ["rxcui", "name", "synonym", "tty", "language", "suppress"]:
        if col not in ref.columns:
            ref[col] = ""
        ref[col] = ref[col].fillna("").astype(str).str.strip()

    # Keep active English rows when available.
    ref = ref[
        (ref["rxcui"] != "")
        & ((ref["language"] == "") | (ref["language"].str.upper() == "ENG"))
        & (ref["suppress"].str.upper() != "Y")
    ].copy()

    tty_priority = {
        "IN": 1,    # ingredient
        "PIN": 2,
        "MIN": 3,
        "SCD": 4,   # semantic clinical drug
        "SBD": 5,   # semantic branded drug
        "GPCK": 6,
        "BPCK": 7,
    }
    ref["tty_rank"] = ref["tty"].str.upper().map(tty_priority).fillna(99).astype(int)
    ref["name_len"] = ref["name"].str.len().fillna(9999).astype(int)
    ref = ref.sort_values(["rxcui", "tty_rank", "name_len", "name"], ascending=[True, True, True, True])
    ref = ref.drop_duplicates(subset=["rxcui"], keep="first")

    ref = ref.rename(columns={"name": "drug_name", "synonym": "drug_synonym"})
    ref = ref[["rxcui", "drug_name", "drug_synonym", "tty"]].copy()

    print(f"[OK] Loaded RXCUI reference rows: {len(ref):,} from {len(files)} files")
    return ref


def build_rxcui_name_maps(rxcui_ref_df):
    """Build lookup dictionaries for fast row-level assignment."""
    if rxcui_ref_df is None or rxcui_ref_df.empty:
        return {}, {}, {}

    key = rxcui_ref_df["rxcui"].astype(str)
    name_map = dict(zip(key, rxcui_ref_df["drug_name"].astype(str)))
    synonym_map = dict(zip(key, rxcui_ref_df["drug_synonym"].astype(str)))
    tty_map = dict(zip(key, rxcui_ref_df["tty"].astype(str)))
    return name_map, synonym_map, tty_map


def create_synthetic_beneficiaries(num_beneficiaries=10000, seed=DEFAULT_SEED):
    """
    Generate beneficiary-level rows without drug-level details.

    Drug-level prescriptions are generated separately from CMS formulary pool.
    """
    rng = np.random.default_rng(seed)

    print("=" * 70)
    print("Synthetic Beneficiary Generation")
    print("=" * 70)
    print(f"Target beneficiaries: {num_beneficiaries:,}")

    bene_ids = [f"SYNTH_{i:06d}" for i in range(num_beneficiaries)]

    # Initial risk controls how many drugs/fills are sampled.
    risk_segments = rng.choice(["LOW", "MED", "HIGH"], size=num_beneficiaries, p=[0.50, 0.30, 0.20])
    insulin_flags = rng.choice([0, 1], size=num_beneficiaries, p=[0.85, 0.15])

    unique_drugs = []
    for risk in risk_segments:
        if risk == "LOW":
            unique_drugs.append(int(rng.integers(1, 4)))
        elif risk == "MED":
            unique_drugs.append(int(rng.integers(3, 7)))
        else:
            unique_drugs.append(int(rng.integers(5, 12)))

    fills_target = [int(max(1, d * rng.uniform(3.0, 5.0))) for d in unique_drugs]

    # Placeholder estimates; later aligned to generated prescription rows.
    total_rx_cost_est = []
    for risk in risk_segments:
        if risk == "LOW":
            total_rx_cost_est.append(round(float(rng.uniform(500.0, 2000.0)), 2))
        elif risk == "MED":
            total_rx_cost_est.append(round(float(rng.uniform(2000.0, 5000.0)), 2))
        else:
            total_rx_cost_est.append(round(float(rng.uniform(5000.0, 15000.0)), 2))

    bene_df = pd.DataFrame(
        {
            "bene_synth_id": bene_ids,
            "risk_segment": risk_segments,
            "unique_drugs": unique_drugs,
            "fills_target": fills_target,
            "total_rx_cost_est": total_rx_cost_est,
            "insulin_user_flag": insulin_flags,
        }
    )

    print(f"[OK] Created {len(bene_df):,} synthetic beneficiaries")
    return bene_df


def load_formulary_drug_pool(db, max_drugs=5000):
    """
    Load CMS-covered NDC pool from bronze.brz_basic_formulary.

    Returns one row per NDC with restriction flags and formulary coverage weight.
    """
    limit_clause = ""
    if max_drugs is not None and int(max_drugs) > 0:
        limit_clause = f"LIMIT {int(max_drugs)}"

    sql = f"""
        WITH normalized AS (
            SELECT
                REPLACE(REPLACE(TRIM(CAST(NDC AS VARCHAR)), '-', ''), ' ', '') AS ndc,
                NULLIF(TRIM(CAST(RXCUI AS VARCHAR)), '') AS rxcui,
                COALESCE(TRY_CAST(TIER_LEVEL_VALUE AS INTEGER), 4) AS tier_level,
                CASE
                    WHEN UPPER(COALESCE(CAST(PRIOR_AUTHORIZATION_YN AS VARCHAR), 'N')) IN ('Y', '1', 'TRUE', 'T')
                    THEN 1 ELSE 0
                END AS has_prior_auth,
                CASE
                    WHEN UPPER(COALESCE(CAST(STEP_THERAPY_YN AS VARCHAR), 'N')) IN ('Y', '1', 'TRUE', 'T')
                    THEN 1 ELSE 0
                END AS has_step_therapy,
                CASE
                    WHEN UPPER(COALESCE(CAST(QUANTITY_LIMIT_YN AS VARCHAR), 'N')) IN ('Y', '1', 'TRUE', 'T')
                    THEN 1 ELSE 0
                END AS has_quantity_limit,
                FORMULARY_ID
            FROM bronze.brz_basic_formulary
            WHERE NDC IS NOT NULL
        ),
        ndc_agg AS (
            SELECT
                ndc,
                ANY_VALUE(rxcui) AS rxcui,
                MIN(tier_level) AS tier_level,
                MAX(has_prior_auth) AS has_prior_auth,
                MAX(has_step_therapy) AS has_step_therapy,
                MAX(has_quantity_limit) AS has_quantity_limit,
                COUNT(DISTINCT FORMULARY_ID) AS formulary_coverage
            FROM normalized
            WHERE ndc <> ''
            GROUP BY ndc
        ),
        insulin_ref AS (
            SELECT DISTINCT
                REPLACE(REPLACE(TRIM(CAST(ndc AS VARCHAR)), '-', ''), ' ', '') AS ndc
            FROM bronze.brz_insulin_ref
            WHERE ndc IS NOT NULL
        )
        SELECT
            a.ndc,
            a.rxcui,
            a.tier_level,
            a.has_prior_auth,
            a.has_step_therapy,
            a.has_quantity_limit,
            a.formulary_coverage,
            CASE WHEN i.ndc IS NOT NULL THEN 1 ELSE 0 END AS is_insulin
        FROM ndc_agg a
        LEFT JOIN insulin_ref i
          ON a.ndc = i.ndc
        ORDER BY a.formulary_coverage DESC, a.ndc
        {limit_clause}
    """

    pool = db.query_df(sql)
    if pool.empty:
        raise RuntimeError("No CMS formulary drugs found in bronze.brz_basic_formulary.")

    pool["ndc"] = pool["ndc"].astype(str)
    pool["rxcui"] = pool["rxcui"].astype(str).replace({"None": "", "nan": ""})
    pool["tier_level"] = pd.to_numeric(pool["tier_level"], errors="coerce").fillna(4).astype(int)
    for col in ["has_prior_auth", "has_step_therapy", "has_quantity_limit", "is_insulin"]:
        pool[col] = pd.to_numeric(pool[col], errors="coerce").fillna(0).astype(int)
    pool["formulary_coverage"] = pd.to_numeric(pool["formulary_coverage"], errors="coerce").fillna(1).clip(lower=1)

    print(f"[OK] Loaded {len(pool):,} CMS formulary NDCs for sampling")
    print(f"[OK] Insulin NDCs in pool: {int(pool['is_insulin'].sum()):,}")
    return pool


def allocate_fills_across_drugs(fills_target, drug_count, rng):
    """
    Allocate annual fills across a beneficiary's selected drugs.

    Guarantees at least one fill per selected drug.
    """
    drug_count = int(max(0, drug_count))
    if drug_count == 0:
        return np.array([], dtype=int)

    target = int(max(drug_count, fills_target))
    if drug_count == 1:
        return np.array([target], dtype=int)

    shares = rng.dirichlet(np.ones(drug_count))
    fills = np.floor(shares * target).astype(int)
    fills = np.maximum(fills, 1)

    current_total = int(fills.sum())
    if current_total < target:
        add_idx = rng.choice(drug_count, size=(target - current_total), replace=True)
        for idx in add_idx:
            fills[int(idx)] += 1
    elif current_total > target:
        reducible = np.where(fills > 1)[0]
        while current_total > target and len(reducible) > 0:
            idx = int(rng.choice(reducible))
            fills[idx] -= 1
            current_total -= 1
            reducible = np.where(fills > 1)[0]

    return fills.astype(int)


def _sample_drug_indices(k, weight_vector, rng):
    """Sample k unique indices from weighted drug pool."""
    n = len(weight_vector)
    if n == 0:
        return np.array([], dtype=int)
    if k >= n:
        return np.arange(n, dtype=int)
    return rng.choice(n, size=k, replace=False, p=weight_vector)


def generate_prescriptions_from_beneficiaries(
    bene_df,
    drug_pool,
    seed=DEFAULT_SEED,
    rxcui_name_map=None,
    rxcui_synonym_map=None,
    rxcui_tty_map=None,
):
    """
    Generate beneficiary-level prescriptions from CMS-covered drug pool.

    Each row represents one beneficiary-NDC prescription pattern.
    """
    if bene_df.empty:
        return pd.DataFrame(
            columns=[
                "bene_synth_id",
                "ndc",
                "rxcui",
                "drug_name",
                "drug_synonym",
                "drug_tty",
                "drug_name_source",
                "fills_per_year",
                "days_supply_mode",
                "qty_per_fill",
                "tier_level",
                "has_prior_auth",
                "has_step_therapy",
                "has_quantity_limit",
                "is_insulin",
                "estimated_annual_drug_cost",
                "source_mode",
            ]
        )

    rng = np.random.default_rng(seed)
    rxcui_name_map = rxcui_name_map or {}
    rxcui_synonym_map = rxcui_synonym_map or {}
    rxcui_tty_map = rxcui_tty_map or {}
    pool = drug_pool.drop_duplicates(subset=["ndc"]).reset_index(drop=True).copy()
    pool_size = len(pool)
    if pool_size == 0:
        raise RuntimeError("Drug pool is empty; cannot generate prescriptions.")

    weights = pool["formulary_coverage"].astype(float).to_numpy()
    weights = weights / weights.sum()

    insulin_indices = np.where(pool["is_insulin"].to_numpy() == 1)[0]
    insulin_weights = None
    if len(insulin_indices) > 0:
        insulin_weights = weights[insulin_indices]
        insulin_weights = insulin_weights / insulin_weights.sum()

    # Rough unit cost anchors by tier, used to derive annual synthetic spend.
    tier_unit_cost = {1: 15.0, 2: 35.0, 3: 80.0, 4: 160.0, 5: 300.0, 6: 450.0, 7: 600.0}

    rows = []
    for row in bene_df.itertuples(index=False):
        bene_id = str(row.bene_synth_id)
        k = int(max(1, getattr(row, "unique_drugs", 1)))
        k = min(k, pool_size)

        selected = _sample_drug_indices(k, weights, rng)

        insulin_flag = int(getattr(row, "insulin_user_flag", 0))
        if insulin_flag == 1 and len(insulin_indices) > 0:
            selected_insulin = np.intersect1d(selected, insulin_indices, assume_unique=False)
            if len(selected_insulin) == 0:
                forced_insulin = int(rng.choice(insulin_indices, p=insulin_weights))
                replace_pos = int(rng.integers(0, len(selected)))
                selected[replace_pos] = forced_insulin
                # Ensure uniqueness after forced replacement.
                selected = np.unique(selected)
                while len(selected) < k:
                    candidate = int(rng.choice(pool_size, p=weights))
                    if candidate not in selected:
                        selected = np.append(selected, candidate)

        fills_target = int(max(k, getattr(row, "fills_target", k)))
        fills_alloc = allocate_fills_across_drugs(fills_target=fills_target, drug_count=len(selected), rng=rng)

        for idx, fills_count in zip(selected, fills_alloc):
            drug = pool.iloc[int(idx)]
            tier_level = int(drug["tier_level"])
            days_supply = int(rng.choice([30, 60, 90], p=[0.70, 0.10, 0.20]))
            qty_per_fill = float(round(max(1.0, rng.normal(30.0, 8.0)), 2))

            base_unit_cost = tier_unit_cost.get(tier_level, 140.0)
            unit_cost = max(1.0, float(base_unit_cost * rng.uniform(0.75, 1.25)))
            annual_cost = round(unit_cost * int(fills_count), 2)

            rxcui = str(drug["rxcui"]).strip()
            if rxcui and rxcui in rxcui_name_map and str(rxcui_name_map[rxcui]).strip():
                drug_name = str(rxcui_name_map[rxcui]).strip()
                drug_name_source = "rxcui_info.name"
            elif rxcui and rxcui in rxcui_synonym_map and str(rxcui_synonym_map[rxcui]).strip():
                drug_name = str(rxcui_synonym_map[rxcui]).strip()
                drug_name_source = "rxcui_info.synonym"
            else:
                drug_name = f"NDC {drug['ndc']}"
                drug_name_source = "fallback_ndc"
            drug_synonym = str(rxcui_synonym_map.get(rxcui, "")).strip()
            drug_tty = str(rxcui_tty_map.get(rxcui, "")).strip()

            rows.append(
                {
                    "bene_synth_id": bene_id,
                    "ndc": str(drug["ndc"]),
                    "rxcui": rxcui,
                    "drug_name": drug_name,
                    "drug_synonym": drug_synonym,
                    "drug_tty": drug_tty,
                    "drug_name_source": drug_name_source,
                    "fills_per_year": int(fills_count),
                    "days_supply_mode": days_supply,
                    "qty_per_fill": qty_per_fill,
                    "tier_level": tier_level,
                    "has_prior_auth": int(drug["has_prior_auth"]),
                    "has_step_therapy": int(drug["has_step_therapy"]),
                    "has_quantity_limit": int(drug["has_quantity_limit"]),
                    "is_insulin": int(drug["is_insulin"]),
                    "estimated_annual_drug_cost": annual_cost,
                    "source_mode": "synthetic",
                }
            )

    rx_df = pd.DataFrame(rows)
    print(f"[OK] Generated {len(rx_df):,} beneficiary prescription rows")
    return rx_df


def align_beneficiary_summary(bene_df, rx_df):
    """
    Align beneficiary-level aggregates with generated prescription rows.

    Ensures `unique_drugs`, `fills_target`, and `total_rx_cost_est` are consistent
    with synthetic.syn_beneficiary_prescriptions.
    """
    if rx_df.empty:
        raise RuntimeError("Prescription dataframe is empty; cannot align beneficiary summary.")

    agg = (
        rx_df.groupby("bene_synth_id", as_index=False)
        .agg(
            unique_drugs=("ndc", "nunique"),
            fills_target=("fills_per_year", "sum"),
            total_rx_cost_est=("estimated_annual_drug_cost", "sum"),
            insulin_user_flag=("is_insulin", "max"),
        )
    )
    agg["risk_segment"] = assign_risk_segment(agg["total_rx_cost_est"])

    keep_cols = [c for c in bene_df.columns if c not in {"unique_drugs", "fills_target", "total_rx_cost_est", "insulin_user_flag", "risk_segment"}]
    merged = bene_df[keep_cols].merge(agg, on="bene_synth_id", how="inner")

    if len(merged) != len(bene_df):
        print(f"[WARN] Beneficiary count changed after alignment: {len(bene_df):,} -> {len(merged):,}")

    return merged


def create_from_pde(
    pde_file="data/pde.csv",
    num_beneficiaries=None,
    rxcui_name_map=None,
    rxcui_synonym_map=None,
    rxcui_tty_map=None,
):
    """
    Generate beneficiary + prescription rows from PDE events.

    Only keeps NDCs that appear in CMS formulary coverage.
    """
    db = get_db()

    print("=" * 70)
    print("Beneficiary Generation from PDE")
    print("=" * 70)
    print(f"PDE file: {pde_file}")

    if not Path(pde_file).exists():
        raise FileNotFoundError(f"PDE file not found: {pde_file}")

    pde_df = pd.read_csv(
        pde_file,
        delimiter="|",
        usecols=["BENE_ID", "PROD_SRVC_ID", "QTY_DSPNSD_NUM", "DAYS_SUPLY_NUM", "FILL_NUM", "TOT_RX_CST_AMT"],
        dtype={"BENE_ID": str, "PROD_SRVC_ID": str},
    )
    print(f"[OK] Loaded PDE events: {len(pde_df):,}")

    pde_df["ndc"] = (
        pde_df["PROD_SRVC_ID"]
        .astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )
    pde_df = pde_df[pde_df["ndc"] != ""].copy()

    if num_beneficiaries:
        keep_benes = pde_df["BENE_ID"].drop_duplicates().head(int(num_beneficiaries))
        pde_df = pde_df[pde_df["BENE_ID"].isin(keep_benes)].copy()
        print(f"[OK] Limited PDE beneficiaries to: {len(keep_benes):,}")

    profiles = (
        pde_df.groupby(["BENE_ID", "ndc"], as_index=False)
        .agg(
            fills_per_year=("FILL_NUM", "count"),
            days_supply_mode=("DAYS_SUPLY_NUM", lambda x: x.mode().iloc[0] if len(x) > 0 else 30),
            qty_per_fill=("QTY_DSPNSD_NUM", "mean"),
            estimated_annual_drug_cost=("TOT_RX_CST_AMT", "sum"),
        )
        .rename(columns={"BENE_ID": "bene_synth_id"})
    )
    profiles["bene_synth_id"] = profiles["bene_synth_id"].astype(str)

    rxcui_name_map = rxcui_name_map or {}
    rxcui_synonym_map = rxcui_synonym_map or {}
    rxcui_tty_map = rxcui_tty_map or {}

    pool_all = load_formulary_drug_pool(db, max_drugs=None)
    rx_df = profiles.merge(
        pool_all[["ndc", "rxcui", "tier_level", "has_prior_auth", "has_step_therapy", "has_quantity_limit", "is_insulin"]],
        on="ndc",
        how="inner",
    )

    if rx_df.empty:
        raise RuntimeError("No PDE drugs matched CMS formulary NDCs.")

    rx_df["rxcui"] = rx_df["rxcui"].fillna("").astype(str).str.strip()
    rx_df["drug_name"] = rx_df["rxcui"].map(rxcui_name_map).fillna("")
    empty_name = rx_df["drug_name"].astype(str).str.strip() == ""
    rx_df.loc[empty_name, "drug_name"] = rx_df.loc[empty_name, "rxcui"].map(rxcui_synonym_map).fillna("")
    empty_name = rx_df["drug_name"].astype(str).str.strip() == ""
    rx_df.loc[empty_name, "drug_name"] = "NDC " + rx_df.loc[empty_name, "ndc"].astype(str)
    rx_df["drug_synonym"] = rx_df["rxcui"].map(rxcui_synonym_map).fillna("")
    rx_df["drug_tty"] = rx_df["rxcui"].map(rxcui_tty_map).fillna("")
    rx_df["drug_name_source"] = np.where(
        rx_df["drug_name"].astype(str).str.startswith("NDC "),
        "fallback_ndc",
        "rxcui_info",
    )
    rx_df["source_mode"] = "pde"

    rx_df = rx_df[
        [
            "bene_synth_id",
            "ndc",
            "rxcui",
            "drug_name",
            "drug_synonym",
            "drug_tty",
            "drug_name_source",
            "fills_per_year",
            "days_supply_mode",
            "qty_per_fill",
            "tier_level",
            "has_prior_auth",
            "has_step_therapy",
            "has_quantity_limit",
            "is_insulin",
            "estimated_annual_drug_cost",
            "source_mode",
        ]
    ].copy()

    # Minimal base dataframe; geography is assigned later.
    bene_base = pd.DataFrame({"bene_synth_id": rx_df["bene_synth_id"].drop_duplicates().astype(str)})
    bene_df = align_beneficiary_summary(bene_base, rx_df)

    print(f"[OK] PDE beneficiaries after formulary match: {len(bene_df):,}")
    print(f"[OK] PDE prescription rows after formulary match: {len(rx_df):,}")
    return bene_df, rx_df


def assign_geography(bene_df, seed=DEFAULT_SEED):
    """
    Assign beneficiaries to counties and states using weighted geographic distribution.
    """
    db = get_db()
    rng = np.random.default_rng(seed)

    print("\nAssigning geographic locations...")

    try:
        county_weights = db.query_df(
            """
            SELECT
                CAST(county_code AS VARCHAR) AS county_code,
                CAST(state_label AS VARCHAR) AS state,
                COUNT(*) AS weight
            FROM bronze.brz_geographic
            GROUP BY county_code, state_label
            ORDER BY weight DESC
            """
        )
    except Exception as exc:
        county_weights = pd.DataFrame()
        print(f"[WARN] Could not load bronze.brz_geographic: {exc}")

    if county_weights.empty:
        default_counties = pd.DataFrame(
            {
                "county_code": ["06037", "17031", "36061", "48201"],
                "state": ["CA", "IL", "NY", "TX"],
                "weight": [100, 100, 100, 100],
            }
        )
        county_weights = default_counties
        print("[WARN] Using fallback county distribution")

    probs = county_weights["weight"].astype(float).to_numpy()
    probs = probs / probs.sum()

    assigned = rng.choice(county_weights["county_code"].to_numpy(), size=len(bene_df), replace=True, p=probs)
    state_map = county_weights.set_index("county_code")["state"].to_dict()

    bene_df = bene_df.copy()
    bene_df["county_code"] = pd.Series(assigned).astype(str)
    bene_df["state"] = bene_df["county_code"].map(state_map).fillna("NA")

    print(f"[OK] Counties assigned: {bene_df['county_code'].nunique():,}")
    print(f"[OK] States assigned: {bene_df['state'].nunique():,}")
    return bene_df


def save_to_database(bene_df, rx_df):
    """Persist synthetic beneficiary and prescription tables."""
    db = get_db()

    print("\nSaving synthetic tables to DuckDB...")
    db.execute("CREATE SCHEMA IF NOT EXISTS synthetic;")
    db.execute("DROP TABLE IF EXISTS synthetic.syn_beneficiary;")
    db.execute("DROP TABLE IF EXISTS synthetic.syn_beneficiary_prescriptions;")

    db.conn.register("bene_data", bene_df)
    db.conn.register("rx_data", rx_df)

    db.execute(
        """
        CREATE TABLE synthetic.syn_beneficiary AS
        SELECT
            CAST(bene_synth_id AS VARCHAR) AS bene_synth_id,
            CAST(state AS VARCHAR) AS state,
            CAST(county_code AS VARCHAR) AS county_code,
            NULL::VARCHAR AS zip_code,  -- assigned by db/ml/02_assign_geography.py
            NULL::DOUBLE AS lat,
            NULL::DOUBLE AS lng,
            NULL::DOUBLE AS density,
            CAST(risk_segment AS VARCHAR) AS risk_segment,
            CAST(unique_drugs AS INTEGER) AS unique_drugs,
            CAST(fills_target AS INTEGER) AS fills_target,
            CAST(total_rx_cost_est AS DOUBLE) AS total_rx_cost_est,
            CAST(insulin_user_flag AS INTEGER) AS insulin_user_flag,
            CURRENT_TIMESTAMP AS created_at
        FROM bene_data
        """
    )

    db.execute(
        """
        CREATE TABLE synthetic.syn_beneficiary_prescriptions AS
        SELECT
            CAST(bene_synth_id AS VARCHAR) AS bene_synth_id,
            CAST(ndc AS VARCHAR) AS ndc,
            CAST(rxcui AS VARCHAR) AS rxcui,
            CAST(drug_name AS VARCHAR) AS drug_name,
            CAST(drug_synonym AS VARCHAR) AS drug_synonym,
            CAST(drug_tty AS VARCHAR) AS drug_tty,
            CAST(drug_name_source AS VARCHAR) AS drug_name_source,
            CAST(fills_per_year AS INTEGER) AS fills_per_year,
            CAST(days_supply_mode AS INTEGER) AS days_supply_mode,
            CAST(qty_per_fill AS DOUBLE) AS qty_per_fill,
            CAST(tier_level AS INTEGER) AS tier_level,
            CAST(has_prior_auth AS INTEGER) AS has_prior_auth,
            CAST(has_step_therapy AS INTEGER) AS has_step_therapy,
            CAST(has_quantity_limit AS INTEGER) AS has_quantity_limit,
            CAST(is_insulin AS INTEGER) AS is_insulin,
            CAST(estimated_annual_drug_cost AS DOUBLE) AS estimated_annual_drug_cost,
            CAST(source_mode AS VARCHAR) AS source_mode,
            CURRENT_TIMESTAMP AS created_at
        FROM rx_data
        """
    )

    db.execute("CREATE INDEX idx_syn_bene_id ON synthetic.syn_beneficiary(bene_synth_id);")
    db.execute("CREATE INDEX idx_syn_county ON synthetic.syn_beneficiary(county_code);")
    db.execute("CREATE INDEX idx_syn_insulin_user ON synthetic.syn_beneficiary(insulin_user_flag);")

    db.execute("CREATE INDEX idx_syn_rx_bene_id ON synthetic.syn_beneficiary_prescriptions(bene_synth_id);")
    db.execute("CREATE INDEX idx_syn_rx_ndc ON synthetic.syn_beneficiary_prescriptions(ndc);")
    db.execute("CREATE INDEX idx_syn_rx_insulin ON synthetic.syn_beneficiary_prescriptions(is_insulin);")

    bene_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
    rx_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary_prescriptions")[0]
    bene_with_rx = db.query_one(
        "SELECT COUNT(DISTINCT bene_synth_id) FROM synthetic.syn_beneficiary_prescriptions"
    )[0]

    print(f"[OK] synthetic.syn_beneficiary rows: {bene_count:,}")
    print(f"[OK] synthetic.syn_beneficiary_prescriptions rows: {rx_count:,}")
    print(f"[OK] beneficiaries with prescriptions: {bene_with_rx:,}")
    return True


def print_summary(bene_df, rx_df):
    """Print concise generation summary."""
    print("\n" + "=" * 70)
    print("Synthetic Generation Complete")
    print("=" * 70)
    print(f"Beneficiaries: {len(bene_df):,}")
    print(f"Prescription rows: {len(rx_df):,}")
    print(f"Avg drugs/beneficiary: {rx_df.groupby('bene_synth_id')['ndc'].nunique().mean():.2f}")
    print(f"Avg fills/beneficiary: {rx_df.groupby('bene_synth_id')['fills_per_year'].sum().mean():.2f}")
    named_pct = 100.0 * float((~rx_df["drug_name"].astype(str).str.startswith("NDC ")).mean()) if len(rx_df) > 0 else 0.0
    print(f"Drug names resolved from RXCUI reference: {named_pct:.1f}%")
    insulin_pct = 100.0 * float(bene_df["insulin_user_flag"].mean()) if len(bene_df) > 0 else 0.0
    print(f"Insulin users: {int(bene_df['insulin_user_flag'].sum()):,} ({insulin_pct:.1f}%)")
    print("Tables:")
    print("  - synthetic.syn_beneficiary")
    print("  - synthetic.syn_beneficiary_prescriptions")
    print("\nNext steps:")
    print("  1. python db/ml/02_assign_geography.py")
    print("  2. python -m db.run_full_pipeline --layers ml")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic beneficiary and prescription tables")
    parser.add_argument("--num-beneficiaries", type=int, default=10000, help="Synthetic beneficiary count")
    parser.add_argument("--from-pde", action="store_true", help="Generate from PDE instead of synthetic sampling")
    parser.add_argument("--pde-file", default="data/pde.csv", help="PDE input file path")
    parser.add_argument("--max-drugs", type=int, default=5000, help="Max CMS formulary NDCs used in synthetic mode")
    parser.add_argument(
        "--rxcui-info-dir",
        default=str(DEFAULT_RXCUI_INFO_DIR),
        help="Directory containing RXCUI property CSV files",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed")
    args = parser.parse_args()

    try:
        rxcui_ref_df = load_rxcui_reference(args.rxcui_info_dir)
        rxcui_name_map, rxcui_synonym_map, rxcui_tty_map = build_rxcui_name_maps(rxcui_ref_df)

        if args.from_pde:
            bene_df, rx_df = create_from_pde(
                pde_file=args.pde_file,
                num_beneficiaries=(args.num_beneficiaries if args.num_beneficiaries != 10000 else None),
                rxcui_name_map=rxcui_name_map,
                rxcui_synonym_map=rxcui_synonym_map,
                rxcui_tty_map=rxcui_tty_map,
            )
        else:
            db = get_db()
            bene_df = create_synthetic_beneficiaries(
                num_beneficiaries=int(args.num_beneficiaries),
                seed=int(args.seed),
            )
            drug_pool = load_formulary_drug_pool(db=db, max_drugs=int(args.max_drugs))
            rx_df = generate_prescriptions_from_beneficiaries(
                bene_df=bene_df,
                drug_pool=drug_pool,
                seed=int(args.seed),
                rxcui_name_map=rxcui_name_map,
                rxcui_synonym_map=rxcui_synonym_map,
                rxcui_tty_map=rxcui_tty_map,
            )
            bene_df = align_beneficiary_summary(bene_df, rx_df)

        bene_df = assign_geography(bene_df, seed=int(args.seed))

        if len(bene_df) == 0 or len(rx_df) == 0:
            raise RuntimeError("Generated data is empty; aborting table write.")

        save_to_database(bene_df, rx_df)
        print_summary(bene_df, rx_df)
        return 0
    except Exception as exc:
        print(f"\n[ERROR] {exc}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
