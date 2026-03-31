"""Research and evaluation helpers for the Streamlit research mode."""

from __future__ import annotations

import pandas as pd
import numpy as np

from recommendation_engine.decision_support import compute_heuristic_score


def load_research_sample(
    db,
    bene_limit: int = 500,
    seed: int = 42,
    risk_segment: str = "All",
    insulin_filter: str = "All",
    state_filter: str = "All",
):
    """Load a deterministic beneficiary sample from the evaluation table."""
    conditions = []
    params = []

    if risk_segment != "All":
        conditions.append("tp.risk_segment = ?")
        params.append(risk_segment)
    if insulin_filter != "All":
        conditions.append("tp.bene_insulin_user = ?")
        params.append(1 if insulin_filter == "Insulin users only" else 0)
    if state_filter != "All":
        conditions.append("tp.bene_state = ?")
        params.append(state_filter)

    where_clause = ""
    if conditions:
        where_clause = " AND " + " AND ".join(conditions)

    sql = f"""
        WITH filtered_benes AS (
            SELECT DISTINCT tp.bene_synth_id
            FROM ml.training_plan_pairs tp
            WHERE 1 = 1 {where_clause}
            ORDER BY HASH(CAST(tp.bene_synth_id AS VARCHAR) || ?)
            LIMIT ?
        )
        SELECT
            tp.*,
            CAST(CASE WHEN gm.plan_key IS NOT NULL THEN 1 ELSE 0 END AS INTEGER) AS has_network_metrics,
            CAST(CASE WHEN COALESCE(tp.formulary_total_drugs, 0) > 0 THEN 1 ELSE 0 END AS INTEGER) AS has_formulary_metrics,
            CAST(
                CASE
                    WHEN COALESCE(tp.covered_drug_count, 0) + COALESCE(tp.uncovered_drug_count, 0) > 0
                    THEN 100.0 * COALESCE(tp.covered_drug_count, 0)
                        / (COALESCE(tp.covered_drug_count, 0) + COALESCE(tp.uncovered_drug_count, 0))
                    ELSE 0.0
                END AS DOUBLE
            ) AS coverage_pct_requested
        FROM ml.training_plan_pairs tp
        JOIN filtered_benes fb
          ON tp.bene_synth_id = fb.bene_synth_id
        LEFT JOIN gold.agg_plan_network_metrics gm
          ON tp.plan_key = gm.plan_key
    """

    sample_df = db.query_df(sql, params + [str(seed), int(bene_limit)])
    if len(sample_df) == 0:
        return sample_df

    sample_df["coverage_pct_requested"] = pd.to_numeric(
        sample_df["coverage_pct_requested"], errors="coerce"
    ).fillna(0.0)
    return sample_df


def score_research_sample(sample_df: pd.DataFrame, model_data: dict) -> pd.DataFrame:
    """Apply the trained model and a transparent heuristic baseline."""
    if len(sample_df) == 0:
        return sample_df

    model = model_data["model"]
    feature_names = model_data["feature_names"]

    scored = sample_df.copy()
    features = pd.DataFrame(
        {
            "premium": pd.to_numeric(scored["plan_premium"], errors="coerce").fillna(0.0),
            "deductible": pd.to_numeric(scored["plan_deductible"], errors="coerce").fillna(0.0),
            "is_ma_pd": (scored["contract_type"] == "MA").astype(int),
            "is_pdp": (scored["contract_type"] == "PDP").astype(int),
            "num_drugs": pd.to_numeric(scored["unique_drugs"], errors="coerce").fillna(1).astype(int),
            "is_insulin_user": pd.to_numeric(scored["bene_insulin_user"], errors="coerce").fillna(0).astype(int),
            "avg_fills_per_year": pd.to_numeric(scored["fills_target"], errors="coerce").fillna(12.0),
            "formulary_generic_pct": pd.to_numeric(scored["formulary_generic_pct"], errors="coerce").fillna(0.0),
            "formulary_specialty_pct": pd.to_numeric(scored["formulary_specialty_pct"], errors="coerce").fillna(0.0),
            "formulary_pa_rate": pd.to_numeric(scored["formulary_pa_rate"], errors="coerce").fillna(0.0),
            "formulary_st_rate": pd.to_numeric(scored["formulary_st_rate"], errors="coerce").fillna(0.0),
            "formulary_ql_rate": pd.to_numeric(scored["formulary_ql_rate"], errors="coerce").fillna(0.0),
            "formulary_restrictiveness": pd.to_numeric(scored["formulary_restrictiveness"], errors="coerce").fillna(0).astype(int),
            "network_preferred_pharmacies": pd.to_numeric(scored["network_preferred_pharmacies"], errors="coerce").fillna(0),
            "network_total_pharmacies": pd.to_numeric(scored["network_total_pharmacies"], errors="coerce").fillna(0),
            "network_adequacy_flag": pd.to_numeric(scored["network_adequacy_flag"], errors="coerce").fillna(0).astype(int),
            "distance_miles": pd.to_numeric(scored["distance_miles"], errors="coerce").fillna(10.0),
            "has_distance_tradeoff": scored["has_distance_tradeoff"].fillna(False).astype(int),
            "total_drug_oop": pd.to_numeric(scored["estimated_annual_oop"], errors="coerce").fillna(0.0),
        }
    )
    features["annual_premium"] = features["premium"] * 12.0
    features["cost_per_drug"] = features["total_drug_oop"] / np.maximum(features["num_drugs"], 1)
    features["premium_to_oop_ratio"] = features["annual_premium"] / np.maximum(features["total_drug_oop"], 1.0)

    for col in feature_names:
        if col not in features.columns:
            features[col] = 0
    features = features[feature_names]

    scored["ml_score"] = model.predict(features)
    baseline_input = pd.DataFrame(
        {
            "total_cost_with_distance": pd.to_numeric(scored["total_cost_with_distance"], errors="coerce").fillna(0.0),
            "distance_miles": pd.to_numeric(scored["distance_miles"], errors="coerce").fillna(10.0),
            "drug_coverage_pct": pd.to_numeric(scored["coverage_pct_requested"], errors="coerce").fillna(0.0),
        }
    )
    scored["heuristic_score"] = compute_heuristic_score(baseline_input)
    return scored


def _top_choice_by_score(scored_df: pd.DataFrame, score_col: str, top_k: int = 1) -> pd.DataFrame:
    """Return top-k rows per beneficiary for a score column."""
    ranked = scored_df.sort_values(
        ["bene_synth_id", score_col, "ranking_cost_objective"],
        ascending=[True, False, True],
    ).copy()
    ranked["selection_rank"] = ranked.groupby("bene_synth_id").cumcount() + 1
    return ranked[ranked["selection_rank"] <= top_k].copy()


def _evaluate_choice_set(scored_df: pd.DataFrame, score_col: str, label: str) -> dict:
    """Evaluate regret, coverage, and access for a score column."""
    top1 = _top_choice_by_score(scored_df, score_col=score_col, top_k=1)
    top3 = _top_choice_by_score(scored_df, score_col=score_col, top_k=3)

    group_min = scored_df.groupby("bene_synth_id")["ranking_cost_objective"].min().rename("group_best_cost")
    top1 = top1.merge(group_min, on="bene_synth_id", how="left")
    top3_min = (
        top3.groupby("bene_synth_id")["ranking_cost_objective"]
        .min()
        .rename("top3_best_cost")
        .reset_index()
    )
    top1 = top1.merge(top3_min, on="bene_synth_id", how="left")

    coverage_pct = pd.to_numeric(top1["coverage_pct_requested"], errors="coerce").fillna(0.0)
    access_burden = pd.to_numeric(top1["distance_miles"], errors="coerce").fillna(0.0)
    top1_regret = pd.to_numeric(top1["ranking_cost_objective"], errors="coerce").fillna(0.0) - pd.to_numeric(top1["group_best_cost"], errors="coerce").fillna(0.0)
    top3_regret = pd.to_numeric(top1["top3_best_cost"], errors="coerce").fillna(0.0) - pd.to_numeric(top1["group_best_cost"], errors="coerce").fillna(0.0)
    best_plan_rate = (top1_regret <= 1e-9).mean() if len(top1) > 0 else 0.0

    return {
        "method": label,
        "beneficiaries": int(top1["bene_synth_id"].nunique()),
        "avg_top1_cost_regret": round(float(top1_regret.mean()), 2),
        "median_top1_cost_regret": round(float(top1_regret.median()), 2),
        "avg_top3_cost_regret": round(float(top3_regret.mean()), 2),
        "avg_requested_drug_coverage": round(float(coverage_pct.mean()), 2),
        "avg_access_burden_miles": round(float(access_burden.mean()), 2),
        "best_plan_hit_rate": round(float(best_plan_rate * 100.0), 2),
    }


def evaluate_model_vs_baseline(scored_df: pd.DataFrame):
    """Compare ML scoring against a transparent baseline."""
    if len(scored_df) == 0:
        return pd.DataFrame(), pd.DataFrame()

    summary_df = pd.DataFrame(
        [
            _evaluate_choice_set(scored_df, "ml_score", "ML model"),
            _evaluate_choice_set(scored_df, "heuristic_score", "Transparent baseline"),
        ]
    )

    details = []
    for label, score_col in [("ML model", "ml_score"), ("Transparent baseline", "heuristic_score")]:
        top1 = _top_choice_by_score(scored_df, score_col=score_col, top_k=1).copy()
        group_min = scored_df.groupby("bene_synth_id")["ranking_cost_objective"].min().rename("group_best_cost")
        top1 = top1.merge(group_min, on="bene_synth_id", how="left")
        top1["cost_regret"] = pd.to_numeric(top1["ranking_cost_objective"], errors="coerce").fillna(0.0) - pd.to_numeric(top1["group_best_cost"], errors="coerce").fillna(0.0)
        top1["method"] = label
        details.append(top1)
    detail_df = pd.concat(details, ignore_index=True)
    return summary_df, detail_df


def compute_preference_stability(scored_df: pd.DataFrame) -> pd.DataFrame:
    """Measure how often the top plan changes under alternate user priorities."""
    if len(scored_df) == 0:
        return pd.DataFrame(columns=["scenario", "beneficiaries", "changed_top_plan_pct"])

    working = scored_df.copy()
    working["cost_rank"] = working.groupby("bene_synth_id")["total_cost_with_distance"].rank(method="first", ascending=True)
    working["access_rank"] = working.groupby("bene_synth_id")["distance_miles"].rank(method="first", ascending=True)
    working["coverage_rank"] = working.groupby("bene_synth_id")["coverage_pct_requested"].rank(method="first", ascending=False)
    working["ml_rank"] = working.groupby("bene_synth_id")["ml_score"].rank(method="first", ascending=False)

    scenario_defs = {
        "default_mix": (0.35, 0.30, 0.15, 0.20),
        "cost_focused": (0.20, 0.55, 0.10, 0.15),
        "access_focused": (0.20, 0.25, 0.40, 0.15),
        "coverage_focused": (0.20, 0.20, 0.10, 0.50),
    }

    scenario_top = {}
    for scenario, (ml_w, cost_w, access_w, coverage_w) in scenario_defs.items():
        working[f"{scenario}_score"] = (
            (ml_w * (100.0 - working["ml_rank"]))
            + (cost_w * (100.0 - working["cost_rank"]))
            + (access_w * (100.0 - working["access_rank"]))
            + (coverage_w * (100.0 - working["coverage_rank"]))
        )
        top = _top_choice_by_score(working, score_col=f"{scenario}_score", top_k=1)[["bene_synth_id", "PLAN_KEY"]].copy()
        top = top.rename(columns={"PLAN_KEY": f"{scenario}_plan_key"})
        scenario_top[scenario] = top

    merged = scenario_top["default_mix"]
    for scenario, top in scenario_top.items():
        if scenario == "default_mix":
            continue
        merged = merged.merge(top, on="bene_synth_id", how="left")

    rows = []
    for scenario in [name for name in scenario_defs if name != "default_mix"]:
        changed = (
            merged[f"{scenario}_plan_key"].fillna("") != merged["default_mix_plan_key"].fillna("")
        ).mean()
        rows.append(
            {
                "scenario": scenario,
                "beneficiaries": int(len(merged)),
                "changed_top_plan_pct": round(float(changed * 100.0), 2),
            }
        )
    return pd.DataFrame(rows)


def build_fairness_tables(detail_df: pd.DataFrame):
    """Summarize evaluation metrics by subgroup."""
    if len(detail_df) == 0:
        return {}

    working = detail_df.copy()
    if "cost_regret" not in working.columns:
        working["cost_regret"] = pd.to_numeric(working["ranking_cost_objective"], errors="coerce").fillna(0.0) - pd.to_numeric(working.get("group_best_cost"), errors="coerce").fillna(0.0)

    density_series = pd.to_numeric(working["bene_density"], errors="coerce")
    if density_series.notna().sum() >= 5:
        working["density_bucket"] = pd.qcut(density_series.fillna(density_series.median()), 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"], duplicates="drop")
    else:
        working["density_bucket"] = "Unknown"

    def summarize(group_cols):
        summary = (
            working.groupby(["method"] + group_cols, dropna=False)
            .agg(
                beneficiaries=("bene_synth_id", "nunique"),
                avg_cost_regret=("cost_regret", "mean"),
                avg_coverage_pct=("coverage_pct_requested", "mean"),
                avg_distance_miles=("distance_miles", "mean"),
            )
            .reset_index()
        )
        return summary.sort_values(["method", "beneficiaries"], ascending=[True, False])

    tables = {
        "insulin": summarize(["bene_insulin_user"]),
        "risk_segment": summarize(["risk_segment"]),
        "density": summarize(["density_bucket"]),
    }

    top_states = working["bene_state"].value_counts().head(10).index.tolist()
    if top_states:
        tables["state"] = summarize(["bene_state"])
        tables["state"] = tables["state"][tables["state"]["bene_state"].isin(top_states)]

    top_counties = working["bene_county"].value_counts().head(10).index.tolist()
    if top_counties:
        tables["county"] = summarize(["bene_county"])
        tables["county"] = tables["county"][tables["county"]["bene_county"].isin(top_counties)]
    return tables


def build_data_coverage_diagnostics(scored_df: pd.DataFrame) -> pd.DataFrame:
    """Report evidence coverage in the current research sample."""
    if len(scored_df) == 0:
        return pd.DataFrame(columns=["metric", "value"])

    metrics = [
        ("Beneficiaries", int(scored_df["bene_synth_id"].nunique())),
        ("Plan rows", int(len(scored_df))),
        ("Rows with network metrics (%)", round(float(100.0 * pd.to_numeric(scored_df["has_network_metrics"], errors="coerce").fillna(0).mean()), 2)),
        ("Rows with formulary metrics (%)", round(float(100.0 * pd.to_numeric(scored_df["has_formulary_metrics"], errors="coerce").fillna(0).mean()), 2)),
        ("Rows with density populated (%)", round(float(100.0 * pd.to_numeric(scored_df["bene_density"], errors="coerce").notna().mean()), 2)),
        ("Average requested-drug coverage (%)", round(float(pd.to_numeric(scored_df["coverage_pct_requested"], errors="coerce").fillna(0.0).mean()), 2)),
    ]
    return pd.DataFrame(metrics, columns=["metric", "value"])
