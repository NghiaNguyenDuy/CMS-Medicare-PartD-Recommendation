"""
ML Prep: Training Plan Pairs Generation

Generate beneficiary-plan training pairs with features:
- Plan characteristics (premium, deductible, formulary, network, distance)
- Beneficiary characteristics
- Cost objective aligned to SPUF-style rules:
  premium + annual OOP + distance penalty

Annual OOP approximation logic (preferred retail channel):
- Copay rows: fixed dollar cost per fill
- Coinsurance rows: percentage of drug gross cost per fill
- Deductible application by tier (DED_APPLIES_YN / DEDUCTIBLE_APPLIES)
- Insulin copay override with IRA-style cap proxy ($35 per 30-day equivalent)
- Uncovered/excluded drugs treated as full gross cost burden

Creates:
    ml.training_plan_pairs - Beneficiary-plan pairs with ML features + cost targets
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from db.db_manager import get_db


# Pricing calibration policy for UNIT_COST-driven annual gross estimates.
# 1) winsorize UNIT_COST by days_supply_code
# 2) bound pricing annual estimate relative to historical synthetic estimate
# 3) blend bounded pricing estimate with historical synthetic estimate
PRICING_WINSOR_LOW_Q = 0.01
PRICING_WINSOR_HIGH_Q = 0.95
PRICING_TO_HIST_RATIO_MIN = 0.50
PRICING_TO_HIST_RATIO_MAX = 12.00
PRICING_HIST_BLEND_WEIGHT = 0.35
PRICING_ANNUAL_ABS_MAX = 25000.0


def _table_exists(db, full_name):
    schema, table = full_name.split(".", 1)
    sql = """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
    """
    return bool(db.query_one(sql, [schema, table])[0] > 0)


def generate_training_pairs():
    """
    Generate beneficiary-plan training pairs with SPUF-style cost approximation.

    Creates:
        ml.training_plan_pairs
    """
    db = get_db()

    print("=" * 70)
    print("ML Prep: Training Pairs Generation")
    print("=" * 70)

    # Check prerequisites
    required = [
        "synthetic.syn_beneficiary",
        "synthetic.syn_beneficiary_prescriptions",
        "bronze.brz_plan_info",
        "bronze.brz_basic_formulary",
        "bronze.brz_beneficiary_cost",
        "bronze.brz_insulin_cost",
    ]
    missing = [t for t in required if not _table_exists(db, t)]
    if missing:
        print("\n[ERROR] Missing prerequisite tables:")
        for t in missing:
            print(f"  - {t}")
        return False

    bene_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary")[0]
    rx_count = db.query_one("SELECT COUNT(*) FROM synthetic.syn_beneficiary_prescriptions")[0]
    has_pricing = _table_exists(db, "bronze.brz_pricing")
    print("\n1. Prerequisites check:")
    print(f"   [OK] synthetic beneficiaries: {bene_count:,}")
    print(f"   [OK] beneficiary prescriptions: {rx_count:,}")

    # Create ML schema
    db.execute("CREATE SCHEMA IF NOT EXISTS ml;")
    db.execute("DROP TABLE IF EXISTS ml.training_plan_pairs;")

    print("\n2. Generating beneficiary-plan pairs with cost-aligned objective...")
    print("   - Geographic constraints (state/county)")
    print("   - Tier-level cost rules (copay/coinsurance)")
    print("   - Deductible + insulin handling")
    if has_pricing:
        print("   - Gross drug cost from bronze.brz_pricing (UNIT_COST x quantity)")
        print(
            "   - Calibration policy: "
            f"winsor[{PRICING_WINSOR_LOW_Q:.2f},{PRICING_WINSOR_HIGH_Q:.2f}], "
            f"ratio[{PRICING_TO_HIST_RATIO_MIN:.2f}x,{PRICING_TO_HIST_RATIO_MAX:.2f}x], "
            f"blend_pricing={PRICING_HIST_BLEND_WEIGHT:.2f}, "
            f"annual_abs_cap=${PRICING_ANNUAL_ABS_MAX:,.0f}"
        )
    else:
        print("   - Gross drug cost fallback to synthetic estimated annual drug cost")

    if has_pricing:
        pricing_cte_sql = f"""
        pricing_map_raw AS (
            SELECT
                PLAN_KEY AS plan_key,
                REPLACE(REPLACE(TRIM(CAST(NDC AS VARCHAR)), '-', ''), ' ', '') AS ndc,
                COALESCE(
                    TRY_CAST(DAYS_SUPPLY_CODE AS INTEGER),
                    CASE
                        WHEN COALESCE(TRY_CAST(DAYS_SUPPLY AS INTEGER), 30) >= 75 THEN 2
                        WHEN COALESCE(TRY_CAST(DAYS_SUPPLY AS INTEGER), 30) >= 45 THEN 4
                        ELSE 1
                    END
                ) AS days_supply_code,
                GREATEST(COALESCE(TRY_CAST(UNIT_COST AS DOUBLE), 0.0), 0.0) AS unit_cost
            FROM bronze.brz_pricing
            WHERE PLAN_KEY IS NOT NULL AND NDC IS NOT NULL
        ),
        pricing_unit_cost_bounds AS (
            SELECT
                days_supply_code,
                quantile_cont(unit_cost, {PRICING_WINSOR_LOW_Q}) AS unit_cost_low,
                quantile_cont(unit_cost, {PRICING_WINSOR_HIGH_Q}) AS unit_cost_high
            FROM pricing_map_raw
            GROUP BY days_supply_code
        ),
        pricing_map AS (
            SELECT
                p.plan_key,
                p.ndc,
                p.days_supply_code,
                AVG(
                    LEAST(
                        GREATEST(
                            p.unit_cost,
                            COALESCE(b.unit_cost_low, p.unit_cost)
                        ),
                        COALESCE(b.unit_cost_high, p.unit_cost)
                    )
                ) AS unit_cost
            FROM pricing_map_raw p
            LEFT JOIN pricing_unit_cost_bounds b
              ON b.days_supply_code = p.days_supply_code
            GROUP BY p.plan_key, p.ndc, p.days_supply_code
        ),
        pricing_map_any AS (
            SELECT
                plan_key,
                ndc,
                AVG(unit_cost) AS unit_cost
            FROM pricing_map
            GROUP BY plan_key, ndc
        ),
"""
    else:
        pricing_cte_sql = """
        pricing_map AS (
            SELECT
                NULL::VARCHAR AS plan_key,
                NULL::VARCHAR AS ndc,
                NULL::INTEGER AS days_supply_code,
                NULL::DOUBLE AS unit_cost
            WHERE FALSE
        ),
        pricing_map_any AS (
            SELECT
                NULL::VARCHAR AS plan_key,
                NULL::VARCHAR AS ndc,
                NULL::DOUBLE AS unit_cost
            WHERE FALSE
        ),
"""

    create_sql = f"""
        CREATE TABLE ml.training_plan_pairs AS
        WITH base_pairs AS (
            SELECT
                -- Beneficiary identifiers
                b.bene_synth_id,
                b.state AS bene_state,
                b.county_code AS bene_county,
                b.zip_code AS bene_zip,
                b.density AS bene_density,

                b.state,
                b.county_code,
                b.zip_code,
                COALESCE(b.risk_segment, 'MED') AS risk_segment,
                COALESCE(b.unique_drugs, 3) AS unique_drugs,
                COALESCE(b.fills_target, 12) AS fills_target,
                COALESCE(b.total_rx_cost_est, 5000) AS total_rx_cost_est,
                COALESCE(b.insulin_user_flag, 0) AS bene_insulin_user,

                -- Plan attributes
                p.plan_key,
                p.formulary_id,
                CASE
                    WHEN p.IS_MA_PD THEN 'MA'
                    WHEN p.IS_PDP THEN 'PDP'
                    ELSE NULL
                END AS contract_type,
                CAST(p.PREMIUM AS DOUBLE) AS plan_premium,
                CAST(p.DEDUCTIBLE AS DOUBLE) AS plan_deductible,
                COALESCE(p.snp, 0) AS plan_snp,

                -- Formulary metrics
                COALESCE(fm.total_drugs, 0) AS formulary_total_drugs,
                COALESCE(fm.generic_tier_pct, 0) AS formulary_generic_pct,
                COALESCE(fm.specialty_tier_pct, 0) AS formulary_specialty_pct,
                COALESCE(fm.pa_rate, 0) AS formulary_pa_rate,
                COALESCE(fm.st_rate, 0) AS formulary_st_rate,
                COALESCE(fm.ql_rate, 0) AS formulary_ql_rate,
                COALESCE(fm.restrictiveness_class, 0) AS formulary_restrictiveness,

                -- Network metrics
                COALESCE(nm.total_pharmacies, 0) AS network_total_pharmacies,
                COALESCE(nm.preferred_pharmacies, 0) AS network_preferred_pharmacies,
                COALESCE(nm.network_adequacy_flag, 0) AS network_adequacy_flag,

                -- Distance features
                COALESCE(df.simulated_distance_miles, 10.0) AS distance_miles,
                COALESCE(df.distance_category, 'nearby') AS distance_category,
                CASE
                    WHEN COALESCE(df.simulated_distance_miles, 10.0) > 15 THEN 200.0
                    WHEN COALESCE(df.simulated_distance_miles, 10.0) > 8 THEN 100.0
                    ELSE 0.0
                END AS distance_penalty
            FROM synthetic.syn_beneficiary b
            JOIN bronze.brz_plan_info p
                ON b.state = p.state
               AND b.county_code = p.county_code
               AND (p.plan_suppressed_yn IS NULL OR p.plan_suppressed_yn != 'Y')
            LEFT JOIN gold.agg_plan_formulary_metrics fm
                ON p.plan_key = fm.plan_key
            LEFT JOIN gold.agg_plan_network_metrics nm
                ON p.plan_key = nm.plan_key
            LEFT JOIN ml.plan_distance_features df
                ON p.plan_key = df.plan_key
               AND b.county_code = df.county_code
        ),
        bene_rx AS (
            SELECT
                bene_synth_id,
                REPLACE(REPLACE(TRIM(CAST(ndc AS VARCHAR)), '-', ''), ' ', '') AS ndc,
                NULLIF(TRIM(CAST(rxcui AS VARCHAR)), '') AS rxcui,
                COALESCE(TRY_CAST(tier_level AS INTEGER), 1) AS rx_tier,
                GREATEST(COALESCE(TRY_CAST(fills_per_year AS DOUBLE), 1.0), 1.0) AS fills_per_year,
                COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) AS days_supply_mode,
                CASE
                    WHEN COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) >= 75 THEN 2   -- 90-day code
                    WHEN COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) >= 45 THEN 4   -- 60-day code
                    ELSE 1                                                                   -- 30-day code
                END AS days_supply_code,
                CASE
                    WHEN COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) >= 75 THEN 3.0
                    WHEN COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) >= 45 THEN 2.0
                    ELSE 1.0
                END AS days_supply_months,
                CASE
                    WHEN COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) >= 75 THEN 90.0
                    WHEN COALESCE(TRY_CAST(days_supply_mode AS INTEGER), 30) >= 45 THEN 60.0
                    ELSE 30.0
                END AS assumed_units_per_fill,
                GREATEST(COALESCE(TRY_CAST(qty_per_fill AS DOUBLE), 0.0), 0.0) AS qty_per_fill,
                GREATEST(COALESCE(estimated_annual_drug_cost, 0.0), 0.0) AS annual_gross_cost_fallback,
                COALESCE(TRY_CAST(is_insulin AS INTEGER), 0) AS is_insulin
            FROM synthetic.syn_beneficiary_prescriptions
            WHERE ndc IS NOT NULL
        ),
        formulary_map AS (
            SELECT
                FORMULARY_ID,
                REPLACE(REPLACE(TRIM(CAST(NDC AS VARCHAR)), '-', ''), ' ', '') AS ndc,
                MIN(COALESCE(TRY_CAST(TIER_LEVEL_VALUE AS INTEGER), 99)) AS formulary_tier,
                ANY_VALUE(NULLIF(TRIM(CAST(RXCUI AS VARCHAR)), '')) AS formulary_rxcui
            FROM bronze.brz_basic_formulary
            WHERE NDC IS NOT NULL
            GROUP BY FORMULARY_ID, ndc
        ),
        excluded_map AS (
            SELECT DISTINCT
                FORMULARY_ID,
                NULLIF(TRIM(CAST(RXCUI AS VARCHAR)), '') AS rxcui
            FROM bronze.brz_excluded_drugs
            WHERE FORMULARY_ID IS NOT NULL AND RXCUI IS NOT NULL
        ),
{pricing_cte_sql}
        cost_rules AS (
            SELECT
                PLAN_KEY,
                COALESCE(TRY_CAST(TIER AS INTEGER), 0) AS tier,
                COALESCE(TRY_CAST(DAYS_SUPPLY AS INTEGER), 1) AS days_supply_code,
                ANY_VALUE(COALESCE(TRY_CAST(COST_TYPE_PREF AS INTEGER), 0)) AS cost_type_pref,
                ANY_VALUE(COALESCE(TRY_CAST(COST_AMT_PREF AS DOUBLE), 0.0)) AS cost_amt_pref,
                MAX(
                    CASE
                        WHEN COALESCE(DEDUCTIBLE_APPLIES, FALSE) THEN 1
                        WHEN UPPER(COALESCE(CAST(DED_APPLIES_YN AS VARCHAR), 'N')) = 'Y' THEN 1
                        ELSE 0
                    END
                ) AS deductible_applies
            FROM bronze.brz_beneficiary_cost
            WHERE PLAN_KEY IS NOT NULL
              AND COALESCE(TRY_CAST(COVERAGE_LEVEL AS INTEGER), 1) = 1 -- initial coverage phase
            GROUP BY PLAN_KEY, tier, days_supply_code
        ),
        insulin_rules AS (
            SELECT
                PLAN_KEY,
                TRY_CAST(TIER AS INTEGER) AS tier,
                COALESCE(TRY_CAST(DAYS_SUPPLY AS INTEGER), 1) AS days_supply_code,
                AVG(COALESCE(copay_amt_pref_insln, MIN_INSULIN_COPAY, MAX_INSULIN_COPAY)) AS pref_insulin_copay
            FROM bronze.brz_insulin_cost
            WHERE PLAN_KEY IS NOT NULL
            GROUP BY PLAN_KEY, tier, days_supply_code
        ),
        pair_drug_base_raw AS (
            SELECT
                bp.bene_synth_id,
                bp.plan_key,
                bp.formulary_id,
                bp.plan_deductible,
                rx.ndc,
                rx.rxcui,
                rx.rx_tier,
                rx.days_supply_code,
                rx.days_supply_months,
                rx.fills_per_year,
                GREATEST(COALESCE(NULLIF(rx.qty_per_fill, 0.0), rx.assumed_units_per_fill), 1.0) AS units_per_fill,
                COALESCE(pm_exact.unit_cost, pm_any.unit_cost) AS unit_cost_resolved,
                GREATEST(rx.annual_gross_cost_fallback, 0.0) AS annual_gross_cost_fallback,
                CASE
                    WHEN COALESCE(pm_exact.unit_cost, pm_any.unit_cost, 0.0) > 0
                    THEN GREATEST(
                        rx.fills_per_year
                        * GREATEST(COALESCE(NULLIF(rx.qty_per_fill, 0.0), rx.assumed_units_per_fill), 1.0)
                        * COALESCE(pm_exact.unit_cost, pm_any.unit_cost),
                        0.0
                    )
                    ELSE NULL
                END AS annual_pricing_raw,
                rx.is_insulin,
                fm.formulary_tier,
                COALESCE(rx.rxcui, fm.formulary_rxcui) AS effective_rxcui,
                CASE WHEN fm.ndc IS NOT NULL THEN 1 ELSE 0 END AS in_formulary
            FROM base_pairs bp
            JOIN bene_rx rx
              ON bp.bene_synth_id = rx.bene_synth_id
            LEFT JOIN formulary_map fm
              ON fm.formulary_id = bp.formulary_id
             AND fm.ndc = rx.ndc
            LEFT JOIN pricing_map pm_exact
              ON pm_exact.plan_key = bp.plan_key
             AND pm_exact.ndc = rx.ndc
             AND pm_exact.days_supply_code = rx.days_supply_code
            LEFT JOIN pricing_map_any pm_any
              ON pm_any.plan_key = bp.plan_key
             AND pm_any.ndc = rx.ndc
        ),
        pair_drug_base AS (
            SELECT
                pdr.bene_synth_id,
                pdr.plan_key,
                pdr.formulary_id,
                pdr.plan_deductible,
                pdr.ndc,
                pdr.rxcui,
                pdr.rx_tier,
                pdr.days_supply_code,
                pdr.days_supply_months,
                pdr.fills_per_year,
                pdr.is_insulin,
                pdr.formulary_tier,
                pdr.effective_rxcui,
                pdr.in_formulary,
                CASE
                    WHEN pdr.annual_pricing_raw IS NULL THEN pdr.annual_gross_cost_fallback
                    ELSE GREATEST(
                        (
                            {PRICING_HIST_BLEND_WEIGHT}
                            * CASE
                                WHEN pdr.annual_gross_cost_fallback > 0 THEN LEAST(
                                    GREATEST(
                                        pdr.annual_pricing_raw,
                                        pdr.annual_gross_cost_fallback * {PRICING_TO_HIST_RATIO_MIN}
                                    ),
                                    pdr.annual_gross_cost_fallback * {PRICING_TO_HIST_RATIO_MAX},
                                    {PRICING_ANNUAL_ABS_MAX}
                                )
                                ELSE LEAST(pdr.annual_pricing_raw, {PRICING_ANNUAL_ABS_MAX})
                            END
                        ) + (
                            (1.0 - {PRICING_HIST_BLEND_WEIGHT})
                            * pdr.annual_gross_cost_fallback
                        ),
                        0.0
                    )
                END AS annual_gross_cost
            FROM pair_drug_base_raw pdr
        ),
        pair_drug_status AS (
            SELECT
                pdb.*,
                CASE
                    WHEN pdb.fills_per_year > 0
                    THEN GREATEST(pdb.annual_gross_cost / pdb.fills_per_year, 0.0)
                    ELSE 0.0
                END AS gross_per_fill,
                COALESCE(pdb.formulary_tier, pdb.rx_tier, 1) AS effective_tier,
                CASE WHEN ex.rxcui IS NOT NULL THEN 1 ELSE 0 END AS is_excluded,
                CASE
                    WHEN pdb.in_formulary = 1 AND ex.rxcui IS NULL THEN 1
                    ELSE 0
                END AS is_covered
            FROM pair_drug_base pdb
            LEFT JOIN excluded_map ex
              ON ex.formulary_id = pdb.formulary_id
             AND ex.rxcui = pdb.effective_rxcui
        ),
        pair_drug_cost_pre AS (
            SELECT
                pds.*,
                cr.cost_type_pref,
                cr.cost_amt_pref,
                COALESCE(cr.deductible_applies, 0) AS deductible_applies,
                COALESCE(ir_exact.pref_insulin_copay, ir_any.pref_insulin_copay) AS insulin_copay_pref,
                CASE
                    WHEN pds.annual_gross_cost <= 0 THEN 0.0
                    WHEN pds.is_covered = 0 THEN pds.gross_per_fill
                    WHEN pds.is_insulin = 1
                         AND COALESCE(ir_exact.pref_insulin_copay, ir_any.pref_insulin_copay) IS NOT NULL
                    THEN LEAST(
                        pds.gross_per_fill,
                        LEAST(
                            COALESCE(ir_exact.pref_insulin_copay, ir_any.pref_insulin_copay),
                            35.0 * pds.days_supply_months
                        )
                    )
                    WHEN COALESCE(cr.cost_type_pref, 0) = 1
                    THEN LEAST(pds.gross_per_fill, GREATEST(COALESCE(cr.cost_amt_pref, 0.0), 0.0))
                    WHEN COALESCE(cr.cost_type_pref, 0) = 2
                    THEN LEAST(
                        pds.gross_per_fill,
                        pds.gross_per_fill * CASE
                            WHEN COALESCE(cr.cost_amt_pref, 0.0) > 1.0
                                THEN COALESCE(cr.cost_amt_pref, 0.0) / 100.0
                            ELSE COALESCE(cr.cost_amt_pref, 0.0)
                        END
                    )
                    ELSE pds.gross_per_fill
                END AS per_fill_post_ded_cost
            FROM pair_drug_status pds
            LEFT JOIN cost_rules cr
              ON cr.plan_key = pds.plan_key
             AND cr.tier = pds.effective_tier
             AND cr.days_supply_code = pds.days_supply_code
            LEFT JOIN insulin_rules ir_exact
              ON ir_exact.plan_key = pds.plan_key
             AND ir_exact.days_supply_code = pds.days_supply_code
             AND ir_exact.tier = pds.effective_tier
            LEFT JOIN insulin_rules ir_any
              ON ir_any.plan_key = pds.plan_key
             AND ir_any.days_supply_code = pds.days_supply_code
             AND ir_any.tier IS NULL
        ),
        pair_drug_cost AS (
            SELECT
                pdcp.*,
                GREATEST(pdcp.fills_per_year * pdcp.per_fill_post_ded_cost, 0.0) AS annual_post_ded_cost,
                CASE
                    WHEN pdcp.annual_gross_cost > 0
                    THEN LEAST(
                        1.0,
                        GREATEST(0.0, (pdcp.fills_per_year * pdcp.per_fill_post_ded_cost) / pdcp.annual_gross_cost)
                    )
                    ELSE 1.0
                END AS effective_share_rate,
                CASE
                    WHEN pdcp.is_covered = 1 AND pdcp.deductible_applies = 1
                    THEN pdcp.annual_gross_cost
                    ELSE 0.0
                END AS ded_eligible_gross
            FROM pair_drug_cost_pre pdcp
        ),
        deductible_pool AS (
            SELECT
                bene_synth_id,
                plan_key,
                LEAST(MAX(COALESCE(plan_deductible, 0.0)), SUM(ded_eligible_gross)) AS deductible_pool_gross,
                SUM(ded_eligible_gross) AS ded_eligible_total_gross
            FROM pair_drug_cost
            GROUP BY bene_synth_id, plan_key
        ),
        pair_drug_oop AS (
            SELECT
                pdc.bene_synth_id,
                pdc.plan_key,
                pdc.is_covered,
                pdc.is_excluded,
                pdc.is_insulin,
                pdc.cost_type_pref,
                pdc.annual_gross_cost,
                pdc.annual_post_ded_cost,
                pdc.effective_share_rate,
                pdc.ded_eligible_gross,
                CASE
                    WHEN pdc.ded_eligible_gross > 0 AND COALESCE(dp.ded_eligible_total_gross, 0.0) > 0
                    THEN COALESCE(dp.deductible_pool_gross, 0.0) * (pdc.ded_eligible_gross / dp.ded_eligible_total_gross)
                    ELSE 0.0
                END AS deductible_alloc
            FROM pair_drug_cost pdc
            LEFT JOIN deductible_pool dp
              ON dp.bene_synth_id = pdc.bene_synth_id
             AND dp.plan_key = pdc.plan_key
        ),
        pair_cost_agg AS (
            SELECT
                pdo.bene_synth_id,
                pdo.plan_key,
                SUM(
                    pdo.annual_post_ded_cost
                    + (pdo.deductible_alloc * (1.0 - COALESCE(pdo.effective_share_rate, 1.0)))
                ) AS estimated_annual_oop,
                SUM(
                    CASE WHEN pdo.is_covered = 1 AND COALESCE(pdo.cost_type_pref, 0) = 1
                        THEN pdo.annual_post_ded_cost ELSE 0.0 END
                ) AS oop_copay_component,
                SUM(
                    CASE WHEN pdo.is_covered = 1 AND COALESCE(pdo.cost_type_pref, 0) = 2
                        THEN pdo.annual_post_ded_cost ELSE 0.0 END
                ) AS oop_coinsurance_component,
                SUM(
                    pdo.deductible_alloc * (1.0 - COALESCE(pdo.effective_share_rate, 1.0))
                ) AS oop_deductible_component,
                SUM(
                    CASE WHEN pdo.is_insulin = 1 AND pdo.is_covered = 1
                        THEN pdo.annual_post_ded_cost ELSE 0.0 END
                ) AS oop_insulin_component,
                SUM(
                    CASE WHEN pdo.is_covered = 0
                        THEN pdo.annual_post_ded_cost ELSE 0.0 END
                ) AS oop_uncovered_component,
                SUM(CASE WHEN pdo.is_covered = 1 THEN 1 ELSE 0 END) AS covered_drug_count,
                SUM(CASE WHEN pdo.is_covered = 0 THEN 1 ELSE 0 END) AS uncovered_drug_count
            FROM pair_drug_oop pdo
            GROUP BY pdo.bene_synth_id, pdo.plan_key
        )
        SELECT
            bp.bene_synth_id,
            bp.bene_state,
            bp.bene_county,
            bp.bene_zip,
            bp.bene_density,
            bp.state,
            bp.county_code,
            bp.zip_code,
            bp.risk_segment,
            bp.unique_drugs,
            bp.fills_target,
            bp.total_rx_cost_est,
            bp.bene_insulin_user,
            bp.plan_key,
            bp.contract_type,
            bp.plan_premium,
            bp.plan_deductible,
            bp.plan_snp,
            CAST(
                COALESCE(
                    pca.estimated_annual_oop,
                    LEAST(bp.total_rx_cost_est, COALESCE(bp.plan_deductible, 0.0) + (bp.fills_target * 20.0))
                ) AS DOUBLE
            ) AS estimated_annual_oop,
            CAST(COALESCE(pca.oop_copay_component, 0.0) AS DOUBLE) AS oop_copay_component,
            CAST(COALESCE(pca.oop_coinsurance_component, 0.0) AS DOUBLE) AS oop_coinsurance_component,
            CAST(COALESCE(pca.oop_deductible_component, 0.0) AS DOUBLE) AS oop_deductible_component,
            CAST(COALESCE(pca.oop_insulin_component, 0.0) AS DOUBLE) AS oop_insulin_component,
            CAST(COALESCE(pca.oop_uncovered_component, 0.0) AS DOUBLE) AS oop_uncovered_component,
            CAST(COALESCE(pca.covered_drug_count, 0) AS INTEGER) AS covered_drug_count,
            CAST(COALESCE(pca.uncovered_drug_count, 0) AS INTEGER) AS uncovered_drug_count,
            bp.formulary_total_drugs,
            bp.formulary_generic_pct,
            bp.formulary_specialty_pct,
            bp.formulary_pa_rate,
            bp.formulary_st_rate,
            bp.formulary_ql_rate,
            bp.formulary_restrictiveness,
            bp.network_total_pharmacies,
            bp.network_preferred_pharmacies,
            bp.network_adequacy_flag,
            bp.distance_miles,
            bp.distance_category,
            bp.distance_penalty,
            CAST((bp.plan_premium * 12.0) + COALESCE(
                pca.estimated_annual_oop,
                LEAST(bp.total_rx_cost_est, COALESCE(bp.plan_deductible, 0.0) + (bp.fills_target * 20.0))
            ) AS DOUBLE) AS total_annual_cost,
            CAST(
                ((bp.plan_premium * 12.0) + COALESCE(
                    pca.estimated_annual_oop,
                    LEAST(bp.total_rx_cost_est, COALESCE(bp.plan_deductible, 0.0) + (bp.fills_target * 20.0))
                ) + bp.distance_penalty) AS DOUBLE
            ) AS total_cost_with_distance,
            CAST(
                ((bp.plan_premium * 12.0) + COALESCE(
                    pca.estimated_annual_oop,
                    LEAST(bp.total_rx_cost_est, COALESCE(bp.plan_deductible, 0.0) + (bp.fills_target * 20.0))
                ) + bp.distance_penalty) AS DOUBLE
            ) AS ranking_cost_objective,
            CASE
                WHEN COALESCE(
                    pca.estimated_annual_oop,
                    LEAST(bp.total_rx_cost_est, COALESCE(bp.plan_deductible, 0.0) + (bp.fills_target * 20.0))
                ) < 2000
                 AND bp.distance_miles > 12
                THEN TRUE
                ELSE FALSE
            END AS has_distance_tradeoff,
            CURRENT_TIMESTAMP AS ml_ts
        FROM base_pairs bp
        LEFT JOIN pair_cost_agg pca
          ON bp.bene_synth_id = pca.bene_synth_id
         AND bp.plan_key = pca.plan_key;
    """

    db.execute(create_sql)

    # Create indexes
    print("\n3. Creating indexes...")
    db.execute("CREATE INDEX idx_train_bene ON ml.training_plan_pairs(bene_synth_id);")
    db.execute("CREATE INDEX idx_train_plan ON ml.training_plan_pairs(plan_key);")
    db.execute("CREATE INDEX idx_train_county ON ml.training_plan_pairs(bene_county);")

    # Validate
    print("\n4. Validation...")
    stats = db.query_df(
        """
        SELECT
            COUNT(*) AS total_pairs,
            COUNT(DISTINCT bene_synth_id) AS unique_benes,
            COUNT(DISTINCT plan_key) AS unique_plans,
            ROUND(AVG(estimated_annual_oop), 2) AS avg_oop,
            ROUND(quantile_cont(estimated_annual_oop, 0.50), 2) AS p50_oop,
            ROUND(quantile_cont(estimated_annual_oop, 0.90), 2) AS p90_oop,
            ROUND(quantile_cont(estimated_annual_oop, 0.99), 2) AS p99_oop,
            ROUND(AVG(oop_copay_component), 2) AS avg_oop_copay,
            ROUND(AVG(oop_coinsurance_component), 2) AS avg_oop_coins,
            ROUND(AVG(oop_deductible_component), 2) AS avg_oop_deductible,
            ROUND(AVG(oop_uncovered_component), 2) AS avg_oop_uncovered,
            ROUND(
                100.0 * SUM(CASE WHEN estimated_annual_oop > 20000 THEN 1 ELSE 0 END) / COUNT(*),
                2
            ) AS pct_oop_gt_20k,
            ROUND(AVG(distance_miles), 2) AS avg_distance,
            SUM(CASE WHEN has_distance_tradeoff THEN 1 ELSE 0 END) AS tradeoff_pairs,
            ROUND(100.0 * SUM(CASE WHEN has_distance_tradeoff THEN 1 ELSE 0 END) / COUNT(*), 2) AS tradeoff_pct
        FROM ml.training_plan_pairs;
        """
    )

    pairs_per_bene = stats["total_pairs"][0] / stats["unique_benes"][0] if stats["unique_benes"][0] > 0 else 0

    print("\n[OK] Training pairs generated:")
    print(f"  - Total pairs: {stats['total_pairs'][0]:,}")
    print(f"  - Unique beneficiaries: {stats['unique_benes'][0]:,}")
    print(f"  - Unique plans: {stats['unique_plans'][0]:,}")
    print(f"  - Avg plans per beneficiary: {pairs_per_bene:.1f}")
    print("\n  Cost objective components:")
    print(f"  - Avg estimated OOP: ${stats['avg_oop'][0]:,.2f}")
    print(
        "  - OOP quantiles (p50/p90/p99): "
        f"${stats['p50_oop'][0]:,.2f} / ${stats['p90_oop'][0]:,.2f} / ${stats['p99_oop'][0]:,.2f}"
    )
    print(f"  - Avg OOP copay component: ${stats['avg_oop_copay'][0]:,.2f}")
    print(f"  - Avg OOP coinsurance component: ${stats['avg_oop_coins'][0]:,.2f}")
    print(f"  - Avg OOP deductible component: ${stats['avg_oop_deductible'][0]:,.2f}")
    print(f"  - Avg OOP uncovered component: ${stats['avg_oop_uncovered'][0]:,.2f}")
    print(f"  - Share OOP > $20k: {stats['pct_oop_gt_20k'][0]}%")
    print(f"  - Avg distance: {stats['avg_distance'][0]} miles")
    print(f"  - Pairs with tradeoff: {stats['tradeoff_pairs'][0]:,} ({stats['tradeoff_pct'][0]}%)")

    return True


if __name__ == "__main__":
    success = generate_training_pairs()
    sys.exit(0 if success else 1)
