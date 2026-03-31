"""
Medicare Part D Plan Recommendation - Streamlit Web Application (Interactive Mode)

NEW: Real-time inference for new beneficiaries!

Features:
- Input beneficiary profile (location, drugs, conditions)
- Real-time ML model inference
- Plan ranking and recommendations
- Cost analysis and explanations

User Workflow:
1. Enter beneficiary profile information
2. System queries available plans in that location
3. ML model ranks plans in real-time
4. Display top 5 recommendations with explanations
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import pickle
import json
import hashlib
import uuid

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.db_manager import get_db
from utils.drug_input import normalize_ndc_token, summarize_medication_rows
from recommendation_engine.plan_data import (
    deduplicate_plan_candidates,
    fetch_plan_drug_coverage,
    fetch_plans_for_service_area,
)
from recommendation_engine.decision_support import (
    DEFAULT_RESEARCH_SEED,
    MedicationListItem,
    PreferenceWeights,
    ProfileInput,
    as_public_types,
    build_recommendation_schema,
    classify_coverage_status,
    compute_feature_coverage,
    compute_heuristic_score,
    create_run_audit,
    estimate_plan_oop_with_breakdown,
    serialize_nested_columns,
)
from recommendation_engine.research_eval import (
    build_data_coverage_diagnostics,
    build_fairness_tables,
    compute_preference_stability,
    evaluate_model_vs_baseline,
    load_research_sample,
    score_research_sample,
)

# ===== Page Configuration =====
st.set_page_config(
    page_title="Medicare Part D Plan Finder (ML)",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== Custom CSS =====
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .plan-card {
        border: 2px solid #e0e0e0;
        border-radius: 10px;
        padding: 1.5rem;
        margin: 1rem 0;
        background-color: #f8f9fa;
    }
    .cost-highlight {
        font-size: 2rem;
        font-weight: bold;
        color: #2ca02c;
    }
    .warning-badge {
        background-color: #ff7f0e;
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 5px;
        font-weight: bold;
    }
    .info-badge {
        background-color: #1f77b4;
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)


# ===== Database Connection =====
@st.cache_resource
def get_database():
    """Get database connection."""
    return get_db(read_only=True)


# ===== Load Trained Model =====
@st.cache_resource
def load_trained_model():
    """
    Load pre-trained LightGBM ranking model.
    
    Returns:
        dict: Model data with model, feature_names, and training_stats
    """
    model_path = Path('models/plan_ranker.pkl')
    
    if not model_path.exists():
        return None
    
    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)
    
    return model_data


# ===== Distance Calculation Utilities =====
def calculate_distance(lat1, lng1, lat2, lng2):
    """
    Calculate distance between two points using Haversine formula.
    
    Args:
        lat1, lng1: First point coordinates
        lat2, lng2: Second point coordinates
    
    Returns:
        float: Distance in miles
    """
    import math
    
    R = 3959  # Earth radius in miles
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c


def categorize_distance(miles):
    """
    Categorize distance for display purposes.
    Matches categories used in training data.
    
    Args:
        miles (float): Distance in miles
    
    Returns:
        str: Distance category
    """
    if miles == 0 or miles < 0.5:
        return "Local"
    elif miles <= 10:
        return "Nearby"
    elif miles <= 25:
        return "Moderate"
    else:
        return "Distant"


def calculate_distance_penalty(miles, distance_weight=50.0):
    """
    Calculate distance penalty in dollars.
    Base thresholds match training data:
    - >15 miles: $200
    - >8 miles: $100
    - else: $0
    User-configured weight scales the base penalty.
    
    Args:
        miles (float): Distance in miles
        distance_weight (float): Penalty weighting factor (50 = training default)
    
    Returns:
        float: Distance penalty in dollars
    """
    if miles > 15:
        base_penalty = 200.0
    elif miles > 8:
        base_penalty = 100.0
    else:
        base_penalty = 0.0
    return base_penalty * (float(distance_weight) / 50.0)


def normalize_series(series, higher_is_better=True):
    """Normalize a numeric series to [0, 1]."""
    s = pd.to_numeric(series, errors='coerce').replace([np.inf, -np.inf], np.nan)

    if s.isna().all():
        norm = pd.Series(0.5, index=series.index)
    else:
        min_val = s.min()
        max_val = s.max()
        if pd.isna(min_val) or pd.isna(max_val) or np.isclose(min_val, max_val):
            norm = pd.Series(0.5, index=series.index)
        else:
            norm = (s - min_val) / (max_val - min_val)
        fill_value = float(norm.median()) if not pd.isna(norm.median()) else 0.5
        norm = norm.fillna(fill_value)

    return norm if higher_is_better else (1.0 - norm)


def build_decision_weights(ml_influence_pct, cost_priority, access_priority, coverage_priority):
    """
    Build normalized weights for final decision score.
    ML influence is explicit; the remaining share is split by user priorities.
    """
    ml_weight = max(0.0, min(100.0, float(ml_influence_pct))) / 100.0
    non_ml_weight = 1.0 - ml_weight

    total_priority = max(1.0, float(cost_priority + access_priority + coverage_priority))
    cost_weight = non_ml_weight * (float(cost_priority) / total_priority)
    access_weight = non_ml_weight * (float(access_priority) / total_priority)
    coverage_weight = non_ml_weight * (float(coverage_priority) / total_priority)

    return {
        'ml': ml_weight,
        'cost': cost_weight,
        'access': access_weight,
        'coverage': coverage_weight
    }


def estimate_plan_oop(beneficiary_profile, plan):
    """
    Estimate annual out-of-pocket cost using plan and beneficiary features.
    This is still approximate but materially better than a flat 30% assumption.
    """
    base_rx_cost = float(beneficiary_profile['total_rx_cost_est'])
    num_drugs = max(int(beneficiary_profile['num_drugs']), 1)
    fills_per_year = max(float(beneficiary_profile['avg_fills_per_year']), 1.0)
    is_insulin_user = bool(beneficiary_profile['is_insulin_user'])

    generic_pct = float(plan.get('formulary_generic_pct', 0) or 0)
    specialty_pct = float(plan.get('formulary_specialty_pct', 0) or 0)
    pa_rate = float(plan.get('formulary_pa_rate', 0) or 0)
    st_rate = float(plan.get('formulary_st_rate', 0) or 0)
    ql_rate = float(plan.get('formulary_ql_rate', 0) or 0)
    deductible = float(plan.get('deductible', 0) or 0)
    network_adequacy_flag = int(plan.get('network_adequacy_flag', 0) or 0)

    # Support both percent-style (0-100) and ratio-style (0-1) inputs.
    generic_ratio = np.clip(generic_pct / 100.0 if generic_pct > 1 else generic_pct, 0.0, 1.0)
    specialty_ratio = np.clip(specialty_pct / 100.0 if specialty_pct > 1 else specialty_pct, 0.0, 1.0)
    pa_ratio = np.clip(pa_rate / 100.0 if pa_rate > 1 else pa_rate, 0.0, 1.0)
    st_ratio = np.clip(st_rate / 100.0 if st_rate > 1 else st_rate, 0.0, 1.0)
    ql_ratio = np.clip(ql_rate / 100.0 if ql_rate > 1 else ql_rate, 0.0, 1.0)

    # Beneficiary intensity term.
    drug_intensity = 0.20 + min(0.25, 0.01 * (num_drugs - 1)) + min(0.15, 0.002 * max(fills_per_year - 12, 0))

    # Plan affordability terms.
    generic_discount = 1.0 - (0.22 * generic_ratio)
    specialty_penalty = 1.0 + (0.25 * specialty_ratio)
    restriction_penalty = 1.0 + (0.18 * pa_ratio) + (0.12 * st_ratio) + (0.08 * ql_ratio)
    deductible_penalty = 1.0 + (min(deductible, 1000.0) / 3500.0)
    network_penalty = 1.08 if network_adequacy_flag == 1 else 1.0
    insulin_adjustment = 0.92 if is_insulin_user else 1.0

    estimated_oop = (
        base_rx_cost
        * drug_intensity
        * generic_discount
        * specialty_penalty
        * restriction_penalty
        * deductible_penalty
        * network_penalty
        * insulin_adjustment
    )

    # Deductible contributes partially; avoid over-penalizing high-deductible plans.
    estimated_oop += min(deductible, 800.0) * 0.35

    return float(max(50.0, estimated_oop))


def compute_decision_support_scores(plans_df, decision_weights):
    """Compute transparent multi-factor decision support scores."""
    ranked = plans_df.copy()

    ranked['ml_component'] = normalize_series(ranked['score'], higher_is_better=True)
    ranked['cost_component'] = normalize_series(ranked['total_cost_with_distance'], higher_is_better=False)

    distance_source = ranked['nearest_preferred_miles'] if 'nearest_preferred_miles' in ranked.columns else ranked['distance_miles']
    nearby_source = ranked['preferred_within_10mi'] if 'preferred_within_10mi' in ranked.columns else ranked['network_preferred_pharmacies']
    accessibility_source = ranked['network_accessibility_score'] if 'network_accessibility_score' in ranked.columns else pd.Series(50.0, index=ranked.index)

    distance_component = normalize_series(distance_source, higher_is_better=False)
    nearby_component = normalize_series(nearby_source, True)
    accessibility_component = normalize_series(accessibility_source, True)
    ranked['access_component'] = (0.5 * distance_component) + (0.3 * nearby_component) + (0.2 * accessibility_component)

    generic_component = normalize_series(ranked['formulary_generic_pct'], True)
    restrictive_component = normalize_series(ranked['formulary_restrictiveness'], False)
    pa_component = normalize_series(ranked['formulary_pa_rate'], False)
    st_component = normalize_series(ranked['formulary_st_rate'], False)
    ql_component = normalize_series(ranked['formulary_ql_rate'], False)
    baseline_coverage_component = (
        0.35 * generic_component
        + 0.25 * restrictive_component
        + 0.20 * pa_component
        + 0.10 * st_component
        + 0.10 * ql_component
    )
    if 'drug_coverage_pct' in ranked.columns:
        requested_drug_component = normalize_series(ranked['drug_coverage_pct'], True)
        ranked['coverage_component'] = 0.55 * requested_drug_component + 0.45 * baseline_coverage_component
    else:
        ranked['coverage_component'] = baseline_coverage_component

    ranked['decision_score'] = 100 * (
        decision_weights['ml'] * ranked['ml_component']
        + decision_weights['cost'] * ranked['cost_component']
        + decision_weights['access'] * ranked['access_component']
        + decision_weights['coverage'] * ranked['coverage_component']
    )

    def build_reason(row):
        signals = []
        if row['cost_component'] >= 0.70:
            signals.append("strong total cost")
        if row['access_component'] >= 0.70:
            signals.append("good pharmacy access")
        if row['coverage_component'] >= 0.70:
            signals.append("favorable formulary profile")
        if 'drug_coverage_pct' in row and float(row['drug_coverage_pct']) >= 90:
            signals.append("high requested-drug coverage")
        if row['ml_component'] >= 0.70:
            signals.append("high ML suitability")
        return ", ".join(signals[:2]) if signals else "balanced across factors"

    ranked['decision_reason'] = ranked.apply(build_reason, axis=1)
    return ranked


# ===== Get Location Data from dim_zipcode =====
@st.cache_data
def get_states():
    """Get list of state names from dim_zipcode."""
    db = get_database()
    query = "SELECT DISTINCT state FROM gold.dim_zipcode ORDER BY state"
    states = db.query_df(query)['state'].tolist()
    return states


@st.cache_data
def get_counties(state_name):
    """Get counties for a state (full names)."""
    db = get_database()
    query = """
        SELECT DISTINCT county 
        FROM gold.dim_zipcode 
        WHERE state = ?
        ORDER BY county
    """
    counties = db.query_df(query, [state_name])['county'].tolist()
    return counties


@st.cache_data
def get_location_from_zip(zip_code):
    """Get state and county from zip code."""
    db = get_database()
    query = """
        SELECT state, county, city
        FROM gold.dim_zipcode
        WHERE zip_code = ?
        LIMIT 1
    """
    result = db.query_df(query, [zip_code])
    if len(result) > 0:
        return result.iloc[0].to_dict()
    return None


@st.cache_data
def get_default_zip_for_county(state_name, county_name):
    """Get a representative ZIP code for county-level search fallback."""
    db = get_database()
    query = """
        SELECT zip_code
        FROM gold.dim_zipcode
        WHERE state = ?
          AND county = ?
          AND zip_code IS NOT NULL
        ORDER BY population DESC NULLS LAST, zip_code
        LIMIT 1
    """
    result = db.query_df(query, [state_name, county_name])
    if len(result) == 0:
        return None
    return str(result['zip_code'].iloc[0])


@st.cache_data
def get_state_county_codes(state_name, county_name):
    """Get state and county codes for querying plans."""
    db = get_database()
    # Get state code (2-letter)
    state_query = """
        SELECT DISTINCT z.state as state_name, p.STATE as state_code
        FROM gold.dim_zipcode z
        JOIN bronze.brz_plan_info p ON z.county_code = p.COUNTY_CODE
        WHERE z.state = ?
        LIMIT 1
    """
    state_result = db.query_df(state_query, [state_name])
    
    if len(state_result) == 0:
        return None, None
    
    state_code = state_result['state_code'].iloc[0]
    
    # Get county code
    county_query = """
        SELECT DISTINCT county_code
        FROM gold.dim_zipcode
        WHERE state = ? AND county = ?
        LIMIT 1
    """
    county_result = db.query_df(county_query, [state_name, county_name])
    
    if len(county_result) == 0:
        return state_code, None
    
    county_code_raw = county_result['county_code'].iloc[0]
    try:
        county_code = str(int(float(county_code_raw))).zfill(5)
    except (TypeError, ValueError):
        county_code = str(county_code_raw)
    
    return state_code, county_code


@st.cache_data
def get_nearby_counties(user_zip, state_name, max_distance_miles=50):
    """
    Get nearby counties within distance threshold.
    
    Args:
        user_zip (str): User's zip code
        state_name (str): State name
        max_distance_miles (int): Maximum distance in miles
    
    Returns:
        pd.DataFrame: Nearby counties with distance
    """
    if not user_zip:
        return pd.DataFrame()

    db = get_database()
    
    # SQL query with Haversine formula
    query = """
    WITH beneficiary_loc AS (
        SELECT AVG(lat) as lat, AVG(lng) as lng
        FROM gold.dim_zipcode
        WHERE zip_code = ?
    ),
    county_centers AS (
        SELECT 
            county_code,
            state,
            county,
            AVG(lat) as clat,
            AVG(lng) as clng
        FROM gold.dim_zipcode
        WHERE state = ?
        GROUP BY county_code, state, county
    ),
    county_distances AS (
        SELECT 
            cc.county_code,
            cc.county,
            cc.state,
            -- Haversine formula in SQL (result in km, convert to miles)
            ROUND(
                6371 * ACOS(
                    GREATEST(-1, LEAST(1,
                        COS(RADIANS(bl.lat)) * COS(RADIANS(cc.clat)) *
                        COS(RADIANS(cc.clng) - RADIANS(bl.lng)) +
                        SIN(RADIANS(bl.lat)) * SIN(RADIANS(cc.clat))
                    ))
                ) * 0.621371,  -- Convert km to miles
                1
            ) as distance_miles
        FROM county_centers cc
        CROSS JOIN beneficiary_loc bl
        WHERE bl.lat IS NOT NULL AND bl.lng IS NOT NULL
    )
    SELECT *
    FROM county_distances
    WHERE distance_miles <= ?
    ORDER BY distance_miles
    """
    
    nearby = db.query_df(query, [user_zip, state_name, max_distance_miles])
    return nearby


# ===== Get Available Plans for Location =====
@st.cache_data(ttl=3600)
def get_plans_for_location(state, county_code):
    """
    Get available plans for a specific location.
    
    Args:
        state (str): State code
        county_code (str): County code
    
    Returns:
        pd.DataFrame: Available plans with features
    """
    db = get_database()
    return fetch_plans_for_service_area(db, state, county_code)


@st.cache_data(ttl=1800)
def search_drug_catalog(query_text, limit=20):
    """
    Search beneficiary-level synthetic prescriptions by drug name or NDC.

    Returns aggregated default attributes for UI prefill.
    """
    query_text_raw = str(query_text or "").strip()
    query_text = query_text_raw.lower()
    ndc_digits = "".join(ch for ch in query_text_raw if ch.isdigit())
    if len(query_text) < 2 and len(ndc_digits) < 4:
        return pd.DataFrame(columns=[
            "drug_name",
            "ndc",
            "tier_level",
            "days_supply_mode",
            "fills_per_year",
            "is_insulin",
            "annual_cost_est",
        ])

    db = get_database()
    like_value = f"%{query_text}%"
    like_ndc = f"%{ndc_digits}%" if len(ndc_digits) >= 4 else "__NO_MATCH__"
    try:
        sql = """
            WITH matched AS (
                SELECT
                    CAST(
                        COALESCE(
                            NULLIF(drug_name, ''),
                            NULLIF(drug_synonym, ''),
                            CONCAT('NDC ', ndc)
                        ) AS VARCHAR
                    ) AS drug_name,
                    CAST(ndc AS VARCHAR) AS ndc,
                    TRY_CAST(tier_level AS INTEGER) AS tier_level,
                    TRY_CAST(days_supply_mode AS INTEGER) AS days_supply_mode,
                    TRY_CAST(fills_per_year AS DOUBLE) AS fills_per_year,
                    TRY_CAST(is_insulin AS INTEGER) AS is_insulin,
                    TRY_CAST(estimated_annual_drug_cost AS DOUBLE) AS annual_cost_est
                FROM synthetic.syn_beneficiary_prescriptions
                WHERE (
                        LOWER(COALESCE(drug_name, '')) LIKE ?
                        OR LOWER(COALESCE(drug_synonym, '')) LIKE ?
                        OR CAST(ndc AS VARCHAR) LIKE ?
                      )
                  AND ndc IS NOT NULL
            )
            SELECT
                CAST(drug_name AS VARCHAR) AS drug_name,
                CAST(ndc AS VARCHAR) AS ndc,
                CAST(COALESCE(MIN(tier_level), 1) AS INTEGER) AS tier_level,
                CAST(
                    CASE
                        WHEN COALESCE(ROUND(AVG(days_supply_mode)), 30) >= 75 THEN 90
                        WHEN COALESCE(ROUND(AVG(days_supply_mode)), 30) >= 45 THEN 60
                        ELSE 30
                    END AS INTEGER
                ) AS days_supply_mode,
                CAST(COALESCE(ROUND(AVG(fills_per_year), 1), 12.0) AS DOUBLE) AS fills_per_year,
                CAST(COALESCE(MAX(is_insulin), 0) AS INTEGER) AS is_insulin,
                CAST(COALESCE(ROUND(AVG(annual_cost_est), 2), 0.0) AS DOUBLE) AS annual_cost_est
            FROM matched
            GROUP BY drug_name, ndc
            ORDER BY drug_name, ndc
            LIMIT ?
        """
        return db.query_df(sql, [like_value, like_value, like_ndc, int(limit)])
    except Exception:
        # Fallback for older table versions with fewer columns.
        try:
            fallback_sql = """
                SELECT
                    CAST(COALESCE(NULLIF(drug_name, ''), CONCAT('NDC ', ndc)) AS VARCHAR) AS drug_name,
                    CAST(ndc AS VARCHAR) AS ndc,
                    CAST(1 AS INTEGER) AS tier_level,
                    CAST(30 AS INTEGER) AS days_supply_mode,
                    CAST(12.0 AS DOUBLE) AS fills_per_year,
                    CAST(0 AS INTEGER) AS is_insulin,
                    CAST(0.0 AS DOUBLE) AS annual_cost_est
                FROM synthetic.syn_beneficiary_prescriptions
                WHERE (
                        LOWER(COALESCE(drug_name, '')) LIKE ?
                        OR CAST(ndc AS VARCHAR) LIKE ?
                      )
                  AND ndc IS NOT NULL
                GROUP BY 1, 2
                ORDER BY drug_name, ndc
                LIMIT ?
            """
            return db.query_df(fallback_sql, [like_value, like_ndc, int(limit)])
        except Exception:
            return pd.DataFrame(columns=[
                "drug_name",
                "ndc",
                "tier_level",
                "days_supply_mode",
                "fills_per_year",
                "is_insulin",
                "annual_cost_est",
            ])


@st.cache_data(ttl=1800)
def get_plan_drug_coverage(plan_keys, requested_ndcs):
    """
    Compute requested-drug coverage for each candidate plan.

    Args:
        plan_keys (tuple[str]): Candidate PLAN_KEYs
        requested_ndcs (tuple[str]): Requested drug NDCs

    Returns:
        pd.DataFrame: PLAN_KEY-level requested-drug coverage summary
    """
    db = get_database()
    return fetch_plan_drug_coverage(db, plan_keys, requested_ndcs)


@st.cache_data(ttl=3600)
def get_plans_with_fallback(state_code, county_code, user_zip, state_name, county_name, 
                             include_nearby=True, max_distance=50):
    """
    Get plans with fallback to nearby counties if needed.
    
    Args:
        state_code (str): State code (2-letter)
        county_code (str): County code
        user_zip (str): User's zip code
        state_name (str): Full state name
        county_name (str): Full county name
        include_nearby (bool): Whether to search nearby if no local plans
        max_distance (int): Maximum distance for nearby search
    
    Returns:
        tuple: (plans_df, search_info dict)
    """
    # Try local county first
    local_plans = get_plans_for_location(state_code, county_code)
    
    if len(local_plans) > 0:
        # Found local plans
        local_plans['is_local'] = True
        local_plans['distance_miles'] = 0.0
        local_plans['distance_category'] = 'Local'
        
        search_info = {
            'local_count': len(local_plans),
            'nearby_count': 0,
            'searched_nearby': False
        }
        
        return local_plans, search_info
    
    # No local plans found
    if not include_nearby:
        search_info = {
            'local_count': 0,
            'nearby_count': 0,
            'searched_nearby': False,
            'fallback_reason': 'Nearby county search disabled'
        }
        return pd.DataFrame(), search_info

    if not user_zip:
        search_info = {
            'local_count': 0,
            'nearby_count': 0,
            'searched_nearby': True,
            'fallback_reason': 'No ZIP code available for nearby county distance search'
        }
        return pd.DataFrame(), search_info
    
    # Search nearby counties
    nearby_counties = get_nearby_counties(user_zip, state_name, max_distance)
    
    if len(nearby_counties) == 0:
        search_info = {
            'local_count': 0,
            'nearby_count': 0,
            'searched_nearby': True,
            'fallback_reason': f'No nearby counties within {max_distance} miles'
        }
        return pd.DataFrame(), search_info
    
    # Get plans from all nearby counties
    all_plans = []
    
    for _, county_row in nearby_counties.iterrows():
        # Get state code for this county
        nearby_state_code, nearby_county_code = get_state_county_codes(
            county_row['state'], 
            county_row['county']
        )
        
        if not nearby_county_code:
            continue
        
        # Get plans for this county
        county_plans = get_plans_for_location(nearby_state_code, str(nearby_county_code))
        
        if len(county_plans) > 0:
            county_plans['is_local'] = False
            county_plans['distance_miles'] = county_row['distance_miles']
            county_plans['distance_category'] = categorize_distance(county_row['distance_miles'])
            county_plans['location_county'] = county_row['county']
            
            all_plans.append(county_plans)
    
    if len(all_plans) == 0:
        search_info = {
            'local_count': 0,
            'nearby_count': 0,
            'searched_nearby': True,
            'counties_searched': len(nearby_counties),
            'fallback_reason': 'Nearby counties found but none had available plans'
        }
        return pd.DataFrame(), search_info
    
    # Combine all plans
    combined_plans = pd.concat(all_plans, ignore_index=True)
    
    search_info = {
        'local_count': 0,
        'nearby_count': len(combined_plans),
        'searched_nearby': True,
        'counties_searched': len(nearby_counties),
        'counties_with_plans': len(all_plans)
    }
    
    return combined_plans, search_info


# ===== Pharmacy-Level Network Analysis =====
@st.cache_data(ttl=3600)
def get_plan_pharmacy_distances(plan_keys, user_zip):
    """
    Calculate distance from user's zip to all pharmacies for given plans.
    Uses bronze.brz_pharmacy_network for zip-code-level accuracy.
    
    Args:
        plan_keys (list): List of PLAN_KEY values
        user_zip (str): User's zip code
    
    Returns:
        pd.DataFrame: Pharmacy distances with columns:
            PLAN_KEY, PHARMACY_ZIPCODE, distance_miles, 
            IS_PREFERRED_RETAIL, IS_IN_AREA, PHARMACY_TYPE
    """
    if not plan_keys or not user_zip:
        return pd.DataFrame()
    
    db = get_database()
    
    # Get user location
    user_loc = db.query_df("""
        SELECT lat, lng
        FROM gold.dim_zipcode
        WHERE zip_code = ?
        LIMIT 1
    """, [user_zip])
    
    if len(user_loc) == 0:
        return pd.DataFrame()
    
    user_lat, user_lng = user_loc.iloc[0]['lat'], user_loc.iloc[0]['lng']
    
    # Build plan key placeholders for parameterized IN clause
    plan_placeholders = ", ".join(["?"] * len(plan_keys))
    
    # Get pharmacies and calculate distances
    query = f"""
    SELECT 
        pn.PLAN_KEY,
        pn.PHARMACY_ZIPCODE,
        pn.IS_PREFERRED_RETAIL,
        pn.IS_PREFERRED_MAIL,
        pn.IS_IN_AREA,
        pn.PHARMACY_TYPE,
        z.lat as pharmacy_lat,
        z.lng as pharmacy_lng,
        z.city as pharmacy_city,
        z.state as pharmacy_state,
        -- Haversine distance calculation
        ROUND(
            6371 * ACOS(
                GREATEST(-1, LEAST(1,
                    COS(RADIANS(?)) * COS(RADIANS(z.lat)) *
                    COS(RADIANS(z.lng) - RADIANS(?)) +
                    SIN(RADIANS(?)) * SIN(RADIANS(z.lat))
                ))
            ) * 0.621371,  -- Convert km to miles
            2
        ) as distance_miles
    FROM bronze.brz_pharmacy_network pn
    JOIN gold.dim_zipcode z ON pn.PHARMACY_ZIPCODE = z.zip_code
    WHERE pn.PLAN_KEY IN ({plan_placeholders})
        AND pn.PHARMACY_TYPE = 'retail'  -- Focus on retail pharmacies
    ORDER BY pn.PLAN_KEY, distance_miles
    """
    
    params = [float(user_lat), float(user_lng), float(user_lat)] + list(plan_keys)
    pharmacy_dist = db.query_df(query, params)
    return pharmacy_dist


def calculate_plan_network_metrics(pharmacy_distances_df):
    """
    Calculate network accessibility metrics from pharmacy distances.
    
    Args:
        pharmacy_distances_df (pd.DataFrame): Output from get_plan_pharmacy_distances
    
    Returns:
        pd.DataFrame: One row per plan with network metrics
    """
    if len(pharmacy_distances_df) == 0:
        return pd.DataFrame()
    
    metrics = []
    
    for plan_key in pharmacy_distances_df['PLAN_KEY'].unique():
        plan_pharmacies = pharmacy_distances_df[
            pharmacy_distances_df['PLAN_KEY'] == plan_key
        ]
        
        # Filter preferred pharmacies
        preferred = plan_pharmacies[plan_pharmacies['IS_PREFERRED_RETAIL'] == True]
        
        # Calculate metrics
        nearest_pref = preferred['distance_miles'].min() if len(preferred) > 0 else 999
        pref_5mi = len(preferred[preferred['distance_miles'] <= 5])
        pref_10mi = len(preferred[preferred['distance_miles'] <= 10])
        total_10mi = len(plan_pharmacies[plan_pharmacies['distance_miles'] <= 10])
        avg_pref_dist = preferred['distance_miles'].mean() if len(preferred) > 0 else 999
        
        # Calculate accessibility score (0-100)
        # Higher is better: closer pharmacies + more coverage
        distance_score = 100 / (1 + nearest_pref / 5)  # Max 100, decreases with distance
        coverage_score = min(50, pref_10mi * 5)  # Up to 50 points for coverage
        accessibility = min(100, distance_score + coverage_score)
        
        metrics.append({
            'PLAN_KEY': plan_key,
            'nearest_preferred_miles': nearest_pref,
            'preferred_within_5mi': pref_5mi,
            'preferred_within_10mi': pref_10mi,
            'total_within_10mi': total_10mi,
            'avg_preferred_distance': avg_pref_dist,
            'network_accessibility_score': accessibility
        })
    
    return pd.DataFrame(metrics)


@st.cache_data(ttl=3600)
def get_county_pharmacy_plan_links(state_code, county_code):
    """
    Get pharmacy-plan links for plans served in a county.
    One row = one plan served by one pharmacy.
    """
    db = get_database()

    query = """
    SELECT
        pn.PHARMACY_NUMBER,
        pn.PHARMACY_ZIPCODE,
        z.city,
        z.state,
        z.county,
        z.lat,
        z.lng,
        p.PLAN_KEY,
        p.PLAN_NAME,
        p.CONTRACT_NAME,
        p.PREMIUM,
        p.DEDUCTIBLE,
        pn.IS_PREFERRED_RETAIL,
        pn.IS_PREFERRED_MAIL,
        pn.IS_IN_AREA
    FROM bronze.brz_plan_info p
    JOIN bronze.brz_pharmacy_network pn
      ON p.PLAN_KEY = pn.PLAN_KEY
    JOIN gold.dim_zipcode z
      ON pn.PHARMACY_ZIPCODE = z.zip_code
    WHERE p.STATE = ?
      AND p.COUNTY_CODE = ?
      AND LPAD(CAST(z.county_code AS VARCHAR), 5, '0') = ?
      AND z.lat IS NOT NULL
      AND z.lng IS NOT NULL
    """

    return db.query_df(query, [state_code, county_code, county_code])


def build_county_pharmacy_summary(pharmacy_links_df):
    """Aggregate pharmacy-plan links into pharmacy-level summary."""
    if len(pharmacy_links_df) == 0:
        return pd.DataFrame()

    df = pharmacy_links_df.copy()
    df['preferred_any'] = df['IS_PREFERRED_RETAIL'].fillna(False) | df['IS_PREFERRED_MAIL'].fillna(False)
    df['preferred_any'] = df['preferred_any'].astype(int)

    summary = (
        df.groupby(
            ['PHARMACY_NUMBER', 'PHARMACY_ZIPCODE', 'city', 'state', 'county', 'lat', 'lng'],
            as_index=False
        )
        .agg(
            plans_served=('PLAN_KEY', 'nunique'),
            preferred_plan_links=('preferred_any', 'sum'),
            avg_premium=('PREMIUM', 'mean')
        )
    )

    sample_plans = (
        df.groupby(['PHARMACY_NUMBER', 'PHARMACY_ZIPCODE'])['PLAN_NAME']
        .agg(lambda s: ", ".join(sorted(set(s))[:6]))
        .reset_index(name='sample_plan_names')
    )

    summary = summary.merge(sample_plans, on=['PHARMACY_NUMBER', 'PHARMACY_ZIPCODE'], how='left')
    return summary.sort_values(['plans_served', 'preferred_plan_links'], ascending=[False, False])


def render_county_access_visualization(state_name, county_name, state_code, county_code):
    """Render county-level pharmacy and plan availability map + summary table."""
    st.markdown("---")
    st.subheader("County Coverage Map: Pharmacies and Plans")

    county_links = get_county_pharmacy_plan_links(state_code, county_code)
    if len(county_links) == 0:
        st.warning(f"No pharmacy-plan network rows found for {county_name}, {state_name}.")
        return

    pharmacy_summary = build_county_pharmacy_summary(county_links)
    if len(pharmacy_summary) == 0:
        st.warning(f"No mappable pharmacies found for {county_name}, {state_name}.")
        return

    total_plans = county_links['PLAN_KEY'].nunique()
    total_pharmacies = pharmacy_summary['PHARMACY_NUMBER'].nunique()
    preferred_pharmacies = county_links[county_links['IS_PREFERRED_RETAIL'] == True]['PHARMACY_NUMBER'].nunique()
    avg_plans_per_pharmacy = pharmacy_summary['plans_served'].mean()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Plans in County", f"{total_plans:,}")
    m2.metric("Pharmacies Mapped", f"{total_pharmacies:,}")
    m3.metric("Preferred Retail Pharmacies", f"{preferred_pharmacies:,}")
    m4.metric("Avg Plans / Pharmacy", f"{avg_plans_per_pharmacy:.1f}")

    map_center_lat = pharmacy_summary['lat'].mean()
    map_center_lng = pharmacy_summary['lng'].mean()

    fig = px.scatter_mapbox(
        pharmacy_summary,
        lat='lat',
        lon='lng',
        size='plans_served',
        color='plans_served',
        color_continuous_scale='Blues',
        hover_name='city',
        hover_data={
            'PHARMACY_NUMBER': True,
            'PHARMACY_ZIPCODE': True,
            'plans_served': True,
            'preferred_plan_links': True,
            'avg_premium': ':.2f',
            'sample_plan_names': True,
            'lat': False,
            'lng': False
        },
        zoom=8,
        height=520
    )
    fig.update_layout(
        mapbox_style='open-street-map',
        mapbox_center={'lat': map_center_lat, 'lon': map_center_lng},
        margin={'l': 0, 'r': 0, 't': 40, 'b': 0},
        title=f"{county_name}, {state_name}: Pharmacies sized by number of plans served"
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("Top Pharmacies by Plan Coverage")
    display_cols = [
        'PHARMACY_NUMBER',
        'PHARMACY_ZIPCODE',
        'city',
        'plans_served',
        'preferred_plan_links',
        'avg_premium',
        'sample_plan_names'
    ]
    top_pharmacies = pharmacy_summary[display_cols].head(25).copy()
    top_pharmacies.rename(
        columns={
            'PHARMACY_NUMBER': 'Pharmacy #',
            'PHARMACY_ZIPCODE': 'ZIP',
            'city': 'City',
            'plans_served': 'Plans Served',
            'preferred_plan_links': 'Preferred Links',
            'avg_premium': 'Avg Premium',
            'sample_plan_names': 'Sample Plans'
        },
        inplace=True
    )
    st.dataframe(top_pharmacies, use_container_width=True)


# ===== Rank Plans using ML Model =====
def rank_plans(
    plans_df,
    beneficiary_profile,
    model_data,
    decision_weights,
    distance_penalty_rate=50.0,
    medication_rows=None,
):
    """
    Rank plans for a beneficiary using ML model.
    Now includes pharmacy-level network analysis for accurate distances.
    
    Args:
        plans_df (pd.DataFrame): Available plans
        beneficiary_profile (dict): Must include 'zip_code' for pharmacy distances
        model_data (dict): Loaded model data
        decision_weights (dict): Weights for ML/cost/access/coverage
        distance_penalty_rate (float): Distance weighting factor (50 = training baseline)
    
    Returns:
        pd.DataFrame: Plans with scores and rankings
    """
    if model_data is None:
        st.error("Model not loaded. Please train the model first.")
        return None
    if len(plans_df) == 0:
        return pd.DataFrame()
    
    model = model_data['model']
    feature_names = model_data['feature_names']
    
    # Get pharmacy-level network metrics
    user_zip = beneficiary_profile.get('zip_code')
    
    if user_zip and len(plans_df) > 0:
        with st.spinner("📍 Calculating distances to pharmacies..."):
            pharmacy_dist = get_plan_pharmacy_distances(
                plans_df['PLAN_KEY'].tolist(),
                str(user_zip)
            )
            
            if len(pharmacy_dist) > 0:
                network_metrics = calculate_plan_network_metrics(pharmacy_dist)
                # Merge with plans
                plans_df = plans_df.merge(network_metrics, on='PLAN_KEY', how='left')
                plans_df['has_pharmacy_distance_data'] = plans_df['nearest_preferred_miles'].notna().astype(int)
                
                # Fill missing values for plans without pharmacy data
                plans_df['nearest_preferred_miles'] = plans_df['nearest_preferred_miles'].fillna(10.0)
                plans_df['preferred_within_10mi'] = plans_df['preferred_within_10mi'].fillna(0)
                plans_df['network_accessibility_score'] = plans_df['network_accessibility_score'].fillna(50)
            else:
                # No pharmacy data available, use defaults
                plans_df['nearest_preferred_miles'] = 10.0
                plans_df['preferred_within_10mi'] = plans_df.get('network_preferred_pharmacies', 0)
                plans_df['network_accessibility_score'] = 50
                plans_df['has_pharmacy_distance_data'] = 0
    else:
        # No zip code, use defaults
        plans_df['nearest_preferred_miles'] = 10.0
        plans_df['preferred_within_10mi'] = plans_df.get('network_preferred_pharmacies', 0)
        plans_df['network_accessibility_score'] = 50
        plans_df['has_pharmacy_distance_data'] = 0
    
    # Create features for each plan
    features_list = []
    oop_breakdowns = []
    
    for _, plan in plans_df.iterrows():
        estimated_oop, oop_breakdown = estimate_plan_oop_with_breakdown(
            beneficiary_profile,
            plan,
            medication_rows=medication_rows,
        )
        model_distance = float(plan.get('nearest_preferred_miles', plan.get('distance_miles', 10.0)))
        distance_penalty = calculate_distance_penalty(model_distance, distance_penalty_rate)
        
        features = {
            # Plan features
            'premium': plan['premium'],
            'deductible': plan['deductible'],
            'is_ma_pd': 1 if plan['contract_type'] == 'MA' else 0,
            'is_pdp': 1 if plan['contract_type'] == 'PDP' else 0,
            
            # Beneficiary features
            'num_drugs': beneficiary_profile['num_drugs'],
            'is_insulin_user': beneficiary_profile['is_insulin_user'],
            'avg_fills_per_year': beneficiary_profile['avg_fills_per_year'],
            
            # Formulary features
            'formulary_generic_pct': plan['formulary_generic_pct'],
            'formulary_specialty_pct': plan['formulary_specialty_pct'],
            'formulary_pa_rate': plan['formulary_pa_rate'],
            'formulary_st_rate': plan['formulary_st_rate'],
            'formulary_ql_rate': plan['formulary_ql_rate'],
            'formulary_restrictiveness': plan['formulary_restrictiveness'],
            
            # Network features (use pharmacy-level counts)
            'network_preferred_pharmacies': int(plan.get('preferred_within_10mi', plan.get('network_preferred_pharmacies', 0))),
            'network_total_pharmacies': int(plan.get('total_within_10mi', plan.get('network_total_pharmacies', 0))),
            'network_adequacy_flag': plan['network_adequacy_flag'],
            
            # Distance features (use pharmacy-level distance)
            'distance_miles': model_distance,
            'has_distance_tradeoff': int(not plan.get('is_local', True) or plan.get('nearest_preferred_miles', 10) > 5),
            
            # Cost
            'total_drug_oop': estimated_oop,
            'distance_penalty': distance_penalty,
            
            # Interaction features
            'annual_premium': plan['premium'] * 12,
            'cost_per_drug': estimated_oop / max(beneficiary_profile['num_drugs'], 1),
            'premium_to_oop_ratio': (plan['premium'] * 12) / max(estimated_oop, 1)
        }
        
        features_list.append(features)
        oop_breakdowns.append(oop_breakdown)
    
    # Create feature matrix
    X = pd.DataFrame(features_list)

    # Ensure model input aligns exactly with training feature columns.
    for col in feature_names:
        if col not in X.columns:
            X[col] = 0
    X = X[feature_names]
    
    # Predict scores
    scores = model.predict(X)
    
    # Add scores and rank
    plans_df = plans_df.copy()
    plans_df['score'] = scores
    plans_df['ml_score'] = scores
    plans_df['estimated_annual_oop'] = [f['total_drug_oop'] for f in features_list]
    plans_df['distance_penalty'] = [f['distance_penalty'] for f in features_list]
    plans_df['oop_breakdown'] = oop_breakdowns
    plans_df['total_annual_cost'] = plans_df['premium'] * 12 + plans_df['estimated_annual_oop']
    plans_df['total_cost_with_distance'] = plans_df['total_annual_cost'] + plans_df['distance_penalty']
    plans_df['estimated_total_annual_cost'] = plans_df['total_cost_with_distance']
    if 'service_area_eligible' not in plans_df.columns:
        plans_df['service_area_eligible'] = True
    if 'eligibility_status' not in plans_df.columns:
        plans_df['eligibility_status'] = 'Eligible'

    plans_df = compute_decision_support_scores(plans_df, decision_weights)
    plans_df['heuristic_score'] = compute_heuristic_score(plans_df)
    plans_df = plans_df.sort_values(['decision_score', 'score'], ascending=[False, False]).reset_index(drop=True)
    plans_df['rank'] = range(1, len(plans_df) + 1)
    
    return plans_df


# ===== Main Application =====
def legacy_main():
    """Main Streamlit application logic."""
    
    # Header
    st.markdown('<div class="main-header">💊 Medicare Part D Plan Finder (ML-Powered)</div>', unsafe_allow_html=True)
    st.markdown("**Real-time AI plan recommendations for new beneficiaries**")
    
    # Load model
    model_data = load_trained_model()
    if model_data is None:
        st.error("⚠️ Trained model not found! Please run: `python ml_model/train_model_from_db.py`")
        st.stop()
    
    st.success(f"✅ Model loaded | Test NDCG@3: {model_data.get('training_stats', {}).get('test_ndcg@3', 'N/A'):.3f}")
    
    # ===== Sidebar: Input Form =====
    st.sidebar.header("📋 Enter Beneficiary Profile")
    
    # Location
    st.sidebar.subheader("Location")
    
    # Zip code (optional - auto-fills state/county)
    zip_code_input = st.sidebar.text_input("Zip Code (optional)", max_chars=5, placeholder="Enter 5-digit zip")
    
    if zip_code_input and len(zip_code_input) == 5:
        location = get_location_from_zip(zip_code_input)
        if location:
            st.sidebar.success(f"✓ {location['city']}, {location['state']}")
            default_state = location['state']
            default_county = location['county']
        else:
            st.sidebar.warning("Zip code not found")
            default_state = get_states()[0]
            default_county = None
    else:
        default_state = get_states()[0]
        default_county = None
    
    # State selection
    state_index = get_states().index(default_state) if default_state in get_states() else 0
    state_name = st.sidebar.selectbox("State", get_states(), index=state_index)
    
    # County selection
    counties = get_counties(state_name)
    if default_county and default_county in counties:
        county_index = counties.index(default_county)
    else:
        county_index = 0
    county_name = st.sidebar.selectbox("County", counties, index=county_index)
    
    st.sidebar.markdown("---")
    
    # Medications
    st.sidebar.subheader("Medications")

    if "selected_medication_rows" not in st.session_state:
        st.session_state["selected_medication_rows"] = []

    drug_name_query = st.sidebar.text_input(
        "Search drug by name or NDC",
        value="",
        placeholder="e.g., metformin or 00002871501",
    )
    drug_lookup = search_drug_catalog(drug_name_query, limit=30)
    lookup_option_labels = ["Select a drug..."]
    lookup_option_map = {}
    if len(drug_lookup) > 0:
        for _, row in drug_lookup.iterrows():
            label = (
                f"{row['drug_name']} [NDC {row['ndc']}] "
                f"(tier {int(row.get('tier_level', 1))}, {int(row.get('days_supply_mode', 30))}d)"
            )
            if label not in lookup_option_map:
                lookup_option_map[label] = row.to_dict()
                lookup_option_labels.append(label)

    selected_lookup_label = st.sidebar.selectbox(
        "Search results",
        options=lookup_option_labels,
        index=0,
    )

    manual_ndc = st.sidebar.text_input(
        "Or add NDC directly",
        value="",
        placeholder="11-digit NDC",
    )
    manual_ndc_values = normalize_ndc_token(manual_ndc)

    add_col1, add_col2 = st.sidebar.columns(2)
    add_selected = add_col1.button(
        "Add drug",
        use_container_width=True,
        disabled=selected_lookup_label == "Select a drug...",
    )
    add_manual = add_col2.button(
        "Add NDC",
        use_container_width=True,
        disabled=len(manual_ndc_values) == 0,
    )

    if add_selected and selected_lookup_label in lookup_option_map:
        selected_row = lookup_option_map[selected_lookup_label]
        selected_days_supply = pd.to_numeric(selected_row.get("days_supply_mode"), errors="coerce")
        selected_tier = pd.to_numeric(selected_row.get("tier_level"), errors="coerce")
        selected_fills = pd.to_numeric(selected_row.get("fills_per_year"), errors="coerce")
        selected_insulin = pd.to_numeric(selected_row.get("is_insulin"), errors="coerce")
        selected_annual_cost = pd.to_numeric(selected_row.get("annual_cost_est"), errors="coerce")
        days_supply_mode = int(selected_days_supply) if pd.notna(selected_days_supply) else 30
        if days_supply_mode not in (30, 60, 90):
            days_supply_mode = 30 if days_supply_mode < 45 else (60 if days_supply_mode < 75 else 90)
        tier_level = int(selected_tier) if pd.notna(selected_tier) else 1
        tier_level = min(7, max(1, tier_level))
        fills_per_year = float(selected_fills) if pd.notna(selected_fills) else 12.0
        fills_per_year = max(1.0, fills_per_year)
        annual_cost_est = float(selected_annual_cost) if pd.notna(selected_annual_cost) else 0.0
        annual_cost_est = max(0.0, annual_cost_est)
        new_row = {
            "drug_name": str(selected_row.get("drug_name", "") or f"NDC {selected_row.get('ndc', '')}"),
            "ndc": str(selected_row.get("ndc", "")),
            "days_supply_mode": days_supply_mode,
            "tier_level": tier_level,
            "fills_per_year": fills_per_year,
            "is_insulin": bool(int(selected_insulin)) if pd.notna(selected_insulin) else False,
            "annual_cost_est": annual_cost_est,
        }
        existing_ndcs = {str(r.get("ndc", "")) for r in st.session_state["selected_medication_rows"]}
        if new_row["ndc"] not in existing_ndcs:
            st.session_state["selected_medication_rows"].append(new_row)
            st.sidebar.success(f"Added {new_row['drug_name']} ({new_row['ndc']})")
        else:
            st.sidebar.info(f"NDC {new_row['ndc']} is already in the medication list.")

    if add_manual and len(manual_ndc_values) > 0:
        ndc_value = manual_ndc_values[-1]
        existing_ndcs = {str(r.get("ndc", "")) for r in st.session_state["selected_medication_rows"]}
        if ndc_value not in existing_ndcs:
            st.session_state["selected_medication_rows"].append(
                {
                    "drug_name": f"NDC {ndc_value}",
                    "ndc": ndc_value,
                    "days_supply_mode": 30,
                    "tier_level": 1,
                    "fills_per_year": 12.0,
                    "is_insulin": False,
                    "annual_cost_est": 0.0,
                }
            )
            st.sidebar.success(f"Added NDC {ndc_value}")
        else:
            st.sidebar.info(f"NDC {ndc_value} is already in the medication list.")

    if st.sidebar.button("Clear medication list", use_container_width=True):
        st.session_state["selected_medication_rows"] = []
        st.sidebar.info("Medication list cleared.")

    medication_rows = st.session_state.get("selected_medication_rows", [])
    if len(medication_rows) > 0:
        med_df = pd.DataFrame(medication_rows)
        edited_med_df = st.sidebar.data_editor(
            med_df,
            hide_index=True,
            num_rows="dynamic",
            use_container_width=True,
            key="selected_medication_editor",
            column_config={
                "drug_name": st.column_config.TextColumn("Drug name", disabled=True),
                "ndc": st.column_config.TextColumn("NDC", disabled=True),
                "days_supply_mode": st.column_config.SelectboxColumn(
                    "Days supply",
                    options=[30, 60, 90],
                    required=True,
                ),
                "tier_level": st.column_config.SelectboxColumn(
                    "Tier",
                    options=[1, 2, 3, 4, 5, 6, 7],
                    required=True,
                ),
                "fills_per_year": st.column_config.NumberColumn(
                    "Fills/year",
                    min_value=1.0,
                    max_value=100.0,
                    step=1.0,
                ),
                "is_insulin": st.column_config.CheckboxColumn("Insulin"),
                "annual_cost_est": st.column_config.NumberColumn(
                    "Annual cost est ($)",
                    min_value=0.0,
                    max_value=100000.0,
                    step=50.0,
                ),
            },
        )
        medication_rows = edited_med_df.to_dict("records")

    medication_summary = summarize_medication_rows(medication_rows)
    st.session_state["selected_medication_rows"] = medication_summary["rows"]

    if medication_summary["num_drugs"] > 0:
        st.sidebar.caption("Unique medications and average fills are derived from the medication list.")
        stats_col1, stats_col2 = st.sidebar.columns(2)
        stats_col1.metric("Unique drugs", int(medication_summary["num_drugs"]))
        stats_col2.metric("Avg fills/year", f"{medication_summary['avg_fills_per_year']:.1f}")
        num_drugs = int(medication_summary["num_drugs"])
        avg_fills_per_year = float(medication_summary["avg_fills_per_year"])
    else:
        st.sidebar.caption("No medication rows selected. Enter profile-level medication values manually.")
        num_drugs = st.sidebar.number_input("Number of medications", min_value=1, max_value=20, value=3)
        avg_fills_per_year = st.sidebar.number_input("Average fills per year", min_value=1, max_value=100, value=12)

    is_insulin_user = st.sidebar.checkbox(
        "Insulin user",
        value=bool(medication_summary["is_insulin_user"]),
        help="Defaults from selected medications; you can override manually.",
    )
    if medication_summary["is_insulin_user"] == 1 and not is_insulin_user:
        st.sidebar.caption("Insulin flag was auto-detected from selected medications and has been manually overridden.")

    st.sidebar.caption("Use selected medications for plan-level coverage filtering.")
    enable_drug_filter = st.sidebar.checkbox(
        "Filter plans by selected medications",
        value=bool(medication_summary["num_drugs"] > 0),
        disabled=medication_summary["num_drugs"] == 0,
        help="When enabled, plans are filtered by coverage of selected NDCs."
    )
    min_drug_coverage_pct = st.sidebar.slider(
        "Minimum requested-drug coverage (%)",
        min_value=0,
        max_value=100,
        value=100,
        step=5,
        disabled=not enable_drug_filter,
        help="100% = every requested drug must be covered."
    )
    
    st.sidebar.markdown("---")
    
    # Cost estimate
    st.sidebar.subheader("Cost Information")
    derived_total_rx_cost = float(medication_summary["total_annual_drug_cost"])
    use_derived_rx_cost = st.sidebar.checkbox(
        "Auto-calculate annual drug cost from medication list",
        value=bool(derived_total_rx_cost > 0),
        disabled=derived_total_rx_cost <= 0,
    )
    if use_derived_rx_cost and derived_total_rx_cost > 0:
        total_rx_cost_est = float(derived_total_rx_cost)
        st.sidebar.info(f"Estimated annual drug cost (derived): ${total_rx_cost_est:,.2f}")
    else:
        total_rx_cost_est = st.sidebar.number_input(
            "Estimated annual drug cost ($)",
            min_value=100.0,
            max_value=50000.0,
            value=2000.0,
            step=100.0
        )
    
    st.sidebar.markdown("---")
    
    # Risk segment (optional)
    risk_segment = st.sidebar.selectbox(
        "Risk Segment (optional)",
        ["Low", "Medium", "High"],
        index=1
    )
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Decision Priorities")
    ml_influence_pct = st.sidebar.slider(
        "ML model influence (%)",
        min_value=10,
        max_value=70,
        value=35,
        step=5,
        help="Higher values rely more on model ranking; lower values rely more on explicit cost/access/coverage trade-offs."
    )
    cost_priority = st.sidebar.slider("Cost priority", 1, 5, 5)
    access_priority = st.sidebar.slider("Pharmacy access priority", 1, 5, 4)
    coverage_priority = st.sidebar.slider("Coverage flexibility priority", 1, 5, 4)

    decision_weights = build_decision_weights(
        ml_influence_pct,
        cost_priority,
        access_priority,
        coverage_priority
    )
    st.sidebar.caption(
        "Weights -> "
        f"ML: {decision_weights['ml']:.0%}, "
        f"Cost: {decision_weights['cost']:.0%}, "
        f"Access: {decision_weights['access']:.0%}, "
        f"Coverage: {decision_weights['coverage']:.0%}"
    )

    # Search Configuration
    st.sidebar.subheader("Search Options")
    
    include_nearby = st.sidebar.checkbox(
        "Include nearby counties if no local plans",
        value=True,
        help="Search neighboring counties when no plans available in your county"
    )
    
    max_distance = st.sidebar.slider(
        "Maximum distance (miles)",
        min_value=10,
        max_value=100,
        value=50,
        step=10,
        help="How far you're willing to travel for better coverage",
        disabled=not include_nearby
    )
    
    distance_penalty_rate = st.sidebar.number_input(
        "Distance weight (50 = training)",
        min_value=10.0,
        max_value=100.0,
        value=50.0,
        step=10.0,
        help="Scales distance penalty tiers used in training: 0 / 100 / 200 dollars per year",
        disabled=not include_nearby
    )
    
    st.sidebar.markdown("---")
    
    # Get recommendations button
    get_recommendations = st.sidebar.button("Get Plan Recommendations", type="primary", use_container_width=True)
    
    # ===== Main Content Area =====
    if get_recommendations:
        requested_ndcs = tuple(medication_summary["requested_ndcs"])
        requested_name_map = dict(medication_summary["requested_name_map"])
        if enable_drug_filter and len(requested_ndcs) > 0:
            num_drugs = max(int(num_drugs), int(medication_summary["num_drugs"]))
            avg_fills_per_year = max(float(avg_fills_per_year), float(medication_summary["avg_fills_per_year"]))

        # Get state and county codes for querying
        state_code, county_code = get_state_county_codes(state_name, county_name)
        
        if not state_code or not county_code:
            st.error(f"Could not find codes for {state_name}, {county_name}")
            st.stop()
        
        effective_zip = zip_code_input if zip_code_input and len(zip_code_input) == 5 else None
        if not effective_zip:
            effective_zip = get_default_zip_for_county(state_name, county_name)

        # Create beneficiary profile
        beneficiary_profile = {
            'state': state_name,
            'county': county_name,
            'state_code': state_code,
            'county_code': county_code,
            'zip_code': effective_zip,
            'num_drugs': num_drugs,
            'avg_fills_per_year': avg_fills_per_year,
            'is_insulin_user': 1 if is_insulin_user else 0,
            'total_rx_cost_est': total_rx_cost_est,
            'risk_segment': risk_segment
        }
        
        st.info(f"📊 Finding plans for: **{state_name}, {county_name}** | {num_drugs} medications | ${total_rx_cost_est:,.0f}/year")
        if enable_drug_filter:
            if len(requested_ndcs) > 0:
                st.caption(
                    f"Drug filter active: {len(requested_ndcs)} selected medication(s), minimum coverage {min_drug_coverage_pct}%."
                )
            else:
                st.caption("Drug filter enabled, but no medications were selected. Proceeding without drug filter.")
        if effective_zip and effective_zip != zip_code_input:
            st.caption(f"Using county representative ZIP for distance-aware ranking: `{effective_zip}`")
        
        # Load available plans (with fallback if enabled)
        with st.spinner("🔍 Searching for plans..."):
            if include_nearby:
                plans_df, search_info = get_plans_with_fallback(
                    state_code, county_code,
                    effective_zip,
                    state_name, county_name,
                    include_nearby=True,
                    max_distance=max_distance
                )
            else:
                # Just search local county
                plans_df = get_plans_for_location(state_code, county_code)
                search_info = {
                    'local_count': len(plans_df),
                    'nearby_count': 0,
                    'searched_nearby': False
                }
                if len(plans_df) > 0:
                    plans_df['is_local'] = True
                    plans_df['distance_miles'] = 0.0
                    plans_df['distance_category'] = 'Local'
        
        # Display search results info
        if search_info['searched_nearby']:
            if search_info['nearby_count'] > 0:
                st.info(f"ℹ️ **No plans in {county_name}.** Found **{search_info['nearby_count']} plans** in **{search_info['counties_with_plans']} nearby counties** within {max_distance} miles.")
            else:
                st.warning(f"⚠️ **No plans found** in {county_name} or within {max_distance} miles.")
                if search_info.get('fallback_reason'):
                    st.caption(f"Reason: {search_info['fallback_reason']}")
                st.stop()
        elif search_info['local_count'] > 0:
            st.success(f"✅ Found **{search_info['local_count']} local plans** in {county_name}")
        else:
            st.warning(f"⚠️ **No plans found** in {county_name}. Enable 'Include nearby counties' to search wider.")
            st.stop()

        if enable_drug_filter and len(requested_ndcs) > 0:
            coverage_df = get_plan_drug_coverage(
                tuple(plans_df["PLAN_KEY"].astype(str).tolist()),
                tuple(requested_ndcs),
            )
            if len(coverage_df) > 0:
                plans_df = plans_df.merge(coverage_df, on="PLAN_KEY", how="left")
                for col, fill_value in [
                    ("requested_drugs", len(requested_ndcs)),
                    ("covered_drugs", 0),
                    ("in_formulary_drugs", 0),
                    ("excluded_drugs", 0),
                    ("drug_coverage_pct", 0.0),
                ]:
                    plans_df[col] = pd.to_numeric(plans_df[col], errors="coerce").fillna(fill_value)
                plans_df["uncovered_ndcs"] = plans_df["uncovered_ndcs"].fillna("")

                eligible_plans = plans_df[plans_df["drug_coverage_pct"] >= float(min_drug_coverage_pct)].copy()
                if len(eligible_plans) == 0:
                    st.warning(
                        f"No plans met {min_drug_coverage_pct}% requested-drug coverage. "
                        "Showing highest-coverage plans instead."
                    )
                    plans_df = plans_df.sort_values(
                        ["drug_coverage_pct", "premium"],
                        ascending=[False, True]
                    ).reset_index(drop=True)
                else:
                    plans_df = eligible_plans.reset_index(drop=True)
                    st.success(
                        f"Drug coverage filter retained {len(plans_df):,} plans at >= {min_drug_coverage_pct}% coverage."
                    )
            else:
                st.warning("Could not evaluate requested-drug coverage for current plans.")
        
        # Rank plans using ML model
        with st.spinner("Ranking plans using ML model..."):
            ranked_plans = rank_plans(
                plans_df,
                beneficiary_profile,
                model_data,
                decision_weights=decision_weights,
                distance_penalty_rate=distance_penalty_rate
            )
        
        if ranked_plans is None:
            st.stop()
        
        # Display top 5 recommendations
        st.markdown("---")
        st.subheader("Top 5 Recommended Plans (Decision Support)")
        
        top_5 = ranked_plans.head(5)
        st.caption(
            f"Scoring weights -> ML: {decision_weights['ml']:.0%}, Cost: {decision_weights['cost']:.0%}, "
            f"Access: {decision_weights['access']:.0%}, Coverage: {decision_weights['coverage']:.0%}"
        )
        
        for idx, (_, plan) in enumerate(top_5.iterrows(), 1):
            with st.container():
                # Plan header
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.markdown(f"### {plan['rank']}. {plan['PLAN_NAME']}")
                    st.markdown(f"**Type:** {plan['contract_type']} | **Contract:** {plan['CONTRACT_NAME']}")
                    if plan['rank'] == 1:
                        st.markdown("🏅 **Best Overall Match**")
                    elif plan['rank'] == 2:
                        st.markdown("🥈 **Strong Alternative**")
                    st.caption(f"Decision rationale: {plan.get('decision_reason', 'balanced profile')}")
                
                with col2:
                    st.metric("Monthly Premium", f"${plan['premium']:.2f}")
                    st.metric("Deductible", f"${plan['deductible']:.2f}")
                
                with col3:
                    st.markdown(f'<p class="cost-highlight">${plan["total_cost_with_distance"]:,.2f}</p>', unsafe_allow_html=True)
                    st.caption("Total Cost (Distance Adjusted)")
                
                # Cost breakdown
                st.markdown("**💰 Cost Breakdown:**")
                annual_premium = plan['premium'] * 12
                col_a, col_b, col_c, col_d = st.columns(4)
                col_a.metric("Annual Premium", f"${annual_premium:,.2f}")
                col_b.metric("Est. Drug Out-of-Pocket", f"${plan['estimated_annual_oop']:,.2f}")
                col_c.metric("Decision Score", f"{plan['decision_score']:.1f}")
                col_d.metric("ML Ranking Score", f"{plan['score']:.2f}")

                if enable_drug_filter and len(requested_ndcs) > 0 and 'drug_coverage_pct' in plan:
                    st.markdown("**🧬 Requested Drug Coverage:**")
                    d1, d2, d3 = st.columns(3)
                    d1.metric("Coverage %", f"{float(plan.get('drug_coverage_pct', 0.0)):.1f}%")
                    d2.metric("Covered Drugs", f"{int(float(plan.get('covered_drugs', 0)))}")
                    d3.metric("Excluded Drugs", f"{int(float(plan.get('excluded_drugs', 0)))}")
                    uncovered = str(plan.get('uncovered_ndcs', '') or '').strip()
                    if uncovered:
                        uncovered_display = []
                        for ndc in [t.strip() for t in uncovered.split(",") if t.strip()]:
                            name = requested_name_map.get(ndc, "")
                            uncovered_display.append(f"{name} (NDC {ndc})" if name else f"NDC {ndc}")
                        st.caption(f"Uncovered requested medications: {', '.join(uncovered_display)}")
                
                
                # Location & Distance info
                if not plan.get('is_local', True):
                    distance = plan.get('distance_miles', plan.get('nearest_preferred_miles', 0))
                    location_county = plan.get('location_county', 'Unknown')
                    distance_cat = plan.get('distance_category', categorize_distance(distance))
                    
                    # Color-coded badge
                    if distance <= 10:
                        badge_color = "🔵"
                        badge_text = "Nearby"
                    elif distance <= 25:
                        badge_color = "🟡"
                        badge_text = "Moderate Distance"
                    else:
                        badge_color = "🔴"
                        badge_text = "Distant"
                    
                    st.markdown(f"**📍 Location:** {location_county} County ({distance:.1f} miles away) {badge_color} {badge_text}")
                    
                    # Distance penalty
                    distance_penalty = float(plan.get('distance_penalty', calculate_distance_penalty(distance, distance_penalty_rate)))
                    st.markdown(f"**🚗 Distance Penalty:** ${distance_penalty:,.0f}/year (weight {distance_penalty_rate:.0f})")
                else:
                    st.markdown(f"**📍 Location:** ✅ Local plan in {county_name}")
                
                # Network info (enhanced with pharmacy-level data)
                st.markdown("**🏥 Pharmacy Network:**")
                
                nearest_pref = plan.get('nearest_preferred_miles', None)
                pref_5mi = plan.get('preferred_within_5mi', None)
                pref_10mi = plan.get('preferred_within_10mi', None)
                total_10mi = plan.get('total_within_10mi', None)
                accessibility = plan.get('network_accessibility_score', None)
                
                if nearest_pref is not None and nearest_pref < 999:
                    # Have pharmacy-level data
                    col_n1, col_n2, col_n3 = st.columns(3)
                    col_n1.metric("Nearest Preferred", f"{nearest_pref:.1f} mi")
                    col_n2.metric("Within 10 mi", f"{pref_10mi if pref_10mi is not None else 0}")
                    col_n3.metric("Accessibility", f"{accessibility:.0f}/100" if accessibility is not None else "N/A")
                    
                    # Accessibility warning/success
                    if nearest_pref > 10:
                        st.warning("⚠️ Nearest preferred pharmacy is over 10 miles away")
                    elif nearest_pref <= 5:
                        st.success("✓ Preferred pharmacy within 5 miles")
                else:
                    # Fallback to plan-level aggregates
                    st.info(f"Preferred pharmacies: {plan.get('network_preferred_pharmacies', 0)} | Total: {plan.get('network_total_pharmacies', 0)}")
                
                st.markdown("---")
                
                # Formulary info
                st.markdown("**💊 Formulary:**")
                col_f1, col_f2, col_f3 = st.columns(3)
                generic_display = float(plan['formulary_generic_pct']) * 100 if float(plan['formulary_generic_pct']) <= 1 else float(plan['formulary_generic_pct'])
                pa_display = float(plan['formulary_pa_rate']) * 100 if float(plan['formulary_pa_rate']) <= 1 else float(plan['formulary_pa_rate'])
                col_f1.metric("Generic %", f"{generic_display:.1f}%")
                col_f2.metric("PA Rate", f"{pa_display:.1f}%")
                col_f3.metric("Restrictiveness Class", f"{int(plan['formulary_restrictiveness'])}")
                
                # Warnings
                warnings = []
                pa_ratio = float(plan['formulary_pa_rate']) / 100.0 if float(plan['formulary_pa_rate']) > 1 else float(plan['formulary_pa_rate'])
                if is_insulin_user and pa_ratio > 0.40:
                    warnings.append("⚠️ High prior authorization rate - may require approval for insulin")
                if nearest_pref and nearest_pref < 999 and nearest_pref > 15:
                    warnings.append(f"⚠️ Nearest preferred pharmacy is {nearest_pref:.1f} miles away")
                elif plan.get('network_preferred_pharmacies', 0) < 10:
                    warnings.append("⚠️ Limited preferred pharmacy network")
                if int(plan['formulary_restrictiveness']) >= 2:
                    warnings.append("⚠️ Restrictive formulary - some drugs may need approval")
                
                if warnings:
                    st.markdown("**⚠️ Important Notices:**")
                    for warning in warnings:
                        st.warning(warning)
        
        # Cost comparison chart
        st.subheader("📈 Cost Comparison (Top 5)")
        
        fig = go.Figure(data=[
            go.Bar(
                name='Annual Premium',
                x=top_5['rank'],
                y=top_5['premium'] * 12,
                marker_color='lightblue'
            ),
            go.Bar(
                name='Drug Out-of-Pocket',
                x=top_5['rank'],
                y=top_5['estimated_annual_oop'],
                marker_color='coral'
            ),
            go.Bar(
                name='Distance Penalty',
                x=top_5['rank'],
                y=top_5['distance_penalty'],
                marker_color='gold'
            )
        ])
        fig.update_layout(
            barmode='stack',
            title='Premium + Out-of-Pocket + Distance Penalty',
            xaxis_title='Recommendation Rank',
            yaxis_title='Cost ($)',
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)

        render_county_access_visualization(state_name, county_name, state_code, county_code)
        
        # Downloadable report
        st.markdown("---")
        st.subheader("📥 Download Recommendations")
        
        report_df = top_5[[
            'rank', 'PLAN_NAME', 'contract_type', 'premium', 'deductible',
            'estimated_annual_oop', 'distance_penalty', 'total_cost_with_distance',
            'decision_score', 'decision_reason', 'score',
            'network_preferred_pharmacies', 'network_total_pharmacies'
        ]].copy()
        if 'drug_coverage_pct' in top_5.columns:
            report_df['drug_coverage_pct'] = top_5['drug_coverage_pct']
            report_df['covered_drugs'] = top_5.get('covered_drugs', 0)
            report_df['requested_drugs'] = top_5.get('requested_drugs', 0)
            report_df['uncovered_ndcs'] = top_5.get('uncovered_ndcs', "")
        
        csv = report_df.to_csv(index=False)
        st.download_button(
            label="Download as CSV",
            data=csv,
            file_name=f"plan_recommendations_{state_code}_{county_code}.csv",
            mime="text/csv"
        )
        
    else:
        # Landing page
        st.markdown("---")
        st.markdown("""
        ### 👋 Welcome to the Real-Time Medicare Part D Plan Finder!
        
        This tool uses **machine learning** to recommend Medicare Part D prescription drug plans
        for **new beneficiaries** based on their profile.
        
        #### How It Works:
        1. **Enter beneficiary information** in the sidebar (location, medications, costs)
        2. *(Optional)* Search and select medication names to enforce drug-based plan coverage filtering
        3. **Click "Get Plan Recommendations"** to run real-time ML inference
        4. **Review ranked plans** with cost breakdowns, network info, and warnings
        
        #### What You'll Get:
        - ✅ **Top 5 AI-ranked plans** optimized for the beneficiary's needs
        - 💰 **Cost analysis** (premium + out-of-pocket drug costs)
        - 🏥 **Network information** (preferred pharmacies)
        - ⚠️ **Important warnings** (insulin coverage, formulary restrictions)
        - 📊 **Visual comparisons** to help choose the best plan
        - 📥 **Downloadable report** in CSV format
        
        #### ML Model Stats:
        """)
        
        if model_data:
            stats = model_data.get('training_stats', {})
            col1, col2, col3 = st.columns(3)
            col1.metric("Test NDCG@3", f"{stats.get('test_ndcg@3', 0):.3f}")
            col2.metric("Test NDCG@5", f"{stats.get('test_ndcg@5', 0):.3f}")
            col3.metric("Best Iteration", stats.get('best_iteration', 'N/A'))
        
        st.markdown("---")
        st.info("**📌 Note:** This is a demonstration using a pre-trained LightGBM ranking model. Enter beneficiary details in the sidebar to get started!")

def get_data_snapshot_id():
    """Build a lightweight identifier for the current DuckDB snapshot."""
    db_path = Path("data/medicare_part_d.duckdb")
    if not db_path.exists():
        return "missing-data-snapshot"

    stat = db_path.stat()
    fingerprint = f"{db_path.name}:{stat.st_size}:{int(stat.st_mtime)}"
    return hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:12]


@st.cache_data(ttl=3600)
def get_research_states():
    """Return available states for research sampling."""
    db = get_database()
    query = """
        SELECT DISTINCT bene_state
        FROM ml.training_plan_pairs
        WHERE bene_state IS NOT NULL
          AND TRIM(CAST(bene_state AS VARCHAR)) != ''
        ORDER BY bene_state
    """
    states = db.query_df(query)["bene_state"].astype(str).tolist()
    return ["All"] + states


def get_model_version(model_data):
    """Create a stable display version for the current model artifact."""
    model_path = Path("models/plan_ranker.pkl")
    feature_names = model_data.get("feature_names", []) if model_data else []
    stats = model_data.get("training_stats", {}) if model_data else {}
    raw = {
        "feature_count": len(feature_names),
        "best_iteration": stats.get("best_iteration"),
        "test_ndcg@3": stats.get("test_ndcg@3"),
    }
    if model_path.exists():
        stat = model_path.stat()
        raw["artifact_size"] = stat.st_size
        raw["artifact_mtime"] = int(stat.st_mtime)
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    return f"plan-ranker-{digest}"


def init_app_state():
    """Initialize Streamlit session state for the new app flows."""
    defaults = {
        "selected_medication_rows": [],
        "recommendation_runs": {},
        "active_run_id": None,
        "research_outputs": None,
        "last_profile_zip_lookup": None,
        "profile_num_drugs": 3,
        "profile_avg_fills_per_year": 12.0,
        "profile_is_insulin_user": False,
        "profile_total_rx_cost_est": 2000.0,
        "profile_risk_segment": "Medium",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _coerce_list(value):
    """Return a list from list-like or JSON-serialized values."""
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return [value]
    return []


def _coerce_dict(value):
    """Return a dict from dict-like or JSON-serialized values."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def render_profile_step():
    """Render beneficiary profile inputs."""
    st.subheader("1. Profile")
    st.caption("Confirm the service area first. Recommendations only become strong when service area, coverage, and evidence quality line up.")

    states = get_states()
    if len(states) == 0:
        st.error("No states were found in `gold.dim_zipcode`.")
        return {}

    if "profile_state_name" not in st.session_state or st.session_state["profile_state_name"] not in states:
        st.session_state["profile_state_name"] = states[0]

    zip_code_raw = st.text_input(
        "ZIP code",
        key="profile_zip_code",
        max_chars=5,
        placeholder="5-digit ZIP",
        help="Used for service area lookup and pharmacy-distance estimation when available.",
    )
    zip_code = "".join(ch for ch in str(zip_code_raw or "") if ch.isdigit())[:5]

    if len(zip_code) == 5:
        location = get_location_from_zip(zip_code)
        if location is not None:
            if st.session_state.get("last_profile_zip_lookup") != zip_code:
                if location["state"] in states:
                    st.session_state["profile_state_name"] = location["state"]
                    county_options = get_counties(location["state"])
                    if location["county"] in county_options:
                        st.session_state["profile_county_name"] = location["county"]
                st.session_state["last_profile_zip_lookup"] = zip_code
            st.success(f"ZIP lookup: {location['city']}, {location['state']} | {location['county']} County")
        else:
            st.warning("ZIP lookup did not find a matching county. You can still select state and county manually.")

    state_name = st.selectbox("State", states, key="profile_state_name")
    counties = get_counties(state_name)
    if len(counties) == 0:
        st.error(f"No counties were found for {state_name}.")
        return {}

    if "profile_county_name" not in st.session_state or st.session_state["profile_county_name"] not in counties:
        st.session_state["profile_county_name"] = counties[0]
    county_name = st.selectbox("County", counties, key="profile_county_name")

    state_code, county_code = get_state_county_codes(state_name, county_name)
    trust_col1, trust_col2 = st.columns(2)
    if state_code and county_code:
        trust_col1.success(f"Service area ready: {state_code} / {county_code}")
    else:
        trust_col1.error("State or county code could not be resolved.")
    representative_zip = get_default_zip_for_county(state_name, county_name)
    trust_col2.info(f"Representative county ZIP: {representative_zip or 'Not available'}")

    st.markdown("#### Baseline Profile")
    col1, col2, col3 = st.columns(3)
    col1.selectbox(
        "Risk segment",
        ["Low", "Medium", "High"],
        key="profile_risk_segment",
        help="Used in research-style beneficiary profiling and evaluation slices.",
    )
    col2.number_input(
        "Fallback number of medications",
        min_value=1,
        max_value=20,
        step=1,
        key="profile_num_drugs",
        help="Used only when no medication list is supplied.",
    )
    col3.number_input(
        "Fallback avg fills per year",
        min_value=1.0,
        max_value=100.0,
        step=1.0,
        key="profile_avg_fills_per_year",
        help="Used only when no medication list is supplied.",
    )

    col4, col5 = st.columns(2)
    col4.number_input(
        "Fallback annual drug cost ($)",
        min_value=100.0,
        max_value=100000.0,
        step=100.0,
        key="profile_total_rx_cost_est",
        help="Medication-list cost estimates override this when available.",
    )
    col5.checkbox(
        "Fallback insulin user flag",
        key="profile_is_insulin_user",
        help="Medication entries can still signal insulin use automatically.",
    )

    return {
        "state_name": state_name,
        "county_name": county_name,
        "state_code": state_code,
        "county_code": county_code,
        "zip_code": zip_code or None,
        "risk_segment": st.session_state["profile_risk_segment"],
        "num_drugs": int(st.session_state["profile_num_drugs"]),
        "avg_fills_per_year": float(st.session_state["profile_avg_fills_per_year"]),
        "is_insulin_user": bool(st.session_state["profile_is_insulin_user"]),
        "total_rx_cost_est": float(st.session_state["profile_total_rx_cost_est"]),
        "representative_zip": representative_zip,
    }


def render_medication_step():
    """Render requested-medication capture and editing."""
    st.subheader("2. Medications")
    st.caption("Requested-drug coverage is treated as a first-class output. Add medications here to move the app from generic comparison toward beneficiary-specific decision support.")

    drug_name_query = st.text_input(
        "Search drug name or NDC",
        key="medication_search_query",
        placeholder="e.g., metformin or 00002871501",
    )
    drug_lookup = search_drug_catalog(drug_name_query, limit=30)

    lookup_option_labels = ["Select a drug..."]
    lookup_option_map = {}
    if len(drug_lookup) > 0:
        for _, row in drug_lookup.iterrows():
            label = (
                f"{row['drug_name']} [NDC {row['ndc']}] "
                f"(tier {int(row.get('tier_level', 1))}, {int(row.get('days_supply_mode', 30))}d)"
            )
            if label not in lookup_option_map:
                lookup_option_map[label] = row.to_dict()
                lookup_option_labels.append(label)

    col1, col2 = st.columns([2, 1])
    selected_lookup_label = col1.selectbox(
        "Search results",
        options=lookup_option_labels,
        index=0,
        key="medication_lookup_selection",
    )
    manual_ndc = col2.text_input(
        "Manual NDC",
        key="manual_ndc_input",
        placeholder="11-digit NDC",
    )
    manual_ndc_values = normalize_ndc_token(manual_ndc)

    action_col1, action_col2, action_col3 = st.columns(3)
    add_selected = action_col1.button(
        "Add selected drug",
        use_container_width=True,
        disabled=selected_lookup_label == "Select a drug...",
    )
    add_manual = action_col2.button(
        "Add manual NDC",
        use_container_width=True,
        disabled=len(manual_ndc_values) == 0,
    )
    clear_medications = action_col3.button("Clear list", use_container_width=True)

    if add_selected and selected_lookup_label in lookup_option_map:
        selected_row = lookup_option_map[selected_lookup_label]
        selected_days_supply = pd.to_numeric(selected_row.get("days_supply_mode"), errors="coerce")
        selected_tier = pd.to_numeric(selected_row.get("tier_level"), errors="coerce")
        selected_fills = pd.to_numeric(selected_row.get("fills_per_year"), errors="coerce")
        selected_insulin = pd.to_numeric(selected_row.get("is_insulin"), errors="coerce")
        selected_annual_cost = pd.to_numeric(selected_row.get("annual_cost_est"), errors="coerce")

        days_supply_mode = int(selected_days_supply) if pd.notna(selected_days_supply) else 30
        if days_supply_mode not in (30, 60, 90):
            days_supply_mode = 30 if days_supply_mode < 45 else (60 if days_supply_mode < 75 else 90)

        new_row = {
            "drug_name": str(selected_row.get("drug_name", "") or f"NDC {selected_row.get('ndc', '')}"),
            "ndc": str(selected_row.get("ndc", "")),
            "days_supply_mode": days_supply_mode,
            "tier_level": min(7, max(1, int(selected_tier) if pd.notna(selected_tier) else 1)),
            "fills_per_year": max(1.0, float(selected_fills) if pd.notna(selected_fills) else 12.0),
            "is_insulin": bool(int(selected_insulin)) if pd.notna(selected_insulin) else False,
            "annual_cost_est": max(0.0, float(selected_annual_cost) if pd.notna(selected_annual_cost) else 0.0),
        }
        existing_ndcs = {str(row.get("ndc", "")) for row in st.session_state["selected_medication_rows"]}
        if new_row["ndc"] not in existing_ndcs:
            st.session_state["selected_medication_rows"].append(new_row)
            st.success(f"Added {new_row['drug_name']} ({new_row['ndc']}).")
        else:
            st.info(f"NDC {new_row['ndc']} is already in the medication list.")

    if add_manual and len(manual_ndc_values) > 0:
        ndc_value = manual_ndc_values[-1]
        existing_ndcs = {str(row.get("ndc", "")) for row in st.session_state["selected_medication_rows"]}
        if ndc_value not in existing_ndcs:
            st.session_state["selected_medication_rows"].append(
                {
                    "drug_name": f"NDC {ndc_value}",
                    "ndc": ndc_value,
                    "days_supply_mode": 30,
                    "tier_level": 1,
                    "fills_per_year": 12.0,
                    "is_insulin": False,
                    "annual_cost_est": 0.0,
                }
            )
            st.success(f"Added NDC {ndc_value}.")
        else:
            st.info(f"NDC {ndc_value} is already in the medication list.")

    if clear_medications:
        st.session_state["selected_medication_rows"] = []
        st.info("Medication list cleared.")

    medication_rows = st.session_state.get("selected_medication_rows", [])
    if len(medication_rows) > 0:
        med_df = pd.DataFrame(medication_rows)
        edited_med_df = st.data_editor(
            med_df,
            hide_index=True,
            num_rows="dynamic",
            use_container_width=True,
            key="wizard_selected_medication_editor",
            column_config={
                "drug_name": st.column_config.TextColumn("Drug name"),
                "ndc": st.column_config.TextColumn("NDC"),
                "days_supply_mode": st.column_config.SelectboxColumn("Days supply", options=[30, 60, 90], required=True),
                "tier_level": st.column_config.SelectboxColumn("Tier", options=[1, 2, 3, 4, 5, 6, 7], required=True),
                "fills_per_year": st.column_config.NumberColumn("Fills/year", min_value=1.0, max_value=100.0, step=1.0),
                "is_insulin": st.column_config.CheckboxColumn("Insulin"),
                "annual_cost_est": st.column_config.NumberColumn("Annual cost est ($)", min_value=0.0, max_value=100000.0, step=50.0),
            },
        )
        medication_rows = edited_med_df.to_dict("records")
    else:
        st.info("No medications added yet. The app will fall back to the baseline profile values from step 1.")

    medication_summary = summarize_medication_rows(medication_rows)
    st.session_state["selected_medication_rows"] = medication_summary["rows"]

    if medication_summary["num_drugs"] > 0:
        metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
        metric_col1.metric("Unique medications", int(medication_summary["num_drugs"]))
        metric_col2.metric("Avg fills/year", f"{medication_summary['avg_fills_per_year']:.1f}")
        metric_col3.metric("Annual drug cost", f"${float(medication_summary['total_annual_drug_cost']):,.0f}")
        metric_col4.metric("Insulin flagged", "Yes" if int(medication_summary["is_insulin_user"]) == 1 else "No")
        st.caption(f"Requested NDCs: {', '.join(medication_summary['requested_ndcs'])}")

    return medication_summary


def render_preferences_step(medication_summary):
    """Render beneficiary priorities and search controls."""
    st.subheader("3. Preferences")
    st.caption("Set how strict the app should be on local eligibility, requested-drug coverage, and access tradeoffs.")

    has_medications = int(medication_summary.get("num_drugs", 0) or 0) > 0
    if has_medications:
        st.info("Medication list supplied. Coverage and cost estimation will use those requested drugs directly.")
    else:
        st.warning("No medication list supplied. Results will rely more on plan-level averages and should be treated as lower-confidence decision support.")

    search_scope = st.radio(
        "Search scope",
        ["Local plans only", "Allow nearby comparison plans"],
        index=0,
        horizontal=True,
        key="preference_search_scope",
        help="Nearby plans are shown for comparison only. They are not treated as eligible recommendations for the selected county.",
    )
    local_only = search_scope == "Local plans only"

    col1, col2, col3 = st.columns(3)
    max_distance = col1.slider(
        "Nearby comparison radius (miles)",
        min_value=10,
        max_value=100,
        value=50,
        step=10,
        disabled=local_only,
    )
    min_drug_coverage_pct = col2.slider(
        "Minimum requested-drug coverage (%)",
        min_value=0,
        max_value=100,
        value=100 if has_medications else 0,
        step=5,
        disabled=not has_medications,
        help="Only plans meeting this threshold are treated as recommendation-eligible when a medication list is present.",
    )
    distance_penalty_rate = col3.slider(
        "Pharmacy-distance sensitivity",
        min_value=10,
        max_value=100,
        value=50,
        step=10,
        help="Higher values penalize plans with weaker nearby preferred pharmacy access more heavily.",
    )

    st.markdown("#### Recommendation Weights")
    weight_col1, weight_col2, weight_col3, weight_col4 = st.columns(4)
    ml_influence_pct = weight_col1.slider("ML influence (%)", min_value=10, max_value=70, value=35, step=5)
    cost_priority = weight_col2.slider("Cost priority", min_value=1, max_value=5, value=5)
    access_priority = weight_col3.slider("Access priority", min_value=1, max_value=5, value=4)
    coverage_priority = weight_col4.slider("Medication-fit priority", min_value=1, max_value=5, value=4)

    decision_weights = build_decision_weights(
        ml_influence_pct,
        cost_priority,
        access_priority,
        coverage_priority,
    )
    st.caption(
        "Final weighting mix: "
        f"ML {decision_weights['ml']:.0%} | "
        f"Cost {decision_weights['cost']:.0%} | "
        f"Access {decision_weights['access']:.0%} | "
        f"Coverage {decision_weights['coverage']:.0%}"
    )

    preference_contract = PreferenceWeights(
        ml_weight=float(decision_weights["ml"]),
        cost_weight=float(decision_weights["cost"]),
        access_weight=float(decision_weights["access"]),
        coverage_weight=float(decision_weights["coverage"]),
        distance_penalty_rate=float(distance_penalty_rate),
        minimum_coverage_pct=float(min_drug_coverage_pct),
        local_only=bool(local_only),
    )

    return {
        "decision_weights": decision_weights,
        "preference_contract": preference_contract,
        "max_distance": int(max_distance),
        "distance_penalty_rate": float(distance_penalty_rate),
        "minimum_coverage_pct": float(min_drug_coverage_pct),
        "local_only": bool(local_only),
        "ml_influence_pct": int(ml_influence_pct),
    }


def enrich_requested_drug_coverage(plans_df, requested_ndcs):
    """Merge requested-drug coverage onto candidate plan rows."""
    if len(plans_df) == 0:
        return plans_df

    requested_ndcs = tuple(str(ndc) for ndc in (requested_ndcs or ()))
    enriched = plans_df.copy()
    if len(requested_ndcs) == 0:
        enriched["requested_drugs"] = 0
        enriched["covered_drugs"] = 0
        enriched["in_formulary_drugs"] = 0
        enriched["excluded_drugs"] = 0
        enriched["drug_coverage_pct"] = 0.0
        enriched["uncovered_ndcs"] = ""
        enriched["coverage_status"] = "Not evaluated"
        return deduplicate_plan_candidates(enriched)

    coverage_df = get_plan_drug_coverage(
        tuple(enriched["PLAN_KEY"].astype(str).tolist()),
        requested_ndcs,
    )
    if len(coverage_df) > 0:
        enriched = enriched.merge(coverage_df, on="PLAN_KEY", how="left")
    else:
        enriched["requested_drugs"] = len(requested_ndcs)
        enriched["covered_drugs"] = 0
        enriched["in_formulary_drugs"] = 0
        enriched["excluded_drugs"] = 0
        enriched["drug_coverage_pct"] = 0.0
        enriched["uncovered_ndcs"] = ""

    for col, fill_value in [
        ("requested_drugs", len(requested_ndcs)),
        ("covered_drugs", 0),
        ("in_formulary_drugs", 0),
        ("excluded_drugs", 0),
        ("drug_coverage_pct", 0.0),
    ]:
        if col not in enriched.columns:
            enriched[col] = fill_value
        enriched[col] = pd.to_numeric(enriched[col], errors="coerce").fillna(fill_value)
    if "uncovered_ndcs" not in enriched.columns:
        enriched["uncovered_ndcs"] = ""
    enriched["uncovered_ndcs"] = enriched["uncovered_ndcs"].fillna("")
    enriched["coverage_status"] = enriched["drug_coverage_pct"].apply(
        lambda pct: classify_coverage_status(pct, requested_drugs=len(requested_ndcs))
    )
    return deduplicate_plan_candidates(enriched)


def get_nearby_comparison_plans(state_name, state_code, county_code, user_zip, max_distance=50):
    """Load nearby-county plans as comparison-only rows."""
    if not user_zip:
        return pd.DataFrame()

    nearby_counties = get_nearby_counties(user_zip, state_name, max_distance)
    if len(nearby_counties) == 0:
        return pd.DataFrame()

    target_county_code = str(county_code or "").zfill(5)
    comparison_frames = []

    for _, county_row in nearby_counties.iterrows():
        county_code_raw = county_row.get("county_code")
        try:
            nearby_county_code = str(int(float(county_code_raw))).zfill(5)
        except (TypeError, ValueError):
            nearby_county_code = str(county_code_raw or "")

        if not nearby_county_code or nearby_county_code == target_county_code:
            continue

        county_plans = get_plans_for_location(state_code, nearby_county_code)
        if len(county_plans) == 0:
            continue

        county_plans = county_plans.copy()
        county_plans["is_local"] = False
        county_plans["service_area_eligible"] = False
        county_plans["comparison_only"] = True
        county_plans["eligibility_status"] = "Comparison only - outside selected service area"
        county_plans["distance_miles"] = float(county_row.get("distance_miles", 0.0) or 0.0)
        county_plans["distance_category"] = categorize_distance(county_plans["distance_miles"].iloc[0])
        county_plans["location_county"] = county_row.get("county", "")
        comparison_frames.append(county_plans)

    if len(comparison_frames) == 0:
        return pd.DataFrame()

    combined = pd.concat(comparison_frames, ignore_index=True)
    return deduplicate_plan_candidates(combined)


def execute_decision_support_run(profile_inputs, medication_summary, preference_inputs, model_data):
    """Run the staged decision-support pipeline and build audit artifacts."""
    state_code = profile_inputs.get("state_code")
    county_code = profile_inputs.get("county_code")
    if not state_code or not county_code:
        return {"error": "The selected state/county could not be resolved to a service area code."}

    requested_ndcs = tuple(medication_summary.get("requested_ndcs", ()))
    requested_name_map = dict(medication_summary.get("requested_name_map", {}))
    medication_rows = medication_summary.get("rows", [])

    num_drugs = int(medication_summary["num_drugs"]) if int(medication_summary.get("num_drugs", 0) or 0) > 0 else int(profile_inputs["num_drugs"])
    avg_fills_per_year = float(medication_summary["avg_fills_per_year"]) if int(medication_summary.get("num_drugs", 0) or 0) > 0 else float(profile_inputs["avg_fills_per_year"])
    total_rx_cost_est = float(medication_summary["total_annual_drug_cost"]) if float(medication_summary.get("total_annual_drug_cost", 0.0) or 0.0) > 0 else float(profile_inputs["total_rx_cost_est"])
    is_insulin_user = int(medication_summary["is_insulin_user"]) if int(medication_summary.get("num_drugs", 0) or 0) > 0 else int(bool(profile_inputs["is_insulin_user"]))
    effective_zip = profile_inputs.get("zip_code") or profile_inputs.get("representative_zip")

    beneficiary_profile = {
        "state": profile_inputs["state_name"],
        "county": profile_inputs["county_name"],
        "state_code": state_code,
        "county_code": county_code,
        "zip_code": effective_zip,
        "num_drugs": num_drugs,
        "avg_fills_per_year": avg_fills_per_year,
        "is_insulin_user": is_insulin_user,
        "total_rx_cost_est": total_rx_cost_est,
        "risk_segment": profile_inputs["risk_segment"],
    }

    profile_contract = ProfileInput(
        state=profile_inputs["state_name"],
        county=profile_inputs["county_name"],
        state_code=state_code,
        county_code=county_code,
        zip_code=effective_zip,
        risk_segment=profile_inputs["risk_segment"],
        num_drugs=num_drugs,
        avg_fills_per_year=avg_fills_per_year,
        is_insulin_user=is_insulin_user,
        total_rx_cost_est=total_rx_cost_est,
    )
    medication_contract = [
        MedicationListItem(
            drug_name=str(row.get("drug_name", "")),
            ndc=str(row.get("ndc", "")),
            fills_per_year=float(row.get("fills_per_year", 12.0) or 12.0),
            days_supply_mode=int(row.get("days_supply_mode", 30) or 30),
            tier_level=int(row.get("tier_level", 1) or 1),
            is_insulin=bool(row.get("is_insulin", False)),
            annual_cost_est=float(row.get("annual_cost_est", 0.0) or 0.0),
        )
        for row in medication_rows
    ]
    preference_contract = preference_inputs["preference_contract"]
    public_contract = as_public_types(profile_contract, medication_contract, preference_contract)

    local_plans = get_plans_for_location(state_code, county_code)
    local_plans = deduplicate_plan_candidates(local_plans)
    local_plans = enrich_requested_drug_coverage(local_plans, requested_ndcs)

    comparison_plans = pd.DataFrame()
    if not preference_inputs["local_only"]:
        comparison_plans = get_nearby_comparison_plans(
            profile_inputs["state_name"],
            state_code,
            county_code,
            effective_zip,
            max_distance=preference_inputs["max_distance"],
        )
        comparison_plans = enrich_requested_drug_coverage(comparison_plans, requested_ndcs)

    if len(local_plans) > 0 and len(requested_ndcs) > 0:
        recommendation_candidates = local_plans[
            pd.to_numeric(local_plans["drug_coverage_pct"], errors="coerce").fillna(0.0) >= float(preference_inputs["minimum_coverage_pct"])
        ].copy()
    else:
        recommendation_candidates = local_plans.copy()

    comparison_only = False
    result_message = ""
    if len(recommendation_candidates) == 0:
        comparison_only = True
        if len(local_plans) > 0:
            recommendation_candidates = local_plans.copy()
            result_message = (
                f"No local plan met the requested-drug threshold of {int(preference_inputs['minimum_coverage_pct'])}%."
                " Showing local plans in comparison-and-warning mode."
            )
        elif len(comparison_plans) > 0:
            recommendation_candidates = comparison_plans.copy()
            result_message = "No eligible local plans were found. Showing nearby county plans for comparison only."
        else:
            return {
                "error": "No plans were found for the selected service area, and no nearby comparison plans were available.",
                "public_contract": public_contract,
            }
    else:
        result_message = "Eligible local plans were found and ranked within the selected service area."

    ranked_recommendations = rank_plans(
        recommendation_candidates,
        beneficiary_profile,
        model_data,
        decision_weights=preference_inputs["decision_weights"],
        distance_penalty_rate=preference_inputs["distance_penalty_rate"],
        medication_rows=medication_rows,
    )
    ranked_comparison = pd.DataFrame()
    if len(comparison_plans) > 0:
        ranked_comparison = rank_plans(
            comparison_plans,
            beneficiary_profile,
            model_data,
            decision_weights=preference_inputs["decision_weights"],
            distance_penalty_rate=preference_inputs["distance_penalty_rate"],
            medication_rows=medication_rows,
        )

    run_id = uuid.uuid4().hex[:12]
    recommendation_schema = build_recommendation_schema(
        ranked_recommendations,
        run_id=run_id,
        requested_drugs=len(requested_ndcs),
    )
    comparison_schema = build_recommendation_schema(
        ranked_comparison,
        run_id=run_id,
        requested_drugs=len(requested_ndcs),
    ) if len(ranked_comparison) > 0 else pd.DataFrame()

    coverage_source = recommendation_schema.copy()
    if len(comparison_schema) > 0:
        coverage_source = pd.concat([coverage_source, comparison_schema], ignore_index=True)
    feature_coverage = compute_feature_coverage(coverage_source)

    audit = create_run_audit(
        user_input_summary=public_contract,
        model_version=get_model_version(model_data),
        data_snapshot=get_data_snapshot_id(),
        seed=DEFAULT_RESEARCH_SEED,
        feature_coverage=feature_coverage,
        recommendations=recommendation_schema,
        run_id=run_id,
    )
    audit["results_mode"] = "comparison_only" if comparison_only else "decision_support"
    audit["result_message"] = result_message
    audit["search_context"] = {
        "local_candidates": int(len(local_plans)),
        "recommendation_candidates": int(len(recommendation_candidates)),
        "nearby_comparison_candidates": int(len(comparison_plans)),
        "requested_drugs": int(len(requested_ndcs)),
        "minimum_coverage_pct": float(preference_inputs["minimum_coverage_pct"]),
        "local_only": bool(preference_inputs["local_only"]),
    }

    trusted_recommendation = False
    if len(recommendation_schema) > 0:
        top_plan = recommendation_schema.iloc[0]
        trusted_recommendation = (
            top_plan.get("eligibility_status") == "Eligible"
            and top_plan.get("coverage_status") != "Poor fit"
            and top_plan.get("confidence_band") in {"Medium", "High"}
            and not comparison_only
        )

    return {
        "run_id": run_id,
        "public_contract": public_contract,
        "audit": audit,
        "feature_coverage": feature_coverage,
        "recommendations": recommendation_schema,
        "comparison_recommendations": comparison_schema,
        "result_message": result_message,
        "comparison_only": comparison_only,
        "trusted_recommendation": trusted_recommendation,
        "requested_ndcs": requested_ndcs,
        "requested_name_map": requested_name_map,
        "beneficiary_profile": beneficiary_profile,
        "effective_zip": effective_zip,
    }


def render_featured_plan(plan_row, requested_name_map):
    """Render the top recommendation card."""
    cost_breakdown = _coerce_dict(plan_row.get("cost_breakdown", {}))
    access_summary = _coerce_dict(plan_row.get("access_summary", {}))
    warning_flags = _coerce_list(plan_row.get("warning_flags", []))
    evidence_gaps = _coerce_list(plan_row.get("evidence_gaps", []))

    st.subheader("Featured Plan")
    st.markdown(f"### {plan_row['PLAN_NAME']}")
    st.caption(f"{plan_row.get('contract_type', 'Unknown')} | {plan_row.get('eligibility_status', 'Unknown')}")

    if plan_row.get("eligibility_status") == "Eligible" and plan_row.get("confidence_band") in {"Medium", "High"}:
        st.success("This plan passed the current trust checks for service area, requested-drug fit, and evidence quality.")
    else:
        st.warning("Use this plan as decision support only. Verify service area, formulary details, and nearby pharmacy access before acting.")

    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)
    metric_col1.metric("Decision score", f"{float(plan_row.get('decision_score', 0.0)):.1f}")
    metric_col2.metric("ML score", f"{float(plan_row.get('ml_score', 0.0)):.2f}")
    metric_col3.metric("Coverage fit", str(plan_row.get("coverage_status", "Unknown")))
    metric_col4.metric("Confidence", str(plan_row.get("confidence_band", "Unknown")))
    metric_col5.metric("Estimated annual total", f"${float(plan_row.get('estimated_total_annual_cost', 0.0)):,.0f}")

    reason_lines = []
    decision_reason = str(plan_row.get("decision_reason", "") or "").strip()
    if decision_reason:
        reason_lines.append(decision_reason.capitalize())
    if float(plan_row.get("coverage_pct_requested", 0.0) or 0.0) > 0:
        reason_lines.append(f"Requested-drug coverage is {float(plan_row.get('coverage_pct_requested', 0.0)):.1f}%.")
    nearest_preferred = float(access_summary.get("nearest_preferred_miles", 0.0) or 0.0)
    if nearest_preferred > 0:
        reason_lines.append(f"Nearest preferred pharmacy estimate: {nearest_preferred:.1f} miles.")
    if float(cost_breakdown.get("annual_premium", 0.0) or 0.0) > 0:
        reason_lines.append(f"Annual premium estimate: ${float(cost_breakdown.get('annual_premium', 0.0)):,.0f}.")

    st.markdown("**Why it surfaced**")
    for line in reason_lines[:4]:
        st.write(f"- {line}")

    uncovered_ndcs = [token.strip() for token in str(plan_row.get("uncovered_ndcs", "") or "").split(",") if token.strip()]
    if uncovered_ndcs:
        uncovered_display = []
        for ndc in uncovered_ndcs:
            drug_name = requested_name_map.get(ndc, "")
            uncovered_display.append(f"{drug_name} (NDC {ndc})" if drug_name else f"NDC {ndc}")
        st.markdown("**Requested medications still to verify**")
        st.write(", ".join(uncovered_display))

    info_col1, info_col2 = st.columns(2)
    info_col1.markdown("**Warnings**")
    if len(warning_flags) > 0:
        for warning in warning_flags:
            info_col1.write(f"- {warning}")
    else:
        info_col1.write("- No major warning flags were raised for the current inputs.")

    info_col2.markdown("**Evidence gaps**")
    if len(evidence_gaps) > 0:
        for gap in evidence_gaps:
            info_col2.write(f"- {gap}")
    else:
        info_col2.write("- The main evidence fields used by this run were available.")


def render_recommendation_expanders(recommendation_df):
    """Render per-plan decision-support details."""
    if len(recommendation_df) == 0:
        return

    st.subheader("Plan Details")
    for _, plan in recommendation_df.head(5).iterrows():
        title = f"{int(plan.get('rank', 0))}. {plan['PLAN_NAME']} | {plan.get('coverage_status', 'Unknown')} | {plan.get('confidence_band', 'Unknown')}"
        with st.expander(title):
            cost_breakdown = _coerce_dict(plan.get("cost_breakdown", {}))
            access_summary = _coerce_dict(plan.get("access_summary", {}))
            warning_flags = _coerce_list(plan.get("warning_flags", []))
            evidence_gaps = _coerce_list(plan.get("evidence_gaps", []))

            metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
            metric_col1.metric("Annual premium", f"${float(cost_breakdown.get('annual_premium', 0.0)):,.0f}")
            metric_col2.metric("Estimated OOP", f"${float(cost_breakdown.get('estimated_oop', 0.0)):,.0f}")
            metric_col3.metric("Distance penalty", f"${float(cost_breakdown.get('distance_penalty', 0.0)):,.0f}")
            metric_col4.metric("Nearest preferred", f"{float(access_summary.get('nearest_preferred_miles', 0.0)):.1f} mi")

            st.markdown("**Why this plan**")
            st.write(str(plan.get("decision_reason", "Balanced across cost, access, and medication fit.")))

            st.markdown("**What to verify**")
            if len(warning_flags) > 0:
                for warning in warning_flags:
                    st.write(f"- {warning}")
            else:
                st.write("- No major verification flags were raised.")

            st.markdown("**What changes if assumptions change**")
            change_lines = []
            uncovered_component = float(_coerce_dict(cost_breakdown.get("oop_components", {})).get("uncovered_component", 0.0) or 0.0)
            if uncovered_component > 0:
                change_lines.append("This plan is sensitive to the requested medications that are not currently covered.")
            if float(access_summary.get("distance_miles", 0.0) or 0.0) > 0:
                change_lines.append("Changing the preferred ZIP or pharmacy distance tolerance could reorder this plan.")
            if len(evidence_gaps) > 0:
                change_lines.append("Better network or formulary evidence could change confidence more than rank.")
            if len(change_lines) == 0:
                change_lines.append("This plan looks relatively stable under the current assumptions.")
            for line in change_lines:
                st.write(f"- {line}")

            if len(evidence_gaps) > 0:
                st.markdown("**Evidence gaps**")
                for gap in evidence_gaps:
                    st.write(f"- {gap}")


def render_research_mode(model_data):
    """Render the research and evaluation page."""
    st.subheader("Research / Evaluation")
    st.caption("This mode compares the trained LightGBM ranker against a transparent heuristic baseline, with deterministic sampling and subgroup reporting.")

    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    sample_size = filter_col1.slider("Beneficiary sample", min_value=100, max_value=2000, value=500, step=100)
    risk_segment = filter_col2.selectbox("Risk segment", ["All", "Low", "Medium", "High"], index=0)
    insulin_filter = filter_col3.selectbox("Insulin slice", ["All", "Insulin users only", "Non-insulin users only"], index=0)
    state_filter = filter_col4.selectbox("State filter", get_research_states(), index=0)

    control_col1, control_col2 = st.columns(2)
    research_seed = control_col1.number_input(
        "Research seed",
        min_value=1,
        max_value=100000,
        value=DEFAULT_RESEARCH_SEED,
        step=1,
    )
    run_research = control_col2.button("Run research evaluation", type="primary", use_container_width=True)

    if run_research:
        with st.spinner("Running deterministic evaluation sample and scoring pipeline..."):
            sample_df = load_research_sample(
                get_database(),
                bene_limit=int(sample_size),
                seed=int(research_seed),
                risk_segment=risk_segment,
                insulin_filter=insulin_filter,
                state_filter=state_filter,
            )
            if len(sample_df) == 0:
                st.session_state["research_outputs"] = {"error": "No research sample rows matched the current filters."}
            else:
                scored_df = score_research_sample(sample_df, model_data)
                summary_df, detail_df = evaluate_model_vs_baseline(scored_df)
                stability_df = compute_preference_stability(scored_df)
                diagnostics_df = build_data_coverage_diagnostics(scored_df)
                fairness_tables = build_fairness_tables(detail_df)
                st.session_state["research_outputs"] = {
                    "summary_df": summary_df,
                    "detail_df": detail_df,
                    "stability_df": stability_df,
                    "diagnostics_df": diagnostics_df,
                    "fairness_tables": fairness_tables,
                    "sample_df": sample_df,
                    "seed": int(research_seed),
                }

    outputs = st.session_state.get("research_outputs")
    if not outputs:
        st.info("Set the sample filters and run the evaluation to compare ML performance against the transparent baseline.")
        return
    if outputs.get("error"):
        st.warning(outputs["error"])
        return

    summary_df = outputs["summary_df"]
    detail_df = outputs["detail_df"]
    stability_df = outputs["stability_df"]
    diagnostics_df = outputs["diagnostics_df"]
    fairness_tables = outputs["fairness_tables"]

    st.markdown("#### Model vs baseline summary")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)

    chart_df = summary_df.melt(
        id_vars=["method"],
        value_vars=["avg_top1_cost_regret", "avg_requested_drug_coverage", "best_plan_hit_rate"],
        var_name="metric",
        value_name="value",
    )
    fig = px.bar(chart_df, x="metric", y="value", color="method", barmode="group", title="ML model vs transparent baseline")
    st.plotly_chart(fig, use_container_width=True)

    diag_col1, diag_col2 = st.columns(2)
    with diag_col1:
        st.markdown("#### Data coverage diagnostics")
        st.dataframe(diagnostics_df, use_container_width=True, hide_index=True)
    with diag_col2:
        st.markdown("#### Recommendation stability")
        st.dataframe(stability_df, use_container_width=True, hide_index=True)

    st.markdown("#### Fairness and subgroup slices")
    fairness_tabs = st.tabs(["Insulin", "Risk", "Density", "State", "County"])
    tab_map = [
        ("insulin", fairness_tabs[0]),
        ("risk_segment", fairness_tabs[1]),
        ("density", fairness_tabs[2]),
        ("state", fairness_tabs[3]),
        ("county", fairness_tabs[4]),
    ]
    for key, tab in tab_map:
        with tab:
            table = fairness_tables.get(key)
            if table is None or len(table) == 0:
                st.info(f"No subgroup rows were available for {key}.")
            else:
                st.dataframe(table, use_container_width=True, hide_index=True)

    st.markdown("#### Methodology summary")
    st.markdown(
        """
        - Candidate rows come from `ml.training_plan_pairs`.
        - Sampling is deterministic for a given seed and filter set.
        - The ML model is compared against a transparent heuristic baseline built from cost, access, and requested-drug coverage.
        - Output tables expose cost regret, requested-drug coverage satisfaction, access burden, and subgroup performance slices.
        - Sparse network or formulary evidence should be interpreted as a limitation, not hidden behind a single score.
        """
    )

    download_col1, download_col2 = st.columns(2)
    download_col1.download_button(
        label="Download summary report (CSV)",
        data=summary_df.to_csv(index=False),
        file_name=f"research_summary_{outputs['seed']}.csv",
        mime="text/csv",
    )
    download_col2.download_button(
        label="Download detailed evaluation (CSV)",
        data=detail_df.to_csv(index=False),
        file_name=f"research_detail_{outputs['seed']}.csv",
        mime="text/csv",
    )


def main():
    """Trust-first decision support and research entrypoint."""
    init_app_state()

    st.markdown('<div class="main-header">Medicare Part D Decision Support</div>', unsafe_allow_html=True)
    st.markdown("**Decision support for beneficiaries, plus reproducible evaluation for research.**")

    model_data = load_trained_model()
    if model_data is None:
        st.error("Trained model not found. Please run `python ml_model/train_model_from_db.py` first.")
        st.stop()

    model_version = get_model_version(model_data)
    data_snapshot = get_data_snapshot_id()
    stats = model_data.get("training_stats", {})

    st.sidebar.header("Workspace")
    app_mode = st.sidebar.radio(
        "Mode",
        ["Beneficiary Decision Support", "Research / Evaluation"],
        index=0,
    )
    st.sidebar.caption(f"Model version: `{model_version}`")
    st.sidebar.caption(f"Data snapshot: `{data_snapshot}`")
    st.sidebar.caption(f"Seed: `{DEFAULT_RESEARCH_SEED}`")
    if "test_ndcg@3" in stats:
        st.sidebar.metric("Test NDCG@3", f"{float(stats.get('test_ndcg@3', 0.0)):.3f}")

    if app_mode == "Research / Evaluation":
        render_research_mode(model_data)
        return

    profile_tab, medication_tab, preference_tab, results_tab = st.tabs(
        ["1. Profile", "2. Medications", "3. Preferences", "4. Results"]
    )

    with profile_tab:
        profile_inputs = render_profile_step()

    with medication_tab:
        medication_summary = render_medication_step()

    with preference_tab:
        preference_inputs = render_preferences_step(medication_summary)

    with results_tab:
        st.subheader("4. Results")
        st.caption("Recommendations are only shown strongly when local eligibility, medication fit, and evidence quality all clear the trust checks.")

        run_recommendation = st.button("Run decision support", type="primary", use_container_width=True)
        if run_recommendation:
            with st.spinner("Running service-area checks, requested-drug coverage, cost estimation, and ML reranking..."):
                result = execute_decision_support_run(
                    profile_inputs,
                    medication_summary,
                    preference_inputs,
                    model_data,
                )
            if result.get("error"):
                st.error(result["error"])
            else:
                st.session_state["active_run_id"] = result["run_id"]
                st.session_state["recommendation_runs"][result["run_id"]] = result

        active_run_id = st.session_state.get("active_run_id")
        if not active_run_id:
            st.info("Complete the first three steps, then run decision support to generate ranked plans and audit artifacts.")
            return

        result = st.session_state["recommendation_runs"].get(active_run_id)
        if not result:
            st.info("No active recommendation run is stored yet.")
            return

        recommendation_df = result["recommendations"]
        comparison_df = result["comparison_recommendations"]
        feature_coverage = result["feature_coverage"]
        audit = result["audit"]

        if result["comparison_only"]:
            st.warning(result["result_message"])
        elif result["trusted_recommendation"]:
            st.success(result["result_message"])
        else:
            st.info(result["result_message"])

        trust_col1, trust_col2, trust_col3, trust_col4 = st.columns(4)
        trust_col1.metric("Candidate plans", int(feature_coverage.get("candidate_plans", 0)))
        trust_col2.metric("Eligible plans", int(feature_coverage.get("eligible_plans", 0)))
        trust_col3.metric("With network metrics", int(feature_coverage.get("plans_with_network_metrics", 0)))
        trust_col4.metric("With pharmacy-distance data", int(feature_coverage.get("plans_with_pharmacy_distance_data", 0)))

        if len(recommendation_df) == 0:
            st.warning("No plans were available after the staged trust checks.")
            return

        featured_plan = recommendation_df.iloc[0]
        render_featured_plan(featured_plan, result["requested_name_map"])

        st.subheader("Side-by-Side Comparison")
        comparison_view = recommendation_df.head(5)[
            [
                "rank",
                "PLAN_NAME",
                "eligibility_status",
                "coverage_status",
                "coverage_pct_requested",
                "estimated_total_annual_cost",
                "decision_score",
                "ml_score",
                "confidence_band",
            ]
        ].copy()
        comparison_view = comparison_view.rename(
            columns={
                "PLAN_NAME": "Plan",
                "eligibility_status": "Eligibility",
                "coverage_status": "Medication fit",
                "coverage_pct_requested": "Coverage %",
                "estimated_total_annual_cost": "Estimated annual total",
                "decision_score": "Decision score",
                "ml_score": "ML score",
                "confidence_band": "Confidence",
            }
        )
        st.dataframe(comparison_view, use_container_width=True, hide_index=True)

        render_recommendation_expanders(recommendation_df)

        if len(comparison_df) > 0:
            with st.expander("Nearby county comparisons"):
                st.caption("These plans are outside the selected county service area and are displayed for comparison only.")
                nearby_view = comparison_df.head(5)[
                    [
                        "PLAN_NAME",
                        "eligibility_status",
                        "coverage_status",
                        "coverage_pct_requested",
                        "estimated_total_annual_cost",
                        "confidence_band",
                    ]
                ].copy()
                nearby_view = nearby_view.rename(
                    columns={
                        "PLAN_NAME": "Plan",
                        "eligibility_status": "Eligibility",
                        "coverage_status": "Medication fit",
                        "coverage_pct_requested": "Coverage %",
                        "estimated_total_annual_cost": "Estimated annual total",
                        "confidence_band": "Confidence",
                    }
                )
                st.dataframe(nearby_view, use_container_width=True, hide_index=True)

        with st.expander("Assumptions and trust signals"):
            st.json(audit["search_context"])
            st.json(result["public_contract"])

        with st.expander("County access context"):
            render_county_access_visualization(
                result["beneficiary_profile"]["state"],
                result["beneficiary_profile"]["county"],
                result["beneficiary_profile"]["state_code"],
                result["beneficiary_profile"]["county_code"],
            )

        export_col1, export_col2 = st.columns(2)
        export_col1.download_button(
            label="Download recommendation report (CSV)",
            data=serialize_nested_columns(recommendation_df).to_csv(index=False),
            file_name=f"recommendations_{active_run_id}.csv",
            mime="text/csv",
        )
        export_col2.download_button(
            label="Download audit record (JSON)",
            data=json.dumps(audit, indent=2),
            file_name=f"recommendation_audit_{active_run_id}.json",
            mime="application/json",
        )


if __name__ == "__main__":
    main()
