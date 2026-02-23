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

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.db_manager import get_db

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
    ranked['coverage_component'] = (
        0.35 * generic_component
        + 0.25 * restrictive_component
        + 0.20 * pa_component
        + 0.10 * st_component
        + 0.10 * ql_component
    )

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
    query = f"""
        SELECT DISTINCT county 
        FROM gold.dim_zipcode 
        WHERE state = '{state_name}'
        ORDER BY county
    """
    counties = db.query_df(query)['county'].tolist()
    return counties


@st.cache_data
def get_location_from_zip(zip_code):
    """Get state and county from zip code."""
    db = get_database()
    query = f"""
        SELECT state, county, city
        FROM gold.dim_zipcode
        WHERE zip_code = '{zip_code}'
        LIMIT 1
    """
    result = db.query_df(query)
    if len(result) > 0:
        return result.iloc[0].to_dict()
    return None


@st.cache_data
def get_default_zip_for_county(state_name, county_name):
    """Get a representative ZIP code for county-level search fallback."""
    db = get_database()
    query = f"""
        SELECT zip_code
        FROM gold.dim_zipcode
        WHERE state = '{state_name}'
          AND county = '{county_name}'
          AND zip_code IS NOT NULL
        ORDER BY population DESC NULLS LAST, zip_code
        LIMIT 1
    """
    result = db.query_df(query)
    if len(result) == 0:
        return None
    return str(result['zip_code'].iloc[0])


@st.cache_data
def get_state_county_codes(state_name, county_name):
    """Get state and county codes for querying plans."""
    db = get_database()
    # Get state code (2-letter)
    state_query = f"""
        SELECT DISTINCT z.state as state_name, p.STATE as state_code
        FROM gold.dim_zipcode z
        JOIN bronze.brz_plan_info p ON z.county_code = p.COUNTY_CODE
        WHERE z.state = '{state_name}'
        LIMIT 1
    """
    state_result = db.query_df(state_query)
    
    if len(state_result) == 0:
        return None, None
    
    state_code = state_result['state_code'].iloc[0]
    
    # Get county code
    county_query = f"""
        SELECT DISTINCT county_code
        FROM gold.dim_zipcode
        WHERE state = '{state_name}' AND county = '{county_name}'
        LIMIT 1
    """
    county_result = db.query_df(county_query)
    
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
    query = f"""
    WITH beneficiary_loc AS (
        SELECT AVG(lat) as lat, AVG(lng) as lng
        FROM gold.dim_zipcode
        WHERE zip_code = '{user_zip}'
    ),
    county_centers AS (
        SELECT 
            county_code,
            state,
            county,
            AVG(lat) as clat,
            AVG(lng) as clng
        FROM gold.dim_zipcode
        WHERE state = '{state_name}'
        GROUP BY county_code, state, county
    )
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
    WHERE distance_miles <= {max_distance_miles}
    ORDER BY distance_miles
    """
    
    nearby = db.query_df(query)
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
    
    query = f"""
        SELECT
            p.PLAN_KEY,
            p.PLAN_NAME,
            p.CONTRACT_NAME,
            CASE WHEN p.IS_MA_PD THEN 'MA' WHEN p.IS_PDP THEN 'PDP' ELSE 'Other' END as contract_type,
            CAST(p.PREMIUM AS DOUBLE) as premium,
            CAST(p.DEDUCTIBLE AS DOUBLE) as deductible,
            
            -- Formulary metrics (corrected column names)
            CAST(COALESCE(fm.generic_tier_pct, 0) AS DOUBLE) as formulary_generic_pct,
            CAST(COALESCE(fm.specialty_tier_pct, 0) AS DOUBLE) as formulary_specialty_pct,
            CAST(COALESCE(fm.pa_rate, 0) AS DOUBLE) as formulary_pa_rate,
            CAST(COALESCE(fm.st_rate, 0) AS DOUBLE) as formulary_st_rate,
            CAST(COALESCE(fm.ql_rate, 0) AS DOUBLE) as formulary_ql_rate,
            CAST(COALESCE(fm.restrictiveness_class, 0) AS INTEGER) as formulary_restrictiveness,
            
            -- Network metrics (corrected column names)
            CAST(COALESCE(nm.preferred_pharmacies, 0) AS INTEGER) as network_preferred_pharmacies,
            CAST(COALESCE(nm.total_pharmacies, 0) AS INTEGER) as network_total_pharmacies,
            CAST(COALESCE(nm.network_adequacy_flag, 0) AS INTEGER) as network_adequacy_flag,
            
            -- Distance (estimate)
            CAST(5.0 AS DOUBLE) as distance_miles,
            CAST(0 AS INTEGER) as has_distance_tradeoff
            
        FROM bronze.brz_plan_info p
        LEFT JOIN gold.agg_plan_formulary_metrics fm ON p.PLAN_KEY = fm.PLAN_KEY
        LEFT JOIN gold.agg_plan_network_metrics nm ON p.PLAN_KEY = nm.plan_key
        WHERE p.STATE = '{state}'
            AND p.COUNTY_CODE = '{county_code}'
            AND p.PREMIUM IS NOT NULL
    """
    
    plans_df = db.query_df(query)
    return plans_df


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
    user_loc = db.query_df(f"""
        SELECT lat, lng
        FROM gold.dim_zipcode
        WHERE zip_code = '{user_zip}'
        LIMIT 1
    """)
    
    if len(user_loc) == 0:
        return pd.DataFrame()
    
    user_lat, user_lng = user_loc.iloc[0]['lat'], user_loc.iloc[0]['lng']
    
    # Build plan keys list for SQL IN clause
    plan_keys_str = ','.join([f"'{pk}'" for pk in plan_keys])
    
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
                    COS(RADIANS({user_lat})) * COS(RADIANS(z.lat)) *
                    COS(RADIANS(z.lng) - RADIANS({user_lng})) +
                    SIN(RADIANS({user_lat})) * SIN(RADIANS(z.lat))
                ))
            ) * 0.621371,  -- Convert km to miles
            2
        ) as distance_miles
    FROM bronze.brz_pharmacy_network pn
    JOIN gold.dim_zipcode z ON pn.PHARMACY_ZIPCODE = z.zip_code
    WHERE pn.PLAN_KEY IN ({plan_keys_str})
        AND pn.PHARMACY_TYPE = 'retail'  -- Focus on retail pharmacies
    ORDER BY pn.PLAN_KEY, distance_miles
    """
    
    pharmacy_dist = db.query_df(query)
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

    query = f"""
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
    WHERE p.STATE = '{state_code}'
      AND p.COUNTY_CODE = '{county_code}'
      AND LPAD(CAST(z.county_code AS VARCHAR), 5, '0') = '{county_code}'
      AND z.lat IS NOT NULL
      AND z.lng IS NOT NULL
    """

    return db.query_df(query)


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
def rank_plans(plans_df, beneficiary_profile, model_data, decision_weights, distance_penalty_rate=50.0):
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
                
                # Fill missing values for plans without pharmacy data
                plans_df['nearest_preferred_miles'] = plans_df['nearest_preferred_miles'].fillna(10.0)
                plans_df['preferred_within_10mi'] = plans_df['preferred_within_10mi'].fillna(0)
                plans_df['network_accessibility_score'] = plans_df['network_accessibility_score'].fillna(50)
            else:
                # No pharmacy data available, use defaults
                plans_df['nearest_preferred_miles'] = 10.0
                plans_df['preferred_within_10mi'] = plans_df.get('network_preferred_pharmacies', 0)
                plans_df['network_accessibility_score'] = 50
    else:
        # No zip code, use defaults
        plans_df['nearest_preferred_miles'] = 10.0
        plans_df['preferred_within_10mi'] = plans_df.get('network_preferred_pharmacies', 0)
        plans_df['network_accessibility_score'] = 50
    
    # Create features for each plan
    features_list = []
    
    for _, plan in plans_df.iterrows():
        estimated_oop = estimate_plan_oop(beneficiary_profile, plan)
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
    plans_df['estimated_annual_oop'] = [f['total_drug_oop'] for f in features_list]
    plans_df['distance_penalty'] = [f['distance_penalty'] for f in features_list]
    plans_df['total_annual_cost'] = plans_df['premium'] * 12 + plans_df['estimated_annual_oop']
    plans_df['total_cost_with_distance'] = plans_df['total_annual_cost'] + plans_df['distance_penalty']

    plans_df = compute_decision_support_scores(plans_df, decision_weights)
    plans_df = plans_df.sort_values(['decision_score', 'score'], ascending=[False, False]).reset_index(drop=True)
    plans_df['rank'] = range(1, len(plans_df) + 1)
    
    return plans_df


# ===== Main Application =====
def main():
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
    num_drugs = st.sidebar.number_input("Number of medications", min_value=1, max_value=20, value=3)
    avg_fills_per_year = st.sidebar.number_input("Average fills per year", min_value=1, max_value=100, value=12)
    is_insulin_user = st.sidebar.checkbox("Insulin user")
    
    st.sidebar.markdown("---")
    
    # Cost estimate
    st.sidebar.subheader("Cost Information")
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
        2. **Click "Get Plan Recommendations"** to run real-time ML inference
        3. **Review ranked plans** with cost breakdowns, network info, and warnings
        
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


if __name__ == "__main__":
    main()
