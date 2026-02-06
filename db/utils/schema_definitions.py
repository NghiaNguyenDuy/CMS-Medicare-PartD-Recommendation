"""
Shared Utilities - Schema Definitions

SQL schema definitions for all medallion layers.
"""

# Bronze Layer Schemas
BRONZE_SCHEMAS = {
    "bronze": """
        CREATE SCHEMA IF NOT EXISTS bronze;
    """,
    
    "brz_plan_info": """
        CREATE TABLE IF NOT EXISTS bronze.brz_plan_info (
            contract_id VARCHAR,
            plan_id VARCHAR,
            segment_id VARCHAR,
            contract_name VARCHAR,
            plan_name VARCHAR,
            formulary_id VARCHAR,
            premium DOUBLE,
            deductible DOUBLE,
            ma_region_code VARCHAR,
            pdp_region_code VARCHAR,
            state VARCHAR,
            county_code VARCHAR,
            snp INTEGER,
            plan_suppressed_yn VARCHAR,
            plan_key VARCHAR,
            -- Metadata
            contract_year INTEGER,
            contract_quarter VARCHAR,
            source_filename VARCHAR,
            ingestion_ts TIMESTAMP,
            updated_at TIMESTAMP,
            record_hash VARCHAR,
            PRIMARY KEY (plan_key, contract_year)
        );
        
        CREATE INDEX IF NOT EXISTS idx_brz_plan_county 
        ON bronze.brz_plan_info(county_code);
        
        CREATE INDEX IF NOT EXISTS idx_brz_plan_state 
        ON bronze.brz_plan_info(state);
    """,
    
    "brz_formulary": """
        CREATE TABLE IF NOT EXISTS bronze.brz_formulary (
            formulary_id VARCHAR,
            formulary_version VARCHAR,
            contract_year VARCHAR,
            rxcui VARCHAR,
            ndc VARCHAR,
            tier_level_value DOUBLE,
            quantity_limit_yn VARCHAR,
            quantity_limit_amount VARCHAR,
            quantity_limit_days VARCHAR,
            prior_authorization_yn VARCHAR,
            step_therapy_yn VARCHAR,
            form_drug_key VARCHAR,
            -- Metadata
            source_filename VARCHAR,
            ingestion_ts TIMESTAMP,
            updated_at TIMESTAMP,
            record_hash VARCHAR,
            PRIMARY KEY (form_drug_key)
        );
        
        CREATE INDEX IF NOT EXISTS idx_brz_form_ndc 
        ON bronze.brz_formulary(ndc);
        
        CREATE INDEX IF NOT EXISTS idx_brz_form_id 
        ON bronze.brz_formulary(formulary_id);
    """,
    
    "brz_geographic": """
        CREATE TABLE IF NOT EXISTS bronze.brz_geographic (
            county_code VARCHAR PRIMARY KEY,
            state VARCHAR,
            statename VARCHAR,
            county VARCHAR,
            pdp_region_code VARCHAR,
            -- Metadata
            source_filename VARCHAR,
            ingestion_ts TIMESTAMP,
            updated_at TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_brz_geo_state 
        ON bronze.brz_geographic(state);
    """
}

# Silver Layer Schemas
SILVER_SCHEMAS = {
    "silver": """
        CREATE SCHEMA IF NOT EXISTS silver;
    """,
    
    "slv_plan": """
        CREATE TABLE IF NOT EXISTS silver.slv_plan (
            plan_key VARCHAR PRIMARY KEY,
            contract_year INTEGER,
            contract_id VARCHAR,
            plan_id VARCHAR,
            segment_id VARCHAR,
            contract_name VARCHAR,
            plan_name VARCHAR,
            contract_type VARCHAR,
            formulary_id VARCHAR,
            premium DECIMAL(10,2),
            deductible DECIMAL(10,2),
            state VARCHAR,
            county_code VARCHAR,
            pdp_region_code VARCHAR,
            snp INTEGER,
            plan_suppressed_yn VARCHAR,
            plan_bk VARCHAR,  -- Business key for joins
            silver_ts TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_slv_plan_county 
        ON silver.slv_plan(county_code);
        
        CREATE INDEX IF NOT EXISTS idx_slv_plan_state 
        ON silver.slv_plan(state);
        
        CREATE INDEX IF NOT EXISTS idx_slv_plan_formulary 
        ON silver.slv_plan(formulary_id);
    """
}

# Gold Layer Schemas
GOLD_SCHEMAS = {
    "gold": """
        CREATE SCHEMA IF NOT EXISTS gold;
    """,
    
    "dim_plan": """
        CREATE TABLE IF NOT EXISTS gold.dim_plan (
            plan_key VARCHAR PRIMARY KEY,
            contract_year INTEGER,
            contract_id VARCHAR,
            plan_id VARCHAR,
            segment_id VARCHAR,
            contract_name VARCHAR,
            plan_name VARCHAR,
            contract_type VARCHAR,
            state VARCHAR,
            county_code VARCHAR,
            premium DECIMAL(10,2),
            deductible DECIMAL(10,2),
            snp INTEGER,
            plan_suppressed_yn VARCHAR,
            gold_ts TIMESTAMP
        );
    """,
    
    "dim_geography": """
        CREATE TABLE IF NOT EXISTS gold.dim_geography (
            county_code VARCHAR PRIMARY KEY,
            state VARCHAR,
            statename VARCHAR,
            county VARCHAR,
            pdp_region_code VARCHAR,
            population INTEGER,
            density_category VARCHAR,
            gold_ts TIMESTAMP
        );
    """,
    
    "dim_zipcode": """
        CREATE TABLE IF NOT EXISTS gold.dim_zipcode (
            zip_code VARCHAR PRIMARY KEY,
            city VARCHAR,
            state VARCHAR,
            county VARCHAR,
            county_code VARCHAR,
            lat DECIMAL(8,6),
            lng DECIMAL(9,6),
            population INTEGER,
            density INTEGER,
            density_category VARCHAR,
            gold_ts TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_dim_zip_county 
        ON gold.dim_zipcode(county_code);
    """
}

# ML Layer Schemas
ML_SCHEMAS = {
    "ml": """
        CREATE SCHEMA IF NOT EXISTS ml;
    """,
    
    "synthetic": """
        CREATE SCHEMA IF NOT EXISTS synthetic;
    """
}


def get_all_schemas():
    """Get all schema definitions."""
    return {
        **BRONZE_SCHEMAS,
        **SILVER_SCHEMAS,
        **GOLD_SCHEMAS,
        **ML_SCHEMAS
    }


def get_layer_schemas(layer):
    """Get schemas for specific layer."""
    layers = {
        'bronze': BRONZE_SCHEMAS,
        'silver': SILVER_SCHEMAS,
        'gold': GOLD_SCHEMAS,
        'ml': ML_SCHEMAS
    }
    return layers.get(layer, {})
