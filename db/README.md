# DuckDB Medallion Architecture

## Overview

This directory contains the DuckDB-based medallion data architecture for Medicare Part D plan recommendation system.

**Architecture Layers**:
- **Bronze**: Raw data ingestion from SPUF files → DuckDB tables
- **Silver**: Cleaned and normalized business entities  
- **Gold**: Analysis-ready dimensions, facts, and aggregations
- **ML**: Feature engineering and training data generation

## Directory Structure

```
db/
├── README.md                      # This file
├── db_manager.py                  # Connection pool & query utilities
├── bronze/                        # Bronze layer: Raw ingestion
│   ├── __init__.py
│   ├── 01_ingest_plan_info.py     # Plan information
│   ├── 02_ingest_formulary.py     # Formulary data
│   ├── 03_ingest_costs.py         # Beneficiary & insulin costs
│   ├── 04_ingest_networks.py      # Pharmacy networks
│   ├── 05_ingest_geography.py     # Geographic dimensions
│   └── run_bronze.py              # Execute all bronze scripts
├── silver/                        # Silver layer: Business entities
│   ├── __init__.py
│   ├── 01_transform_plans.py      # Normalize plan data
│   ├── 02_transform_formulary.py  # Clean formulary
│   ├── 03_transform_costs.py      # Process cost structures
│   ├── 04_transform_networks.py   # Network adequacy
│   ├── 05_transform_geography.py  # Geo standardization
│   └── run_silver.py              # Execute all silver scripts
├── gold/                          # Gold layer: Analytics
│   ├── __init__.py
│   ├── 01_dim_plan.py             # Plan dimension
│   ├── 02_dim_geography.py        # Geography dimension
│   ├── 03_dim_zipcode.py          # Zipcode dimension
│   ├── 04_dim_drug.py             # Drug/NDC dimension
│   ├── 05_agg_formulary.py        # Formulary metrics
│   ├── 06_agg_costs.py            # Cost metrics
│   ├── 07_agg_networks.py         # Network metrics
│   ├── 08_agg_insulin.py          # Insulin metrics
│   ├── 09_feature_plan_summary.py # ML feature table
│   └── run_gold.py                # Execute all gold scripts
├── ml/                            # ML preparation layer
│   ├── __init__.py
│   ├── 01_generate_bene_profiles.py   # Synthetic beneficiaries
│   ├── 02_assign_geography.py         # Zip code assignment
│   ├── 03_calculate_distance.py       # Distance proxy
│   ├── 04_calculate_oop.py            # Personalized OOP
│   ├── 05_training_pairs.py           # Generate training data
│   ├── 06_recommendation_explainer.py # Explainable recommendations
│   └── run_ml_prep.py                 # Execute all ML prep
└── utils/                         # Shared utilities
    ├── __init__.py
    ├── schema_definitions.py      # SQL schemas
    ├── transform_helpers.py       # Transform functions
    └── validation.py              # Data quality checks
```

## Usage

### 1. Initial Setup: Bronze Ingestion

Migrate SPUF parquet files to bronze tables:

```bash
# One-time migration (creates bronze.* tables)
python scripts/migrate_to_duckdb.py --force
```

Or run individual bronze scripts:

```bash
python -m db.bronze.run_bronze
```

### 2. Silver Transformation

Clean and normalize data:

```bash
python -m db.silver.run_silver
```

### 3. Gold Enrichment

Create dimensions and aggregations:

```bash
python -m db.gold.run_gold
```

### 4. ML Preparation

Generate training data:

```bash
python -m db.ml.run_ml_prep
```

### 5. Full Pipeline

Run all layers sequentially:

```bash
python -m db.run_full_pipeline
```

## DuckDB Schemas

The database is organized into schemas by layer:

- `bronze.*` - Raw ingested tables (minimal processing)
- `silver.*` - Cleaned normalized tables
- `gold.*` - Dimension/fact tables + aggregations
- `ml.*` - ML features and training data
- `synthetic.*` - Synthetic beneficiary data

## Key Tables

### Bronze Layer
- `bronze.brz_plan_info` - Plan master data
- `bronze.brz_formulary` - Drug formulary
- `bronze.brz_beneficiary_cost` - Copay/coins structures
- `bronze.brz_insulin_cost` - Insulin-specific costs
- `bronze.brz_pharmacy_network` - Network lists
- `bronze.brz_geographic` - County/state mappings

### Silver Layer
- `silver.slv_plan` - Normalized plans
- `silver.slv_formulary` - Clean formulary
- `silver.slv_beneficiary_cost` - Cost structures
- `silver.slv_pharmacy_network` - Networks

### Gold Layer
- `gold.dim_plan` - Plan dimension
- `gold.dim_geography` - County dimension
- `gold.dim_zipcode` - Zip code dimension
- `gold.dim_drug` - Drug/NDC dimension
- `gold.agg_plan_formulary_metrics` - Formulary stats
- `gold.agg_plan_cost_metrics` - Cost stats
- `gold.agg_plan_network_metrics` - Network adequacy
- `gold.feature_plan_summary` - ML features (wide table)

### ML Layer
- `ml.training_plan_labels` - Normalized plan features
- `ml.plan_distance_features` - Distance proxy
- `ml.training_plan_pairs` - Bene-plan pairs
- `ml.recommendation_explanations` - Explainable recs

## Development

### Adding New Transformations

1. Create script in appropriate layer directory
2. Follow naming convention: `##_action_entity.py`
3. Import from `db.utils` for shared functions
4. Update `run_*.py` orchestrator script

### Data Quality Checks

Each layer includes validation:
- Row count verification
- Null check on critical fields
- Referential integrity
- Distribution checks

Run validation:

```bash
python -m db.utils.validation --layer gold
```

## Performance Tips

- Bronze tables are compressed (~500MB-1GB)
- Indexes added on key columns (plan_key, county_code, ndc)
- Use `db_manager.cached_query()` for frequent queries
- Set `memory_limit` and `threads` in config
