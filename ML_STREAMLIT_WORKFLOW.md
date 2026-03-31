# ML + Streamlit Workflow

## Current executable flow

1. Build ML tables:
```bash
python -m db.run_full_pipeline --layers gold ml
```

2. Train ranking model:
```bash
python ml_model/train_model_from_db.py
```

3. Run interactive app:
```bash
streamlit run app/streamlit_app_interactive.py
```

## Objective consistency

The ranking objective is now aligned across training pairs, explainer, and app:

`objective_cost = annual_premium + estimated_annual_oop + distance_penalty`

Specifically:
- `db/ml/05_training_pairs.py`: materializes `total_annual_cost` and `total_cost_with_distance`
- `db/ml/06_recommendation_explainer.py`: ranks by `total_cost_with_distance`
- `ml_model/train_model_from_db.py`: labels by `ranking_cost_objective` (`total_cost_with_distance`)
- `app/streamlit_app_interactive.py`: computes `total_cost_with_distance` with the same cost structure

## Read-only inference

- App uses `get_db(read_only=True)` for inference queries.
- Model training and pipeline scripts use read-write mode where table creation/update is required.

## Legacy modules

Deprecated repository/engine modules not used by the active pipeline were moved to `archive/`.
