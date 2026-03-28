# Cost Calculation Examples

These examples match the implemented project behavior in `src/medicare_partd/recommend.py`.

## Example 1: Minimal Counselor Input

Input:

```python
BeneficiaryInput(
    zipcode="43004",
    age_band="65-74",
    lis_status="none",
    pharmacy_preference="auto",
    top_n=5,
)
```

```python
[
    MedicationInput(
        drug_name="albuterol 0.21 MG/ML Inhalation Solution",
        tier_family="generic",
        day_supply=30,
    ),
    MedicationInput(
        drug_name="insulin glargine",
        tier_family="brand",
        day_supply=30,
    ),
]
```

Engine behavior:

- resolve each medication to RXCUI and NDC when possible
- infer quantity and fills per year from PDE defaults
- find candidate plans that serve the beneficiary ZIP
- score each plan across valid pharmacy channels
- return ranked plans with explanation strings and per-drug breakdowns

## Example 2: Quantity Override

If the counselor knows the real fill size, provide it explicitly:

```python
MedicationInput(
    drug_name="insulin glargine",
    tier_family="brand",
    day_supply=30,
    quantity_override=10,
    fills_per_year_override=12,
)
```

This bypasses the PDE-derived default utilization for that drug.

## Example 3: Channel Selection

With `pharmacy_preference="auto"` the engine evaluates:

- preferred retail
- nonpreferred retail
- preferred mail
- nonpreferred mail

and keeps the lowest beneficiary cost among available channels.

With `pharmacy_preference="retail"` it ignores mail channels.

With `pharmacy_preference="mail"` it ignores retail channels.

## Example 4: Negotiated Price Approximation

For a brand drug with:

- `unit_cost = 5.00`
- `quantity = 10`
- `brand_fee_30 = 1.00`
- `floor_price = 0`

the negotiated price approximation is:

```text
max(5.00 * 10 + 1.00, 0.00) = 51.00
```

If initial coverage coinsurance is 25%:

```text
per_fill_oop = 51.00 * 0.25 = 12.75
```

## Example 5: Deductible Exposure

Suppose:

- deductible remaining = `100`
- negotiated price = `51`
- tier is deductible-applicable

Then the fill contributes to deductible exposure before the beneficiary transitions into standard cost-sharing for later fills. The project tracks this as a counselor-facing warning:

```text
"Projected deductible exposure is about $X.XX."
```

## Example 6: Insulin Override

If a drug is flagged as insulin and the plan has insulin beneficiary cost rows:

- insulin-specific copay takes precedence over general tier logic
- deductible does not apply
- the result is still bounded by negotiated price and annual OOP cap

This is surfaced in explanations when savings depend on nonpreferred channels.

## Example 7: Partial Coverage Ranking

If a plan does not price or cover one requested drug:

- the plan stays in the candidate set as a fallback
- `coverage_status` becomes `partial`
- the explanation list includes:

```text
Does not cover: <drug_name>.
```

- full-coverage plans rank ahead of partial-coverage plans

## Example 8: CLI Build

Build the database:

```powershell
$env:PYTHONPATH='src'
.venv\Scripts\python.exe -m medicare_partd build
```

Alternative wrapper:

```powershell
.venv\Scripts\python.exe scripts\run_pipeline.py build
```

## Example 9: CLI Recommendation

```powershell
$env:PYTHONPATH='src'
.venv\Scripts\python.exe -m medicare_partd recommend `
  --zipcode 43004 `
  --lis-status none `
  --pharmacy-preference auto `
  --top-n 5 `
  --medication-json "[{\"drug_name\":\"albuterol 0.21 MG/ML Inhalation Solution\",\"tier_family\":\"generic\",\"day_supply\":30}]"
```

Output shape:

```json
[
  {
    "plan_key": "H1000001000",
    "plan_name": "Example Plan",
    "annual_drug_oop": 240.0,
    "annual_premium": 360.0,
    "annual_total_cost": 600.0,
    "coverage_status": "full",
    "best_channel_mix": "pref_retail:12",
    "network_flag": "adequate",
    "restriction_summary": "no major restrictions flagged",
    "explanations": [
      "Projected deductible exposure is about $100.00."
    ]
  }
]
```

## Example 10: Streamlit Input Format

The app expects one medication per line:

```text
albuterol 0.21 MG/ML Inhalation Solution,generic,30
insulin glargine,brand,30
```

Each line maps to:

- column 1: drug name
- column 2: tier family
- column 3: day supply
