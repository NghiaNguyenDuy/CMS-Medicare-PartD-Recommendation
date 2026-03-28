# Cost Calculation Logic for Beneficiary Drug Costs in CMS SPUF 2025

## Executive summary

The 2025 **SPUF (Prescription Drug Plan Formulary, Pharmacy Network, and Pricing Information Public Use File)** is a quarterly snapshot of data used by the **Medicare Plan Finder (MPF)** drug-cost feature. It is published as 9 pipe-delimited ASCII files and is built from the first new MPF posting in the last month of each quarter; MPF pricing/network data itself updates every two weeks, so SPUF may not match the current MPF display. 

Computing the beneficiary **out-of-pocket (OOP) drug cost** in a way that is “Plan Finder-like” requires integrating (at minimum) four SPUF components: **PRICING (UNIT_COST)**, **PHARMACY_NETWORK (dispensing fees + FLOOR_PRICE + channel/preferred flags)**, **BENEFICIARY_COST (cost-sharing by benefit phase/tier/days supply/channel + deductible applicability)**, and **INSULIN_BENEFICIARY_COST (insulin-specific copays by channel/days supply)**. The SPUF record layout defines the semantics for each of these inputs, including how copay vs coinsurance amounts are interpreted, and how “pre-deductible / initial coverage / catastrophic” are encoded as `COVERAGE_LEVEL`. 

However, a key limitation must be stated explicitly: **SPUF PRICING does not expose the full MPF pricing submission structure** (for example, MPF submissions historically include pharmacy “price IDs” and retail vs mail indicators). SPUF instead provides an **average unit cost at in-area retail pharmacies** for 30/60/90 day supplies. Therefore, SPUF can support a technically rigorous, auditable **approximation** of MPF costs, but it cannot perfectly reproduce pharmacy-specific negotiated prices in all cases without additional (non-SPUF) upstream submission artifacts. 

For 2025 Part D rules, CMS describes a redesigned three-phase benefit (deductible, initial coverage, catastrophic), elimination of the coverage gap, an annual OOP threshold of **$2,000**, and **no beneficiary cost sharing in catastrophic**; covered insulin has a **$35/month maximum** and is deductible-exempt under the defined standard rules. 

## Source artifacts and data elements used in cost calculations

### SPUF timing and suppression implications

SPUF is created from MPF postings and is published quarterly; plans whose data are suppressed appear only in the Plan Information file with `PLAN_SUPPRESSED_YN='Y'` and do not appear in other files. Any downstream cost computation must therefore short-circuit when a plan is suppressed, because the referenced pricing/network/cost-sharing rows may be absent by design. 

### Core SPUF fields used in cost computation

All field definitions below come directly from the SPUF 2025 record layout; “Required/Optional” is an implementation inference based on whether the field is necessary to compute the requested outputs (explicitly noted as an assumption where CMS does not label requiredness). 

| SPUF file | Field | Type/size (CMS) | Practical type | Required for cost calc | Description (CMS) | Example |
|---|---|---|---|---|---|---|
| PLAN_INFORMATION | CONTRACT_ID | Char(5) | string | Required | Contract number; first letter indicates plan type | `S1234` |
| PLAN_INFORMATION | PLAN_ID | Char(3) | string | Required | Plan identifier | `001` |
| PLAN_INFORMATION | SEGMENT_ID | Char(3) | string | Required | Segment ID (0 for all other) | `000` |
| PLAN_INFORMATION | DEDUCTIBLE | 9(12.2) | decimal(12,2) | Required | Annual deductible amount | `590.00` |
| PLAN_INFORMATION | PLAN_SUPPRESSED_YN | Char(1) | string | Required | Suppression flag | `N` |
| PRICING | NDC | Char(11) | string | Required | 11-digit proxy NDC | `00012345678` |
| PRICING | DAYS_SUPPLY | 9(2) | int | Required | Days supply (30/60/90) | `30` |
| PRICING | UNIT_COST | 9(8.4) | decimal(8,4) | Required | Average unit cost at in-area retail pharmacies; refer to Pricing Data Guidelines | `0.2500` |
| PHARMACY_NETWORKS | PHARMACY_NUMBER | Char(12) | string | Required | 12-digit pharmacy number (NPI w/ leading one and zero) | `100123456789` |
| PHARMACY_NETWORKS | PHARMACY_RETAIL | Char(1) | string | Required | Retail outlet? (Y/N) | `Y` |
| PHARMACY_NETWORKS | PHARMACY_MAIL | Char(1) | string | Required | Mail outlet? (Y/N) | `N` |
| PHARMACY_NETWORKS | PREFERRED_STATUS_RETAIL | Char(1) | string | Conditionally required | Preferred retail? (Y/N) | `Y` |
| PHARMACY_NETWORKS | PREFERRED_STATUS_MAIL | Char(1) | string | Conditionally required | Preferred mail? (Y/N) | `N` |
| PHARMACY_NETWORKS | IN_AREA_FLAG | 9(1) | int | Required | Pharmacy ZIP places it in plan service area (1=yes) | `1` |
| PHARMACY_NETWORKS | FLOOR_PRICE | 9(8.4) | decimal(8,4) | Required | Negotiated minimum price for filling a prescription | `3.0000` |
| PHARMACY_NETWORKS | BRAND_DISPENSING_FEE_30/60/90 | 9(8.4) | decimal(8,4) | Required | Brand dispensing fee by days supply | `2.0000` |
| PHARMACY_NETWORKS | GENERIC_DISPENSING_FEE_30/60/90 | 9(8.4) | decimal(8,4) | Required | Generic dispensing fee by days supply | `1.5000` |
| BENEFICIARY_COST | COVERAGE_LEVEL | 9(1) | int | Required | 0=pre-deductible, 1=initial coverage, 3=catastrophic (non-subsidy beneficiary) | `1` |
| BENEFICIARY_COST | TIER | 9(2) | int | Required | Cost share tier value | `3` |
| BENEFICIARY_COST | DAYS_SUPPLY | 9(1) | int | Required | 1=30, 2=90, 3=other, 4=60 | `1` |
| BENEFICIARY_COST | COST_TYPE_* | 9(1) | int | Required | 0=not offered, 1=copay, 2=coinsurance | `2` |
| BENEFICIARY_COST | COST_AMT_* | 9(12.2) | decimal(12,2) | Required | Copay dollars or coinsurance fraction (e.g., .25 means 25%) | `0.25` |
| BENEFICIARY_COST | COST_MIN_AMT_* | Char(12) | string/decimal | Optional | Minimum beneficiary payment | `0.00` |
| BENEFICIARY_COST | COST_MAX_AMT_* | 9(12.2) | decimal(12,2) | Optional | Maximum beneficiary payment | `999.00` |
| BENEFICIARY_COST | TIER_SPECIALTY_YN | Char(1) | string | Optional | Specialty tier indicator | `1` |
| BENEFICIARY_COST | DED_APPLIES_YN | Char(1) | string | Required | Does deductible apply to this tier? | `1` |
| INSULIN_BENEFICIARY_COST | TIER | 9(2) | int | Conditionally required | Missing for defined standard plans | `2` |
| INSULIN_BENEFICIARY_COST | DAYS_SUPPLY | 9(1) | int | Required | 1=30, 2=90, 3=other, 4=60 | `1` |
| INSULIN_BENEFICIARY_COST | COPAY_AMT_*_INSLN | 9(12.2) | decimal(12,2) | Required | Insulin copay by channel | `35.00` |

Key linkage and interpretation notes:

- `BENEFICIARY_COST` is explicitly for a **beneficiary with no subsidy**.   
- `UNIT_COST` is only stated as an **average unit cost** for in-area retail pharmacies; the record layout does not specify whether “average” is weighted or unweighted.   
- Application of floor pricing in MPF is explicitly documented by CMS: **MPF computes cost as (unit cost × quantity) + dispensing fee; if floor price exceeds that calculated cost, floor price becomes the starting point for copay/coinsurance calculations**, and beneficiary payment uses a “lesser-than” rule (e.g., copay cannot exceed negotiated price). 

## Benefit-phase model and price construction for 2025

### 2025 benefit-phase rules (non-LIS, defined standard framing)

For CY 2025, CMS describes a redesigned Part D benefit with:

- **Three phases**: deductible, initial coverage, catastrophic (coverage gap eliminated).   
- **Annual OOP threshold** statutorily set to **$2,000** in 2025; catastrophic begins at the annual OOP threshold.   
- **No beneficiary cost sharing in catastrophic** (policy instituted in CY 2024 and continued).   
- **Covered insulin**: deductible-exempt and subject to a **$35 maximum copayment for a one‑month supply** in the defined standard parameters; CMS also notes the insulin exception such that cost sharing is eliminated in initial coverage and capped at $35/month. 

SPUF operationalization of phases:

- SPUF encodes the beneficiary phase dimension for cost sharing as `COVERAGE_LEVEL` where **0=pre-deductible, 1=initial coverage, 3=catastrophic**. 

### Negotiated price (“full cost”) construction from SPUF inputs

MPF’s documented negotiated-price construction is:

1. **Calculated drug cost** = `(unit cost × quantity dispensed) + dispensing fee`.   
2. **Floor pricing**: if calculated drug cost is less than floor price, the starting negotiated price becomes the floor price; otherwise the calculated cost is used.   
3. **Beneficiary cost sharing** (copay/coinsurance) is then computed using that negotiated starting point; for copays, the beneficiary effectively cannot pay more than the negotiated price (“lesser-than rule”). 

Mapping that to SPUF:

- Use `PRICING.UNIT_COST` as the unit cost input (noting it is an *average* unit cost in SPUF).   
- Use `PHARMACY_NETWORKS.(BRAND|GENERIC)_DISPENSING_FEE_(30|60|90)` for dispensing fee by days supply.   
- Use `PHARMACY_NETWORKS.FLOOR_PRICE` as the negotiated minimum price.   

Assumption (explicit): SPUF does not provide a direct “brand vs generic” classifier per NDC, but dispensing fees are separated into brand and generic. Therefore, any consumer implementation must obtain a **drug type indicator** (brand/generic) from a trusted external reference (e.g., NDC directory / RxNorm / plan-submitted classification) to select the correct dispensing fee.

## Step-by-step algorithms for beneficiary OOP cost

This section distinguishes two related but different algorithms:

- **Algorithm A (MPF conceptual)**: what MPF is documented to do (using sponsor submissions including unit cost, quantity, dispensing fee, and floor).  
- **Algorithm B (SPUF-reconstructable)**: a rigorous approximation using only SPUF tables plus minimal external inputs (quantity and brand/generic flag), producing results consistent with MPF’s documented rules but not guaranteed to match pharmacy-specific MPF prices because `UNIT_COST` is averaged in SPUF. 

### Inputs required at runtime

For a single drug fill estimate:

- Plan key: `(CONTRACT_ID, PLAN_ID, SEGMENT_ID)`   
- Drug key: `NDC` (proxy NDC)   
- Days supply: one of 30/60/90 (SPUF pricing); cost-sharing tables also support 30/60/90/other via bucket code   
- Pharmacy selection: `PHARMACY_NUMBER` plus whether retail vs mail channel is used   
- Quantity dispensed (units): **required but not contained in SPUF** (assumption: comes from user drug list entry in MPF)   
- Drug attributes (external): `is_insulin`, `is_brand` vs `is_generic` (needed for dispensing fee choice; insulin table selection) (assumption)  
- Beneficiary “state” for multi-fill annual computation:
  - Year-to-date remaining plan deductible dollars
  - Year-to-date accumulated **beneficiary OOP toward the $2,000 cap** (simplified; see TrOOP discussion below)

### Algorithm A: MPF negotiated price and cost share per fill

**A1. Compute negotiated price (“starting point”)**

1. Determine `unit_cost` for the NDC and days supply (MPF submission).  
2. Determine `disp_fee` (brand/generic + days supply) for the selected pharmacy/channel.  
3. Compute `calc_cost = unit_cost × quantity + disp_fee`.   
4. Apply floor: `negotiated_price = max(calc_cost, floor_price)`.   

**A2. Select cost-sharing rule (copay vs coinsurance) and channel**

Using SPUF naming (MPF has equivalent fields), select the appropriate `COST_TYPE_*` / `COST_AMT_*` based on:

- Pharmacy channel: retail vs mail (`PHARMACY_RETAIL`/`PHARMACY_MAIL`)   
- Preferred status: preferred vs standard (nonpreferred) `PREFERRED_STATUS_RETAIL` / `PREFERRED_STATUS_MAIL`   
- Benefit phase: deductible/pre-deductible, initial coverage, catastrophic (`COVERAGE_LEVEL`)   
- Tier and days supply bucket (`TIER`, `DAYS_SUPPLY` coding)   

Interpretation rules:

- If `COST_TYPE_* = 1` → copay; `COST_AMT_*` is dollars.   
- If `COST_TYPE_* = 2` → coinsurance; `COST_AMT_*` is a fraction (e.g., `.25` = 25%).   

**A3. Compute beneficiary payment for the phase**

Let:

- `cost_type` ∈ {0,1,2}
- `cost_amt` numeric
- `min_amt` optional
- `max_amt` optional

Rules:

1. If `cost_type = 0`: cost sharing not offered for that channel; treat as **not applicable** and fall back to another channel if MPF would (assumption: MPF selects a valid channel; if none exists, treat as unavailable).   
2. If copay: `oop_raw = cost_amt`.   
3. If coinsurance: `oop_raw = negotiated_price × cost_amt`.   
4. Apply min/max if populated:  
   `oop_bounded = clamp(oop_raw, min_amt, max_amt)` (assumption: `COST_MIN_AMT_*` is numeric when present).   
5. Apply “lesser-than” rule: `oop_phase = min(oop_bounded, negotiated_price)` (beneficiary cannot pay more than negotiated price for the fill; explicitly illustrated for floor pricing scenarios).   

### Algorithm B: SPUF-reconstructable annual OOP progression for 2025

Algorithm B composes Algorithm A’s per-fill computations with 2025 phase progression (deductible → initial → catastrophic).

#### B0. Pre-check suppression and basic join integrity

1. Read `PLAN_SUPPRESSED_YN` from `PLAN_INFORMATION`. If `Y`, return **“suppressed plan—cost not computable from SPUF because downstream files are omitted”**.   
2. Validate presence of matching rows in:
   - `BENEFICIARY_COST` for required `(coverage_level, tier, days_bucket)` combos  
   - `PRICING` for `(NDC, days_supply)`  
   - `PHARMACY_NETWORKS` for the pharmacy and plan   

#### B1. Normalize days supply

Given `days_supply` ∈ {30, 60, 90}:

- Map to `days_bucket` used in BENEFICIARY_COST / INSULIN_BENEFICIARY_COST:  
  - 30 → 1  
  - 60 → 4  
  - 90 → 2   

(“Other” = bucket 3 exists but SPUF PRICING explicitly supports only 30/60/90 in this layout and in MPF submission validations; treat “other” as out-of-scope for pricing unless a separate pricing source exists.)   

#### B2. Construct negotiated price from SPUF (approximation)

Inputs:

- `UNIT_COST` from PRICING for the plan/NDC/days_supply  
- `disp_fee` chosen from PHARMACY_NETWORKS (brand/generic × 30/60/90)  
- `FLOOR_PRICE` from PHARMACY_NETWORKS  
- `qty_units` (external)

Steps:

1. `calc_cost = UNIT_COST × qty_units + disp_fee`   
2. `negotiated_price = max(calc_cost, FLOOR_PRICE)`   

Assumption about `UNIT_COST` averaging:

- **Unweighted average** possibility: `UNIT_COST` is the arithmetic mean across in-area retail pharmacies’ effective unit prices (after plan/PBM processing).  
- **Weighted average** possibility: `UNIT_COST` is weighted by expected utilization or by some distribution of pharmacy use.  

SPUF does not specify which approach is used; it only states “average unit cost” and references Pricing Data Guidelines for submission context, which do not define SPUF’s aggregation method in the record layout itself. 

#### B3. Determine whether insulin override applies

If the NDC corresponds to a **covered insulin product** (external classification), then:

1. Select insulin copay row in `INSULIN_BENEFICIARY_COST` by plan + days_bucket, and (if present) tier. The record layout notes tier may be missing for defined standard plans.   
2. Choose copay amount by selected channel:
   - Preferred retail → `COPAY_AMT_PREF_INSLN`
   - Standard (nonpreferred) retail → `COPAY_AMT_NONPREF_INSLN`
   - Preferred mail → `COPAY_AMT_MAIL_PREF_INSLN`
   - Standard mail → `COPAY_AMT_MAIL_NONPREF_INSLN`   
3. Apply “lesser-than” cap: `oop_phase = min(insulin_copay, negotiated_price)` (assumption; consistent with MPF copay logic).   

#### B4. Compute OOP in each benefit phase for an annual run

State variables:

- `ded_rem`: remaining plan deductible dollars (initially `PLAN_INFORMATION.DEDUCTIBLE`, but see deductible applicability rules below)   
- `oop_to_cap`: accumulated beneficiary OOP toward annual OOP threshold (simplified)  
- `OOP_CAP_2025 = 2000`   

For each fill (in chronological order), allocate negotiated price across phases:

**Phase allocation**

1. If `oop_to_cap >= 2000`: entire fill is catastrophic → beneficiary pays $0.   
2. Else:
   - Determine tier cost-sharing row for `COVERAGE_LEVEL=0` (pre-deductible) and/or `COVERAGE_LEVEL=1` (initial).   
   - Determine `DED_APPLIES_YN` for the tier from BENEFICIARY_COST.   

**Deductible handling**

- If `DED_APPLIES_YN='1'` and `ded_rem > 0`:
  - Amount of negotiated price applied to deductible:  
    `ded_applied = min(negotiated_price, ded_rem)`  
  - Beneficiary pays `ded_applied` (assumption for non-LIS standard deductible behavior; consistent with “beneficiaries responsible for all drug costs until deductible met” in defined standard).   
  - Update: `ded_rem -= ded_applied`, remaining price `price_left = negotiated_price - ded_applied`
- If `DED_APPLIES_YN='0'` (deductible does not apply to this tier) or `ded_rem = 0`:
  - `price_left = negotiated_price`

**Initial coverage handling (until OOP cap)**

- If `price_left > 0` and `oop_to_cap < 2000`:
  - Compute per-fill OOP for initial coverage by applying BENEFICIARY_COST for `COVERAGE_LEVEL=1` (unless insulin override applies).   
  - However, **cap effect**: beneficiary cannot exceed the remaining cap:
    - `cap_rem = 2000 - oop_to_cap`
    - `oop_initial_capped = min(oop_initial_calc, cap_rem)`
  - Update `oop_to_cap += oop_initial_capped`
  - Any remaining price beyond what was “needed” to hit the cap transitions to catastrophic (beneficiary pays $0 thereafter).   

**Catastrophic handling**

- Once `oop_to_cap >= 2000`, beneficiary pays **$0** for covered Part D drugs for the remainder of the year.   

Important compliance note (assumption): True MPF progression uses **TrOOP** rules (what counts toward the threshold) and has specific inclusion/exclusion logic for third-party payments and manufacturer discounts under the 2025 redesign. SPUF does not contain those third-party payment streams, so any TrOOP-accurate modeling from SPUF alone requires an external TrOOP accounting model; CMS explicitly discusses TrOOP as the determinant for phase transitions. 

### Cost computation pipeline flowchart

```mermaid
flowchart TD
  A[Input: plan (contract/plan/segment)\ninput: NDC, qty_units, days_supply\ninput: pharmacy + channel] --> B{PLAN_SUPPRESSED_YN='Y'?}
  B -->|Yes| X[Stop: suppressed plan\n(no downstream files in SPUF)]
  B -->|No| C[Join PRICING:\nUNIT_COST by NDC + days_supply]
  C --> D[Join PHARMACY_NETWORK:\nFLOOR_PRICE + DISP_FEE + preferred flags]
  D --> E[Compute calc_cost = UNIT_COST*QTY + DISP_FEE]
  E --> F[negotiated_price = max(calc_cost, FLOOR_PRICE)]
  F --> G{Drug is covered insulin?}
  G -->|Yes| H[Join INSULIN_BENEFICIARY_COST\nselect copay by channel]
  G -->|No| I[Join BASIC_DRUGS_FORMULARY\nget tier\nJoin BENEFICIARY_COST\nby tier + days_bucket + phase]
  H --> J[Compute OOP for phase\napply min/max if any\nOOP = min(copay, negotiated_price)]
  I --> K[Compute OOP:\ncoinsurance=rate*negotiated_price\ncopay=amount\napply min/max + lesser-than]
  J --> L[Apply deductible + OOP cap progression\n(2025: cap=2000, catastrophic=$0)]
  K --> L
  L --> M[Output:\nper-fill OOP\nannual OOP totals\nphase transitions + audit trail]
```

## Worked numeric scenarios with intermediate steps

All scenarios use synthetic numbers to illustrate deterministic behavior. These examples are **not** derived from a specific plan record. Where SPUF lacks a required input (quantity, brand/generic, insulin classification), it is treated as an explicit assumption.

Common constants:

- 2025 OOP cap: `OOP_CAP = $2,000`   
- Rounding assumption: keep intermediate math at ≥4 decimals; round displayed money to **2 decimals** at the end of each fill (SPUF does not specify presentation rounding).   

### Scenario one: Preferred retail, copay, 30-day fill, floor price binds

Assumptions:

- Plan deductible already met (`ded_rem=0`).
- NDC is generic (so use generic dispensing fee).
- SPUF-like values:  
  - `UNIT_COST = 0.0200` (per unit)   
  - `qty_units = 30` (external)  
  - `GENERIC_DISPENSING_FEE_30 = 1.5000`   
  - `FLOOR_PRICE = 3.0000`   
  - Initial coverage preferred retail copay: `COST_TYPE_PREF=1`, `COST_AMT_PREF=10.00`   

Steps:

1. `calc_cost = UNIT_COST*qty + disp_fee = 0.02*30 + 1.50 = 2.10`
2. `negotiated_price = max(2.10, 3.00) = 3.00` (floor binds)   
3. Copay raw = $10.00  
4. Lesser-than cap: `oop = min(10.00, 3.00) = 3.00` (beneficiary pays negotiated price)   

**Final beneficiary OOP for fill: $3.00.**

### Scenario two: Preferred retail, coinsurance, 90-day fill, no floor binding

Assumptions:

- Deductible met.
- NDC is brand.
- Values:  
  - `UNIT_COST = 5.0000`  
  - `qty_units = 90`  
  - `BRAND_DISPENSING_FEE_90 = 3.0000`   
  - `FLOOR_PRICE = 3.0000`  
  - Coinsurance rate: `COST_TYPE_PREF=2`, `COST_AMT_PREF=0.25` (=25%)   

Steps:

1. `calc_cost = 5.00*90 + 3.00 = 453.00`
2. `negotiated_price = max(453.00, 3.00) = 453.00`
3. Coinsurance raw: `oop_raw = 0.25 * 453.00 = 113.25`   
4. No min/max assumed. Lesser-than cap irrelevant because 113.25 < 453.00.

**Final beneficiary OOP: $113.25.**

### Scenario three: Deductible not met, deductible applies to tier, split into deductible + initial coverage

Assumptions:

- Plan deductible remaining `ded_rem = 100.00`; cap not near (`oop_to_cap = 0`).
- `DED_APPLIES_YN='1'` for this tier.   
- Fill negotiated price computed as $180.00 (from UNIT_COST/fees/floor).
- Initial coverage uses coinsurance 25% (example).   

Steps:

1. Deductible portion: `ded_applied = min(180.00, 100.00) = 100.00` → beneficiary pays $100.00
2. Remaining price: `price_left = 180.00 - 100.00 = 80.00`
3. Initial coverage coinsurance: `oop_initial = 0.25*80.00 = 20.00`
4. Total OOP for fill = `100.00 + 20.00 = 120.00`
5. Update tracking:
   - `ded_rem = 0.00`
   - `oop_to_cap = 120.00`

**Final beneficiary OOP: $120.00.**

### Scenario four: Insulin special case, deductible not met, insulin cap applies

Assumptions:

- Covered insulin product (external classification); deductible remaining `ded_rem = 400.00`.
- Retail preferred pharmacy.
- Negotiated price computed as $250.00 (high cost).
- Insulin beneficiary cost file provides `COPAY_AMT_PREF_INSLN = 35.00` for 30-day bucket.   

Steps:

1. Compute negotiated price normally (not shown): `negotiated_price = 250.00`
2. Insulin override: copay = 35.00   
3. Lesser-than cap: `oop = min(35.00, 250.00) = 35.00`   
4. Deductible interaction (assumption consistent with CMS deductible exemption for insulin in defined standard): beneficiary pays insulin copay even if deductible not met, and fill does **not** consume plan deductible dollars for insulin-exempt drugs.   

**Final beneficiary OOP: $35.00.**

### Scenario five: Suppressed plan—cost cannot be computed from SPUF

Assumptions:

- `PLAN_SUPPRESSED_YN = 'Y'` in Plan Information.

Behavior:

- Per SPUF methodology, suppressed plans do not appear in other files; therefore there is no authoritative PRICING/PHARMACY_NETWORK/BENEFICIARY_COST row-set to compute from.   

**Final: return “suppressed” and do not compute costs.**

### Scenario six: Outlier unit cost and validation expectations

Assumptions:

- This plan submits an unusually high `UNIT_COST`, causing unusually high negotiated price and OOP.
- Validation inventory describes plan/pricing outliers, including “High Unit Cost” checks and other suppressible errors.   

Inputs:

- `UNIT_COST = 200.0000`
- `qty_units = 30`
- Brand dispensing fee 30 = `2.0000`
- Floor price irrelevant (`3.00`)
- Coinsurance 25% in initial coverage.

Steps:

1. `calc_cost = 200*30 + 2 = 6002.00`
2. `negotiated_price = 6002.00`
3. `oop = 0.25*6002 = 1500.50`

Cap interaction example:

- If year-to-date `oop_to_cap = 600.00`, remaining cap is `1400.00`.
- Capped OOP for this fill under 2025 rules: `oop_capped = min(1500.50, 1400.00) = 1400.00` → beneficiary hits the $2,000 cap and pays $0 for covered drugs thereafter. 

**Final beneficiary OOP for this fill: $1,400.00 (cap-limited), then catastrophic $0 afterward.**

## Validation rules, edge cases, and implementation guidance at scale

### Validation checks to implement when consuming SPUF

The following checks are directly motivated by SPUF schema constraints and by CMS validation concepts documented for MPF pricing submissions (useful for building an internal compliance-grade pipeline). 

**Domain and format checks**

- `PRICING.DAYS_SUPPLY` must be in {30,60,90}; MPF submission guidance treats other values as fatal format errors.   
- `BENEFICIARY_COST.COVERAGE_LEVEL` must be in {0,1,3}.   
- `BENEFICIARY_COST.DAYS_SUPPLY` must be in {1,2,3,4}.   
- `COST_TYPE_*` must be in {0,1,2}.   

**Cross-file join integrity**

- Enforce that all PRICING/PHARMACY_NETWORK/BENEFICIARY_COST rows reference plans that exist in PLAN_INFORMATION and are not suppressed; if `PLAN_SUPPRESSED_YN='Y'`, expect missing downstream records by design.   

**Pricing sanity and consistency**

- Reject negative costs: CMS notes MPF cannot accept negative unit cost (submission context).   
- Allow true $0 unit cost but flag for review; CMS documents $0 unit cost handling and separate outlier checks.   
- Floor-price checks: CMS validates against outliers such as floor price below dispensing fee and floor price exceeding thresholds (submission context).   

**Pharmacy classification integrity**

- Flag pharmacies that are neither retail nor mail; CMS treats as suppressible error in pricing submissions.   
- Validate preferred flags vs plan benefit offerings where available (submission context).   

### Edge-case handling rules

**Missing fields / nullables**

- `COST_MIN_AMT_*` is Char(12) and may be blank; treat as NULL and skip min bounding.   
- If a required BENEFICIARY_COST row is missing for a tier/days/phase/channel, fall back to another channel only if your business rule allows; otherwise mark as “incomplete plan design” and exclude from cost results (assumption; aligns with CMS treating missing pricing/benefit inconsistencies as errors).   

**IN_AREA_FLAG = 0**

- SPUF `UNIT_COST` is explicitly defined for **in-area retail pharmacies**. If a selected pharmacy is out-of-area (`IN_AREA_FLAG=0`), SPUF cannot guarantee correct negotiated pricing; return an “out-of-area price not representable from SPUF PRICING” marker or compute using the same unit cost with a documented caveat (assumption).   

**Rounding**

- CMS does not specify rounding for SPUF-based recomputation. For compliance-grade reproducibility, adopt:
  - internal precision ≥ 4 decimals
  - final displayed monetary rounding to 2 decimals using bankers’ rounding or round-half-up, but document which you choose (assumption).  

**Specialty tiers**

- `TIER_SPECIALTY_YN` is informational; the SPUF record layout does not define special cost mechanics in this file beyond marking the tier. Use it for audit (e.g., ensure specialty-tier coinsurance within allowed ranges per CMS bidding/program guidance), but compute cost using the same copay/coinsurance rules above.   

### Recommended SQL/pseudocode implementations

#### Pseudocode: negotiated price + OOP for a single fill

```text
function compute_fill_oop(plan_key, ndc, days_supply, qty_units, pharmacy_number, channel, phase, is_insulin, is_brand):
  assert phase in {0,1,3}

  plan = PLAN_INFORMATION[plan_key]
  if plan.PLAN_SUPPRESSED_YN == 'Y':
    return {status:'SUPPRESSED'}

  pharm = PHARMACY_NETWORKS[plan_key, pharmacy_number]
  pricing = PRICING[plan_key, ndc, days_supply]

  disp_fee = choose_disp_fee(pharm, is_brand, days_supply)   # 30/60/90
  calc_cost = pricing.UNIT_COST * qty_units + disp_fee
  negotiated_price = max(calc_cost, pharm.FLOOR_PRICE)

  if phase == 3:
    return {oop: 0.00, negotiated_price: negotiated_price}   # 2025 catastrophic = $0

  if is_insulin:
    ibc = INSULIN_BENEFICIARY_COST.lookup(plan_key, days_bucket(days_supply), optional_tier)
    copay = pick_insulin_copay(ibc, channel, pharm.preferred_flags)
    oop = min(copay, negotiated_price)
    return {oop: round2(oop), negotiated_price: negotiated_price}

  tier = BASIC_DRUGS_FORMULARY.get_tier(plan.FORMULARY_ID, ndc)
  bc = BENEFICIARY_COST.lookup(plan_key, phase, tier, days_bucket(days_supply))

  (cost_type, cost_amt, min_amt, max_amt) = select_channel_cost_fields(bc, channel, pharm.preferred_flags)
  if cost_type == 1:  # copay
    oop_raw = cost_amt
  else if cost_type == 2:  # coinsurance
    oop_raw = negotiated_price * cost_amt
  else:
    return {status:'NO_COST_RULE'}

  oop_bounded = apply_min_max(oop_raw, min_amt, max_amt)
  oop = min(oop_bounded, negotiated_price)

  return {oop: round2(oop), negotiated_price: negotiated_price, tier: tier}
```

This directly implements the cost-type interpretation rules and floor-based negotiated-price rule documented by CMS and encoded in SPUF. 

#### SQL pattern: negotiated price staging view (SPUF-only)

```sql
-- Example: negotiated price for a plan+pharmacy+ndc+days_supply
WITH base AS (
  SELECT
    p.contract_id, p.plan_id, p.segment_id,
    pn.pharmacy_number,
    pr.ndc,
    pr.days_supply,
    pr.unit_cost,
    pn.floor_price,
    CASE
      WHEN :is_brand = 1 AND pr.days_supply = 30 THEN pn.brand_dispensing_fee_30
      WHEN :is_brand = 1 AND pr.days_supply = 60 THEN pn.brand_dispensing_fee_60
      WHEN :is_brand = 1 AND pr.days_supply = 90 THEN pn.brand_dispensing_fee_90
      WHEN :is_brand = 0 AND pr.days_supply = 30 THEN pn.generic_dispensing_fee_30
      WHEN :is_brand = 0 AND pr.days_supply = 60 THEN pn.generic_dispensing_fee_60
      WHEN :is_brand = 0 AND pr.days_supply = 90 THEN pn.generic_dispensing_fee_90
    END AS dispensing_fee
  FROM pricing pr
  JOIN pharmacy_networks pn
    ON pr.contract_id = pn.contract_id
   AND pr.plan_id     = pn.plan_id
   AND pr.segment_id  = pn.segment_id
  JOIN plan_information p
    ON p.contract_id = pr.contract_id
   AND p.plan_id     = pr.plan_id
   AND p.segment_id  = pr.segment_id
  WHERE p.plan_suppressed_yn <> 'Y'
    AND pr.ndc = :ndc
    AND pr.days_supply = :days_supply
    AND pn.pharmacy_number = :pharmacy_number
)
SELECT
  *,
  (unit_cost * :qty_units + dispensing_fee) AS calc_cost,
  GREATEST((unit_cost * :qty_units + dispensing_fee), floor_price) AS negotiated_price
FROM base;
```

This matches CMS’s documented negotiated-price construction and floor application. 

### Summary of what is specified vs assumed

**Specified by CMS/SPUF**

- Field semantics for cost types and phase coding in SPUF.   
- Floor pricing application logic in MPF (starting point for copay/coinsurance).   
- 2025 three-phase benefit, $2,000 OOP threshold, catastrophic cost sharing = $0, and insulin $35/month maximum in defined standard parameter discussion.   

**Not specified (treated as explicit assumptions)**

- The exact averaging method used to produce SPUF `UNIT_COST` (“simple” vs “weighted”).   
- Brand/generic and insulin classification per NDC (required for correct fee selection and insulin override).  
- Exact rounding strategy used for MPF displayed dollar amounts (SPUF provides types/precision but not UI rounding rules).  
- Full TrOOP accounting for phase transitions (SPUF lacks third-party payment streams; CMS defines TrOOP conceptually but SPUF alone cannot implement it end-to-end). 