"""Helpers for parsing and normalizing user-entered drug identifiers."""

import re


def _to_float(value, default=0.0):
    """Best-effort float coercion."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value, default=0):
    """Best-effort integer coercion."""
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return int(default)


def _to_bool(value):
    """Best-effort boolean coercion."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    text = str(value or "").strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def normalize_ndc_token(token):
    """
    Normalize a single token into one or more NDC candidates.

    Returns:
        list[str]: Candidate NDC strings (deduplicated, order preserved)
    """
    cleaned = re.sub(r"[^0-9]", "", str(token or ""))
    if not cleaned:
        return []

    candidates = [cleaned]
    if len(cleaned) < 11:
        candidates.append(cleaned.zfill(11))

    deduped = []
    seen = set()
    for ndc in candidates:
        if ndc not in seen:
            deduped.append(ndc)
            seen.add(ndc)
    return deduped


def parse_ndc_text(raw_text):
    """Parse free-text NDC input into a normalized list of candidates."""
    tokens = re.split(r"[\s,;\n\r\t]+", str(raw_text or ""))
    ndcs = []
    seen = set()
    for token in tokens:
        for ndc in normalize_ndc_token(token):
            if ndc not in seen:
                ndcs.append(ndc)
                seen.add(ndc)
    return ndcs


def build_requested_ndcs(raw_text, selected_ndcs=None):
    """
    Build final requested NDC list from free-text + selected suggestion values.
    """
    selected_ndcs = selected_ndcs or []
    ndcs = []
    seen = set()

    for ndc in parse_ndc_text(raw_text):
        if ndc not in seen:
            ndcs.append(ndc)
            seen.add(ndc)

    for value in selected_ndcs:
        for ndc in normalize_ndc_token(value):
            if ndc not in seen:
                ndcs.append(ndc)
                seen.add(ndc)

    return ndcs


def normalize_medication_rows(rows):
    """
    Normalize user-edited medication rows from UI.

    Expected fields per row:
    - drug_name
    - ndc
    - fills_per_year
    - days_supply_mode
    - tier_level
    - is_insulin
    - annual_cost_est
    """
    normalized = []
    for row in rows or []:
        ndc_values = normalize_ndc_token((row or {}).get("ndc", ""))
        if len(ndc_values) == 0:
            continue

        ndc = ndc_values[-1]
        drug_name = str((row or {}).get("drug_name") or "").strip() or f"NDC {ndc}"
        fills_per_year = max(1.0, _to_float((row or {}).get("fills_per_year"), 12.0))
        days_supply_mode = _to_int((row or {}).get("days_supply_mode"), 30)
        if days_supply_mode not in (30, 60, 90):
            days_supply_mode = 30 if days_supply_mode < 45 else (60 if days_supply_mode < 75 else 90)
        tier_level = min(7, max(1, _to_int((row or {}).get("tier_level"), 1)))
        is_insulin = _to_bool((row or {}).get("is_insulin"))
        annual_cost_est = max(0.0, _to_float((row or {}).get("annual_cost_est"), 0.0))

        normalized.append(
            {
                "drug_name": drug_name,
                "ndc": ndc,
                "fills_per_year": fills_per_year,
                "days_supply_mode": days_supply_mode,
                "tier_level": tier_level,
                "is_insulin": is_insulin,
                "annual_cost_est": annual_cost_est,
            }
        )
    return normalized


def summarize_medication_rows(rows):
    """
    Summarize normalized medication rows for profile + filtering.

    Returns:
        dict with keys:
        - rows
        - requested_ndcs
        - requested_name_map
        - num_drugs
        - avg_fills_per_year
        - is_insulin_user
        - total_annual_drug_cost
    """
    normalized = normalize_medication_rows(rows)
    deduped = {}

    for row in normalized:
        ndc = row["ndc"]
        if ndc not in deduped:
            deduped[ndc] = row.copy()
            continue

        # Merge duplicate NDC rows by keeping the strongest signal.
        existing = deduped[ndc]
        existing["fills_per_year"] = max(float(existing["fills_per_year"]), float(row["fills_per_year"]))
        existing["days_supply_mode"] = max(int(existing["days_supply_mode"]), int(row["days_supply_mode"]))
        existing["tier_level"] = min(int(existing["tier_level"]), int(row["tier_level"]))
        existing["is_insulin"] = bool(existing["is_insulin"] or row["is_insulin"])
        existing["annual_cost_est"] = max(float(existing["annual_cost_est"]), float(row["annual_cost_est"]))
        if str(existing.get("drug_name") or "").startswith("NDC ") and not str(row.get("drug_name") or "").startswith("NDC "):
            existing["drug_name"] = row["drug_name"]

    rows_out = list(deduped.values())
    requested_ndcs = tuple(row["ndc"] for row in rows_out)
    requested_name_map = {row["ndc"]: row["drug_name"] for row in rows_out}

    num_drugs = len(rows_out)
    avg_fills_per_year = (
        sum(float(row["fills_per_year"]) for row in rows_out) / num_drugs
        if num_drugs > 0
        else 0.0
    )
    is_insulin_user = 1 if any(bool(row["is_insulin"]) for row in rows_out) else 0
    total_annual_drug_cost = sum(float(row["annual_cost_est"]) for row in rows_out)

    return {
        "rows": rows_out,
        "requested_ndcs": requested_ndcs,
        "requested_name_map": requested_name_map,
        "num_drugs": num_drugs,
        "avg_fills_per_year": avg_fills_per_year,
        "is_insulin_user": is_insulin_user,
        "total_annual_drug_cost": total_annual_drug_cost,
    }
