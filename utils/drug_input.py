"""Helpers for parsing and normalizing user-entered drug identifiers."""

import re


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
