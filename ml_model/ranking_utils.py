"""Utilities for ranking data preparation."""

import numpy as np

MAX_RELEVANCE_LABEL = 30


def create_ranking_labels_from_cost(cost_series, max_relevance_label=MAX_RELEVANCE_LABEL):
    """
    Convert costs to LightGBM relevance labels.

    Lower cost gets higher relevance.
    """
    ranks = cost_series.rank(method="dense", ascending=True)
    labels = ranks - 1
    inverted = labels.max() - labels
    if labels.max() > max_relevance_label:
        inverted = (inverted * max_relevance_label / labels.max()).round()
    return np.clip(inverted, 0, max_relevance_label).astype(int).to_numpy()


def groups_are_contiguous(group_ids):
    """Return True when each group appears in one contiguous block."""
    seen = set()
    current = object()
    for group_id in group_ids:
        if group_id != current:
            if group_id in seen:
                return False
            seen.add(group_id)
            current = group_id
    return True
