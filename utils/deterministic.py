"""Deterministic helpers for reproducible scoring and sampling."""

from __future__ import annotations

import hashlib


def stable_fraction(key: str, seed: int = 42) -> float:
    """Return a deterministic pseudo-random fraction in [0, 1)."""
    digest = hashlib.sha256(f"{seed}|{key}".encode("utf-8")).hexdigest()[:16]
    return int(digest, 16) / float(0xFFFFFFFFFFFFFFFF)


def stable_uniform(key: str, low: float, high: float, seed: int = 42) -> float:
    """Return a deterministic pseudo-random value in [low, high]."""
    if high <= low:
        return float(low)
    return float(low + ((high - low) * stable_fraction(key, seed=seed)))
