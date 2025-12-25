from __future__ import annotations

from typing import Dict


def diff_dtypes(before: Dict[str, str], after: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for col, after_t in after.items():
        before_t = before.get(col)
        if before_t is not None and before_t != after_t:
            out[col] = {"before": before_t, "after": after_t}
    return out


def diff_missing_fraction(before: Dict[str, float], after: Dict[str, float]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for col, after_v in after.items():
        before_v = before.get(col)
        if before_v is not None and before_v != after_v:
            out[col] = {"before": float(before_v), "after": float(after_v)}
    return out


def diff_missing_counts(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for col, after_v in after.items():
        before_v = before.get(col)
        if before_v is not None and before_v != after_v:
            out[col] = {"before": int(before_v), "after": int(after_v)}
    return out