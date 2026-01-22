from __future__ import annotations

from typing import Any, Dict, List, Optional

from .schemas import CleaningPlan


def _estimate_overall_missing_pct(missing_fraction: Dict[str, Any]) -> float:
    if not isinstance(missing_fraction, dict) or not missing_fraction:
        return 0.0

    vals: List[float] = []
    for v in missing_fraction.values():
        try:
            fv = float(v)
            if 0.0 <= fv <= 1.0:
                fv *= 100.0
            vals.append(fv)
        except Exception:
            continue

    return (sum(vals) / len(vals)) if vals else 0.0


def _get_int(d: Dict[str, Any], keys: List[str], default: int = 0) -> int:
    for k in keys:
        if k in d:
            return _safe_int(d.get(k), default=default)
    return default


def _get_float(d: Dict[str, Any], keys: List[str], default: Optional[float] = None) -> Optional[float]:
    for k in keys:
        if k in d:
            try:
                return float(d.get(k))
            except Exception:
                return default
    return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _clamp_float(x: Any, lo: float, hi: float, default: float) -> float:
    try:
        v = float(x)
    except Exception:
        return float(default)
    if v < lo or v > hi:
        return float(default)
    return float(v)


def _sanitize_plan(plan: CleaningPlan) -> CleaningPlan:
    defaults = CleaningPlan.default().params

    plan.params["missing_threshold"] = _clamp_float(
        plan.params.get("missing_threshold"),
        lo=0.10, hi=0.90,
        default=float(defaults["missing_threshold"]),
    )

    plan.params["datetime_success_ratio"] = _clamp_float(
        plan.params.get("datetime_success_ratio"),
        lo=0.50, hi=0.99,
        default=float(defaults["datetime_success_ratio"]),
    )

    allowed_numeric = {"mean", "median", "constant", None}
    if plan.params.get("numeric_strategy") not in allowed_numeric:
        plan.params["numeric_strategy"] = defaults["numeric_strategy"]

    allowed_cat = {"mode", "constant", None}
    if plan.params.get("categorical_strategy") not in allowed_cat:
        plan.params["categorical_strategy"] = defaults["categorical_strategy"]

    allowed_dt = {"ffill", "bfill", None}
    if plan.params.get("datetime_strategy") not in allowed_dt:
        plan.params["datetime_strategy"] = defaults["datetime_strategy"]

    try:
        plan.params["categorical_numeric_max_unique"] = int(
            plan.params.get("categorical_numeric_max_unique", defaults["categorical_numeric_max_unique"])
        )
    except Exception:
        plan.params["categorical_numeric_max_unique"] = int(defaults["categorical_numeric_max_unique"])

    if plan.params["categorical_numeric_max_unique"] < 2:
        plan.params["categorical_numeric_max_unique"] = int(defaults["categorical_numeric_max_unique"])

    plan.params["impute"] = bool(plan.enabled_steps.get("impute_missing", True))
    return plan