from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd

from ._01_snapshots import snapshot
from ._02_normalize import normalize_columns
from ._03_drop_rules import (
    drop_empty_columns,
    drop_constant_columns,
    drop_high_missing_columns,
)
from ._04_datetime_inference import infer_datetime_columns
from ._05_impute_missing import impute_missing_values
from ._06_differences import diff_dtypes, diff_missing_fraction, diff_missing_counts

from ..profiling.profiling import profile_dataframe
from ..agents.cleaning_policy_agent import build_cleaning_plan
from ..agents.schemas import CleaningPlan


def run_cleaning_pipeline(
    df: pd.DataFrame,
    # function defaults (still supported)
    missing_threshold: float = 0.5,
    impute: bool = True,
    numeric_strategy: Optional[str] = "mean",
    categorical_strategy: Optional[str] = "mode",
    datetime_strategy: Optional[str] = None,
    fill_value: Union[int, float, str, None] = 0,
    datetime_success_ratio: float = 0.8,
    categorical_numeric_max_unique: int = 20,
    # new knobs
    use_llm: bool = False,
    llm_model: str = "gemini-2.5-flash",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Orchestrates cleaning and returns (clean_df, report).

    Flow:
      1) pre_profile (raw)
      2) cleaning_plan (rule-based or LLM)
      3) execute deterministic pandas pipeline steps (enabled/disabled by plan)
      4) post_profile (cleaned)

    The LLM never sees raw rows; it only influences which deterministic steps run
    and what parameters they use.
    """

    report: Dict[str, Any] = {}

    # -------------------------
    # 0) PRE-PROFILE (raw)
    # -------------------------
    pre_profile = profile_dataframe(df)
    report["pre_profile"] = pre_profile

    # -------------------------
    # 0.5) POLICY PLAN (agent)
    # -------------------------
    plan: CleaningPlan = build_cleaning_plan(pre_profile, use_llm=use_llm, model=llm_model)
    report["cleaning_plan"] = plan.to_dict()

    enabled: Dict[str, bool] = dict(plan.enabled_steps or {})
    params: Dict[str, Any] = dict(plan.params or {})

    # Merge agent params with function defaults (defaults are fallback)
    missing_threshold = float(params.get("missing_threshold", missing_threshold))
    datetime_success_ratio = float(params.get("datetime_success_ratio", datetime_success_ratio))
    numeric_strategy = params.get("numeric_strategy", numeric_strategy)
    categorical_strategy = params.get("categorical_strategy", categorical_strategy)
    datetime_strategy = params.get("datetime_strategy", datetime_strategy)
    fill_value = params.get("fill_value", fill_value)
    categorical_numeric_max_unique = int(params.get("categorical_numeric_max_unique", categorical_numeric_max_unique))

    # IMPORTANT: impute is controlled by enabled_steps.
    # If enabled, we impute=True. If disabled, we skip the step entirely.
    # (We do NOT trust params["impute"] more than enabled_steps.)
    impute_enabled = bool(enabled.get("impute_missing", True))
    # still allow caller to force off globally:
    if not impute:
        impute_enabled = False

    clean_df = df.copy()

    # -------------------------
    # 1) BEFORE snapshot
    # -------------------------
    snapshots_enabled = bool(enabled.get("snapshots", True))
    if snapshots_enabled:
        before = snapshot(clean_df)
        report["rows_before"] = before["rows"]
        report["cols_before"] = before["cols"]
        report["dtypes_before"] = before["dtypes"]
        report["missing_fraction_before"] = before["missing_fraction"]
        report["missing_counts_before"] = before["missing_counts"]
    else:
        report["rows_before"] = int(clean_df.shape[0])
        report["cols_before"] = int(clean_df.shape[1])
        report["snapshots"] = {"skipped": True}

    # -------------------------
    # 2) Normalize column names
    # -------------------------
    if enabled.get("normalize", True):
        clean_df, normalized_map = normalize_columns(clean_df)
        report["normalized_columns"] = normalized_map
    else:
        report["normalized_columns"] = {"skipped": True}

    # -------------------------
    # 3) Drop rules (empty/constant/high-missing)
    # -------------------------
    dropped_empty: list = []
    dropped_constant: list = []
    dropped_high: list = []

    if enabled.get("drop_rules", True):
        clean_df, dropped_empty = drop_empty_columns(clean_df)
        clean_df, dropped_constant = drop_constant_columns(clean_df)

        report["dropped_empty_columns"] = dropped_empty
        report["dropped_constant_columns"] = dropped_constant

        if snapshots_enabled:
            mid = snapshot(clean_df)
            report["missing_fraction_mid"] = mid["missing_fraction"]
            report["missing_counts_mid"] = mid["missing_counts"]

        clean_df, dropped_high = drop_high_missing_columns(clean_df, missing_threshold)
        report["dropped_high_missing_columns"] = dropped_high
        report["missing_threshold"] = float(missing_threshold)
    else:
        report["drop_rules"] = {"skipped": True}
        report["dropped_empty_columns"] = []
        report["dropped_constant_columns"] = []
        report["dropped_high_missing_columns"] = []
        report["missing_threshold"] = float(missing_threshold)

    # -------------------------
    # 4) Datetime inference
    # -------------------------
    if enabled.get("datetime_inference", True):
        clean_df, inferred_dt_cols = infer_datetime_columns(clean_df, datetime_success_ratio)
        report["inferred_datetime_columns"] = inferred_dt_cols
        report["datetime_success_ratio"] = float(datetime_success_ratio)
    else:
        report["datetime_inference"] = {"skipped": True}
        report["inferred_datetime_columns"] = []
        report["datetime_success_ratio"] = float(datetime_success_ratio)

    # -------------------------
    # 5) Imputation (strictly optional)
    # -------------------------
    if impute_enabled:
        clean_df, imputation_report = impute_missing_values(
            clean_df,
            impute=True,
            numeric_strategy=numeric_strategy,
            categorical_strategy=categorical_strategy,
            datetime_strategy=datetime_strategy,
            fill_value=fill_value,
            categorical_numeric_max_unique=categorical_numeric_max_unique,
        )
        report["imputation"] = imputation_report
    else:
        report["imputation"] = {"impute": False, "skipped": True}

    # -------------------------
    # 6) AFTER snapshot
    # -------------------------
    if snapshots_enabled:
        after = snapshot(clean_df)
        report["rows_after"] = after["rows"]
        report["cols_after"] = after["cols"]
        report["dtypes_after"] = after["dtypes"]
        report["missing_fraction_after"] = after["missing_fraction"]
        report["missing_counts_after"] = after["missing_counts"]
        report["too_few_rows"] = after["rows"] < 10
    else:
        report["rows_after"] = int(clean_df.shape[0])
        report["cols_after"] = int(clean_df.shape[1])
        report["too_few_rows"] = int(clean_df.shape[0]) < 10

    # -------------------------
    # 7) Differences (report diff, not time-series diff)
    # -------------------------
    if bool(enabled.get("differences", True)) and snapshots_enabled:
        report["dtypes_changed"] = diff_dtypes(
            report.get("dtypes_before", {}),
            report.get("dtypes_after", {}),
        )
        report["missing_changed"] = diff_missing_fraction(
            report.get("missing_fraction_before", {}),
            report.get("missing_fraction_after", {}),
        )
        report["missing_counts_changed"] = diff_missing_counts(
            report.get("missing_counts_before", {}),
            report.get("missing_counts_after", {}),
        )
    else:
        report["differences"] = {"skipped": True}
        report["dtypes_changed"] = {}
        report["missing_changed"] = {}
        report["missing_counts_changed"] = {}

    # Aggregates
    report["dropped_total"] = int(len(dropped_empty) + len(dropped_constant) + len(dropped_high))

    # -------------------------
    # 8) POST-PROFILE (clean)
    # -------------------------
    post_profile = profile_dataframe(clean_df)
    report["post_profile"] = post_profile

    return clean_df, report