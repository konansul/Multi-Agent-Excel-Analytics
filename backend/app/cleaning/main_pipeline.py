from __future__ import annotations

import pandas as pd
from typing import Tuple, Dict, Optional, Union

from ._01_snapshots import snapshot
from ._02_normalize import normalize_columns
from ._03_drop_rules import drop_empty_columns, drop_constant_columns, drop_high_missing_columns
from ._04_datetime_inference import infer_datetime_columns
from ._05_impute_missing import impute_missing_values
from ._06_differences import diff_dtypes, diff_missing_fraction, diff_missing_counts


def run_cleaning_pipeline(
    df: pd.DataFrame,
    missing_threshold: float = 0.5,
    impute: bool = True,
    numeric_strategy: Optional[str] = 'mean',        # "mean" | "median" | "constant" | None
    categorical_strategy: Optional[str] = 'mode',    # "mode" | "constant" | None
    datetime_strategy: Optional[str] = None,         # "ffill" | "bfill" | None
    fill_value: Union[int, float, str, None] = 0,
    datetime_success_ratio: float = 0.8,
    categorical_numeric_max_unique: int = 20,        # numeric-as-categorical threshold
) -> Tuple[pd.DataFrame, Dict]:

    # Orchestrates cleaning pipeline and returns (clean_df, report)

    report: Dict = {}

    # BEFORE snapshot
    before = snapshot(df)
    report['rows_before'] = before['rows']
    report['cols_before'] = before['cols']
    report['dtypes_before'] = before['dtypes']
    report['missing_fraction_before'] = before['missing_fraction']
    report['missing_counts_before'] = before['missing_counts']

    # Normalize column names
    clean_df, normalized_map = normalize_columns(df)
    report["normalized_columns"] = normalized_map

    # Drop rules (empty/constant)
    clean_df, dropped_empty = drop_empty_columns(clean_df)
    report["dropped_empty_columns"] = dropped_empty

    clean_df, dropped_constant = drop_constant_columns(clean_df)
    report["dropped_constant_columns"] = dropped_constant

    # MID snapshot (after empty/constant drops)
    mid = snapshot(clean_df)
    report["missing_fraction_mid"] = mid["missing_fraction"]
    report["missing_counts_mid"] = mid["missing_counts"]

    # Drop high-missing columns
    clean_df, dropped_high = drop_high_missing_columns(clean_df, missing_threshold)
    report["dropped_high_missing_columns"] = dropped_high
    report["missing_threshold"] = float(missing_threshold)

    # Infer datetime columns
    clean_df, inferred_dt_cols = infer_datetime_columns(clean_df, datetime_success_ratio)
    report["inferred_datetime_columns"] = inferred_dt_cols
    report["datetime_success_ratio"] = float(datetime_success_ratio)


    # Imputation
    clean_df, imputation_report = impute_missing_values(
        clean_df,
        impute=impute,
        numeric_strategy=numeric_strategy,
        categorical_strategy=categorical_strategy,
        datetime_strategy=datetime_strategy,
        fill_value=fill_value,
        categorical_numeric_max_unique=categorical_numeric_max_unique,
    )
    report["imputation"] = imputation_report

    # AFTER snapshot
    after = snapshot(clean_df)
    report["rows_after"] = after["rows"]
    report["cols_after"] = after["cols"]
    report["dtypes_after"] = after["dtypes"]
    report["missing_fraction_after"] = after["missing_fraction"]
    report["missing_counts_after"] = after["missing_counts"]
    report["too_few_rows"] = after["rows"] < 10

    # Differences
    report["dtypes_changed"] = diff_dtypes(report["dtypes_before"], report["dtypes_after"])
    report["missing_changed"] = diff_missing_fraction(
        report["missing_fraction_before"], report["missing_fraction_after"]
    )
    report["missing_counts_changed"] = diff_missing_counts(
        report["missing_counts_before"], report["missing_counts_after"]
    )

    # Aggregates
    report["dropped_total"] = int(len(dropped_empty) + len(dropped_constant) + len(dropped_high))

    return clean_df, report