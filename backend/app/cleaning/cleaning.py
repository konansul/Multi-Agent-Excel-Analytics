from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Tuple, Dict, List


def clean_dataframe(df: pd.DataFrame, missing_threshold: float = 0.5) -> Tuple[pd.DataFrame, Dict]:

    report: Dict = { }

    report["rows_before"] = int(df.shape[0])
    report["cols_before"] = int(df.shape[1])

    clean_df = df.copy()

    original_columns = list(clean_df.columns)

    clean_df.columns = (
        clean_df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    report["normalized_columns"] = {
        old: new for old, new in zip(original_columns, clean_df.columns)
        if old != new
    }

    empty_columns: List[str] = [
        col for col in clean_df.columns
        if clean_df[col].isna().all()
    ]

    clean_df.drop(columns=empty_columns, inplace=True)

    report["dropped_empty_columns"] = empty_columns

    constant_columns: List[str] = [
        col for col in clean_df.columns
        if clean_df[col].nunique(dropna=True) <= 1
    ]

    clean_df.drop(columns=constant_columns, inplace=True)

    report["dropped_constant_columns"] = constant_columns

    missing_fraction = clean_df.isna().mean().to_dict()

    report["missing_fraction"] = {
        col: round(float(frac), 4)
        for col, frac in missing_fraction.items()
    }

    high_missing_columns = [
        col for col, frac in missing_fraction.items()
        if frac > missing_threshold
    ]

    clean_df.drop(columns=high_missing_columns, inplace=True)

    report["dropped_high_missing_columns"] = high_missing_columns
    report["missing_threshold"] = missing_threshold

    inferred_datetime_columns = []

    for col in clean_df.columns:
        if clean_df[col].dtype != object:
            continue

        sample = clean_df[col].dropna().astype(str).head(50)
        if sample.empty:
            continue

        letters_ratio = sample.str.contains(r"[A-Za-zА-Яа-я]", regex=True).mean()
        if letters_ratio > 0.3:
            continue

        converted = pd.to_datetime(clean_df[col], errors="coerce", infer_datetime_format=True)

        non_null = clean_df[col].notna().sum()
        ok = converted.notna().sum()
        if non_null > 0 and (ok / non_null) >= 0.8:
            clean_df[col] = converted
            inferred_datetime_columns.append(col)

    report["inferred_datetime_columns"] = inferred_datetime_columns

    report["dtypes_after"] = {
        col: str(dtype)
        for col, dtype in clean_df.dtypes.items()
    }

    report["rows_after"] = int(clean_df.shape[0])
    report["cols_after"] = int(clean_df.shape[1])

    report["too_few_rows"] = clean_df.shape[0] < 10

    return clean_df, report