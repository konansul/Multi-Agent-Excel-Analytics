from __future__ import annotations

import pandas as pd
from typing import List, Tuple


def infer_datetime_columns(df: pd.DataFrame, datetime_success_ratio: float = 0.8) -> Tuple[pd.DataFrame, List[str]]:
    """
    Try to convert object columns to datetime if conversion succeeds for
    at least datetime_success_ratio of non-null values.
    """
    clean_df = df.copy()
    inferred: List[str] = []

    for col in clean_df.columns:
        if clean_df[col].dtype != object:
            continue

        sample = clean_df[col].dropna().astype(str).head(50)
        if sample.empty:
            continue

        # avoid converting text-like columns (names, categories) into dates
        letters_ratio = sample.str.contains(r"[A-Za-zА-Яа-я]", regex=True).mean()
        if float(letters_ratio) > 0.3:
            continue

        converted = pd.to_datetime(clean_df[col], errors="coerce")

        non_null = int(clean_df[col].notna().sum())
        ok = int(converted.notna().sum())

        if non_null > 0 and (ok / non_null) >= float(datetime_success_ratio):
            clean_df[col] = converted
            inferred.append(col)

    return clean_df, inferred