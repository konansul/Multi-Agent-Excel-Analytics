from __future__ import annotations

import pandas as pd
from typing import List, Tuple


def drop_empty_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:

    # Drop columns where all values are NaN
    cols = [c for c in df.columns if df[c].isna().all()]
    if not cols:
        return df, [ ]
    return df.drop(columns = cols), cols


def drop_constant_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:

    # Drop columns with <= 1 unique non-null value

    cols = [c for c in df.columns if df[c].nunique(dropna=True) <= 1]
    if not cols:
        return df, [ ]
    return df.drop(columns = cols), cols


def drop_high_missing_columns(df: pd.DataFrame, missing_threshold: float) -> Tuple[pd.DataFrame, List[str]]:

    # Drop columns where missing fraction > missing_threshold

    missing_frac = df.isna().mean()
    cols = [c for c, frac in missing_frac.items() if float(frac) > float(missing_threshold)]
    if not cols:
        return df, [ ]
    return df.drop(columns = cols), cols