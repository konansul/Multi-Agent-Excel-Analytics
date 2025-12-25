from __future__ import annotations

import pandas as pd
from typing import Dict, Tuple, Optional, Union, Any


def impute_missing_values(
    df: pd.DataFrame,
    impute: bool = True,
    numeric_strategy: Optional[str] = "mean",
    categorical_strategy: Optional[str] = "mode",
    datetime_strategy: Optional[str] = None,
    fill_value: Union[int, float, str, None] = 0,
    categorical_numeric_max_unique: int = 20,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Fill missing values and return (df, imputation_report).
    - datetime: ffill / bfill (optional)
    - numeric: mean / median / constant (optional)
      BUT if numeric column has low cardinality (nunique <= categorical_numeric_max_unique),
      treat it as categorical and fill via mode/constant to avoid 0.2/0.3 artifacts.
    - categorical/object: mode / constant (optional)
    """
    clean_df = df.copy()

    report: Dict[str, Any] = {
        "enabled": bool(impute),
        "numeric_strategy": numeric_strategy,
        "categorical_strategy": categorical_strategy,
        "datetime_strategy": datetime_strategy,
        "categorical_numeric_max_unique": int(categorical_numeric_max_unique),
        "filled_counts": {},
        "fill_values_used": {},
        "categorical_numeric_columns": [],
        "total_filled": 0,
    }

    if not impute:
        return clean_df, report

    for col in clean_df.columns:
        missing_before = int(clean_df[col].isna().sum())
        if missing_before == 0:
            continue

        s = clean_df[col]

        # Datetime columns
        if pd.api.types.is_datetime64_any_dtype(s):
            if datetime_strategy == "ffill":
                clean_df[col] = s.ffill()
            elif datetime_strategy == "bfill":
                clean_df[col] = s.bfill()
            else:
                continue  # keep NaT

        # Numeric columns (some are categorical codes)
        elif pd.api.types.is_numeric_dtype(s):
            nunique = int(s.nunique(dropna=True))

            if nunique <= categorical_numeric_max_unique:
                # treat as categorical codes
                if categorical_strategy == "mode":
                    modes = s.mode(dropna=True)
                    if modes.empty:
                        continue
                    value = modes.iloc[0]
                    clean_df[col] = s.fillna(value)
                    report["fill_values_used"][col] = value
                    report["categorical_numeric_columns"].append(col)

                elif categorical_strategy == "constant":
                    clean_df[col] = s.fillna(fill_value)
                    report["fill_values_used"][col] = fill_value
                    report["categorical_numeric_columns"].append(col)

                else:
                    continue

            else:
                # true numeric
                if numeric_strategy == "mean":
                    value = s.mean()
                    if pd.isna(value):
                        continue
                    value = float(value)
                    clean_df[col] = s.fillna(value)
                    report["fill_values_used"][col] = value

                elif numeric_strategy == "median":
                    value = s.median()
                    if pd.isna(value):
                        continue
                    value = float(value)
                    clean_df[col] = s.fillna(value)
                    report["fill_values_used"][col] = value

                elif numeric_strategy == "constant":
                    clean_df[col] = s.fillna(fill_value)
                    report["fill_values_used"][col] = fill_value

                else:
                    continue

        # Categorical/object columns
        else:
            if categorical_strategy == "mode":
                modes = s.mode(dropna=True)
                if modes.empty:
                    continue
                value = modes.iloc[0]
                clean_df[col] = s.fillna(value)
                report["fill_values_used"][col] = value

            elif categorical_strategy == "constant":
                clean_df[col] = s.fillna(fill_value)
                report["fill_values_used"][col] = fill_value

            else:
                continue

        missing_after = int(clean_df[col].isna().sum())
        filled = missing_before - missing_after
        if filled > 0:
            report["filled_counts"][col] = int(filled)

    report["total_filled"] = int(sum(report["filled_counts"].values()))
    return clean_df, report