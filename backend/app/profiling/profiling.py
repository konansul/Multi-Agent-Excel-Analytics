from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def profile_dataframe(
    df: pd.DataFrame,
    *,
    max_categories: int = 30,
    top_k: int = 10,
    max_corr_numeric_cols: int = 30,
    corr_sample_rows: int = 50_000,
    skew_min_rows: int = 20,
) -> Dict[str, Any]:
    """
    Generate structured dataset signals (NO LLM).

    Works for both:
    - pre-clean profiling (raw df)
    - post-clean profiling (clean df)

    Use it like:
      pre = profile_dataframe(raw_df)
      post = profile_dataframe(clean_df)
    """

    profile: Dict[str, Any] = {}

    # -------------------------
    # 1) Shape
    # -------------------------
    n_rows, n_cols = df.shape
    profile["n_rows"] = int(n_rows)
    profile["n_cols"] = int(n_cols)

    # -------------------------
    # 2) Column groups
    # -------------------------
    # datetime: true datetime dtypes only
    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

    # bool: true bool dtype
    bool_cols = [c for c in df.columns if pd.api.types.is_bool_dtype(df[c])]

    # numeric: all numeric (includes ints/floats); excludes bool automatically in pandas usually, but keep safe
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and c not in bool_cols]

    # categorical: object or category (avoid deprecated categorical_dtype helpers)
    categorical_cols = [
        c for c in df.columns
        if (pd.api.types.is_object_dtype(df[c]) or str(df[c].dtype) == "category")
        and c not in datetime_cols
    ]

    profile["columns"] = {
        "datetime": datetime_cols,
        "numeric": numeric_cols,
        "boolean": bool_cols,
        "categorical": categorical_cols,
    }
    profile["counts"] = {
        "datetime": int(len(datetime_cols)),
        "numeric": int(len(numeric_cols)),
        "boolean": int(len(bool_cols)),
        "categorical": int(len(categorical_cols)),
    }

    # -------------------------
    # 3) Time index heuristics
    # -------------------------
    has_time_index = len(datetime_cols) > 0
    time_col = datetime_cols[0] if datetime_cols else None

    profile["has_time_index"] = bool(has_time_index)
    profile["time_column"] = time_col

    # dataset type heuristic
    if has_time_index and len(numeric_cols) > 0:
        dataset_type = "time_series"
    elif not has_time_index:
        dataset_type = "tabular"
    else:
        dataset_type = "mixed"

    profile["dataset_type"] = dataset_type

    # -------------------------
    # 4) Missingness
    # -------------------------
    if n_cols == 0:
        overall_missing_pct = 0.0
        top_missing = {}
    else:
        missing_frac = df.isna().mean(numeric_only=False)
        overall_missing_pct = float(missing_frac.mean() * 100.0)

        top_missing = (
            missing_frac.sort_values(ascending=False)
            .head(top_k)
            .mul(100.0)
            .round(3)
            .to_dict()
        )

    profile["missingness"] = {
        "overall_missing_%": round(overall_missing_pct, 3),
        "top_missing_columns": top_missing,
    }

    # Convenience keys at top level (helps agents/UI)
    profile["overall_missing_%"] = profile["missingness"]["overall_missing_%"]
    profile["top_missing_columns"] = profile["missingness"]["top_missing_columns"]

    # -------------------------
    # 5) Categorical cardinality + warnings
    # -------------------------
    cat_info: List[Dict[str, Any]] = []
    for c in categorical_cols:
        # nunique on huge cols can be costly; dropna=True is fine
        try:
            nunique = int(df[c].nunique(dropna=True))
        except Exception:
            nunique = -1
        cat_info.append({"column": c, "unique_values": nunique})

    cat_info.sort(key=lambda x: x["unique_values"], reverse=True)
    profile["categorical_cardinality"] = cat_info

    warnings: List[str] = []
    for item in cat_info:
        if item["unique_values"] > max_categories:
            warnings.append(
                f"Column '{item['column']}' has high cardinality ({item['unique_values']} categories)."
            )
    profile["warnings"] = warnings

    # -------------------------
    # 6) Skewness (numeric)
    # -------------------------
    skewness: Dict[str, float] = {}
    for c in numeric_cols:
        series = df[c].dropna()
        if len(series) < skew_min_rows:
            continue
        try:
            skewness[c] = float(np.round(series.skew(), 4))
        except Exception:
            continue

    top_abs_skewed = dict(
        sorted(skewness.items(), key=lambda kv: abs(kv[1]), reverse=True)[:top_k]
    )

    profile["skewness"] = {
        "per_numeric_column": skewness,
        "top_abs_skewed": top_abs_skewed,
    }

    # Convenience key for the policy agent
    profile["skewness_top_abs"] = top_abs_skewed

    # -------------------------
    # 7) Correlation (numeric)
    # -------------------------
    corr_signals: Dict[str, Any] = {"top_abs_pairs": [], "max_abs_corr": None}

    # choose a subset of numeric columns if too many
    corr_cols = numeric_cols[:max_corr_numeric_cols]

    if len(corr_cols) >= 2:
        work_df = df[corr_cols]

        # sample rows if very large
        if len(work_df) > corr_sample_rows:
            work_df = work_df.sample(n=corr_sample_rows, random_state=42)

        try:
            corr = work_df.corr(numeric_only=True)
            cols_list = list(corr.columns)

            pairs: List[Tuple[str, str, float]] = []
            for i in range(len(cols_list)):
                for j in range(i + 1, len(cols_list)):
                    val = corr.iloc[i, j]
                    if pd.isna(val):
                        continue
                    pairs.append((cols_list[i], cols_list[j], float(val)))

            pairs.sort(key=lambda x: abs(x[2]), reverse=True)
            top_pairs = pairs[:top_k]

            corr_signals["top_abs_pairs"] = [
                {"col_x": a, "col_y": b, "corr": float(np.round(v, 4))} for a, b, v in top_pairs
            ]
            if top_pairs:
                corr_signals["max_abs_corr"] = float(np.round(abs(top_pairs[0][2]), 4))
        except Exception:
            pass

    profile["correlation"] = corr_signals

    return profile