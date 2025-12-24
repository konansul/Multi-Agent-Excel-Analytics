from __future__ import annotations

import pandas as pd
from typing import Dict, Tuple


def normalize_columns(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Normalize column names:
    - convert to string
    - strip whitespace
    - lowercase
    - replace spaces with underscores

    Returns:
        clean_df: DataFrame with normalized column names
        renamed:  dict {old_name: new_name} for changed columns only
    """
    clean_df = df.copy()

    original_columns = list(clean_df.columns)

    clean_df.columns = (
        clean_df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    renamed = {
        old: new
        for old, new in zip(original_columns, clean_df.columns)
        if str(old) != str(new)
    }

    return clean_df, renamed