from __future__ import annotations

import pandas as pd
from typing import Dict


def snapshot(df: pd.DataFrame) -> Dict:

    return {
        'rows': int(df.shape[0]),
        'cols': int(df.shape[1]),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
        'missing_fraction': {
            col: round(float(v), 4)
            for col, v in df.isna().mean().items()
        },
        'missing_counts': {
            col: int(v)
            for col, v in df.isna().sum().items()
        },
    }