import pandas as pd
import numpy as np
from typing import Dict, Any

def get_rich_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculates Mean, SD, and R^2 correlations for the Agent."""
    metrics = {"stats": {}, "correlations": []}

    # 1. Mean and Standard Deviation
    numeric_df = df.select_dtypes(include=[np.number])
    if not numeric_df.empty:
        desc = numeric_df.describe().to_dict()
        for col in numeric_df.columns:
            metrics["stats"][col] = {
                "mean": round(desc[col]["mean"], 2),
                "sd": round(desc[col]["std"], 2),
                "cv": round(desc[col]["std"] / desc[col]["mean"], 2) if desc[col]["mean"] != 0 else 0
            }

    # 2. Correlation and Implied R^2
    if len(numeric_df.columns) > 1:
        corr_matrix = numeric_df.corr().abs().unstack()
        # Filter out self-correlations and get top pairs
        pairs = corr_matrix[corr_matrix < 1.0].sort_values(ascending=False).head(6)
        for (c1, c2), val in pairs.items():
            metrics["correlations"].append({
                "columns": [c1, c2],
                "r_squared": round(val**2, 3), # Statistical R^2
                "correlation": round(val, 2)
            })

    return metrics