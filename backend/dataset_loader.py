from __future__ import annotations

import io
import pandas as pd

from dataclasses import dataclass
from typing import List, Union
from pathlib import Path


@dataclass
class SheetContext:

    file_name: str
    sheet_name: str
    dataset_id: str
    df: pd.DataFrame
    shape: tuple
    dtypes: dict


def _read_excel_all_sheets(source: Union[str, Path, io.BytesIO], file_name: str) -> List[SheetContext]:
    xls = pd.ExcelFile(source)
    sheet_contexts: List[SheetContext] = [ ]

    for idx, sheet_name in enumerate(xls.sheet_names):
        df = pd.read_excel(source, sheet_name = sheet_name)

        dataset_id = f"{file_name}::{sheet_name}::{idx}"

        sheet_contexts.append(
            SheetContext(
                file_name = file_name,
                sheet_name = sheet_name,
                dataset_id = dataset_id,
                df = df,
                shape = df.shape,
                dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()}
            )
        )

    return sheet_contexts