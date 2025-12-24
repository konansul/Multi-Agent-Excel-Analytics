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

        dataset_id = f'{file_name}::{sheet_name}::{idx}'

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


def _read_csv(source: Union[str, Path, io.BytesIO], file_name: str) -> List[SheetContext]:

    df = pd.read_csv(source)

    sheet_name = 'CSV'
    dataset_id = f'{file_name}::{sheet_name}::0'

    return [
        SheetContext(
            file_name = file_name,
            sheet_name = sheet_name,
            dataset_id = dataset_id,
            df = df,
            shape = df.shape,
            dtypes = {col: str(dtype) for col, dtype in df.dtypes.items()},
        )
    ]


def load_from_path(path: Union[str, Path]) -> List[SheetContext]:

    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f'File not found: {path}')

    suffix = path.suffix.lower()

    if suffix in ['.xlsx', '.xls']:
        return _read_excel_all_sheets(path, file_name = path.name)
    elif suffix == '.csv':
        return _read_csv(path, file_name = path.name)
    else:
        raise ValueError(f'Unsupported file type: {suffix}. Supported: .xlsx, .xls, .csv')


def load_from_upload(file_bytes: bytes, filename: str) -> List[SheetContext]:

    suffix = Path(filename).suffix.lower()
    buffer = io.BytesIO(file_bytes)

    if suffix in ['.xlsx', '.xls']:
        return _read_excel_all_sheets(buffer, file_name = filename)
    elif suffix == '.csv':
        return _read_csv(buffer, file_name = filename)
    else:
        raise ValueError(f'Unsupported file type: {suffix}. Supported: .xlsx, .xls, .csv')