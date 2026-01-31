# backend/database/storage.py
from __future__ import annotations

import os
import numpy as np
import pandas as pd

from pathlib import Path
from typing import Any, Optional
from datetime import date, datetime


BLOB_DIR = Path(os.getenv("LOCAL_BLOB_DIR", "storage")).resolve()


def ensure_blob_dir() -> None:
    BLOB_DIR.mkdir(parents=True, exist_ok=True)


def _sanitize_key(key: str) -> str:
    key = (key or "").strip().lstrip("/").replace("\\", "/")
    parts = [p for p in key.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise ValueError("Invalid blob key: contains '..'")
    return "/".join(parts)


def _full_path(key: str) -> Path:
    key = _sanitize_key(key)
    path = (BLOB_DIR / key).resolve()

    try:
        path.relative_to(BLOB_DIR)
    except ValueError:
        raise ValueError("Invalid blob key: resolves outside blob dir")

    return path


def put_bytes(key: str, data: bytes, content_type: Optional[str] = None) -> None:
    ensure_blob_dir()
    path = _full_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def get_bytes(key: str) -> bytes:
    path = _full_path(key)
    if not path.exists():
        raise FileNotFoundError(f"Blob not found: {key}")
    return path.read_bytes()


def exists(key: str) -> bool:
    try:
        return _full_path(key).exists()
    except Exception:
        return False


def delete_key(key: str) -> None:
    path = _full_path(key)
    if path.exists():
        path.unlink()

def to_jsonable(x: Any) -> Any:
    # Recursively convert objects to JSON-serializable python types
    if x is None:
        return None

    if isinstance(x, (np.integer, np.floating, np.bool_)):
        return x.item()

    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    if isinstance(x, (pd.Timestamp, datetime, date)):
        return x.isoformat()

    if isinstance(x, dict):
        return {str(k): to_jsonable(v) for k, v in x.items()}

    if isinstance(x, (list, tuple, set)):
        return [to_jsonable(v) for v in x]

    if isinstance(x, pd.Series):
        return [to_jsonable(v) for v in x.tolist()]
    if isinstance(x, pd.Index):
        return [to_jsonable(v) for v in x.tolist()]
    if isinstance(x, pd.DataFrame):
        return to_jsonable(x.to_dict(orient="records"))
    if isinstance(x, np.ndarray):
        return [to_jsonable(v) for v in x.tolist()]

    return x