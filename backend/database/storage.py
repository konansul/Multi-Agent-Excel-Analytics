from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

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