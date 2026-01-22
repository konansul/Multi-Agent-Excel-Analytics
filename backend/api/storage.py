from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parents[1] / "storage"

DATASETS_DIR = BASE_DIR / "datasets"
PROFILES_DIR = BASE_DIR / "profiles"
RUNS_DIR = BASE_DIR / "runs"


def ensure_storage_dirs() -> None:
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def dataset_dir(dataset_id: str) -> Path:
    return DATASETS_DIR / dataset_id


def profile_dir(profile_id: str) -> Path:
    return PROFILES_DIR / profile_id


def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id


def write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def dataset_paths(dataset_id: str) -> Dict[str, Path]:
    """
    Standard files for dataset:
      - raw.bin (original bytes)
      - meta.json
      - current.parquet (dataframe persisted)
    """
    ddir = dataset_dir(dataset_id)
    return {
        "dir": ddir,
        "raw": ddir / "raw.bin",
        "meta": ddir / "meta.json",
        "current_parquet": ddir / "current.parquet",
        "current_xlsx": ddir / "current.xlsx",
        "current_csv": ddir / "current.csv",
    }


def run_paths(run_id: str) -> Dict[str, Path]:
    rdir = run_dir(run_id)
    return {
        "dir": rdir,
        "status": rdir / "status.json",
        "report": rdir / "report.json",
        "cleaned_xlsx": rdir / "cleaned.xlsx",
        "cleaned_parquet": rdir / "cleaned.parquet",
    }


def profile_paths(profile_id: str) -> Dict[str, Path]:
    pdir = profile_dir(profile_id)
    return {
        "dir": pdir,
        "report": pdir / "report.json",
    }