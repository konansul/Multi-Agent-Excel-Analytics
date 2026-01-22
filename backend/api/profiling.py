from __future__ import annotations

from typing import Any, Dict

import pandas as pd
from fastapi import APIRouter, HTTPException

from backend.api.models import ProfilingRequest, ProfilingResponse
from backend.api.storage import dataset_paths, new_id, profile_paths, read_json, write_json

from backend.app.profiling.profiling import profile_dataframe

router = APIRouter()


@router.post("/profiling", response_model=ProfilingResponse)
def run_profiling(req: ProfilingRequest):
    ds_paths = dataset_paths(req.dataset_id)
    if not ds_paths["current_parquet"].exists():
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        df = pd.read_parquet(ds_paths["current_parquet"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read dataset: {e}")

    options = req.options or {}
    try:
        report = profile_dataframe(df, **options)
    except TypeError as e:
        raise HTTPException(status_code=400, detail=f"Bad profiling options: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Profiling failed: {e}")

    profile_id = new_id("prof")
    ppaths = profile_paths(profile_id)
    write_json(ppaths["report"], report)

    return {"profile_id": profile_id}


@router.get("/profiling/{profile_id}")
def get_profiling_report(profile_id: str) -> Dict[str, Any]:
    ppaths = profile_paths(profile_id)
    if not ppaths["report"].exists():
        raise HTTPException(status_code=404, detail="Profile report not found")
    return read_json(ppaths["report"])