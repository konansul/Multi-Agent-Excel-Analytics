from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field


class DatasetRef(BaseModel):
    dataset_id: str


class UploadResponse(BaseModel):
    datasets: list[Dict[str, Any]]  # each sheet context meta
    # пример элемента:
    # {
    #   "dataset_id": "...",
    #   "file_name": "...",
    #   "sheet_name": "...",
    #   "shape": [rows, cols],
    #   "dtypes": {...}
    # }


class PreviewResponse(BaseModel):
    dataset_id: str
    columns: list[str]
    rows: list[Dict[str, Any]]


class ProfilingRequest(BaseModel):
    dataset_id: str
    options: Optional[Dict[str, Any]] = None


class ProfilingResponse(BaseModel):
    profile_id: str


class PolicySuggestRequest(BaseModel):
    dataset_id: str
    mode: Literal["rule_based", "llm"] = "rule_based"
    llm_model: str = "gemini-2.5-flash"


class PolicySuggestResponse(BaseModel):
    policy: Dict[str, Any]
    source: str
    notes: list[str] = Field(default_factory=list)


class CleaningRunRequest(BaseModel):
    dataset_id: str
    use_llm: bool = False
    llm_model: str = "gemini-2.5-flash"

    # overrides (optional): если хочешь дать UI ручные knobs
    missing_threshold: Optional[float] = None
    impute: Optional[bool] = None
    numeric_strategy: Optional[str] = None
    categorical_strategy: Optional[str] = None
    datetime_strategy: Optional[str] = None
    fill_value: Optional[Any] = None
    datetime_success_ratio: Optional[float] = None
    categorical_numeric_max_unique: Optional[int] = None


class CleaningRunResponse(BaseModel):
    run_id: str

class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserMeResponse(BaseModel):
    user_id: str
    email: str