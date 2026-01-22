from __future__ import annotations

from typing import Any, Dict

from .schemas import CleaningPlan, validate_plan_dict
from .llm_client import LLMClient
from .cleaning_policy_utils import _sanitize_plan


def build_cleaning_plan_llm(pre_profile: Dict[str, Any], client: LLMClient) -> CleaningPlan:
    prompt = make_llm_prompt(pre_profile)

    raw_text = client.complete(prompt)
    plan_dict = client.extract_json(raw_text)

    plan = validate_plan_dict(plan_dict)

    defaults = CleaningPlan.default().params
    for k, v in defaults.items():
        plan.params.setdefault(k, v)

    return _sanitize_plan(plan)


def make_llm_prompt(pre_profile: Dict[str, Any]) -> str:
    return f"""
You are a data-cleaning policy agent.

IMPORTANT:
- Return only valid JSON (no markdown, no extra text).
- You do NOT receive raw dataset rows, only summary signals.

Goal:
Create a safe, deterministic cleaning plan for a pandas pipeline.

Allowed steps:
snapshots, normalize, drop_rules, datetime_inference, impute_missing, differences

Return JSON exactly in this schema:
{{
  "version": 1,
  "source": "llm",
  "enabled_steps": {{
    "snapshots": true,
    "normalize": true,
    "drop_rules": true,
    "datetime_inference": true,
    "impute_missing": true,
    "differences": true
  }},
  "params": {{
    "missing_threshold": 0.5,
    "datetime_success_ratio": 0.8,
    "impute": true,
    "numeric_strategy": "mean",
    "categorical_strategy": "mode",
    "datetime_strategy": null,
    "fill_value": 0,
    "categorical_numeric_max_unique": 20
  }},
  "notes": ["short reason 1", "short reason 2"]
}}

Constraints:
- missing_threshold in [0.10, 0.90] (default 0.50)
- datetime_success_ratio in [0.50, 0.99] (default 0.80)
- Disable datetime_inference if no datetime/time index is likely.
- Disable impute_missing if missingness is very low.
- Disable differences if numeric columns < 2.
- Prefer numeric_strategy="median" when skewness is likely.
- Keep params consistent with enabled steps.
- DO NOT invent columns that do not exist.
IMPORTANT:
- "differences" means: compute report differences (before vs after) for dtypes/missingness.
- It is NOT time-series differencing (df.diff) and does NOT require a time index.
Guideline:
- Keep differences enabled unless the dataset is extremely small or you explicitly want a minimal report.

Signals (pre_profile dict):
{pre_profile}
""".strip()