from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Literal


# Все шаги, которыми может управлять policy/LLM.
# Названия лучше держать стабильными — на них завязаны и пайплайн, и UI.
StepName = Literal[
    "snapshots",
    "normalize",
    "drop_rules",
    "datetime_inference",
    "impute_missing",
    "differences",
]


@dataclass(frozen=True)
class CleaningPlan:
    """
    A structured, testable plan for running the cleaning pipeline.

    - enabled_steps: which pipeline steps are enabled/disabled
    - params: thresholds/strategies for deterministic Pandas logic
    - notes: human-readable reasoning/explanations (from rule-based or LLM)
    - source: where the plan came from (rule_based / llm)
    - version: plan schema version (helps you evolve safely)
    """
    enabled_steps: Dict[StepName, bool]
    params: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)
    source: Literal["rule_based", "llm"] = "rule_based"
    version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "source": self.source,
            "enabled_steps": dict(self.enabled_steps),
            "params": dict(self.params),
            "notes": list(self.notes),
        }

    @staticmethod
    def default(
        *,
        missing_threshold: float = 0.5,
        datetime_success_ratio: float = 0.8,
        impute: bool = True,
        numeric_strategy: Optional[str] = "mean",
        categorical_strategy: Optional[str] = "mode",
        datetime_strategy: Optional[str] = None,
        fill_value: Any = 0,
        categorical_numeric_max_unique: int = 20,
    ) -> "CleaningPlan":
        """
        Default plan: run everything (except you can toggle impute via param).
        This keeps backward compatibility with your current pipeline behavior.
        """
        enabled: Dict[StepName, bool] = {
            "snapshots": True,
            "normalize": True,
            "drop_rules": True,
            "datetime_inference": True,
            "impute_missing": bool(impute),
            "differences": True,
        }

        params: Dict[str, Any] = {
            "missing_threshold": float(missing_threshold),
            "datetime_success_ratio": float(datetime_success_ratio),
            "impute": bool(impute),
            "numeric_strategy": numeric_strategy,
            "categorical_strategy": categorical_strategy,
            "datetime_strategy": datetime_strategy,
            "fill_value": fill_value,
            "categorical_numeric_max_unique": int(categorical_numeric_max_unique),
        }

        return CleaningPlan(enabled_steps=enabled, params=params, notes=[], source="rule_based", version=1)


def validate_plan_dict(plan: Dict[str, Any]) -> CleaningPlan:
    """
    Convert a plain dict (e.g., from LLM JSON) into a validated CleaningPlan.

    This prevents random/untrusted keys from silently breaking your pipeline.
    """
    if not isinstance(plan, dict):
        raise TypeError("Cleaning plan must be a dict")

    enabled_steps_raw = plan.get("enabled_steps", {})
    params = plan.get("params", {})
    notes = plan.get("notes", [])
    source = plan.get("source", "llm")

    if not isinstance(enabled_steps_raw, dict):
        raise TypeError("enabled_steps must be a dict")
    if not isinstance(params, dict):
        raise TypeError("params must be a dict")
    if not isinstance(notes, list):
        raise TypeError("notes must be a list")

    allowed_steps = {
        "snapshots",
        "normalize",
        "drop_rules",
        "datetime_inference",
        "impute_missing",
        "differences",
    }

    enabled_steps: Dict[StepName, bool] = {}

    # Default: if a step is missing, we assume True (safe, and backward compatible)
    for step in allowed_steps:
        value = enabled_steps_raw.get(step, True)
        enabled_steps[step] = bool(value)  # type: ignore[assignment]

    # Minimal sanitation for common params used by your pipeline
    if "missing_threshold" in params:
        params["missing_threshold"] = float(params["missing_threshold"])
    if "datetime_success_ratio" in params:
        params["datetime_success_ratio"] = float(params["datetime_success_ratio"])
    if "categorical_numeric_max_unique" in params:
        params["categorical_numeric_max_unique"] = int(params["categorical_numeric_max_unique"])
    if "impute" in params:
        params["impute"] = bool(params["impute"])

    # Ensure notes are strings
    notes = [str(x) for x in notes]

    src: Literal["rule_based", "llm"] = "llm" if source != "rule_based" else "rule_based"

    return CleaningPlan(
        enabled_steps=enabled_steps,
        params=params,
        notes=notes,
        source=src,
        version=int(plan.get("version", 1)),
    )