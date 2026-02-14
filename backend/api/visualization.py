# backend/api/visualization.py
from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from pydantic import BaseModel

from backend.app.visualization.agent import VisualizationAgent
from backend.app.visualization.schemas import VisualizationPlan
from backend.app.visualization.schemas import ExplainResponse
from backend.app.visualization.schemas import ExplainRequest
from backend.api.auth import get_current_user
from backend.database.models import User
from backend.app.visualization.service import get_rich_metrics
from backend.api.datasets import load_dataset_as_dataframe
router = APIRouter()



class VizRequest(BaseModel):
    dataset_id: str
    profile_data: Dict[str, Any]

@router.post("/visualization/suggest", response_model=VisualizationPlan)
def suggest_visualizations(
        req: VizRequest,
        current_user: User = Depends(get_current_user)
):
    """
    Triggers the Visualization Agent.
    Accepts the dataset profile (signals) and returns a plot plan.
    """
    if not req.profile_data:
        raise HTTPException(status_code=400, detail="Profile signals are required.")

    try:

        # 1. Load data and calculate math
        df = load_dataset_as_dataframe(req.dataset_id)
        metrics = get_rich_metrics(df)

        agent = VisualizationAgent()
        plan = agent.create_plan(req.dataset_id, req.profile_data)
        return plan
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Visualization Agent failed: {str(e)}")


@router.post("/visualization/explain", response_model=ExplainResponse) #explaining the visualizations
def explain_chart_endpoint(
        req: ExplainRequest,
        current_user: User = Depends(get_current_user)
):
    """
    Generates a text explanation for a specific chart.
    """
    try:
        agent = VisualizationAgent()
        # Call the agent
        text_result = agent.explain_visualization(req.plot_title, req.axis_info)

        # Return strict JSON matching the response_model
        return ExplainResponse(explanation=text_result)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Explanation failed: {str(e)}")