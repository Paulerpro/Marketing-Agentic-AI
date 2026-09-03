"""HTTP API for deterministic multi-agent demo runs, plus admin endpoints for the
Streamlit UI (segments, campaign send/history, Q&A, model hub)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from langchain_core.messages import AIMessage, HumanMessage
from pydantic import BaseModel, Field

from src.agents.graph import compile_graph
from ml.config import MODEL_NAME
from src.mcp_tools.campaign_logger_tool import get_campaign_history
from src.mcp_tools.email_sender_tool import send_retention_email
from src.mcp_tools.qa_sql_tool import answer_customer_question
from src.mcp_tools.segmentation_tool import segment_customers, summarize_segments

logger = logging.getLogger(__name__)

app = FastAPI(title="AI Marketing Agents", version="0.1.0")

_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = compile_graph()
    return _graph


class ChatRequest(BaseModel):
    message: str = Field(..., examples=["demo full"])
    thread_id: str = Field(default="default")


def _serialize_messages(messages: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in messages:
        if isinstance(m, HumanMessage):
            role = "human"
        elif isinstance(m, AIMessage):
            role = "assistant"
        else:
            role = getattr(m, "type", m.__class__.__name__)
        content = m.content
        if not isinstance(content, str):
            content = str(content)
        out.append({"role": role, "content": content})
    return out


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/config")
def config() -> dict[str, Any]:
    """Read-only view of the effective backend thresholds (Settings page)."""
    from ml.config import MIN_AUC, MIN_RECALL, TRAINING_WINDOW_DAYS, LABEL_DELAY_DAYS
    from ml.drift import PSI_DRIFT_THRESHOLD
    from src.mcp_tools.segmentation_tool import LOW_MAX, MEDIUM_MAX

    return {
        "risk_bands": {"low_max": LOW_MAX, "medium_max": MEDIUM_MAX},
        "psi_drift_threshold": PSI_DRIFT_THRESHOLD,
        "training_window_days": TRAINING_WINDOW_DAYS,
        "label_delay_days": LABEL_DELAY_DAYS,
        "min_auc": MIN_AUC,
        "min_recall": MIN_RECALL,
    }


@app.post("/chat")
async def chat(req: ChatRequest) -> dict[str, Any]:
    """
    Run one user turn through the supervisor graph (stateless compile by default).

    Use a fresh ``thread_id`` once you add a checkpointer to avoid sticky
    ``completed_workers`` from prior demos.
    """
    graph = get_graph()
    initial = {
        "messages": [HumanMessage(content=req.message)],
        "completed_workers": [],
        "next": None,
        "campaign_plan": None,
        "compliance_result": None,
        "qa_result": None,
    }
    result = await graph.ainvoke(initial)
    return {
        "thread_id": req.thread_id,
        "messages": _serialize_messages(result["messages"]),
        "completed_workers": result.get("completed_workers"),
        "campaign_plan": result.get("campaign_plan"),
        "compliance_result": result.get("compliance_result"),
        "qa_result": result.get("qa_result"),
        "next": result.get("next"),
    }


@app.get("/tools/summary")
def tools_summary() -> str:
    """Tiny discovery endpoint for humans (not used by the graph)."""
    return json.dumps(
        {
            "workers": ["supervisor", "data_analyst", "campaign_planner", "compliance", "qa"],
            "hints": [
                "Say 'demo full' to run data → campaign → compliance.",
                "Say 'analyze churn' for data_analyst only.",
                "Say 'ask: <question>' to run the Q&A worker.",
            ],
        }
    )


# ---------------------------------------------------------------------------
# Segmentation (Dashboard / Campaign Manager pages)
# ---------------------------------------------------------------------------


@app.get("/segments/summary")
def segments_summary() -> dict[str, Any]:
    return summarize_segments()


@app.get("/segments")
def segments(min_score: float = 0.0, max_score: float = 1.0, max_customers: int = 500) -> dict[str, Any]:
    rows = segment_customers(min_score=min_score, max_score=max_score, max_customers=max_customers)
    return {"items": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Campaigns (Campaign Manager / Analytics pages)
# ---------------------------------------------------------------------------


class SendEmailRequest(BaseModel):
    customer_id: str
    email: str
    first_name: str
    churn_score: float
    campaign_id: str | None = None
    dry_run: bool | None = None


@app.post("/campaigns/send")
def campaigns_send(req: SendEmailRequest) -> dict[str, Any]:
    return send_retention_email(
        customer_id=req.customer_id,
        email=req.email,
        first_name=req.first_name,
        churn_score=req.churn_score,
        campaign_id=req.campaign_id,
        dry_run=req.dry_run,
    )


@app.get("/campaigns/history")
def campaigns_history(limit: int = 50) -> dict[str, Any]:
    rows = get_campaign_history(limit=limit)
    return {"items": rows, "count": len(rows)}


# ---------------------------------------------------------------------------
# Customer Q&A (PDF Workflow 2)
# ---------------------------------------------------------------------------


class QARequest(BaseModel):
    question: str


@app.post("/qa")
def qa(req: QARequest) -> dict[str, Any]:
    try:
        return answer_customer_question(req.question)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


# ---------------------------------------------------------------------------
# Model Hub (champion/challenger, promote, trigger retrain)
# ---------------------------------------------------------------------------


def _mlflow_client():
    from mlflow.tracking import MlflowClient

    return MlflowClient()


def _version_payload(client, mv) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    try:
        run = client.get_run(mv.run_id)
        metrics = run.data.metrics
        tags = run.data.tags
    except Exception:
        tags = {}
    return {
        "version": mv.version,
        "stage": mv.current_stage,
        "auc": metrics.get("auc"),
        "recall": metrics.get("recall_churn"),
        "precision": metrics.get("precision_churn"),
        "promotion_decision": tags.get("promotion_decision"),
        "trained_on": datetime.fromtimestamp(mv.creation_timestamp / 1000, tz=timezone.utc).isoformat(),
        "run_id": mv.run_id,
    }


@app.get("/model/champion")
def model_champion() -> dict[str, Any] | None:
    client = _mlflow_client()
    versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
    if not versions:
        return None
    return _version_payload(client, versions[0])


@app.get("/model/challenger")
def model_challenger() -> dict[str, Any] | None:
    """Most recently trained version not currently in Production (Staging preferred)."""
    client = _mlflow_client()
    try:
        all_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    except Exception:
        return None
    candidates = [v for v in all_versions if v.current_stage != "Production"]
    if not candidates:
        return None
    candidates.sort(key=lambda v: int(v.version), reverse=True)
    return _version_payload(client, candidates[0])


class PromoteRequest(BaseModel):
    version: str


@app.post("/model/promote")
def model_promote(req: PromoteRequest) -> dict[str, Any]:
    client = _mlflow_client()
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=req.version,
        stage="Production",
        archive_existing_versions=True,
    )
    return {"promoted_version": req.version}


@app.post("/model/retrain")
def model_retrain() -> dict[str, Any]:
    """Runs synchronously (~seconds) - fine for a manual admin click from the UI."""
    from ml.retrain import run_retraining

    return run_retraining()
