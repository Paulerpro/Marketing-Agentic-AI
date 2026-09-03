"""Smoke tests for tools and graph (no LLM). DB / MLflow-heavy paths are optional."""

from __future__ import annotations

import asyncio
import json

import pytest
from langchain_core.messages import HumanMessage

from src.agents.tools.compliance_tools import validate_campaign_plan
from src.agents.tools.ml_tools import score_churn_features
from src.utils.email_templates import select_template
from ml.config import CHURN_SCORING_FEATURE_COLUMNS


def _sample_plan():
    return {
        "version": "test-v1",
        "segment": {"name": "high_risk", "filters": {}},
        "constraints": {"channel": "email", "max_discount_pct": 10},
        "actions": [{"type": "email", "template_id": select_template(0.8)["template_id"]}],
        "notes": "",
    }


def test_validate_campaign_plan_approve():
    out = validate_campaign_plan(_sample_plan())
    assert out["approved"] is True
    assert out["reasons"] == []


def test_validate_campaign_plan_rejects_bad_discount():
    plan = _sample_plan()
    plan["constraints"]["max_discount_pct"] = 99
    out = validate_campaign_plan(plan)
    assert out["approved"] is False
    assert "discount_cap_exceeded" in out["reasons"]


def test_score_churn_features_requires_columns():
    import pandas as pd

    with pytest.raises(ValueError, match="Missing feature columns"):
        score_churn_features(pd.DataFrame({"total_price": [1.0]}))


@pytest.mark.integration
def test_build_merged_optional_db():
    """Requires working DB credentials and populated tables."""
    from src.agents.tools.data_tools import build_merged_features_for_ml

    df = build_merged_features_for_ml()
    assert len(df) >= 0
    missing = set(CHURN_SCORING_FEATURE_COLUMNS) - set(df.columns)
    assert not missing, f"merged frame missing columns: {missing}"


def _initial_state(message: str) -> dict:
    return {
        "messages": [HumanMessage(content=message)],
        "completed_workers": [],
        "next": None,
        "campaign_plan": None,
        "compliance_result": None,
        "qa_result": None,
    }


@pytest.mark.integration
def test_graph_demo_full_optional():
    """Needs DB + a Production churn model (via MCP subprocess); LLM steps degrade
    gracefully to rendered templates without ANTHROPIC_API_KEY."""
    from src.agents.graph import compile_graph

    g = compile_graph()
    out = asyncio.run(g.ainvoke(_initial_state("demo full")))
    assert "compliance" in (out.get("completed_workers") or [])
    assert out.get("campaign_plan")
    assert out.get("compliance_result")


def test_graph_analyze_keyword_no_db_crash():
    """Should complete even when DB/model unavailable (worker catches errors)."""
    from src.agents.graph import compile_graph

    g = compile_graph()
    out = asyncio.run(g.ainvoke(_initial_state("analyze churn")))
    assert "data_analyst" in (out.get("completed_workers") or [])
    last = out["messages"][-1].content
    assert isinstance(last, str)
    body = json.loads(last)
    assert body.get("worker") == "data_analyst"


def test_graph_qa_keyword_no_key_crash():
    """Should complete even without ANTHROPIC_API_KEY / DB (worker catches errors)."""
    from src.agents.graph import compile_graph

    g = compile_graph()
    out = asyncio.run(g.ainvoke(_initial_state("ask: how many customers do we have?")))
    assert "qa" in (out.get("completed_workers") or [])
    last = out["messages"][-1].content
    body = json.loads(last)
    assert body.get("worker") == "qa"
