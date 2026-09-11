"""Smoke tests for tools and the agentic engine. DB / MLflow / LLM-heavy paths are optional."""

from __future__ import annotations

import asyncio

import pytest

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


def test_agentic_no_llm_key_graceful(monkeypatch):
    """No ANTHROPIC_API_KEY/GEMINI_API_KEY - should return a clear message, not raise."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    from src.agents.agentic import run_agentic_turn

    out = asyncio.run(run_agentic_turn("what's the churn status of CUST0001?"))
    assert out["completed_workers"] == []
    assert out["campaign_plan"] is None
    assert "no llm configured" in out["messages"][-1]["content"].lower()


@pytest.mark.integration
def test_agentic_email_action_optional():
    """Needs DB + a Production churn model + an LLM key (via MCP subprocess): asks the
    agent to find the top-risk customer and email them, then checks the email_sender
    tool call actually happened and was forced into dry_run regardless of what the
    agent requested."""
    from src.agents.agentic import run_agentic_turn

    out = asyncio.run(
        run_agentic_turn("Find the single highest churn-risk customer and send them a retention offer email.")
    )
    assert "email_sender" in out["completed_workers"]
    assert out["campaign_plan"]["actions"]
    assert all(a["dry_run"] is True for a in out["campaign_plan"]["actions"])
    assert out["compliance_result"] is not None


@pytest.mark.integration
def test_agentic_qa_optional():
    """Needs DB + an LLM key: asks a data question, expects the agent to call
    customer_qa and surface a structured qa_result."""
    from src.agents.agentic import run_agentic_turn

    out = asyncio.run(run_agentic_turn("How many customers do we have in total?"))
    assert "customer_qa" in out["completed_workers"]
    assert out["qa_result"] is not None
    assert out["qa_result"].get("sql")
