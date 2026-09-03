"""LangGraph supervisor wiring (deterministic routing, no LLM in the loop)."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Literal

from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, StateGraph

from src.agents.mcp_client import mcp_tools, parse_tool_result
from src.agents.state import SupervisorState
from src.agents.tools.compliance_tools import validate_campaign_plan
from src.agents.tools.data_tools import summarize_data_sources
from src.utils.email_templates import render_template, select_template

logger = logging.getLogger(__name__)

RouteTarget = Literal["data_analyst", "campaign_planner", "compliance", "qa"]

# Overridable so a cost-conscious deployment can point CopyAgent at a cheaper model
# without touching code.
COPY_AGENT_MODEL = os.getenv("COPY_AGENT_MODEL", "claude-opus-5")


def _user_text(messages: list[Any]) -> str:
    for m in reversed(messages):
        if isinstance(m, HumanMessage):
            c = m.content
            return c if isinstance(c, str) else str(c)
    return ""


def supervisor_node(state: SupervisorState) -> dict[str, Any]:
    """Pick the next worker from keywords and completed_workers (no LLM)."""
    text = _user_text(state["messages"]).lower()
    completed = list(state.get("completed_workers") or [])

    if "demo full" in text or "full pipeline" in text:
        order: list[RouteTarget] = ["data_analyst", "campaign_planner", "compliance"]
        for w in order:
            if w not in completed:
                return {"next": w}
        return {"next": "__end__"}

    if any(
        k in text
        for k in (
            "analyze",
            "data",
            "segment",
            "feature",
            "score churn",
            "churn",
            "inventory",
        )
    ):
        if "data_analyst" not in completed:
            return {"next": "data_analyst"}

    if any(k in text for k in ("campaign", "draft plan", "email blast")):
        if "campaign_planner" not in completed:
            return {"next": "campaign_planner"}

    if any(k in text for k in ("compliance", "approve", "policy")):
        if "compliance" not in completed:
            return {"next": "compliance"}

    if any(k in text for k in ("qa", "ask", "question", "query")):
        if "qa" not in completed:
            return {"next": "qa"}

    return {"next": "__end__"}


def route_after_supervisor(state: SupervisorState) -> str:
    n = state.get("next")
    if n in ("__end__", None):
        return END
    if n in ("data_analyst", "campaign_planner", "compliance", "qa"):
        return n
    return END


async def data_analyst_node(state: SupervisorState) -> dict[str, Any]:
    completed = list(state.get("completed_workers") or [])
    payload: dict[str, Any] = {}
    try:
        payload["data_inventory"] = summarize_data_sources()
        inv = payload["data_inventory"]
        if inv.get("customers", 0) > 0 and inv.get("transactions", 0) > 0:
            async with mcp_tools() as tools:
                raw = await tools["churn_scorer"].ainvoke({"max_customers": 20})
            payload["churn_score_sample"] = parse_tool_result(raw).get("items", [])
        else:
            payload["note"] = "empty_tables_skip_merge_and_scoring"
    except Exception as e:
        logger.exception("data_analyst_node failed")
        payload = {"error": str(e), "error_type": type(e).__name__}

    body = json.dumps({"worker": "data_analyst", "result": payload})
    return {
        "messages": [AIMessage(content=body)],
        "completed_workers": completed + ["data_analyst"],
    }


async def campaign_planner_node(state: SupervisorState) -> dict[str, Any]:
    """CopyAgent: draft retention emails for the current high-risk segment.

    Uses Claude (via ChatAnthropic) to personalize each email, falling back to the
    plain rendered template (src/utils/email_templates.py) if no ANTHROPIC_API_KEY is
    set or the call fails - the graph stays usable with zero LLM spend.
    """
    completed = list(state.get("completed_workers") or [])
    async with mcp_tools() as tools:
        seg_raw = await tools["segmentation"].ainvoke({"min_score": 0.6, "max_score": 1.0})
    high_risk = parse_tool_result(seg_raw).get("items", [])[:5]

    actions = await asyncio.gather(*(_draft_action(c) for c in high_risk))

    plan = {
        "version": "copyagent-v1",
        "segment": {"name": "high_risk", "filters": {"min_score": 0.6}},
        "constraints": {"channel": "email", "max_discount_pct": 30},
        "actions": actions,
        "notes": f"CopyAgent draft for {len(actions)} high-risk customers.",
    }
    body = json.dumps({"worker": "campaign_planner", "campaign_plan": plan})
    return {
        "messages": [AIMessage(content=body)],
        "campaign_plan": plan,
        "completed_workers": completed + ["campaign_planner"],
    }


async def _draft_action(customer: dict[str, Any]) -> dict[str, Any]:
    score = customer.get("churn_probability", 0.0)
    template = select_template(score)
    first_name = (customer.get("name") or "there").split()[0].title()

    subject, body_text = None, None
    try:
        from langchain_anthropic import ChatAnthropic
        from pydantic import BaseModel, Field

        class DraftEmail(BaseModel):
            subject: str = Field(description="Email subject line")
            body: str = Field(description="Email body, 80-150 words")

        llm = ChatAnthropic(model=COPY_AGENT_MODEL, max_tokens=600)
        structured = llm.with_structured_output(DraftEmail)
        prompt = (
            f"Write a retention email for {first_name}, churn risk score {score:.2f} "
            f"(band: {template['score_band']}). Use this template for tone and offer, "
            f"personalize naturally, keep the same offer and CTA intent:\n\n"
            f"Subject: {template['subject']}\nBody:\n{template['body']}"
        )
        draft = await structured.ainvoke(prompt)
        subject, body_text = draft.subject, draft.body
    except Exception as e:
        logger.info("CopyAgent LLM draft unavailable (%s), using rendered template", e)

    if subject is None or body_text is None:
        rendered = render_template(template, {"first_name": first_name})
        subject, body_text = rendered["subject"], rendered["body"]

    return {
        "type": "email",
        "customer_id": customer.get("customer_id"),
        "email": customer.get("email"),
        "first_name": first_name,
        "churn_score": score,
        "template_id": template["template_id"],
        "subject": subject,
        "body": body_text,
    }


def compliance_node(state: SupervisorState) -> dict[str, Any]:
    completed = list(state.get("completed_workers") or [])
    plan = state.get("campaign_plan")
    result = validate_campaign_plan(plan)
    body = json.dumps({"worker": "compliance", "compliance": result})
    return {
        "messages": [AIMessage(content=body)],
        "compliance_result": result,
        "completed_workers": completed + ["compliance"],
    }


async def qa_node(state: SupervisorState) -> dict[str, Any]:
    """PDF Workflow 2 (RouterAgent -> SQLAgent -> AnalysisAgent), via the customer_qa
    MCP tool. Requires ANTHROPIC_API_KEY - returns an error payload without one."""
    completed = list(state.get("completed_workers") or [])
    question = _user_text(state["messages"])
    payload: dict[str, Any]
    try:
        async with mcp_tools() as tools:
            raw = await tools["customer_qa"].ainvoke({"question": question})
        payload = parse_tool_result(raw)
    except Exception as e:
        logger.exception("qa_node failed")
        payload = {"error": str(e), "error_type": type(e).__name__}

    body = json.dumps({"worker": "qa", "result": payload})
    return {
        "messages": [AIMessage(content=body)],
        "qa_result": payload,
        "completed_workers": completed + ["qa"],
    }


def build_graph() -> StateGraph:
    builder = StateGraph(SupervisorState)
    builder.add_node("supervisor", supervisor_node)
    builder.add_node("data_analyst", data_analyst_node)
    builder.add_node("campaign_planner", campaign_planner_node)
    builder.add_node("compliance", compliance_node)
    builder.add_node("qa", qa_node)

    builder.add_edge(START, "supervisor")
    builder.add_conditional_edges(
        "supervisor",
        route_after_supervisor,
        {
            "data_analyst": "data_analyst",
            "campaign_planner": "campaign_planner",
            "compliance": "compliance",
            "qa": "qa",
            END: END,
        },
    )
    builder.add_edge("data_analyst", "supervisor")
    builder.add_edge("campaign_planner", "supervisor")
    builder.add_edge("compliance", "supervisor")
    builder.add_edge("qa", "supervisor")
    return builder


def compile_graph(**compile_kwargs: Any):
    """
    Compile the supervisor graph.

    Pass checkpointer via compile_kwargs when you want multi-turn memory, e.g.
    compile_graph(checkpointer=MemorySaver()).
    """
    return build_graph().compile(**compile_kwargs)
