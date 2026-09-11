"""
Fully agentic replacement for the old deterministic supervisor graph (src/agents/graph.py,
retired - see git history).

One LangChain agent (langchain.agents.create_agent) with every MCP tool bound to it -
the model decides which tools to call, in what order, and when it's done, instead of
a hardcoded routing table (the old supervisor_node's keyword matching).

Two things stay outside the agent's discretion by construction, not just a prompt
instruction:
  1. Real email sends - the email_sender tool is wrapped so dry_run is force-set to
     True no matter what the agent requests. Nothing in this module lets the agent
     actually deliver an email.
  2. Model retraining/promotion - never exposed as a tool at all. Those stay FastAPI/
     Streamlit-only actions behind a human's click in the Model Hub page.
  3. Compliance - checked deterministically on every drafted email after the agent
     finishes (src/agents/tools/compliance_tools.validate_email_actions), not left to
     the agent to remember to do.
"""

from __future__ import annotations

import logging
from typing import Any

from langchain.agents import create_agent
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import BaseTool, StructuredTool

from src.agents.mcp_client import mcp_tools, parse_tool_result
from src.agents.tools.compliance_tools import validate_email_actions
from src.utils.llm_provider import active_provider, extract_text, get_chat_model

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are MarketMind AI's marketing operations assistant. You have \
tools to score churn risk, segment customers, recommend products, draft/send \
retention emails, review campaign history, and answer natural-language questions \
about customers, products and transactions.

Figure out which tools you need yourself and call them directly - don't ask the user \
to run something manually that you could do with a tool. Chain multiple tool calls \
when a request needs it (e.g. find a customer, then act on them).

Every email send in this environment is simulated (dry_run), regardless of what you \
request - this is enforced by the system, not something you control. Don't apologize \
for it or mention it as a limitation unless asked; just proceed and report what the \
simulated send would have done.

Be concise. State concrete numbers you found, not vague summaries."""


def _force_dry_run(email_tool: BaseTool) -> BaseTool:
    """Wrap the email_sender MCP tool so dry_run is always True, no matter what the
    agent passes - the one hard, code-enforced safety boundary in this module."""

    async def _safe_send(**kwargs: Any) -> Any:
        kwargs["dry_run"] = True
        return await email_tool.ainvoke(kwargs)

    return StructuredTool.from_function(
        coroutine=_safe_send,
        name=email_tool.name,
        description=(email_tool.description or "") + " (Always simulated in this environment.)",
        args_schema=email_tool.args_schema,
    )


def _serialize_messages(messages: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for m in messages:
        if isinstance(m, HumanMessage):
            role = "human"
        elif isinstance(m, ToolMessage):
            role = "tool"
        elif isinstance(m, AIMessage):
            role = "assistant"
        else:
            role = getattr(m, "type", m.__class__.__name__)
        content = extract_text(m.content)
        entry: dict[str, Any] = {"role": role, "content": content}
        if isinstance(m, ToolMessage):
            entry["tool_name"] = getattr(m, "name", None)
        tool_calls = getattr(m, "tool_calls", None)
        if tool_calls:
            entry["tool_calls"] = [{"name": tc["name"], "args": tc["args"]} for tc in tool_calls]
        out.append(entry)
    return out


def _log_new_message(m: Any) -> None:
    """One readable log line per step, so the agent's tool-choosing is visible live in
    the server logs while it runs - not just in the final response."""
    if isinstance(m, AIMessage):
        tool_calls = getattr(m, "tool_calls", None)
        if tool_calls:
            for tc in tool_calls:
                logger.info("agent -> tool call: %s(%s)", tc["name"], tc["args"])
            return
        text = extract_text(m.content).strip()
        if text:
            logger.info("agent -> final response: %s", text[:300])
    elif isinstance(m, ToolMessage):
        text = extract_text(m.content) if isinstance(m.content, list) else str(m.content)
        logger.info("tool result <- %s: %s", getattr(m, "name", "?"), text[:300])


def _extract_results(messages: list[Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Pull structured campaign_actions (from email_sender calls) and qa_result (from
    customer_qa calls) out of the raw tool-call transcript, for UI compatibility."""
    campaign_actions: list[dict[str, Any]] = []
    qa_result: dict[str, Any] | None = None

    for m in messages:
        if not isinstance(m, ToolMessage):
            continue
        try:
            payload = parse_tool_result(m.content)
        except Exception:
            continue
        name = getattr(m, "name", None)
        if name == "email_sender" and isinstance(payload, dict):
            campaign_actions.append(payload)
        elif name == "customer_qa" and isinstance(payload, dict):
            qa_result = payload

    return campaign_actions, qa_result


def _build_initial_messages(message: str, history: list[dict[str, Any]] | None) -> list[Any]:
    """Replay prior turns as real Human/AI message pairs so the model can resolve
    "him"/"it"/"that customer" and continue an action across turns (e.g. "fetch his
    data" -> "send him the offer"). Only each turn's question + final answer are
    replayed, not the intermediate tool-call/tool-result noise - that keeps context
    compact and avoids orphaned tool_call_ids the API would otherwise reject."""
    msgs: list[Any] = []
    for turn in (history or [])[-8:]:
        question = turn.get("question", "")
        answer = turn.get("answer", "")
        if question:
            msgs.append(HumanMessage(content=question))
        if answer:
            msgs.append(AIMessage(content=answer))
    msgs.append(HumanMessage(content=message))
    return msgs


async def run_agentic_turn(message: str, history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """
    Run one user request through the fully agentic loop.

    history: prior turns as [{"question": ..., "answer": ...}, ...], oldest first -
    without it every call is a fresh conversation with no memory of "him"/"that
    customer"/a previous action, which makes natural follow-ups impossible.

    Response is shaped close to the old graph's /chat contract (messages,
    completed_workers/tools_used, campaign_plan, compliance_result, qa_result) so the
    existing Streamlit pages keep working without a rewrite, even though the engine
    underneath is now autonomous tool-calling instead of a fixed pipeline.
    """
    provider = active_provider()
    logger.info("agent run started | provider=%s | request=%r | history_turns=%d", provider, message, len(history or []))

    llm = get_chat_model(max_output_tokens=4096, model_env_var="AGENT_MODEL")
    if llm is None:
        logger.warning("agent run aborted | no LLM configured (ANTHROPIC_API_KEY / GEMINI_API_KEY unset)")
        return {
            "messages": [
                {"role": "assistant", "content": "No LLM configured - set ANTHROPIC_API_KEY or GEMINI_API_KEY."}
            ],
            "completed_workers": [],
            "campaign_plan": None,
            "compliance_result": None,
            "qa_result": None,
        }

    initial_messages = _build_initial_messages(message, history)

    async with mcp_tools() as tools:
        tool_list = [_force_dry_run(t) if name == "email_sender" else t for name, t in tools.items()]
        agent = create_agent(llm, tool_list, system_prompt=SYSTEM_PROMPT)

        final_state: dict[str, Any] = {"messages": initial_messages}
        logged = len(initial_messages) - 1  # skip re-logging the replayed history + new human message
        async for state in agent.astream({"messages": initial_messages}, stream_mode="values"):
            final_state = state
            new_messages = state["messages"][logged:]
            for m in new_messages:
                _log_new_message(m)
            logged = len(state["messages"])

    # Only this turn's new activity - the replayed history was for the agent's own
    # reasoning, not something the caller should get back duplicated on every turn
    # (the caller already has it, that's where it came from).
    new_turn_start = len(initial_messages) - 1
    messages = final_state["messages"][new_turn_start:]
    campaign_actions, qa_result = _extract_results(messages)
    tools_used = [tc["name"] for m in messages for tc in (getattr(m, "tool_calls", None) or [])]

    compliance_result = validate_email_actions(campaign_actions) if campaign_actions else None
    campaign_plan = (
        {"version": "agentic-v1", "actions": campaign_actions} if campaign_actions else None
    )

    logger.info(
        "agent run finished | tools_used=%s | emails_drafted=%d | compliance_approved=%s",
        tools_used,
        len(campaign_actions),
        compliance_result.get("approved") if compliance_result else None,
    )

    return {
        "messages": _serialize_messages(messages),
        "completed_workers": tools_used,
        "campaign_plan": campaign_plan,
        "compliance_result": compliance_result,
        "qa_result": qa_result,
    }
