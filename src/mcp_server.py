"""
MCP server exposing MarketMind AI's tools (churn scoring, segmentation, product
matching, email dispatch, campaign history, customer Q&A).

Two consumers:
1. The LangGraph agent (src/agents/mcp_client.py) - spawns this over stdio and loads
   the tools as LangChain tools via langchain-mcp-adapters.
2. Claude Desktop / Claude Code directly, via the repo-root .mcp.json (stdio transport)
   - lets you ask e.g. "what's our current high-risk segment?" against this platform's
     own data without going through the Streamlit UI.

Run directly for local testing:
    python -m src.mcp_server
"""

from __future__ import annotations

import logging
from typing import Any

from mcp.server.fastmcp import FastMCP

from src.mcp_tools.campaign_logger_tool import get_campaign_history
from src.mcp_tools.churn_scorer_tool import score_all_customers, score_one_customer
from src.mcp_tools.email_sender_tool import send_retention_email
from src.mcp_tools.product_match_tool import match_products_for_customer
from src.mcp_tools.qa_sql_tool import answer_customer_question
from src.mcp_tools.segmentation_tool import segment_customers, summarize_segments

logging.basicConfig(level=logging.INFO)

mcp = FastMCP("marketmind-ai")


def _items(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap a list result in an object - MCP structured content must be an object,
    and FastMCP otherwise emits one text block per list item, which is ambiguous
    with a single-object result on the client side. Always {'items': [...], 'count': N}."""
    return {"items": rows, "count": len(rows)}


@mcp.tool()
def churn_scorer(min_score: float = 0.0, max_customers: int = 100) -> dict[str, Any]:
    """Score churn risk for customers, highest risk first."""
    return _items(score_all_customers(min_score=min_score, max_customers=max_customers))


@mcp.tool()
def churn_scorer_for_customer(customer_id: str) -> dict[str, Any] | None:
    """Look up the current churn score for a single customer_id."""
    return score_one_customer(customer_id)


@mcp.tool()
def segmentation(min_score: float = 0.0, max_score: float = 1.0) -> dict[str, Any]:
    """List customers within a churn-score range, tagged with a low/medium/high risk_label."""
    return _items(segment_customers(min_score=min_score, max_score=max_score))


@mcp.tool()
def segmentation_summary() -> dict[str, Any]:
    """Counts and average churn score per risk band (Dashboard metric tiles)."""
    return summarize_segments()


@mcp.tool()
def product_match(customer_id: str, top_n: int = 3) -> dict[str, Any]:
    """Recommend top-N products for a customer based on their most-purchased category."""
    return _items(match_products_for_customer(customer_id, top_n=top_n))


@mcp.tool()
def email_sender(
    customer_id: str,
    email: str,
    first_name: str,
    churn_score: float,
    campaign_id: str | None = None,
    dry_run: bool | None = None,
) -> dict[str, Any]:
    """
    Send a retention email chosen by churn-score band (soft / value-led / stakes).
    Defaults to dry_run unless SENDGRID_API_KEY is set and dry_run=False is passed
    explicitly - safe to call without a live send.
    """
    return send_retention_email(
        customer_id=customer_id,
        email=email,
        first_name=first_name,
        churn_score=churn_score,
        campaign_id=campaign_id,
        dry_run=dry_run,
    )


@mcp.tool()
def campaign_history(limit: int = 50) -> dict[str, Any]:
    """Most recent campaign_log rows, newest first (Analytics page)."""
    return _items(get_campaign_history(limit=limit))


@mcp.tool()
def customer_qa(question: str) -> dict[str, Any]:
    """
    Answer a natural-language question about customers, products, or transactions.
    Generates read-only SQL, runs it against the 3 core tables, and narrates the result.
    """
    return answer_customer_question(question)


if __name__ == "__main__":
    mcp.run(transport="stdio")
