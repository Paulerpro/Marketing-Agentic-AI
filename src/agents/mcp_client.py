"""Spawns src/mcp_server.py over stdio and exposes its tools as LangChain tools.

Used by the LangGraph nodes so agent orchestration (graph.py) is decoupled from tool
execution (mcp_server.py / mcp_tools/*) - the same tools are also reachable directly
from Claude Desktop / Claude Code via the repo-root .mcp.json.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from langchain_core.tools import BaseTool
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools as _load_mcp_tools_for_session

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SERVER_NAME = "marketmind-ai"

_CONNECTIONS = {
    _SERVER_NAME: {
        "transport": "stdio",
        "command": sys.executable,
        "args": ["-m", "src.mcp_server"],
        "cwd": str(_REPO_ROOT),
        "env": dict(os.environ),
    }
}

_client: MultiServerMCPClient | None = None


def get_mcp_client() -> MultiServerMCPClient:
    global _client
    if _client is None:
        _client = MultiServerMCPClient(_CONNECTIONS)
    return _client


@asynccontextmanager
async def mcp_tools() -> AsyncIterator[dict[str, BaseTool]]:
    """
    One MCP session (one subprocess spawn) shared by every tool call made inside
    the `async with` block - list_tools + each call_tool go over the same connection
    instead of client.get_tools()'s per-call reconnect (each spawn re-imports pandas/
    mlflow/catboost, so that adds up fast across a multi-tool node).

    Must be entered and exited within the same asyncio task (anyio's cancel-scope
    rule for the underlying stdio transport) - i.e. open it at the top of one node
    function and make all of that node's tool calls inside the block. Don't stash
    the yielded tools and call them after the block exits, and don't share one
    instance across concurrent tasks (e.g. separate HTTP requests).
    """
    client = get_mcp_client()
    async with client.session(_SERVER_NAME) as session:
        tools = await _load_mcp_tools_for_session(session)
        yield {t.name: t for t in tools}


def parse_tool_result(result: Any) -> Any:
    """MCP tool calls return a list of content blocks (usually one text block with
    JSON) via the LangChain adapter; unwrap it back to plain Python data."""
    if isinstance(result, str):
        return json.loads(result)
    if isinstance(result, list) and result and isinstance(result[0], dict) and "text" in result[0]:
        return json.loads(result[0]["text"])
    return result
