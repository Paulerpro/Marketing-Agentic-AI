"""Pick an LLM provider based on which API key is actually configured.

Claude is preferred when ANTHROPIC_API_KEY is set (matches the project's primary
design - see README); GEMINI_API_KEY is the free-tier alternative. Neither set ->
returns None, and callers (CopyAgent, Q&A) fall back to non-LLM behavior rather than
erroring - the app stays usable with zero LLM spend.
"""

from __future__ import annotations

import os
from typing import Any

DEFAULT_CLAUDE_MODEL = "claude-opus-5"
# gemini-3.6-flash's free tier caps at 20 requests/day (quota is per-model) - too tight
# for interactive use. gemini-flash-lite-latest has separate, more usable headroom and
# still supports structured output + tool-calling fine. Override via GEMINI_MODEL /
# COPY_AGENT_MODEL / QA_MODEL / AGENT_MODEL env vars if you have paid quota on 3.6.
DEFAULT_GEMINI_MODEL = "gemini-flash-lite-latest"


def active_provider() -> str | None:
    if os.getenv("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.getenv("GEMINI_API_KEY"):
        return "gemini"
    return None


def get_chat_model(*, max_output_tokens: int = 2048, model_env_var: str = "", **kwargs: Any):
    """
    A configured LangChain chat model for whichever provider has a key set, or None.

    model_env_var: name of an env var to check first for a model-name override
    (e.g. "COPY_AGENT_MODEL", "QA_MODEL") before falling back to the provider default.

    Gemini's 3.x models spend part of the output budget on hidden reasoning before
    writing the actual reply - for anything beyond a trivial prompt (e.g. generating
    SQL over a multi-table join) that can burn through even a 2048-token budget before
    a single word of the real answer is written, silently truncating it. Default
    reasoning_effort to "low" to keep that overhead small; pass reasoning_effort=
    "medium"/"high" explicitly for a task that actually needs more deliberation.
    """
    provider = active_provider()

    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        model = os.getenv(model_env_var, DEFAULT_CLAUDE_MODEL) if model_env_var else DEFAULT_CLAUDE_MODEL
        return ChatAnthropic(model=model, max_tokens=max_output_tokens, **kwargs)

    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI

        model = os.getenv(model_env_var, DEFAULT_GEMINI_MODEL) if model_env_var else DEFAULT_GEMINI_MODEL
        kwargs.setdefault("reasoning_effort", "low")
        return ChatGoogleGenerativeAI(
            model=model,
            google_api_key=os.getenv("GEMINI_API_KEY"),
            max_output_tokens=max_output_tokens,
            **kwargs,
        )

    return None


def extract_text(content: Any) -> str:
    """
    Normalize a LangChain AIMessage.content across providers.

    ChatAnthropic's .content is a plain string for a simple text reply; Gemini's
    always returns a list of content-block dicts (even for one text block), e.g.
    [{"type": "text", "text": "...", "extras": {...}}]. Handle both.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            block.get("text", "") for block in content if isinstance(block, dict) and block.get("type") == "text"
        )
    return str(content)
