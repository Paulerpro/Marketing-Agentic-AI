"""Shared FastAPI client helpers for every page - keeps httpx/session_state
boilerplate in one place."""

from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st


def base_url() -> str:
    if "api_base" not in st.session_state:
        # API_BASE_URL lets Docker Compose point Streamlit at the "api" service by its
        # Docker DNS name instead of localhost - defaults to local dev otherwise.
        st.session_state.api_base = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
    return st.session_state.api_base.rstrip("/")


def get(path: str, timeout: float = 60.0, **params: Any) -> Any:
    with httpx.Client() as client:
        r = client.get(f"{base_url()}{path}", params=params or None, timeout=timeout)
        r.raise_for_status()
        return r.json()


def post(path: str, json_body: dict[str, Any] | None = None, timeout: float = 300.0) -> Any:
    with httpx.Client() as client:
        r = client.post(f"{base_url()}{path}", json=json_body, timeout=timeout)
        r.raise_for_status()
        return r.json()


def api_health() -> tuple[bool, str]:
    try:
        data = get("/health", timeout=10.0)
        return bool(data.get("ok")), ""
    except Exception as e:
        return False, str(e)


def render_sidebar(active_hint: str = "") -> None:
    with st.sidebar:
        st.markdown("### MarketMind AI")
        st.caption("AI Marketing Platform")
        st.text_input("API base URL", key="api_base", value=base_url())
        ok, detail = api_health()
        if ok:
            st.success("API reachable", icon="✅")
        else:
            st.error(f"API not reachable: {detail}", icon="⚠️")
        if active_hint:
            st.caption(active_hint)


def error_banner(e: Exception, context: str) -> None:
    if isinstance(e, httpx.HTTPStatusError):
        st.error(f"{context}: HTTP {e.response.status_code} - {e.response.text}")
    else:
        st.error(f"{context}: {e}")
