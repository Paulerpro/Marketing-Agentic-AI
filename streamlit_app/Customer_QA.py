"""Customer Intelligence Q&A - powered by the same fully agentic engine as Dashboard/
Campaign Builder (src/agents/agentic.py via /chat), not a narrow SQL-only lookup tool.
It can look up data, give advice, AND act on it (draft/send retention emails - always
simulated, see src/agents/agentic.py's dry_run enforcement) within one conversation,
because the whole thread's history is sent with every turn."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app._api_client import error_banner, post, render_sidebar

st.set_page_config(page_title="MarketMind AI - Customer Q&A", page_icon="💬", layout="wide")
render_sidebar("Customer Q&A: ask, get advice, and have the agent act on it.")

st.title("Customer Intelligence Q&A")
st.caption(
    "Ask about your data, get a recommendation, and have the agent act on it in the "
    "same conversation - e.g. \"fetch Paul Silas's data\", then \"what should we do "
    "about him\", then \"send him the offer\"."
)

if "qa_history" not in st.session_state:
    st.session_state.qa_history = []


def _render_extras(sql: str | None, rows: list | None, actions: list | None, tools_used: list | None) -> None:
    if sql:
        with st.expander("SQL used"):
            st.code(sql, language="sql")
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    if actions:
        with st.expander(f"📧 {len(actions)} email action(s) - always simulated here"):
            for a in actions:
                st.markdown(f"**{a.get('subject', '(no subject)')}** → {a.get('email')}")
                st.caption(a.get("body", "")[:300])
    if tools_used:
        st.caption("Tools used: " + ", ".join(tools_used))


for turn in st.session_state.qa_history:
    with st.chat_message("user"):
        st.write(turn["question"])
    with st.chat_message("assistant"):
        st.write(turn["answer"])
        _render_extras(turn.get("sql"), turn.get("rows"), turn.get("actions"), turn.get("tools_used"))

examples = [
    "Which product category has the highest churn rate?",
    "What's the average total spent for high-risk customers?",
    "Which customers are inactive for 60+ days?",
]
cols = st.columns(len(examples))
example_click = None
for col, ex in zip(cols, examples):
    if col.button(ex, use_container_width=True):
        example_click = ex

question = st.chat_input("Ask about your customers, transactions or products...") or example_click

if question:
    with st.chat_message("user"):
        st.write(question)
    with st.chat_message("assistant"):
        with st.spinner("Agent working - may look up data, reason, and act..."):
            try:
                history = [
                    {"question": t["question"], "answer": t["answer"]} for t in st.session_state.qa_history
                ]
                result = post("/chat", {"message": question, "history": history}, timeout=180.0)

                messages = result.get("messages") or []
                answer = messages[-1]["content"] if messages else "(no response)"
                st.write(answer)

                qa_result = result.get("qa_result") or {}
                sql = qa_result.get("sql")
                rows = qa_result.get("rows")
                actions = (result.get("campaign_plan") or {}).get("actions") or []
                tools_used = result.get("completed_workers") or []
                _render_extras(sql, rows, actions, tools_used)

                st.session_state.qa_history.append(
                    {
                        "question": question,
                        "answer": answer,
                        "sql": sql,
                        "rows": rows,
                        "actions": actions,
                        "tools_used": tools_used,
                    }
                )
            except Exception as e:
                error_banner(e, "Agent request failed (needs ANTHROPIC_API_KEY or GEMINI_API_KEY on the API server)")
