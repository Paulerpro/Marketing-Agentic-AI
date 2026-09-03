"""Customer Intelligence Q&A - natural-language queries over customer, transaction
and product data (PDF Workflow 2 / mockup p.6). Needs ANTHROPIC_API_KEY on the API
server; without one, questions return a clear error instead of a fake answer."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app._api_client import error_banner, post, render_sidebar

st.set_page_config(page_title="MarketMind AI - Customer Q&A", page_icon="💬", layout="wide")
render_sidebar("Customer Q&A: ask questions about customers, products, transactions.")

st.title("Customer Intelligence Q&A")
st.caption("Ask natural-language questions about your customers, products and transactions")

if "qa_history" not in st.session_state:
    st.session_state.qa_history = []

for turn in st.session_state.qa_history:
    with st.chat_message("user"):
        st.write(turn["question"])
    with st.chat_message("assistant"):
        st.write(turn["narrative"])
        with st.expander("SQL used"):
            st.code(turn["sql"], language="sql")
        if turn.get("rows"):
            st.dataframe(pd.DataFrame(turn["rows"]), use_container_width=True, hide_index=True)

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
        with st.spinner("SQLAgent generating query, AnalysisAgent summarizing..."):
            try:
                result = post("/qa", {"question": question}, timeout=120.0)
                st.write(result.get("narrative", ""))
                with st.expander("SQL used"):
                    st.code(result.get("sql", ""), language="sql")
                if result.get("rows"):
                    st.dataframe(pd.DataFrame(result["rows"]), use_container_width=True, hide_index=True)
                st.session_state.qa_history.append(
                    {"question": question, "narrative": result.get("narrative", ""),
                     "sql": result.get("sql", ""), "rows": result.get("rows")}
                )
            except Exception as e:
                error_banner(e, "Q&A failed (needs ANTHROPIC_API_KEY on the API server)")
