"""Dashboard - landing page. Live churn metrics, at-risk customer table, one-click
'Run churn workflow' button (PDF mockup p.5)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app._api_client import error_banner, get, post, render_sidebar

st.set_page_config(page_title="MarketMind AI - Dashboard", page_icon="📣", layout="wide")
render_sidebar("Dashboard: live churn metrics + at-risk customers.")

st.title("Churn Dashboard")

try:
    summary = get("/segments/summary")
except Exception as e:
    error_banner(e, "Could not load segment summary")
    summary = {"total": 0, "high": 0, "avg_score": 0.0}

try:
    champion = get("/model/champion")
except Exception:
    champion = None

try:
    history = get("/campaigns/history", limit=1000).get("items", [])
except Exception:
    history = []

col1, col2, col3, col4 = st.columns(4)
col1.metric("High-risk customers", summary.get("high", 0))
col2.metric("Avg churn score", f"{summary.get('avg_score', 0):.2f}")
col3.metric("Emails sent (logged)", len(history))
col4.metric("Model AUC (champion)", f"{champion['auc']:.2f}" if champion and champion.get("auc") else "n/a")

st.divider()
st.subheader("Top at-risk customers")

try:
    rows = get("/segments", min_score=0.5, max_customers=15).get("items", [])
except Exception as e:
    error_banner(e, "Could not load at-risk customers")
    rows = []

if rows:
    df = pd.DataFrame(rows)[["customer_id", "name", "email", "churn_probability", "risk_label"]]
    df = df.rename(columns={"churn_probability": "churn_score"})
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "churn_score": st.column_config.ProgressColumn("churn_score", min_value=0.0, max_value=1.0, format="%.2f"),
        },
    )
else:
    st.caption("No at-risk customers found - check the API is reachable and the DB is seeded.")

st.divider()
col_a, col_b = st.columns(2)
with col_a:
    if st.button("Run churn workflow →", type="primary", use_container_width=True):
        with st.spinner("Running DataAgent → ScoringAgent → CopyAgent → SendAgent (compliance)..."):
            try:
                result = post("/chat", {"message": "demo full"})
                st.session_state["last_campaign_plan"] = result.get("campaign_plan")
                st.success(f"Completed: {', '.join(result.get('completed_workers') or [])}")
                st.caption("Open **Campaign Builder** or **Email Review** to see the drafted emails.")
            except Exception as e:
                error_banner(e, "Workflow run failed")
with col_b:
    if st.button("Explain scores →", use_container_width=True):
        st.info(
            "Churn score = CatBoost model probability of churn, trained on recency, "
            "frequency, monetary, and category-affinity features (see Model Hub for "
            "champion/challenger metrics). Risk bands: low < 0.50, medium 0.50-0.69, "
            "high ≥ 0.70."
        )
