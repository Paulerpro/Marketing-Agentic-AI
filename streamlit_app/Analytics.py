"""Campaign History & Analytics - past campaigns, dispatch log, open/click
attribution (PDF campaign_log table; open/click tracking needs a SendGrid webhook,
not yet wired - see README)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app._api_client import error_banner, get, render_sidebar

st.set_page_config(page_title="MarketMind AI - Analytics", page_icon="📊", layout="wide")
render_sidebar("Analytics: campaign send log and attribution.")

st.title("Campaign History & Analytics")

limit = st.slider("Rows to show", min_value=10, max_value=500, value=100, step=10)

try:
    rows = get("/campaigns/history", limit=limit).get("items", [])
except Exception as e:
    error_banner(e, "Could not load campaign history")
    rows = []

if not rows:
    st.info("No campaigns sent yet. Send one from **Campaign Builder** or **Email Review**.")
    st.stop()

df = pd.DataFrame(rows)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total logged", len(df))
col2.metric("Live sends", int((~df["dry_run"]).sum()))
col3.metric("Opened", int(df["opened"].sum()))
col4.metric("Clicked", int(df["clicked"].sum()))

st.caption("Opened/clicked columns are placeholders until a SendGrid event webhook is wired to update them.")

st.divider()
st.subheader("By template")
st.bar_chart(df["template"].value_counts())

st.subheader("Send log")
st.dataframe(
    df[["sent_at", "customer_id", "campaign_id", "template", "subject", "churn_score", "dry_run", "opened", "clicked"]],
    use_container_width=True,
    hide_index=True,
)
