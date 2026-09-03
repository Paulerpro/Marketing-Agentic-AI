"""Customer segmentation - filterable table of scored customers (TODO.txt's
"Customer Segmentation View": table of selected customers, churn score, risk band)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_app._api_client import error_banner, get, render_sidebar

st.set_page_config(page_title="MarketMind AI - Segmentation", page_icon="🧩", layout="wide")
render_sidebar("Segmentation: filter customers by churn-score range.")

st.title("Customer Segmentation")
st.caption("Filter customers by churn-score range. Backed by GET /segments.")

min_score, max_score = st.slider(
    "Churn score range", min_value=0.0, max_value=1.0, value=(0.5, 1.0), step=0.05
)
max_rows = st.number_input("Max rows", min_value=10, max_value=2000, value=200, step=10)

try:
    rows = get("/segments", min_score=min_score, max_score=max_score, max_customers=max_rows).get("items", [])
except Exception as e:
    error_banner(e, "Could not load segment")
    rows = []

st.metric("Matching customers", len(rows))

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(
        df.rename(columns={"churn_probability": "churn_score"}),
        use_container_width=True,
        hide_index=True,
        column_config={
            "churn_score": st.column_config.ProgressColumn("churn_score", min_value=0.0, max_value=1.0, format="%.2f"),
        },
    )

    with st.expander("Risk-band breakdown"):
        counts = df["risk_label"].value_counts()
        st.bar_chart(counts)
else:
    st.caption("No customers in this range.")
