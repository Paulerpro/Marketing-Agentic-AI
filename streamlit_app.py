"""
MarketMind AI - Streamlit multi-page shell (st.Page / st.navigation, Streamlit 1.36+).

Run the API first:
  python -m uvicorn src.api.main:app --reload --port 8000

Then from the repo root:
  streamlit run streamlit_app.py
"""

from __future__ import annotations

import streamlit as st

pages = {
    "Workflows": [
        st.Page("streamlit_app/Home.py", title="Dashboard", icon="📣", default=True),
        st.Page("streamlit_app/Segmentation_View.py", title="Segmentation", icon="🧩"),
        st.Page("streamlit_app/Campaign_Builder.py", title="Campaigns", icon="✉️"),
        st.Page("streamlit_app/Email_Review.py", title="Email Review", icon="📝"),
        st.Page("streamlit_app/Customer_QA.py", title="Customer Q&A", icon="💬"),
    ],
    "Analytics": [
        st.Page("streamlit_app/Analytics.py", title="Analytics", icon="📊"),
    ],
    "ML Ops": [
        st.Page("streamlit_app/Model_Hub.py", title="Model Hub", icon="🧠"),
    ],
    "Config": [
        st.Page("streamlit_app/Settings.py", title="Settings", icon="⚙️"),
    ],
}

nav = st.navigation(pages, position="sidebar")
nav.run()
