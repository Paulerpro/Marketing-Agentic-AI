"""Settings - effective backend thresholds (read-only, from GET /config) plus
session-local UI overrides other pages can read for their own default filters.

Session-local values do NOT change the deployed model, the segmentation bands used
by /segments, or the drift gate - those are code-level constants (ml/config.py,
ml/drift.py, src/mcp_tools/segmentation_tool.py). Changing them for real means
editing those files and redeploying; this page is for visibility + UI convenience,
not remote config.
"""

from __future__ import annotations

import streamlit as st

from streamlit_app._api_client import error_banner, get, render_sidebar

st.set_page_config(page_title="MarketMind AI - Settings", page_icon="⚙️", layout="wide")
render_sidebar("Settings: effective thresholds + session-local UI defaults.")

st.title("Settings")

try:
    cfg = get("/config")
except Exception as e:
    error_banner(e, "Could not load config")
    cfg = {}

st.subheader("Effective backend thresholds")
st.caption("Read-only - set in ml/config.py, ml/drift.py, src/mcp_tools/segmentation_tool.py.")
if cfg:
    c1, c2, c3 = st.columns(3)
    c1.metric("Risk band: medium starts at", cfg["risk_bands"]["low_max"])
    c1.metric("Risk band: high starts at", cfg["risk_bands"]["medium_max"])
    c2.metric("PSI drift threshold", cfg["psi_drift_threshold"])
    c2.metric("Training window (days)", cfg["training_window_days"])
    c3.metric("Min AUC to promote", cfg["min_auc"])
    c3.metric("Min recall to promote", cfg["min_recall"])

st.divider()
st.subheader("Session-local UI defaults")
st.caption("Adjusts only what this browser session sees on the Dashboard / Segmentation pages by "
           "default - not a remote config write.")

st.session_state.setdefault("ui_default_min_score", 0.5)
st.slider(
    "Default 'at-risk' cutoff used on the Dashboard",
    min_value=0.0, max_value=1.0, step=0.05, key="ui_default_min_score",
)
st.caption(f"Currently: {st.session_state.ui_default_min_score}")
