"""Model Hub - champion vs challenger, promote (human gate), trigger retrain
(PDF mockup p.7 / Workflow 3)."""

from __future__ import annotations

import streamlit as st

from streamlit_app._api_client import error_banner, get, post, render_sidebar

st.set_page_config(page_title="MarketMind AI - Model Hub", page_icon="🧠", layout="wide")
render_sidebar("Model Hub: manage, retrain and promote churn models.")

st.title("Model Hub")
st.caption("Manage, retrain and promote churn prediction models")


def _f1(precision, recall) -> float | None:
    if not precision or not recall or (precision + recall) == 0:
        return None
    return 2 * precision * recall / (precision + recall)


def _render_card(title: str, info: dict | None, badge: str | None = None):
    with st.container(border=True):
        header = title if badge is None else f"{title} — {badge}"
        st.subheader(header)
        if not info:
            st.caption("None registered yet.")
            return None
        f1 = _f1(info.get("precision"), info.get("recall"))
        st.metric("AUC-ROC", f"{info['auc']:.3f}" if info.get("auc") is not None else "n/a")
        st.metric("F1 score", f"{f1:.3f}" if f1 is not None else "n/a")
        st.caption(f"Version {info.get('version')}  ·  Trained {info.get('trained_on', '')[:10]}")
        st.caption(f"Decision at training time: {info.get('promotion_decision')}")
        st.caption("Format: CatBoost, tracked in MLflow. ONNX export is attempted per run but "
                    "CatBoost's ONNX exporter doesn't yet support categorical features, so this "
                    "model isn't served via ONNX Runtime.")
        return info


col1, col2 = st.columns(2)
try:
    champion = get("/model/champion")
except Exception as e:
    error_banner(e, "Could not load champion")
    champion = None
try:
    challenger = get("/model/challenger")
except Exception as e:
    error_banner(e, "Could not load challenger")
    challenger = None

with col1:
    _render_card("Champion", champion, badge="Live" if champion else None)
with col2:
    challenger_info = _render_card("Challenger", challenger, badge="Pending review" if challenger else None)
    if challenger_info:
        if st.button("Promote to Production →", type="primary", use_container_width=True):
            try:
                post("/model/promote", {"version": str(challenger_info["version"])})
                st.success(f"Promoted version {challenger_info['version']} to Production.")
                st.rerun()
            except Exception as e:
                error_banner(e, "Promote failed")

st.divider()
st.subheader("Retraining workflow")
if st.button("Trigger retrain →", type="primary"):
    with st.spinner("Running PSI drift check, and retraining if drift is detected (may take ~30s)..."):
        try:
            result = post("/model/retrain", timeout=180.0)
            status = result.get("status")
            if status == "skipped_no_drift":
                st.info(f"No drift detected - skipped retrain. PSI scores: {result.get('psi_scores')}")
            elif status == "trained":
                st.success(f"Retrained: decision={result.get('decision')}, AUC={result.get('auc'):.3f}, "
                           f"recall={result.get('recall'):.3f}")
                st.rerun()
            elif status == "failed":
                st.error(f"Retrain failed: {result.get('error')} - champion left untouched.")
            else:
                st.warning(f"Retrain returned: {result}")
        except Exception as e:
            error_banner(e, "Retrain call failed")

st.caption(
    "Drift gate: PSI > 0.10 on any of total_price, days_since_last_purchase, recency_days, "
    "avg_purchase_value, purchase_frequency triggers a retrain; otherwise the run is skipped "
    "and logged. Promotion always requires this human click - no auto-promotion."
)
