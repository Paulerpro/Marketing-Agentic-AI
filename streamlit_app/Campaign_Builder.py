"""Campaign Manager - configure target segment, review the agent pipeline, preview
AI-generated email copy, and dispatch (PDF mockup p.6)."""

from __future__ import annotations

import streamlit as st

from streamlit_app._api_client import error_banner, get, post, render_sidebar

st.set_page_config(page_title="MarketMind AI - Campaigns", page_icon="✉️", layout="wide")
render_sidebar("Campaign Manager: draft + send retention emails.")

st.title("Campaign Manager")
st.caption("Configure, preview and dispatch retention campaigns")

SEGMENTS = {
    "High risk (score >= 0.6)": (0.6, 1.0),
    "Medium risk (0.4-0.6)": (0.4, 0.6),
    "All at-risk (>= 0.4)": (0.4, 1.0),
}

col_left, col_right = st.columns((1, 1.3))

with col_left:
    st.subheader("Target segment")
    choice = st.radio("Segment", list(SEGMENTS.keys()), label_visibility="collapsed")
    lo, hi = SEGMENTS[choice]
    try:
        count = get("/segments", min_score=lo, max_score=hi, max_customers=5000).get("count", 0)
        st.caption(f"{count} customers in range")
    except Exception as e:
        error_banner(e, "Could not count segment")

    st.subheader("Agent pipeline")
    for step in ["DataAgent — fetch features", "ScoringAgent — filter segment",
                 "CopyAgent — generate email copy (Claude)", "SendAgent — dispatch + log"]:
        st.markdown(f"- {step}")

    if st.button("Preview campaign →", type="primary", use_container_width=True):
        with st.spinner("CopyAgent drafting emails for the highest-risk customers..."):
            try:
                result = post("/chat", {"message": "draft a retention email campaign"})
                st.session_state["last_campaign_plan"] = result.get("campaign_plan")
                st.session_state["last_compliance_result"] = result.get("compliance_result")
            except Exception as e:
                error_banner(e, "Preview failed")

with col_right:
    st.subheader("Email preview")
    plan = st.session_state.get("last_campaign_plan")
    actions = (plan or {}).get("actions") or []

    if not actions:
        st.caption("Click **Preview campaign** to draft emails for the current top high-risk customers.")
    else:
        compliance = st.session_state.get("last_compliance_result")
        if compliance is not None:
            if compliance.get("approved"):
                st.success("Compliance: approved", icon="✅")
            else:
                st.warning(f"Compliance: {compliance.get('reasons')}", icon="⚠️")

        for i, action in enumerate(actions):
            with st.container(border=True):
                st.markdown(f"**{action.get('subject', '(no subject)')}**")
                st.caption(f"To: {action.get('email')}  ·  template: {action.get('template_id')}  ·  score: {action.get('churn_score', 0):.2f}")
                st.text(action.get("body", ""))

        st.divider()
        dry_run = st.toggle("Dry run (log only, don't call SendGrid)", value=True)
        if st.button(f"Send campaign ({len(actions)} emails) →", type="primary", use_container_width=True):
            sent, failed = 0, 0
            progress = st.progress(0.0)
            for i, action in enumerate(actions):
                try:
                    post(
                        "/campaigns/send",
                        {
                            "customer_id": action.get("customer_id"),
                            "email": action.get("email"),
                            "first_name": action.get("first_name") or "there",
                            "churn_score": action.get("churn_score") or 0.0,
                            "campaign_id": plan.get("version"),
                            "dry_run": dry_run,
                        },
                    )
                    sent += 1
                except Exception:
                    failed += 1
                progress.progress((i + 1) / len(actions))
            st.success(f"Sent {sent} email(s), {failed} failed. See Analytics for the log.")
