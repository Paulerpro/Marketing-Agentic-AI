"""Email Preview & Approval - human-in-the-loop review queue over the last drafted
campaign (TODO.txt vision: draft preview + Approve & Send / Request Revisions)."""

from __future__ import annotations

import streamlit as st

from streamlit_app._api_client import error_banner, post, render_sidebar

st.set_page_config(page_title="MarketMind AI - Email Review", page_icon="📝", layout="wide")
render_sidebar("Email Review: approve or skip each drafted email before sending.")

st.title("Email Preview & Approval")

plan = st.session_state.get("last_campaign_plan")
actions = (plan or {}).get("actions") or []

if not actions:
    st.info("No drafted campaign in this session yet. Run **Campaign Builder → Preview campaign** "
             "or **Dashboard → Run churn workflow** first.")
    st.stop()

if "email_review_status" not in st.session_state:
    st.session_state.email_review_status = {}

st.caption(f"{len(actions)} drafted emails from the last campaign run.")

for action in actions:
    cid = action.get("customer_id")
    status = st.session_state.email_review_status.get(cid, "approved")

    with st.container(border=True):
        col_a, col_b = st.columns((3, 1))
        with col_a:
            st.markdown(f"**{action.get('subject', '(no subject)')}**")
            st.caption(f"To: {action.get('email')} ({action.get('first_name')})  ·  score {action.get('churn_score', 0):.2f}")
            st.text(action.get("body", ""))
        with col_b:
            if st.button("✅ Approve", key=f"approve_{cid}", use_container_width=True,
                         type="primary" if status == "approved" else "secondary"):
                st.session_state.email_review_status[cid] = "approved"
                st.rerun()
            if st.button("⛔ Skip", key=f"skip_{cid}", use_container_width=True,
                         type="primary" if status == "skipped" else "secondary"):
                st.session_state.email_review_status[cid] = "skipped"
                st.rerun()
            st.caption(f"Status: **{status}**")

st.divider()
approved = [a for a in actions if st.session_state.email_review_status.get(a.get("customer_id"), "approved") == "approved"]
dry_run = st.toggle("Dry run (log only, don't call SendGrid)", value=True)

if st.button(f"Send {len(approved)} approved email(s) →", type="primary", disabled=not approved):
    sent, failed = 0, 0
    for action in approved:
        try:
            post(
                "/campaigns/send",
                {
                    "customer_id": action.get("customer_id"),
                    "email": action.get("email"),
                    "first_name": action.get("first_name") or "there",
                    "churn_score": action.get("churn_score") or 0.0,
                    "campaign_id": (plan or {}).get("version"),
                    "dry_run": dry_run,
                },
            )
            sent += 1
        except Exception as e:
            failed += 1
            error_banner(e, f"Failed to send to {action.get('customer_id')}")
    st.success(f"Sent {sent} email(s), {failed} failed.")
