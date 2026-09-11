"""Mailgun dispatch for retention emails. Falls back to dry_run when no API key is set.

Note on Mailgun sandbox domains (sandboxXXXX.mailgun.org, the free tier used during
dev): they can only send to "Authorized Recipients" you've added and verified in the
Mailgun dashboard (Sending > Domain settings > Authorized Recipients) - sending to any
other address fails with a 400, not a delivery failure. That's a Mailgun sandbox
restriction, not a bug here.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from src.mcp_tools.campaign_logger_tool import log_campaign_event
from src.utils.email_templates import render_template, select_template

logger = logging.getLogger(__name__)


def _mailgun_dispatch(to_email: str, subject: str, body: str) -> dict[str, Any]:
    import httpx

    api_key = os.getenv("MAILGUN_API_KEY")
    domain = os.getenv("MAILGUN_DOMAIN") or os.getenv("SANDBOX_DOMAIN")
    base_url = (os.getenv("BASE_URL") or "https://api.mailgun.net").rstrip("/")
    from_email = os.getenv("MAILGUN_FROM_EMAIL", f"MarketMind AI <mailgun@{domain}>")

    response = httpx.post(
        f"{base_url}/v3/{domain}/messages",
        auth=("api", api_key),
        data={"from": from_email, "to": to_email, "subject": subject, "text": body},
        timeout=30.0,
    )
    response.raise_for_status()
    payload = response.json()
    return {"status_code": response.status_code, "mailgun_id": payload.get("id"), "message": payload.get("message")}


def send_retention_email(
    customer_id: str,
    email: str,
    first_name: str,
    churn_score: float,
    campaign_id: str | None = None,
    dry_run: bool | None = None,
) -> dict[str, Any]:
    """
    Send (or simulate) one retention email chosen by churn score band.

    dry_run: defaults to True unless explicitly False AND MAILGUN_API_KEY is set -
    prevents accidental real sends when the key is missing or in demo/portfolio runs.
    """
    template = select_template(churn_score)
    rendered = render_template(template, {"first_name": first_name})
    campaign_id = campaign_id or f"campaign-{uuid.uuid4().hex[:8]}"

    has_key = bool(os.getenv("MAILGUN_API_KEY")) and bool(
        os.getenv("MAILGUN_DOMAIN") or os.getenv("SANDBOX_DOMAIN")
    )
    effective_dry_run = True if dry_run is None else dry_run
    if not effective_dry_run and not has_key:
        logger.warning("MAILGUN_API_KEY/domain not set - forcing dry_run for %s", customer_id)
        effective_dry_run = True

    dispatch_result: dict[str, Any] = {"simulated": True}
    if not effective_dry_run:
        dispatch_result = _mailgun_dispatch(email, rendered["subject"], rendered["body"])
        dispatch_result["simulated"] = False

    log_row = log_campaign_event(
        customer_id=customer_id,
        campaign_id=campaign_id,
        churn_score=churn_score,
        template=template["template_id"],
        subject=rendered["subject"],
        dry_run=effective_dry_run,
    )

    return {
        "customer_id": customer_id,
        "campaign_id": campaign_id,
        "template_id": template["template_id"],
        "subject": rendered["subject"],
        "body": rendered["body"],
        "dry_run": effective_dry_run,
        "dispatch": dispatch_result,
        "log": log_row,
    }
