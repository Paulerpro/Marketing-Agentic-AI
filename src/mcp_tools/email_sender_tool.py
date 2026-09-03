"""SendGrid dispatch for retention emails. Falls back to dry_run when no API key is set."""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any

from src.mcp_tools.campaign_logger_tool import log_campaign_event
from src.utils.email_templates import render_template, select_template

logger = logging.getLogger(__name__)


def _sendgrid_dispatch(to_email: str, subject: str, body: str) -> dict[str, Any]:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    api_key = os.getenv("SENDGRID_API_KEY")
    from_email = os.getenv("SENDGRID_FROM_EMAIL", "retention@example.com")

    message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        plain_text_content=body,
    )
    client = SendGridAPIClient(api_key)
    response = client.send(message)
    return {"status_code": response.status_code}


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

    dry_run: defaults to True unless explicitly False AND SENDGRID_API_KEY is set —
    prevents accidental real sends when the key is missing or in demo/portfolio runs.
    """
    template = select_template(churn_score)
    rendered = render_template(template, {"first_name": first_name})
    campaign_id = campaign_id or f"campaign-{uuid.uuid4().hex[:8]}"

    has_key = bool(os.getenv("SENDGRID_API_KEY"))
    effective_dry_run = True if dry_run is None else dry_run
    if not effective_dry_run and not has_key:
        logger.warning("SENDGRID_API_KEY not set — forcing dry_run for %s", customer_id)
        effective_dry_run = True

    dispatch_result: dict[str, Any] = {"simulated": True}
    if not effective_dry_run:
        dispatch_result = _sendgrid_dispatch(email, rendered["subject"], rendered["body"])
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
