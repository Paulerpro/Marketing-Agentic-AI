"""Stub compliance checks — extend with policy engine / human review queues."""

from __future__ import annotations

import re
from typing import Any


def validate_campaign_plan(plan: dict[str, Any] | None) -> dict[str, Any]:
    """
    Validate a campaign plan dict. Returns a fixed compliance envelope.

    approved: False if obvious policy violations in stub rules; True otherwise.
    """
    if not plan or not isinstance(plan, dict):
        return {
            "approved": False,
            "severity": "error",
            "reasons": ["missing_or_invalid_plan"],
            "required_actions": ["fix_plan_payload"],
            "redactions": [],
        }

    reasons: list[str] = []
    redactions: list[str] = []

    discount = plan.get("constraints", {}).get("max_discount_pct")
    if isinstance(discount, (int, float)) and discount > 50:
        reasons.append("discount_cap_exceeded")

    text_blob = " ".join(
        str(x)
        for x in (
            plan.get("notes", ""),
            str(plan.get("actions", [])),
        )
    )
    if re.search(r"\b(ssn|social security)\b", text_blob, re.I):
        reasons.append("potential_pii_keywords")
        redactions.append("remove_pii_from_copy")

    approved = len(reasons) == 0
    return {
        "approved": approved,
        "severity": "ok" if approved else "warn",
        "reasons": reasons,
        "required_actions": [] if approved else ["human_review"],
        "redactions": redactions,
    }


_DISCOUNT_PATTERN = re.compile(r"(\d{1,3})\s*%\s*(?:off|discount)", re.I)
_MAX_DISCOUNT_PCT = 50


def validate_email_actions(actions: list[dict[str, Any]] | None) -> dict[str, Any]:
    """
    Same envelope as validate_campaign_plan(), adapted for the agentic path: there's
    no upfront "plan" with declared constraints anymore (the agent just calls
    email_sender directly, however many times, for whatever customers it picked), so
    this checks each already-rendered email's actual subject/body text instead -
    still a hard, code-level step that runs regardless of what the agent decided,
    not something it can skip or talk its way around.
    """
    if not actions:
        return {"approved": True, "severity": "ok", "reasons": [], "required_actions": [], "redactions": []}

    reasons: list[str] = []
    redactions: list[str] = []

    for action in actions:
        text_blob = f"{action.get('subject', '')} {action.get('body', '')}"

        for match in _DISCOUNT_PATTERN.finditer(text_blob):
            if int(match.group(1)) > _MAX_DISCOUNT_PCT:
                reasons.append(f"discount_cap_exceeded:{action.get('customer_id', '?')}")

        if re.search(r"\b(ssn|social security)\b", text_blob, re.I):
            reasons.append(f"potential_pii_keywords:{action.get('customer_id', '?')}")
            redactions.append(f"remove_pii_from_copy:{action.get('customer_id', '?')}")

    approved = len(reasons) == 0
    return {
        "approved": approved,
        "severity": "ok" if approved else "warn",
        "reasons": reasons,
        "required_actions": [] if approved else ["human_review"],
        "redactions": redactions,
    }
