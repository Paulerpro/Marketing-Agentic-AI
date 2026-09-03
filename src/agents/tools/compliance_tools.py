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
