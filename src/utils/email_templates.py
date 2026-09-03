"""Retention email templates, selected by churn score band (see PDF plan p.4)."""

from __future__ import annotations

from typing import Any, TypedDict


class EmailTemplate(TypedDict):
    template_id: str
    score_band: str
    subject: str
    body: str


SOFT_PERSONAL: EmailTemplate = {
    "template_id": "soft_personal",
    "score_band": "score < 0.50",
    "subject": "We'd love to keep you, {first_name}",
    "body": (
        "Hi {first_name},\n\n"
        "We noticed it's been a little while since we've seen you, and we genuinely miss "
        "having you around. As one of our Premium members, you've always been part of what "
        "makes our community special.\n\n"
        "Here's something just for you: 25% off your next two months, no strings attached.\n\n"
        "[CTA button] Claim your discount"
    ),
}

VALUE_LED_DIRECT: EmailTemplate = {
    "template_id": "value_led_direct",
    "score_band": "score 0.50-0.69",
    "subject": "Your Premium benefits are still here for you, {first_name}",
    "body": (
        "Hi {first_name},\n\n"
        "We noticed your activity has slowed. As a Premium member, you have access to "
        "{key_feature_1}, {key_feature_2}, and priority support.\n\n"
        "We'd like to offer you a free 60-day trial of {new_feature} — Premium only.\n\n"
        "[CTA button] Activate your free trial"
    ),
}

REENGAGEMENT_WITH_STAKES: EmailTemplate = {
    "template_id": "reengagement_with_stakes",
    "score_band": "score >= 0.70",
    "subject": "Before you go, {first_name} — a note from us",
    "body": (
        "Hi {first_name},\n\n"
        "We'll keep this short. We'd like to offer you: 30% off your next 3 months, a "
        "dedicated onboarding call, and priority support for 6 months.\n\n"
        "This offer expires in 7 days.\n\n"
        "[CTA button] Keep my Premium membership"
    ),
}


def select_template(churn_score: float) -> EmailTemplate:
    if churn_score >= 0.70:
        return REENGAGEMENT_WITH_STAKES
    if churn_score >= 0.50:
        return VALUE_LED_DIRECT
    return SOFT_PERSONAL


def render_template(template: EmailTemplate, fields: dict[str, Any]) -> dict[str, str]:
    """Fill in a template's placeholders, leaving unresolved ones untouched."""
    defaults = {
        "first_name": "there",
        "key_feature_1": "advanced analytics",
        "key_feature_2": "priority support",
        "new_feature": "our newest feature",
    }
    merged = {**defaults, **fields}
    return {
        "subject": template["subject"].format(**merged),
        "body": template["body"].format(**merged),
    }
