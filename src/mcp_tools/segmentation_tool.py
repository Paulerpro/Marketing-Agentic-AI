"""Bucket scored customers into risk segments (Dashboard tiles, Campaign Manager targeting)."""

from __future__ import annotations

from typing import Any

from src.mcp_tools.churn_scorer_tool import score_all_customers

LOW_MAX = 0.50
MEDIUM_MAX = 0.70


def _risk_label(score: float) -> str:
    if score >= MEDIUM_MAX:
        return "high"
    if score >= LOW_MAX:
        return "medium"
    return "low"


def segment_customers(
    min_score: float = 0.0, max_score: float = 1.0, max_customers: int = 500
) -> list[dict[str, Any]]:
    """Scored customers within [min_score, max_score], each tagged with a risk_label."""
    rows = score_all_customers(min_score=min_score, max_customers=max_customers)
    out = []
    for row in rows:
        score = row.get("churn_probability", 0.0)
        if score > max_score:
            continue
        out.append({**row, "risk_label": _risk_label(score)})
    return out


def summarize_segments() -> dict[str, Any]:
    """Counts + average score per risk band, for Dashboard metric tiles."""
    rows = score_all_customers(min_score=0.0, max_customers=100_000)
    if not rows:
        return {"total": 0, "high": 0, "medium": 0, "low": 0, "avg_score": 0.0}

    scores = [r.get("churn_probability", 0.0) for r in rows]
    labels = [_risk_label(s) for s in scores]
    return {
        "total": len(rows),
        "high": labels.count("high"),
        "medium": labels.count("medium"),
        "low": labels.count("low"),
        "avg_score": round(sum(scores) / len(scores), 4),
    }
