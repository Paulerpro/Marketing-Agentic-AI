"""Read/write access to the campaign_log table (dispatch history + attribution)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from src.db.config import Base, Session, engine
from src.db.models.campaign_log import CampaignLogDB

logger = logging.getLogger(__name__)

_TABLE_READY = False


def ensure_campaign_log_table() -> None:
    """Create the campaign_log table if it doesn't exist yet (idempotent)."""
    global _TABLE_READY
    if _TABLE_READY:
        return
    Base.metadata.create_all(bind=engine, tables=[CampaignLogDB.__table__])
    _TABLE_READY = True


def log_campaign_event(
    customer_id: str,
    campaign_id: str,
    segment: str | None = None,
    churn_score: float | None = None,
    template: str | None = None,
    subject: str | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Write one row to campaign_log after an email send attempt."""
    ensure_campaign_log_table()
    session = Session()
    try:
        row = CampaignLogDB(
            customer_id=customer_id,
            campaign_id=campaign_id,
            segment=segment,
            churn_score=churn_score,
            template=template,
            subject=subject,
            dry_run=dry_run,
            sent_at=datetime.utcnow(),
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return {
            "log_id": row.log_id,
            "customer_id": row.customer_id,
            "campaign_id": row.campaign_id,
            "dry_run": row.dry_run,
            "sent_at": row.sent_at.isoformat(),
        }
    finally:
        session.close()


def get_campaign_history(limit: int = 50) -> list[dict[str, Any]]:
    """Most recent campaign_log rows, newest first (Analytics page)."""
    ensure_campaign_log_table()
    session = Session()
    try:
        rows = (
            session.query(CampaignLogDB)
            .order_by(CampaignLogDB.sent_at.desc())
            .limit(limit)
            .all()
        )
        return [
            {
                "log_id": r.log_id,
                "customer_id": r.customer_id,
                "campaign_id": r.campaign_id,
                "segment": r.segment,
                "churn_score": r.churn_score,
                "template": r.template,
                "subject": r.subject,
                "dry_run": r.dry_run,
                "sent_at": r.sent_at.isoformat() if r.sent_at else None,
                "opened": r.opened,
                "clicked": r.clicked,
            }
            for r in rows
        ]
    finally:
        session.close()
