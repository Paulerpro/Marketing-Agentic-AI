from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String

from src.db.config import Base


class CampaignLogDB(Base):
    """
    Derived table written by the email-sending tool after each dispatch.
    Feeds campaign history / attribution analytics (Analytics page).
    """
    __tablename__ = "campaign_log"

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String, ForeignKey("clean_customers.customer_id"), nullable=False)
    campaign_id = Column(String, nullable=False, index=True)
    segment = Column(String)
    churn_score = Column(Float)
    template = Column(String)
    subject = Column(String)
    dry_run = Column(Boolean, nullable=False, default=True)
    sent_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    opened = Column(Boolean, default=False)
    clicked = Column(Boolean, default=False)

    def __repr__(self):
        return f"<CampaignLog(id={self.log_id}, customer='{self.customer_id}', campaign='{self.campaign_id}')>"
