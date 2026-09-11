from datetime import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, String

from src.db.config import Base


class ChurnScoreDB(Base):
    """
    One row per customer, holding their most recent model prediction (overwritten on
    each scoring run - see ml/model_loader.py for which run this came from via
    model_version). This is what makes the live churn_probability queryable by
    Customer Q&A (src/mcp_tools/qa_sql_tool.py) instead of only the static historical
    `churn` label on clean_customers, which is a different fact entirely.
    """
    __tablename__ = "churn_scores"

    customer_id = Column(String, ForeignKey("clean_customers.customer_id"), primary_key=True)
    score = Column(Float, nullable=False)
    risk_label = Column(String, nullable=False)
    run_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    model_version = Column(String)

    def __repr__(self):
        return f"<ChurnScore(customer='{self.customer_id}', score={self.score}, label='{self.risk_label}')>"
