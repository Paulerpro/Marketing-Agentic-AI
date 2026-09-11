"""Churn scoring, collapsed to one row per customer for agent/MCP consumption."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.agents.tools.data_tools import build_merged_features_for_ml
from src.agents.tools.ml_tools import score_merged_features
from src.db.config import Base, Session, engine
from src.db.models.churn_scores import ChurnScoreDB

logger = logging.getLogger(__name__)

_OUTPUT_COLUMNS = ["customer_id", "name", "email", "churn_probability", "churn_prediction"]

# Risk bands, shared with segmentation_tool.py (imports these rather than redefining
# them) and used to tag the risk_label column persisted by persist_scores().
LOW_MAX = 0.50
MEDIUM_MAX = 0.70


def risk_label(score: float) -> str:
    if score >= MEDIUM_MAX:
        return "high"
    if score >= LOW_MAX:
        return "medium"
    return "low"


def score_all_customers(min_score: float = 0.0, max_customers: int = 100) -> list[dict[str, Any]]:
    """
    Score churn risk for every customer with at least one transaction.

    Scoring itself runs on the transaction-level feature frame (matches ml/retrain.py);
    this collapses to one row per customer (most recent transaction) since that's the
    unit agents and UI pages reason about. Pure read - no DB writes; see
    score_and_persist_all_customers() for the version that also persists.
    """
    merged = build_merged_features_for_ml()
    if merged.empty:
        return []

    scored = score_merged_features(merged)
    # score_churn_features() slices down to just the model's feature columns before
    # scoring, so identity/date columns don't survive - reattach them by index (both
    # frames share merged's original row index).
    identity_cols = [c for c in ("customer_id", "name", "email", "purchase_date") if c in merged.columns]
    scored = scored.join(merged[identity_cols])

    scored = scored.sort_values("purchase_date").groupby("customer_id", as_index=False).tail(1)
    scored = scored[scored["churn_probability"] >= min_score].sort_values(
        "churn_probability", ascending=False
    )

    cols = [c for c in _OUTPUT_COLUMNS if c in scored.columns]
    return scored[cols].head(max_customers).to_dict(orient="records")


def score_one_customer(customer_id: str) -> dict[str, Any] | None:
    rows = score_all_customers(min_score=0.0, max_customers=10_000)
    for row in rows:
        if row.get("customer_id") == customer_id:
            return row
    return None


def persist_scores(rows: list[dict[str, Any]], model_version: str | None = None) -> int:
    """
    Upsert rows (from score_all_customers) into churn_scores - one row per customer,
    overwritten on every call. This is what makes churn_probability queryable by
    Customer Q&A (src/mcp_tools/qa_sql_tool.py), which only ever sees database tables.
    """
    if not rows:
        return 0

    Base.metadata.create_all(bind=engine, tables=[ChurnScoreDB.__table__])
    run_date = datetime.utcnow()

    session = Session()
    try:
        for row in rows:
            score = row.get("churn_probability", 0.0)
            stmt = pg_insert(ChurnScoreDB).values(
                customer_id=row["customer_id"],
                score=score,
                risk_label=risk_label(score),
                run_date=run_date,
                model_version=model_version,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[ChurnScoreDB.customer_id],
                set_={
                    "score": stmt.excluded.score,
                    "risk_label": stmt.excluded.risk_label,
                    "run_date": stmt.excluded.run_date,
                    "model_version": stmt.excluded.model_version,
                },
            )
            session.execute(stmt)
        session.commit()
        return len(rows)
    finally:
        session.close()


def score_and_persist_all_customers(
    min_score: float = 0.0, max_customers: int = 100, model_version: str | None = None
) -> list[dict[str, Any]]:
    """
    Score everyone and persist every result, then return the top max_customers rows
    matching min_score - used by the churn_scorer MCP tool (an explicit action,
    unlike the plain GET-style score_all_customers).

    Persistence always covers the full customer base regardless of max_customers -
    that only limits what's returned to the caller. Otherwise a low max_customers
    would leave everyone outside the top N with a stale/missing churn_scores row.
    """
    all_rows = score_all_customers(min_score=0.0, max_customers=100_000)
    try:
        persist_scores(all_rows, model_version=model_version)
    except Exception:
        logger.exception("persist_scores failed - returning scores anyway, churn_scores table is stale")
    filtered = [r for r in all_rows if r.get("churn_probability", 0.0) >= min_score]
    return filtered[:max_customers]
