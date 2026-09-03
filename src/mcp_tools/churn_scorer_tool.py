"""Churn scoring, collapsed to one row per customer for agent/MCP consumption."""

from __future__ import annotations

import logging
from typing import Any

from src.agents.tools.data_tools import build_merged_features_for_ml
from src.agents.tools.ml_tools import score_merged_features

logger = logging.getLogger(__name__)

_OUTPUT_COLUMNS = ["customer_id", "name", "email", "churn_probability", "churn_prediction"]


def score_all_customers(min_score: float = 0.0, max_customers: int = 100) -> list[dict[str, Any]]:
    """
    Score churn risk for every customer with at least one transaction.

    Scoring itself runs on the transaction-level feature frame (matches ml/retrain.py);
    this collapses to one row per customer (most recent transaction) since that's the
    unit agents and UI pages reason about.
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
