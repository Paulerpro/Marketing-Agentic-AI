"""Churn scoring helpers built on ml/score_churn.py."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from ml.config import CHURN_SCORING_FEATURE_COLUMNS
from ml.score_churn import score_churn

logger = logging.getLogger(__name__)


def score_churn_features(features: pd.DataFrame) -> pd.DataFrame:
    """
    Run production churn model on a frame that already contains CHURN_SCORING_FEATURE_COLUMNS.
    """
    missing = set(CHURN_SCORING_FEATURE_COLUMNS) - set(features.columns)
    if missing:
        raise ValueError(f"Missing feature columns for churn model: {sorted(missing)}")
    subset = features[CHURN_SCORING_FEATURE_COLUMNS].copy()
    return score_churn(subset)


def score_merged_features(merged: pd.DataFrame) -> pd.DataFrame:
    """Score transaction-level merged features (same layout as training pipeline)."""
    return score_churn_features(merged)


def churn_scores_to_records(df: pd.DataFrame, max_rows: int = 50) -> list[dict[str, Any]]:
    """Trim wide model output for LLM / API payloads."""
    cols = ["customer_id"] if "customer_id" in df.columns else []
    cols += [
        c
        for c in ("churn_probability", "churn_prediction")
        if c in df.columns
    ]
    if not cols:
        return []
    out = df[cols].head(max_rows)
    return out.to_dict(orient="records")
