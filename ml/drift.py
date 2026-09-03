"""PSI-based drift gate: only retrain when the incoming feature distribution has
moved meaningfully vs. the baseline captured by the last training run.

Baseline is persisted as an MLflow artifact (feature_baseline.json) on every
training run (per the PDF hardening notes: "log all runs, even failed/skipped
ones"), so the next run can pull the most recent one and compare.
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient

# Representative numeric features to monitor (PDF: "PSI on top-5 features").
DRIFT_MONITORED_COLUMNS = [
    "total_price",
    "days_since_last_purchase",
    "recency_days",
    "avg_purchase_value",
    "purchase_frequency",
]

PSI_DRIFT_THRESHOLD = 0.10
_BUCKETS = 10


def _quantile_edges(values: pd.Series, buckets: int = _BUCKETS) -> list[float]:
    edges = np.unique(np.quantile(values.dropna(), np.linspace(0, 1, buckets + 1)))
    if len(edges) < 3:
        # Degenerate distribution (near-constant column) - widen artificially so
        # np.digitize below still produces at least one bin.
        edges = np.array([values.min() - 1, values.max() + 1])
    return edges.tolist()


def _bucket_proportions(values: pd.Series, edges: list[float]) -> list[float]:
    clean = values.dropna()
    if clean.empty:
        return [0.0] * (len(edges) - 1)
    bucket_idx = np.digitize(clean, edges[1:-1], right=True)
    counts = np.bincount(bucket_idx, minlength=len(edges) - 1)
    return (counts / counts.sum()).tolist()


def build_baseline(df: pd.DataFrame, columns: list[str] = DRIFT_MONITORED_COLUMNS) -> dict[str, Any]:
    """Snapshot each monitored column's quantile edges + bucket proportions."""
    baseline: dict[str, Any] = {}
    for col in columns:
        if col not in df.columns:
            continue
        edges = _quantile_edges(df[col])
        baseline[col] = {"edges": edges, "proportions": _bucket_proportions(df[col], edges)}
    return baseline


def _psi(expected_proportions: list[float], actual_proportions: list[float]) -> float:
    eps = 1e-6
    total = 0.0
    for e, a in zip(expected_proportions, actual_proportions):
        e, a = max(e, eps), max(a, eps)
        total += (a - e) * np.log(a / e)
    return float(total)


def compute_drift(df: pd.DataFrame, baseline: dict[str, Any]) -> dict[str, float]:
    """PSI per monitored column that has a baseline entry."""
    scores: dict[str, float] = {}
    for col, base in baseline.items():
        if col not in df.columns:
            continue
        actual_proportions = _bucket_proportions(df[col], base["edges"])
        scores[col] = _psi(base["proportions"], actual_proportions)
    return scores


def load_last_baseline(experiment_name: str) -> dict[str, Any] | None:
    """Most recent training run's feature_baseline.json artifact, if any."""
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        return None

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=20,
    )
    for run in runs:
        artifacts = [a.path for a in client.list_artifacts(run.info.run_id, "drift")]
        if "drift/feature_baseline.json" in artifacts:
            local_path = client.download_artifacts(run.info.run_id, "drift/feature_baseline.json")
            with open(local_path) as f:
                return json.load(f)
    return None


def drift_detected(psi_scores: dict[str, float], threshold: float = PSI_DRIFT_THRESHOLD) -> bool:
    return any(score > threshold for score in psi_scores.values())
