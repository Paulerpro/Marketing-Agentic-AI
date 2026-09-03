"""Churn model retraining, with a PSI drift gate, temporal train/test split, and a
try/except wrapper so a failed run never takes the current champion down.

See TODO.txt's documented retraining logic and the PDF plan's "Model Retraining -
Hardening Notes" - this implements all of them:
- PSI-based drift gate (only retrain when PSI > 0.10 on a monitored feature)
- temporal split (train on the older 80% of the window, test on the newest 20% -
  never a random shuffle for time-series)
- as_of_end_date wired through feature engineering, so recency/signup-age features
  and the transactions used for training don't leak information past the cutoff
- every run (skipped, trained, or failed) is logged to MLflow
- feature schema + a drift baseline are logged as artifacts alongside the model
- best-effort ONNX export, logged as an artifact (serving stays on the native
  CatBoost/MLflow path for now - see README notes on why)
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import mlflow
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import classification_report, precision_score, recall_score, roc_auc_score

from ml.config import (
    CATBOOST_PARAMS,
    CHURN_SCORING_FEATURE_COLUMNS,
    FEATURE_VERSION,
    MIN_AUC,
    MIN_RECALL,
    AUC_IMPROVEMENT,
    RECALL_IMPROVEMENT,
    MODEL_NAME,
    TRAINING_WINDOW_DAYS,
    compute_training_window,
)
from ml.drift import DRIFT_MONITORED_COLUMNS, build_baseline, compute_drift, drift_detected, load_last_baseline
from ml.promotion import get_production_metrics, should_promote
from ml.register_model import apply_registry_decision
from src.data_pipeline.preprocess import Feature_Engineer
from src.db.config import Session
from src.utils.customers_utils.customers import CustomerUtils
from src.utils.logger import logger
from src.utils.product_utils.product import ProductUtils
from src.utils.transaction_utils.transaction import TransactionUtils

EXPERIMENT_NAME = "churn_catboost_retraining"
TEST_FRACTION = 0.2


def _build_training_frame() -> pd.DataFrame:
    """Engineer features as-of the rolling training window's end, anchored to the
    most recent transaction date actually in the data (see compute_training_window)."""
    customers_df = CustomerUtils.get_all_customers()
    prods_df = ProductUtils.get_all_products()
    tx_df = TransactionUtils.get_all_transactions()

    session = Session()
    session.close()

    if tx_df.empty:
        return tx_df

    purchase_dates = pd.to_datetime(tx_df["purchase_date"], utc=True, errors="coerce")
    latest_known_date = purchase_dates.max().date()
    train_start, train_end = compute_training_window(latest_known_date)
    logger.info("Training window: [%s, %s] (latest data: %s)", train_start, train_end, latest_known_date)

    feat_eng = Feature_Engineer()
    customers_eng = feat_eng.engineer_customer_features(
        customers=customers_df, products=prods_df, transactions=tx_df, as_of_end_date=train_end
    )
    products_eng = feat_eng.engineer_product_features(
        transactions=tx_df, products=prods_df, as_of_end_date=train_end
    )
    tx_eng = feat_eng.engineer_tx_features(tx_df, as_of_end_date=train_end)

    merged = feat_eng.merge_all_datasets(tx_eng, customers_eng, products_eng)

    merged["purchase_date"] = pd.to_datetime(merged["purchase_date"], utc=True, errors="coerce")
    train_start_ts = pd.Timestamp(train_start, tz="UTC")
    train_end_ts = pd.Timestamp(train_end, tz="UTC")
    merged = merged[(merged["purchase_date"] >= train_start_ts) & (merged["purchase_date"] <= train_end_ts)]
    merged = merged.sort_values("purchase_date").dropna(subset=["churn"])

    return merged


def _temporal_split(merged: pd.DataFrame, feature_cols: list[str], target: str, test_frac: float = TEST_FRACTION):
    """Chronological split - oldest rows train, newest rows test. Never shuffle
    time-series data before splitting."""
    split_idx = int(len(merged) * (1 - test_frac))
    train_df, test_df = merged.iloc[:split_idx], merged.iloc[split_idx:]
    return train_df[feature_cols], test_df[feature_cols], train_df[target], test_df[target]


def _export_onnx(model: CatBoostClassifier, feature_cols: list[str]) -> str | None:
    """Best-effort ONNX export, logged as an artifact. Never blocks the pipeline."""
    try:
        with tempfile.TemporaryDirectory() as tmp:
            onnx_path = Path(tmp) / "model.onnx"
            model.save_model(str(onnx_path), format="onnx")
            mlflow.log_artifact(str(onnx_path), artifact_path="onnx")
            return str(onnx_path)
    except Exception:
        logger.exception("ONNX export failed - continuing without it (serving stays on CatBoost/MLflow).")
        return None


def _log_skip_run(run_name: str, tag: str, psi_scores: dict[str, float], baseline: dict[str, Any]) -> None:
    with mlflow.start_run(run_name=run_name):
        mlflow.set_tag("promotion_decision", tag)
        if psi_scores:
            mlflow.log_metrics({f"psi_{k}": v for k, v in psi_scores.items()})
        mlflow.log_dict(baseline, "drift/feature_baseline.json")


def run_retraining() -> dict[str, Any]:
    mlflow.set_experiment(EXPERIMENT_NAME)

    merged = _build_training_frame()
    if merged.empty:
        logger.warning("No transactions available to train on - skipping retrain.")
        return {"status": "skipped_no_data"}

    fresh_baseline = build_baseline(merged, DRIFT_MONITORED_COLUMNS)
    prior_baseline = load_last_baseline(EXPERIMENT_NAME)
    is_first_run = get_production_metrics(MODEL_NAME) is None
    psi_scores = compute_drift(merged, prior_baseline) if prior_baseline else {}

    if prior_baseline is not None and not is_first_run and not drift_detected(psi_scores):
        logger.info("No drift detected (PSI scores: %s) - skipping retrain.", psi_scores)
        _log_skip_run("drift_check_skip", "SKIPPED_NO_DRIFT", psi_scores, fresh_baseline)
        return {"status": "skipped_no_drift", "psi_scores": psi_scores}

    try:
        with mlflow.start_run(run_name="catboost_retrain") as run:
            target = "churn"
            feature_cols = CHURN_SCORING_FEATURE_COLUMNS

            X_train, X_test, y_train, y_test = _temporal_split(merged, feature_cols, target)

            model = CatBoostClassifier(**CATBOOST_PARAMS)
            cat_features = X_train.select_dtypes(include=["object", "category"]).columns.tolist()
            cat_feature_indices = [X_train.columns.get_loc(col) for col in cat_features]
            model.fit(X_train, y_train, cat_features=cat_feature_indices)

            preds = model.predict(X_test)
            proba = model.predict_proba(X_test)[:, 1]

            auc = roc_auc_score(y_test, proba)
            recall = recall_score(y_test, preds, pos_label=1)
            precision = precision_score(y_test, preds, pos_label=1)

            logger.info(f"classification_report: {classification_report(y_test, preds)}")
            logger.info(f"AUC Score: {auc}")

            mlflow.log_params(
                {
                    "model_type": "CatBoost",
                    "feature_version": FEATURE_VERSION,
                    "training_window_days": TRAINING_WINDOW_DAYS,
                    "train_rows": len(X_train),
                    "test_rows": len(X_test),
                    "split": "temporal",
                    "train_as_of": str(merged.loc[X_train.index, "purchase_date"].max().date()),
                    "test_as_of": str(merged.loc[X_test.index, "purchase_date"].max().date()),
                }
            )
            mlflow.log_metrics(
                {
                    "auc": auc,
                    "recall_churn": recall,
                    "precision_churn": precision,
                    **{f"psi_{k}": v for k, v in psi_scores.items()},
                }
            )

            mlflow.catboost.log_model(model, artifact_path="model", registered_model_name=MODEL_NAME)

            schema = {c: str(X_train[c].dtype) for c in feature_cols}
            mlflow.log_dict(schema, "schema/feature_schema.json")
            mlflow.log_dict(fresh_baseline, "drift/feature_baseline.json")
            _export_onnx(model, feature_cols)

            new_metrics = {
                "auc": auc,
                "recall_churn": recall,
                "min_auc": MIN_AUC,
                "min_recall": MIN_RECALL,
                "auc_improvement": AUC_IMPROVEMENT,
                "recall_improvement": RECALL_IMPROVEMENT,
            }
            current_production_metrics = get_production_metrics(MODEL_NAME)
            decision = should_promote(new_metrics, current_production_metrics)
            mlflow.set_tag("promotion_decision", decision)
            logger.info(f"MODEL PROMOTION DECISION: {decision}")

            apply_registry_decision(MODEL_NAME, run.info.run_id, decision)

            return {
                "status": "trained",
                "decision": decision,
                "auc": auc,
                "recall": recall,
                "run_id": run.info.run_id,
            }

    except Exception as e:
        # mlflow.start_run() already marks this run FAILED and re-raises - the
        # champion in the registry is untouched since apply_registry_decision()
        # never ran. Report the failure instead of crashing the caller (e.g. a
        # scheduled/cron retrain).
        logger.exception("Retraining failed - keeping current champion live.")
        return {"status": "failed", "error": str(e), "error_type": type(e).__name__}


if __name__ == "__main__":
    run_retraining()
