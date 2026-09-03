import pandas as pd

from ml.model_loader import load_production_churn_model
from src.utils.logger import logger
from ml.config import MODEL_NAME

_MODEL = None

CHURN_THRESHOLD = 0.5  # can be tuned later


def _get_model():
    """Load Production model on first use so imports work without MLflow."""
    global _MODEL
    if _MODEL is None:
        _MODEL = load_production_churn_model(MODEL_NAME)
    return _MODEL


def score_churn(features: pd.DataFrame) -> pd.DataFrame:
    """
    Scores churn risk for customers.

    Args:
        features: Pandas DataFrame with same feature columns used in training

    Returns:
        DataFrame with churn_probability and churn_prediction
    """

    if features.empty:
        raise ValueError("Input features DataFrame is empty")

    try:
        model = _get_model()
        probabilities = model.predict_proba(features)[:, 1]
        predictions = (probabilities >= CHURN_THRESHOLD).astype(int)

        result = features.copy()
        result["churn_probability"] = probabilities
        result["churn_prediction"] = predictions

        return result

    except Exception as e:
        logger.exception("Churn scoring failed")
        raise RuntimeError("Churn scoring error") from e
