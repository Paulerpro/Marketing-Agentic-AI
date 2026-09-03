import mlflow.catboost
from mlflow.exceptions import MlflowException

def load_production_churn_model(model_name: str):
    """
    Loads the current Production churn model from MLflow Model Registry.

    Uses the catboost flavor (not pyfunc) so callers can use predict_proba()
    directly - pyfunc's wrapper only exposes predict().

    Returns:
        Loaded native CatBoost model
    Raises:
        RuntimeError if no Production model exists
    """
    model_uri = f"models:/{model_name}/Production"

    try:
        model = mlflow.catboost.load_model(model_uri)
        return model

    except MlflowException as e:
        raise RuntimeError(
            f"No Production model found for '{model_name}'. "
            "Ensure a model has been promoted to Production."
        ) from e
