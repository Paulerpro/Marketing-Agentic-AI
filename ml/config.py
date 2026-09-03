from datetime import timedelta, date

# TRAINING WINDOW
TRAINING_WINDOW_DAYS = 180
LABEL_DELAY_DAYS = 7   # buffer for late data


def compute_training_window(latest_known_date: date) -> tuple[date, date]:
    """
    (start, end) of the rolling training window, anchored to the most recent date
    actually present in the data rather than wall-clock "today".

    A wall-clock anchor silently produces an empty training window the moment the
    data stops being live-updated (e.g. a static/demo dataset) - anchoring to the
    data's own latest date keeps retraining well-defined either way.
    """
    end = latest_known_date - timedelta(days=LABEL_DELAY_DAYS)
    start = end - timedelta(days=TRAINING_WINDOW_DAYS)
    return start, end

MODEL_NAME = "churn_predictor"
FEATURE_VERSION = "v1"

# Must match columns passed to the churn model at scoring / training time.
CHURN_SCORING_FEATURE_COLUMNS = [
    "total_price",
    "days_since_last_purchase",
    "product_name",
    "category",
    "popularity_score",
    "category_popularity",
    "country",
    "city",
    "age",
    "gender",
    "interests",
    "purchase_frequency",
    "num_purchases",
    "avg_purchase_value",
    "recency_days",
    "top_category",
]

CATBOOST_PARAMS = {
    "iterations": 500,
    "depth": 6,
    "learning_rate": 0.05,
    "loss_function": "Logloss",
    "eval_metric": "AUC",
    "random_seed": 42,
    "verbose": 0
}

# PROMOTION THRESHOLDS
MIN_AUC = 0.75
MIN_RECALL = 0.55

AUC_IMPROVEMENT = 0.01
RECALL_IMPROVEMENT = 0.03
