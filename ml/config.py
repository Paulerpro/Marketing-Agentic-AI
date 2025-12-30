from datetime import timedelta, date

# TRAINING WINDOW
TRAINING_WINDOW_DAYS = 180
LABEL_DELAY_DAYS = 7   # buffer for late data

TRAIN_END_DATE = date.today() - timedelta(days=LABEL_DELAY_DAYS)
TRAIN_START_DATE = TRAIN_END_DATE - timedelta(days=TRAINING_WINDOW_DAYS)

MODEL_NAME = "churn_predictor"
FEATURE_VERSION = "v1"

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
