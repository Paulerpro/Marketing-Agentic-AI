from src.utils.customers_utils.customers import CustomerUtils
from src.utils.product_utils.product import ProductUtils
from src.utils.transaction_utils.transaction import TransactionUtils
from src.db.config import Session
from src.data_pipeline.preprocess import Feature_Engineer
from src.utils.logger import logger
from ml.config import *
from ml.promotion import should_promote, get_production_metrics
from ml.register_model import apply_registry_decision

from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score, recall_score, precision_score

from catboost import CatBoostClassifier

import mlflow

def run_retraining():

    mlflow.set_experiment("churn_catboost_retraining")

    with mlflow.start_run(run_name="catboost_retrain"):

        customers_df = CustomerUtils.get_all_customers()
        prods_df = ProductUtils.get_all_products()
        tx_df = TransactionUtils.get_all_transactions()

        session = Session()
        session.close()

        feat_eng = Feature_Engineer()
        customers_eng = feat_eng.engineer_customer_features(
            customers=customers_df, products=prods_df, transactions=tx_df
        )
        products_eng = feat_eng.engineer_product_features(transactions=tx_df, products=prods_df)
        tx_eng = feat_eng.engineer_tx_features(tx_df)

        merged = feat_eng.merge_all_datasets(tx_eng, customers_eng, products_eng)
        
        target = "churn"
        feature_cols = [
            'total_price', 'days_since_last_purchase', 'product_name', 'category', 
            'popularity_score', 'category_popularity', 'country', 'city', 'age', 'gender', 'interests', 
            'purchase_frequency', 'num_purchases', 'avg_purchase_value', 'recency_days', 'top_category'
        ]

        X = merged[feature_cols]
        y = merged[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, stratify=y, random_state=42
        )

        model = CatBoostClassifier(**CATBOOST_PARAMS)

        cat_features = X_train.select_dtypes(
            include=["object", "category"]
        ).columns.tolist()

        cat_feature_indices = [
            X_train.columns.get_loc(col) for col in cat_features
        ]

        model.fit(
            X_train,
            y_train,
            cat_features=cat_feature_indices
        )

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
            "training_window_days": TRAINING_WINDOW_DAYS
        }
        )

        mlflow.log_metric(
            {
            "auc": auc,
            "recall_churn": recall,
            "precision_churn": precision
        }
        )

        mlflow.catboost.log_model(
            model, 
            artifact_path="model",
            registered_model_name=MODEL_NAME
        )

        new_metrics = {
            "auc": auc,
            "recall_churn": recall,
            "min_auc": MIN_AUC,
            "min_recall": MIN_RECALL,
            "auc_improvement": AUC_IMPROVEMENT,
            "recall_improvement": RECALL_IMPROVEMENT
        }

        current_production_metrics = get_production_metrics(MODEL_NAME)

        decision = should_promote(new_metrics, current_production_metrics)

        mlflow.set_tag("promotion_decision", decision)

        logger.info(f"MODEL PROMOTION DECISION: {decision}")

        run_id = mlflow.active_run().info.run_id
        apply_registry_decision(MODEL_NAME, run_id, decision)


if __name__ == "__main__":
    run_retraining()