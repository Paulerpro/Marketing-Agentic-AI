"""Load and feature-engineer marketing data from the database (session-safe)."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.db.config import Session
from src.db.models.customers import CustomerDB
from src.db.models.products import ProductDB
from src.db.models.transactions import TransactionDB

logger = logging.getLogger(__name__)


def get_customers_dataframe() -> pd.DataFrame:
    session = Session()
    try:
        rows = session.query(CustomerDB).all()
        return pd.DataFrame(
            [
                {
                    "customer_id": c.customer_id,
                    "email": c.email,
                    "name": c.name,
                    "age": c.age,
                    "gender": c.gender,
                    "country": c.country,
                    "city": c.city,
                    "phone_number": c.phone_number,
                    "interests": c.interests,
                    "signup_date": c.signup_date,
                    "last_purchase_date": c.last_purchase_date,
                    "total_spent": c.total_spent,
                    "purchase_frequency": c.purchase_frequency,
                    "churn": c.churn,
                }
                for c in rows
            ]
        )
    finally:
        session.close()


def get_products_dataframe() -> pd.DataFrame:
    session = Session()
    try:
        rows = session.query(ProductDB).all()
        return pd.DataFrame(
            [
                {
                    "product_id": p.product_id,
                    "product_name": p.product_name,
                    "category": p.category,
                    "price": p.price,
                    "stock_status": p.stock_status,
                }
                for p in rows
            ]
        )
    finally:
        session.close()


def get_transactions_dataframe() -> pd.DataFrame:
    session = Session()
    try:
        rows = session.query(TransactionDB).all()
        return pd.DataFrame(
            [
                {
                    "transaction_id": t.transaction_id,
                    "customer_id": t.customer_id,
                    "product_id": t.product_id,
                    "total_price": t.total_price,
                    "quantity": t.quantity,
                    "purchase_date": t.purchase_date,
                }
                for t in rows
            ]
        )
    finally:
        session.close()


def build_merged_features_for_ml() -> pd.DataFrame:
    """
    Build the same wide transaction-level frame used in ml/retrain.py for training
    and scoring (after feature engineering).
    """
    from src.data_pipeline.preprocess import Feature_Engineer

    customers_df = get_customers_dataframe()
    products_df = get_products_dataframe()
    transactions_df = get_transactions_dataframe()

    feat_eng = Feature_Engineer()
    customers_eng = feat_eng.engineer_customer_features(
        customers=customers_df,
        products=products_df,
        transactions=transactions_df,
    )
    products_eng = feat_eng.engineer_product_features(
        transactions=transactions_df,
        products=products_df,
    )
    tx_eng = feat_eng.engineer_tx_features(transactions_df)
    merged = feat_eng.merge_all_datasets(tx_eng, customers_eng, products_eng)
    logger.info("build_merged_features_for_ml: rows=%s", len(merged))
    return merged


def summarize_data_sources() -> dict[str, Any]:
    """Lightweight counts for agents / health checks without building full merge."""
    return {
        "customers": len(get_customers_dataframe()),
        "products": len(get_products_dataframe()),
        "transactions": len(get_transactions_dataframe()),
    }
