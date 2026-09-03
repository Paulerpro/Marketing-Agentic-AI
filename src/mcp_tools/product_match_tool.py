"""Recommend products for a customer, based on their top purchase category + product popularity."""

from __future__ import annotations

from typing import Any

from src.agents.tools.data_tools import (
    build_merged_features_for_ml,
    get_products_dataframe,
    get_transactions_dataframe,
)
from src.data_pipeline.preprocess import Feature_Engineer

_OUTPUT_COLUMNS = ["product_id", "product_name", "category", "price", "popularity_score"]


def _products_with_popularity():
    products = get_products_dataframe()
    transactions = get_transactions_dataframe()
    engineered = Feature_Engineer.engineer_product_features(
        transactions=transactions, products=products
    )
    return engineered.reset_index()


def match_products_for_customer(customer_id: str, top_n: int = 3) -> list[dict[str, Any]]:
    """Top-N most popular products in the customer's most-purchased category."""
    merged = build_merged_features_for_ml()
    customer_rows = merged[merged["customer_id"] == customer_id]
    if customer_rows.empty:
        return []

    top_category = customer_rows.iloc[0].get("top_category", "unknown")
    products = _products_with_popularity()

    candidates = products
    if "category" in products.columns and top_category != "unknown":
        in_category = products[products["category"] == top_category]
        if not in_category.empty:
            candidates = in_category

    candidates = candidates.sort_values("popularity_score", ascending=False)
    cols = [c for c in _OUTPUT_COLUMNS if c in candidates.columns]
    return candidates[cols].head(top_n).to_dict(orient="records")
