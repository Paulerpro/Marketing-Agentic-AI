import pandas as pd

from src.utils.logger import logger
from src.db.config import engine

class Preprocessor:

    @staticmethod
    def clean_customer_data(data) -> pd.DataFrame:
        customers = data.copy()

        customers["name"] = customers["name"].str.lower()

        customers["signup_date"] = pd.to_datetime(customers["signup_date"], errors="coerce", utc=True)

        customers.dropna(subset=["customer_id", "email", "signup_date"], inplace=True)

        customers.drop_duplicates(subset=["customer_id"], inplace=True)

        return customers
    
    @staticmethod
    def clean_product_data(data) -> pd.DataFrame:
        products = data.copy()

        products["product_name"] = products["product_name"].str.lower()

        products.dropna(subset=["product_id", "product_name"], inplace=True)

        products.drop_duplicates(subset=["product_id"], inplace=True)

        return products
    
    @staticmethod
    def clean_tx_data(data) -> pd.DataFrame:
        transactions = data.copy()

        transactions["purchase_date"] = pd.to_datetime(
            transactions["purchase_date"], errors="coerce", utc=True
        )

        transactions.dropna(
            subset=["transaction_id", "customer_id", "product_id", "purchase_date"],
            inplace=True,
        )

        transactions.drop_duplicates(subset=["transaction_id"], inplace=True)

        return transactions

class Feature_Engineer:
    
    @staticmethod
    def engineer_customer_features(
        customers: pd.DataFrame,
        transactions: pd.DataFrame,
        products: pd.DataFrame,
        as_of_end_date = None):

        """
        as_of_end_date anchors "now" for recency/signup-age features and bounds which
        transactions are visible, so training on a historical window doesn't leak
        transactions/labels that hadn't happened yet as of that point. Defaults to the
        real current time for live scoring, where "now" genuinely is the as-of point.
        """
        as_of = pd.Timestamp(as_of_end_date, tz="UTC") if as_of_end_date is not None else pd.Timestamp.now(tz="UTC")

        customers["signup_date"] = pd.to_datetime(
            customers["signup_date"], utc=True, errors="coerce"
        )
        transactions["purchase_date"] = pd.to_datetime(
            transactions["purchase_date"], utc=True, errors="coerce"
        )
        transactions = transactions[transactions["purchase_date"] <= as_of]

        merged = transactions.merge(products, on="product_id", how="left")
        merged["total_purchase_value"] = merged["price"] * merged["quantity"]

        total_spent = merged.groupby("customer_id")["total_purchase_value"].sum()

        num_purchases = merged.groupby("customer_id")["transaction_id"].count()

        avg_purchase = total_spent / num_purchases

        last_purchase = merged.groupby("customer_id")["purchase_date"].max()
        recency_days = (as_of - last_purchase).dt.days

        customers = customers.set_index("customer_id")

        days_since_signup = (as_of - customers["signup_date"]).dt.days

        # add fnew eatures to the customers data
        customers["total_spent"] = total_spent
        customers["num_purchases"] = num_purchases
        customers["avg_purchase_value"] = avg_purchase
        customers["recency_days"] = recency_days
        customers["days_since_signup"] = days_since_signup

        # fill missing with 0 for customers that did not purchase anything
        customers.fillna(
            {
                "total_spent": 0,
                "num_purchases": 0,
                "avg_purchase_value": 0,
                "recency_days": 999, # Temporal fillup
            },
            inplace=True,
        )

        if "category" in products.columns:
            merged["category"] = merged["category"].fillna("unknown")
            top_category = (
                merged.groupby("customer_id")["category"]
                .agg(lambda x: x.value_counts().index[0])
            )
            customers["top_category"] = top_category
        else:
            customers["top_category"] = "unknown"

        return customers

    @staticmethod
    def engineer_product_features(
            transactions: pd.DataFrame,
            products: pd.DataFrame,
            as_of_end_date = None
        ):

        if as_of_end_date is not None:
            as_of = pd.Timestamp(as_of_end_date, tz="UTC")
            purchase_date = pd.to_datetime(transactions["purchase_date"], utc=True, errors="coerce")
            transactions = transactions[purchase_date <= as_of]

        popularity = transactions.groupby("product_id")["transaction_id"].count()

        products = products.set_index("product_id")
        products["popularity_score"] = popularity.fillna(0)

        if "category" in products.columns:
            category_counts = (
                products.groupby("category")["popularity_score"].sum()
            )
            products["category_popularity"] = products["category"].map(
                category_counts
            )
        else:
            products["category_popularity"] = 0

        return products
    
    @staticmethod
    def engineer_tx_features(
            transactions: pd.DataFrame,
            as_of_end_date = None
        ):

        if as_of_end_date is not None:
            as_of = pd.Timestamp(as_of_end_date, tz="UTC")
            purchase_date = pd.to_datetime(transactions["purchase_date"], utc=True, errors="coerce")
            transactions = transactions[purchase_date <= as_of]

        transactions = transactions.sort_values(
            by=["customer_id", "purchase_date"]
        )

        # days_since_last_purchase (per customer)
        transactions["days_since_last_purchase"] = (
            transactions.groupby("customer_id")["purchase_date"]
            .diff()
            .dt.days
        )

        transactions["days_since_last_purchase"] = transactions["days_since_last_purchase"].fillna(
            transactions["days_since_last_purchase"].median())

        return transactions
    
    @staticmethod
    def merge_all_datasets(
            transactions: pd.DataFrame, 
            customers: pd.DataFrame,
            products: pd.DataFrame, 
    ):

        # attach product & customer features to tx data (for embeddings and other usecases.)
        transactions = transactions.merge(
            products.reset_index(), on="product_id", how="left"
        )
        all_merged = transactions.merge(
            customers.reset_index(), on="customer_id", how="left"
        )

        logger.info("datasets merged...")

        return all_merged


if __name__ == "__main__":
    pass
    