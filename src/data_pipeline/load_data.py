import pandas as pd
from typing import Optional

class DataLoader:
    def __init__(self, data_dir: str = "data"):
        """
        Initialize the DataLoader with the path to the data directory.

        Args:
            data_dir (str): Path to the folder containing CSV files.
        """
        self.data_dir = data_dir

    def _validate_dataframe(self, df: pd.DataFrame, required_columns: list, dtypes: dict):
        """
        Internal method to validate that DataFrame has the required columns and data types.
        """
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")

        for col, dtype in dtypes.items():
            if col in df.columns:
                try:
                    if dtype == 'datetime':
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    else:
                        df[col] = df[col].astype(dtype)
                except Exception as e:
                    raise ValueError(f"Error converting {col} to {dtype}: {e}")

        return df

    def load_customers(self, nrows: Optional[int] = None) -> pd.DataFrame:
        path = f"{self.data_dir}/customers.csv"
        df = pd.read_csv(path, nrows=nrows)
        return self._validate_dataframe(
            df,
            required_columns=["customer_id", "name", "email", "signup_date"],
            dtypes={"customer_id": str, "name": str, "email": str, "signup_date": "datetime"}
        )

    def load_products(self, nrows: Optional[int] = None) -> pd.DataFrame:
        path = f"{self.data_dir}/products.csv"
        df = pd.read_csv(path, nrows=nrows)
        return self._validate_dataframe(
            df,
            required_columns=["product_id", "product_name", "price"],
            dtypes={"product_id": str, "product_name": str, "price": float}
        )

    def load_transactions(self, nrows: Optional[int] = None) -> pd.DataFrame:
        path = f"{self.data_dir}/transactions.csv"
        df = pd.read_csv(path, nrows=nrows)
        return self._validate_dataframe(
            df,
            required_columns=["transaction_id", "customer_id", "product_id", "purchase_date", "quantity"],
            dtypes={
                "transaction_id": str,
                "customer_id": str,
                "product_id": str,
                "purchase_date": "datetime",
                "quantity": int
            }
        )


if __name__ == "__main__":
    # Example
    loader = DataLoader("data")
    customers_df = loader.load_customers()
    print(customers_df.head())