from src.db.models.transactions import TransactionDB
from src.db.config import Session
import pandas as pd


class TransactionUtils:

    @staticmethod
    def get_all_transactions() -> pd.DataFrame:

        session = Session()
        transactions = session.query(TransactionDB).all()

        transactions_df = pd.DataFrame([
            {
                "transaction_id": t.transaction_id,
                "customer_id": t.customer_id,
                "product_id": t.product_id,
                "total_price": t.total_price,
                "quantity": t.quantity,
                "purchase_date": t.purchase_date,
            }
            for t in transactions
        ])
    
        return transactions_df