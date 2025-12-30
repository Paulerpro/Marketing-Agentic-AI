from src.db.models.products import ProductDB
from src.db.config import Session
import pandas as pd


class ProductUtils:

    @staticmethod
    def get_all_products() -> pd.DataFrame:

        session = Session()
        products = session.query(ProductDB).all()

        products_df = pd.DataFrame([
            {
                "product_id": p.product_id,
                "product_name": p.product_name,
                "category": p.category,
                "price": p.price,
                "stock_status": p.stock_status,
            }
            for p in products
        ])
    
        return products_df