from src.db.models.customers import CustomerDB
from src.db.config import Session
import pandas as pd


class CustomerUtils:

    @staticmethod
    def get_all_customers() -> pd.DataFrame:

        session = Session()
        customers = session.query(CustomerDB).all()

        customers_df = pd.DataFrame([
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
            for c in customers
        ])
    
        return customers_df