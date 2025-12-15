from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class RawCustomer(BaseModel):
    customer_id: str
    email: Optional[str]
    name: Optional[str]
    age: Optional[str]  
    gender: Optional[str]       
    country: Optional[str]
    city: Optional[str]
    phone_number: Optional[str]
    interests: Optional[str]
    signup_date: Optional[str] 
    last_purchase_date: Optional[str] 
    total_spent: Optional[str] 
    purchase_frequency: Optional[float]
    churn: Optional[str] 

    ingest_batch_id: str
    ingested_at: datetime
    source: str # CSV file, API, or external database

from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional
from datetime import date

class CleanCustomer(BaseModel):
    customer_id: str
    email: EmailStr
    name: str
    age: int
    gender: str 
    country: str
    city: str
    phone_number: int
    interests: str
    signup_date: date
    last_purchase_date: date 
    total_spent: float
    purchase_frequency: float
    churn: int 
    created_at: date

    @field_validator("age")
    def validate_age(cls, v):
        if v is not None and (v < 0 or v > 120):
            raise ValueError("Invalid age")
        return v

class CustomerFeatures(BaseModel):
    customer_id: str
    email: EmailStr
    name: str
    age: int
    gender: str 
    country: str
    city: str
    phone_number: int
    interests: str
    signup_date: date
    last_purchase_date: date 
    total_spent: float
    purchase_frequency: float
    churn: int 

    # engineered features
    num_purchases: int
    avg_purchase_value: float
    recency_days: int
    days_since_signup: int
    top_category: str

    created_at: date
