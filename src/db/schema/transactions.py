from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class RawTransaction(BaseModel):
    transaction_id: str
    customer_id: str
    product_id: Optional[str]
    total_price: Optional[str]    
    quantity: Optional[str]
    purchase_date: Optional[str]

    ingest_batch_id: str
    ingested_at: datetime
    source: str

class CleanTransaction(BaseModel):
    transaction_id: str
    customer_id: str
    product_id: str
    total_price: float    
    quantity: int
    purchase_date: datetime

    created_at: datetime

class TransactionFeatures(BaseModel):
    transaction_id: str
    customer_id: str
    product_id: str
    total_price: float    
    quantity: int
    purchase_date: datetime
    days_since_last_purchase: float

    created_at: datetime
