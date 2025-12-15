from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class RawProduct(BaseModel):
    product_id: str
    product_name: Optional[str]
    category: Optional[str]
    description: Optional[str]
    price: Optional[str]
    stock_status: Optional[str]

    ingest_batch_id: str
    ingested_at: datetime
    source: str

class CleanProduct(BaseModel):
    product_id: str
    product_name: str
    category: str
    description: str
    price: float
    stock_status: str

    created_at: datetime

class ProductFeatures(BaseModel):
    product_id: str
    product_name: str
    category: str
    description: str
    price: float
    stock_status: str
    popularity_score: int         
    category_popularity: int     

    created_at: datetime

    
