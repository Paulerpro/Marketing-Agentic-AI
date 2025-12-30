from pydantic import BaseModel
from typing import Optional
from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime, func

from src.db.config import Base

class ProductDB(Base):
    """
    SQLAlchemy model definition corresponding to the Pydantic CleanProduct schema.
    This maps to a 'clean_products' table in the PostgreSQL database.
    """
    __tablename__ = 'clean_products'

    product_id = Column(String, primary_key=True, index=True)
    product_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    description = Column(String) 
    price = Column(Float, nullable=False)
    stock_status = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    
    def __repr__(self):
        return f"<Product(name='{self.product_name}', price='{self.price}')>"

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

    
