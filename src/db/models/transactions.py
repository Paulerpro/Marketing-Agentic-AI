from pydantic import BaseModel
from typing import Optional
from datetime import datetime

from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, func
from sqlalchemy.orm import relationship

from src.db.config import Base

class TransactionDB(Base):
    """
    SQLAlchemy model definition for clean transaction data, 
    including foreign key relationships to customers and products.
    """
    __tablename__ = 'clean_transactions'

    transaction_id = Column(String, primary_key=True, index=True)
    total_price = Column(Float, nullable=False)    
    quantity = Column(Integer, nullable=False)
    purchase_date = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    
    
    customer_id = Column(String, ForeignKey('clean_customers.customer_id'), nullable=False)
    product_id = Column(String, ForeignKey('clean_products.product_id'), nullable=False)

    customer = relationship("CustomerDB", backref="transactions")
    product = relationship("ProductDB", backref="transactions")

    def __repr__(self):
        return f"<Transaction(id='{self.transaction_id}', customer='{self.customer_id}', total='{self.total_price}')>"


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
