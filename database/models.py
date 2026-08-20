from sqlalchemy import Column, Integer, String, ForeignKey, Float
from sqlalchemy.orm import relationship
from database.db import Base

class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    location_code = Column(String, unique=True, index=True) # e.g., "NYC-01"
    address = Column(String)

    # Relationship to inventory
    inventory = relationship("Inventory", back_populates="store")

class Inventory(Base):
    __tablename__ = "inventory"

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String, index=True) # Stock Keeping Unit
    quantity = Column(Integer, default=0)
    store_id = Column(Integer, ForeignKey("stores.id"))

    store = relationship("Store", back_populates="inventory")