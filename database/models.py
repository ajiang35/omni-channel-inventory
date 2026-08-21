from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, JSON, String
from sqlalchemy import Index, UniqueConstraint
from sqlalchemy.orm import relationship
from datetime import datetime, timezone
from database.db import Base

class Store(Base):
    __tablename__ = "stores"
    __table_args__ = (
        CheckConstraint("length(trim(name)) > 0", name="ck_stores_name_not_blank"),
        CheckConstraint("length(trim(location_code)) > 0", name="ck_stores_location_not_blank"),
    )

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    location_code = Column(String(32), unique=True, index=True, nullable=False)
    address = Column(String)

    # Relationship to inventory
    inventory = relationship(
        "Inventory",
        back_populates="store",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    members = relationship(
        "StoreMember",
        back_populates="store",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

class StoreMember(Base):
    """Grants a specific authenticated user (by JWT `sub`) access to a store."""
    __tablename__ = "store_members"
    __table_args__ = (
        UniqueConstraint("store_id", "user_sub", name="uq_store_member_user"),
        CheckConstraint("role in ('owner', 'staff')", name="ck_store_member_role_valid"),
    )

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False)
    user_sub = Column(String(255), nullable=False, index=True)
    role = Column(String(20), nullable=False, default="staff")
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    store = relationship("Store", back_populates="members")

class Inventory(Base):
    __tablename__ = "inventory"
    __table_args__ = (
        UniqueConstraint("sku", "store_id", name="uq_inventory_sku_store"),
        CheckConstraint("quantity >= 0", name="ck_inventory_quantity_non_negative"),
        Index("ix_inventory_store_sku", "store_id", "sku"),
    )

    id = Column(Integer, primary_key=True, index=True)
    sku = Column(String(80), nullable=False)
    quantity = Column(Integer, nullable=False, default=0)
    store_id = Column(Integer, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False)

    store = relationship("Store", back_populates="inventory")

class Order(Base):
    __tablename__ = "orders"

    id = Column(String(36), primary_key=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    status = Column(String(20), nullable=False, default="CART")
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    store = relationship("Store")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")

class OrderItem(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        UniqueConstraint("order_id", "sku", name="uq_order_item_sku"),
        CheckConstraint("quantity > 0", name="ck_order_item_quantity_positive"),
    )

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False)
    sku = Column(String(80), nullable=False)
    quantity = Column(Integer, nullable=False)

    order = relationship("Order", back_populates="items")

class Receipt(Base):
    __tablename__ = "receipts"

    id = Column(String(36), primary_key=True)
    receipt_number = Column(String(24), unique=True, nullable=False, index=True)
    order_id = Column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False, unique=True)
    store_id = Column(Integer, ForeignKey("stores.id"), nullable=False)
    issued_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    lines = Column(JSON, nullable=False)
    total_units = Column(Integer, nullable=False)

    order = relationship("Order")
    store = relationship("Store")