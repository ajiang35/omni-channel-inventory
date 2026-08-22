from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

class StoreCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=120)
    location_code: str = Field(
        min_length=2,
        max_length=32,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    address: str = Field(min_length=1, max_length=255)

class StoreCreateResponse(BaseModel):
    message: str
    store_id: int = Field(gt=0)

class StoreUpdate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    name: Optional[str] = Field(default=None, min_length=1, max_length=120)
    location_code: Optional[str] = Field(
        default=None,
        min_length=2,
        max_length=32,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    address: Optional[str] = Field(default=None, min_length=1, max_length=255)

class StoreResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(gt=0)
    name: str
    location_code: str
    address: Optional[str] = None

class StoreMemberCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    user_sub: str = Field(min_length=1, max_length=255)
    role: str = Field(default="staff", pattern=r"^(owner|staff)$")

class StoreMemberResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(gt=0)
    store_id: int = Field(gt=0)
    user_sub: str
    role: str

class InventoryAdd(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    sku: str = Field(
        min_length=1,
        max_length=80,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    )
    quantity: int = Field(ge=0, le=1_000_000_000)
    store_id: int = Field(gt=0)

class InventoryAddResponse(BaseModel):
    message: str
    sku: str
    quantity: int = Field(ge=0)

class InventoryUpdate(BaseModel):
    quantity: int = Field(ge=0, le=1_000_000_000)

class InventoryResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(gt=0)
    sku: str
    quantity: int = Field(ge=0)
    store_id: int = Field(gt=0)

class PaginatedResponse(BaseModel):
    items: list
    page: int = Field(gt=0)
    page_size: int = Field(gt=0, le=100)
    total_items: int = Field(ge=0)
    total_pages: int = Field(ge=0)
    has_next: bool
    has_previous: bool

class CartItemCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    sku: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    quantity: int = Field(gt=0, le=1_000_000_000)

class CartCreate(BaseModel):
    store_id: int = Field(gt=0)

class CartItemResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str
    quantity: int = Field(gt=0)

class CartResponse(BaseModel):
    id: UUID
    store_id: int = Field(gt=0)
    status: str
    items: list[CartItemResponse]

class OrderConfirmationResponse(BaseModel):
    order_id: UUID
    status: str
    message: str
    receipt: "ReceiptResponse"

class ReceiptLineResponse(BaseModel):
    sku: str
    quantity: int = Field(gt=0)

class ReceiptResponse(BaseModel):
    id: UUID
    receipt_number: str
    order_id: UUID
    store_id: int = Field(gt=0)
    issued_at: str
    lines: list[ReceiptLineResponse]
    total_units: int = Field(gt=0)