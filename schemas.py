from pydantic import BaseModel

class StoreCreate(BaseModel):
    name: str
    location_code: str
    address: str

class InventoryAdd(BaseModel):
    sku: str
    quantity: int
    store_id: int