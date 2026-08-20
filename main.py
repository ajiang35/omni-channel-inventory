from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from database.db import get_db, redis_client
from database.models import Store, Inventory
from schemas import StoreCreate, InventoryAdd

app = FastAPI(title="Omni-Channel Inventory")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Omni-Channel Inventory API!"}

@app.get("/health/db")
def test_databases(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        pg_status = "Connected successfully!"
    except Exception as e:
        pg_status = f"Failed: {str(e)}"
        
    try:
        redis_client.ping()
        redis_status = "Connected successfully!"
    except Exception as e:
        redis_status = f"Failed: {str(e)}"
        
    return {
        "postgres": pg_status,
        "redis": redis_status
    }

@app.post("/stores/")
def create_store(store: StoreCreate, db: Session = Depends(get_db)):
    db_store = Store(name=store.name, location_code=store.location_code, address=store.address)
    db.add(db_store)
    db.commit()
    db.refresh(db_store)
    return {"message": "Store created successfully", "store_id": db_store.id}

@app.post("/inventory/")
def add_inventory(item: InventoryAdd, db: Session = Depends(get_db)):
    # Verify store exists
    store = db.query(Store).filter(Store.id == item.store_id).first()
    if not store:
        raise HTTPException(status_code=404, message="Store not found")
        
    db_item = Inventory(sku=item.sku, quantity=item.quantity, store_id=item.store_id)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    
    # Also mirror this into Redis for fast flash-sale lookup
    redis_key = f"inventory:{item.sku}:store:{item.store_id}"
    redis_client.set(redis_key, item.quantity)
    
    return {"message": "Inventory added successfully", "sku": db_item.sku, "quantity": db_item.quantity}

@app.post("/orders/purchase")
def purchase_item(sku: str, store_id: int, db: Session = Depends(get_db)):
    redis_key = f"inventory:{sku}:store:{store_id}"
    
    # 1. ATOMIC DECREMENT in Redis
    # decrby returns the new value after decrementing
    new_stock = redis_client.decrby(redis_key, 1)
    
    if new_stock < 0:
        # 2. If it drops below zero, roll it back immediately
        redis_client.incrby(redis_key, 1)
        raise HTTPException(status_code=400, detail="Item out of stock!")
    
    # 3. If we are here, we successfully reserved the item
    # Now you would typically emit a Kafka event for order processing...
    return {"message": "Success! Item reserved.", "remaining_stock": new_stock}