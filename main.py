import json
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from aiokafka import AIOKafkaProducer
from database.db import get_db, redis_client
from database.models import Store, Inventory
from schemas import StoreCreate, InventoryAdd

# Global variable for the Kafka producer
kafka_producer: AIOKafkaProducer | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Kafka Producer
    global kafka_producer
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    try:
        await kafka_producer.start()
        print("Kafka Producer started successfully!")
    except Exception as exc:
        print(f"Kafka unavailable at {kafka_bootstrap_servers}: {exc}")
        await kafka_producer.stop()
        kafka_producer = None

    yield

    # Shutdown: Stop Kafka Producer
    if kafka_producer is not None:
        await kafka_producer.stop()
        print("Kafka Producer stopped.")

app = FastAPI(title="Omni-Channel Inventory Mesh", lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Omni-Channel Inventory Mesh API!"}

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
    store = db.query(Store).filter(Store.id == item.store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
        
    db_item = Inventory(sku=item.sku, quantity=item.quantity, store_id=item.store_id)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    
    redis_key = f"inventory:{item.sku}:store:{item.store_id}"
    redis_client.set(redis_key, item.quantity)
    
    return {"message": "Inventory added successfully", "sku": db_item.sku, "quantity": db_item.quantity}

@app.post("/orders/purchase")
async def purchase_item(sku: str, store_id: int, db: Session = Depends(get_db)):
    redis_key = f"inventory:{sku}:store:{store_id}"

    # 1. ATOMIC DECREMENT in Redis to handle high traffic races
    new_stock = redis_client.decrby(redis_key, 1)

    if new_stock < 0:
        # Roll back if oversold
        redis_client.incrby(redis_key, 1)
        raise HTTPException(status_code=400, detail="Item out of stock!")

    if kafka_producer is None:
        redis_client.incrby(redis_key, 1)
        raise HTTPException(status_code=503, detail="Kafka broker unavailable. Order was not published.")

    # 2. Build the order event payload
    order_event = {
        "sku": sku,
        "store_id": store_id,
        "status": "ORDER_PLACED",
        "remaining_stock": new_stock
    }

    # 3. Produce event to Kafka topic 'orders.created'
    await kafka_producer.send_and_wait(
        "orders.created",
        json.dumps(order_event).encode("utf-8")
    )

    return {
        "message": "Success! Item reserved and event published to Kafka.",
        "remaining_stock": new_stock
    }