import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer
from sqlalchemy.orm import Session
from database.db import SessionLocal
from database.models import Inventory

async def consume_orders():
    # 1. Initialize the Kafka Consumer
    consumer = AIOKafkaConsumer(
        "orders.created",
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="inventory-update-group",
        auto_offset_reset="earliest"
    )
    
    await consumer.start()
    print("Kafka Consumer Worker started and listening for orders...")
    
    try:
        # 2. Continuously listen for incoming messages
        async for msg in consumer:
            event_data = json.loads(msg.value.decode("utf-8"))
            sku = event_data.get("sku")
            store_id = event_data.get("store_id")
            
            print(f"Received Order Event: SKU {sku} at Store {store_id}")
            
            # 3. Update PostgreSQL asynchronously using a database session
            db: Session = SessionLocal()
            try:
                inventory_item = db.query(Inventory).filter(
                    Inventory.sku == sku, 
                    Inventory.store_id == store_id
                ).first()
                
                if inventory_item:
                    # Decrement the actual database quantity to stay synced with Redis
                    if inventory_item.quantity > 0:
                        inventory_item.quantity -= 1
                        db.commit()
                        print(f"DB Updated: SKU {sku} new quantity is {inventory_item.quantity}")
                    else:
                        print(f"Warning: DB quantity already 0 for SKU {sku}")
                else:
                    print(f"Error: Inventory record not found for SKU {sku} at Store {store_id}")
            except Exception as e:
                db.rollback()
                print(f"Database error while processing event: {str(e)}")
            finally:
                db.close()
                
    finally:
        await consumer.stop()

if __name__ == "__main__":
    # Run the async consumer loop
    asyncio.run(consume_orders())