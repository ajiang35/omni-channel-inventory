import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from jwt import PyJWKClient
from sqlalchemy.orm import Session
from sqlalchemy import text
from aiokafka import AIOKafkaProducer

from database.db import get_db, redis_client
from database.models import Store, Inventory
from schemas import StoreCreate, InventoryAdd

# --- CLEAN JWT / OIDC BEARER AUTHENTICATION ---
# This explicitly creates a single "Bearer" field in Swagger UI
security = HTTPBearer()

# Auth0 JWKS client to automatically fetch and cache public keys for token verification
AUTH0_DOMAIN = "https://dev-esduje7m3lh8oo8n.us.auth0.com/"  # Replace with your Auth0 domain
AUTH0_API_AUDIENCE = "https://api.omni-inventory.com"    # Your Auth0 API Identifier
jwks_url = f"{AUTH0_DOMAIN}.well-known/jwks.json"
jwks_client = PyJWKClient(jwks_url)

def verify_oidc_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    token = credentials.credentials
    try:
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=AUTH0_API_AUDIENCE,
            issuer=AUTH0_DOMAIN,
        )
        return payload  # Returns the decoded user claims/token data
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Invalid or expired token: {str(e)}")

# Global variable for the Kafka producer
kafka_producer: AIOKafkaProducer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer
    kafka_producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await kafka_producer.start()
    print("Kafka Producer started successfully!")
    
    yield
    
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

# --- SECURED ENDPOINTS ---

@app.post("/stores/")
def create_store(
    store: StoreCreate, 
    db: Session = Depends(get_db), 
    user: dict = Depends(verify_oidc_token)  # <--- Clean single Bearer security dependency
):
    db_store = Store(name=store.name, location_code=store.location_code, address=store.address)
    db.add(db_store)
    db.commit()
    db.refresh(db_store)
    return {"message": "Store created successfully", "store_id": db_store.id}

@app.post("/inventory/")
def add_inventory(
    item: InventoryAdd, 
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token)
):
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
async def purchase_item(
    sku: str, 
    store_id: int, 
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token)
):
    redis_key = f"inventory:{sku}:store:{store_id}"
    
    new_stock = redis_client.decrby(redis_key, 1)
    
    if new_stock < 0:
        redis_client.incrby(redis_key, 1)
        raise HTTPException(status_code=400, detail="Item out of stock!")
    
    order_event = {
        "sku": sku,
        "store_id": store_id,
        "status": "ORDER_PLACED",
        "remaining_stock": new_stock
    }
    
    await kafka_producer.send_and_wait(
        "orders.created", 
        json.dumps(order_event).encode("utf-8")
    )
    
    return {
        "message": "Success! Item reserved and event published to Kafka.", 
        "remaining_stock": new_stock
    }