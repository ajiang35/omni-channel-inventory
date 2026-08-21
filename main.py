import json
import os
from uuid import uuid4
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from jwt import PyJWKClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy import text
from aiokafka import AIOKafkaProducer

from database.db import get_db, redis_client
from database.models import Inventory, Order, OrderItem, Receipt, Store
from schemas import (
    InventoryAdd,
    InventoryAddResponse,
    InventoryResponse,
    InventoryUpdate,
    CartCreate,
    CartItemCreate,
    CartResponse,
    OrderConfirmationResponse,
    ReceiptResponse,
    StoreCreate,
    StoreCreateResponse,
    StoreResponse,
    StoreUpdate,
)

# --- CLEAN JWT / OIDC BEARER AUTHENTICATION ---
# This explicitly creates a single "Bearer" field in Swagger UI
security = HTTPBearer()

# Auth0 JWKS client to automatically fetch and cache public keys for token verification
AUTH0_DOMAIN = os.getenv(
    "AUTH0_DOMAIN",
    "https://dev-esduje7m3lh8oo8n.us.auth0.com/",
)
AUTH0_API_AUDIENCE = os.getenv(
    "AUTH0_API_AUDIENCE",
    "https://api.omni-inventory.com",
)
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
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    await kafka_producer.start()
    print("Kafka Producer started successfully!")
    
    yield
    
    await kafka_producer.stop()
    print("Kafka Producer stopped.")

app = FastAPI(title="Omni-Channel Inventory Mesh", lifespan=lifespan)

def inventory_redis_key(sku: str, store_id: int) -> str:
    return f"inventory:{sku}:store:{store_id}"

RESERVE_CART_SCRIPT = """
for index, key in ipairs(KEYS) do
    local available = tonumber(redis.call('GET', key) or '0')
    local requested = tonumber(ARGV[index])
    if available < requested then
        return 0
    end
end
for index, key in ipairs(KEYS) do
    redis.call('DECRBY', key, ARGV[index])
end
return 1
"""

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

@app.post("/stores/", response_model=StoreCreateResponse)
def create_store(
    store: StoreCreate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
) -> StoreCreateResponse:
    db_store = Store(name=store.name, location_code=store.location_code, address=store.address)
    db.add(db_store)
    try:
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(status_code=409, detail="Location code already exists") from error
    db.refresh(db_store)
    return {"message": "Store created successfully", "store_id": db_store.id}

@app.get("/stores/", response_model=list[StoreResponse])
def list_stores(
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    return db.query(Store).order_by(Store.id).all()

@app.get("/stores/{store_id}", response_model=StoreResponse)
def get_store(
    store_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
    return store

@app.patch("/stores/{store_id}", response_model=StoreResponse)
def update_store(
    store_id: int,
    changes: StoreUpdate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    for field, value in changes.model_dump(exclude_unset=True).items():
        setattr(store, field, value)

    try:
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(status_code=409, detail="Location code already exists") from error
    db.refresh(store)
    return store

@app.delete("/stores/{store_id}", status_code=204)
def delete_store(
    store_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    store = db.query(Store).filter(Store.id == store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    for item in store.inventory:
        redis_client.delete(inventory_redis_key(item.sku, store_id))
    db.delete(store)
    db.commit()

@app.post("/inventory/", response_model=InventoryAddResponse)
def add_inventory(
    item: InventoryAdd, 
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token)
)-> InventoryAddResponse:
    store = db.query(Store).filter(Store.id == item.store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")
        
    db_item = Inventory(sku=item.sku, quantity=item.quantity, store_id=item.store_id)
    db.add(db_item)
    try:
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Inventory for this SKU already exists at this store",
        ) from error
    db.refresh(db_item)
    
    redis_key = inventory_redis_key(item.sku, item.store_id)
    redis_client.set(redis_key, item.quantity)
    
    return {"message": "Inventory added successfully", "sku": db_item.sku, "quantity": db_item.quantity}

@app.get("/inventory/", response_model=list[InventoryResponse])
def list_inventory(
    store_id: int | None = None,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    query = db.query(Inventory).order_by(Inventory.id)
    if store_id is not None:
        query = query.filter(Inventory.store_id == store_id)
    return query.all()

@app.get("/inventory/{inventory_id}", response_model=InventoryResponse)
def get_inventory(
    inventory_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    item = db.query(Inventory).filter(Inventory.id == inventory_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Inventory item not found")
    return item

@app.patch("/inventory/{inventory_id}", response_model=InventoryResponse)
def update_inventory(
    inventory_id: int,
    changes: InventoryUpdate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    item = db.query(Inventory).filter(Inventory.id == inventory_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Inventory item not found")

    item.quantity = changes.quantity
    db.commit()
    db.refresh(item)
    redis_client.set(inventory_redis_key(item.sku, item.store_id), item.quantity)
    return item

@app.delete("/inventory/{inventory_id}", status_code=204)
def delete_inventory(
    inventory_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    item = db.query(Inventory).filter(Inventory.id == inventory_id).first()
    if not item:
        raise HTTPException(status_code=404, detail="Inventory item not found")

    redis_client.delete(inventory_redis_key(item.sku, item.store_id))
    db.delete(item)
    db.commit()

@app.post("/carts", response_model=CartResponse, status_code=201)
def create_cart(
    cart: CartCreate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    if not db.query(Store).filter(Store.id == cart.store_id).first():
        raise HTTPException(status_code=404, detail="Store not found")
    order = Order(id=str(uuid4()), store_id=cart.store_id, status="CART")
    db.add(order)
    db.commit()
    db.refresh(order)
    return order

@app.post("/carts/{order_id}/items", response_model=CartResponse)
def add_cart_item(
    order_id: str,
    item: CartItemCreate,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Cart not found")
    if order.status != "CART":
        raise HTTPException(status_code=409, detail="Cart is no longer editable")
    if not db.query(Inventory).filter(
        Inventory.sku == item.sku, Inventory.store_id == order.store_id
    ).first():
        raise HTTPException(status_code=404, detail="Inventory item not found at this store")

    cart_item = db.query(OrderItem).filter(
        OrderItem.order_id == order_id, OrderItem.sku == item.sku
    ).first()
    if cart_item:
        cart_item.quantity += item.quantity
    else:
        order.items.append(OrderItem(sku=item.sku, quantity=item.quantity))
    db.commit()
    db.refresh(order)
    return order

@app.get("/carts/{order_id}", response_model=CartResponse)
def get_cart(
    order_id: str,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Cart not found")
    return order

@app.post("/carts/{order_id}/confirm", response_model=OrderConfirmationResponse)
async def confirm_cart(
    order_id: str,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Cart not found")
    if order.status != "CART":
        raise HTTPException(status_code=409, detail="Cart is already confirmed")
    if not order.items:
        raise HTTPException(status_code=400, detail="Cart is empty")

    keys = [inventory_redis_key(item.sku, order.store_id) for item in order.items]
    quantities = [str(item.quantity) for item in order.items]
    reserved = redis_client.eval(RESERVE_CART_SCRIPT, len(keys), *keys, *quantities)
    if reserved != 1:
        raise HTTPException(status_code=400, detail="Insufficient stock for one or more items")

    order_event = {
        "order_id": order.id,
        "store_id": order.store_id,
        "status": "ORDER_PLACED",
        "items": [{"sku": item.sku, "quantity": item.quantity} for item in order.items],
    }

    try:
        await kafka_producer.send_and_wait("orders.created", json.dumps(order_event).encode("utf-8"))
        order.status = "CONFIRMED"
        receipt_id = str(uuid4())
        receipt = Receipt(
            id=receipt_id,
            receipt_number=f"R-{receipt_id[:12].upper()}",
            order_id=order.id,
            store_id=order.store_id,
            lines=[
                {"sku": item.sku, "quantity": item.quantity}
                for item in order.items
            ],
            total_units=sum(item.quantity for item in order.items),
        )
        db.add(receipt)
        db.commit()
        db.refresh(receipt)
    except Exception as error:
        db.rollback()
        for key, quantity in zip(keys, quantities):
            redis_client.incrby(key, int(quantity))
        raise HTTPException(status_code=503, detail="Order event service unavailable") from error

    return OrderConfirmationResponse(
        order_id=order.id,
        status=order.status,
        message="Order confirmed and published to Kafka",
        receipt=ReceiptResponse(
            id=receipt.id,
            receipt_number=receipt.receipt_number,
            order_id=receipt.order_id,
            store_id=receipt.store_id,
            issued_at=receipt.issued_at.isoformat(),
            lines=receipt.lines,
            total_units=receipt.total_units,
        ),
    )

@app.get("/receipts/{receipt_id}", response_model=ReceiptResponse)
def get_receipt(
    receipt_id: str,
    db: Session = Depends(get_db),
    user: dict = Depends(verify_oidc_token),
):
    receipt = db.query(Receipt).filter(Receipt.id == receipt_id).first()
    if not receipt:
        raise HTTPException(status_code=404, detail="Receipt not found")
    return ReceiptResponse(
        id=receipt.id,
        receipt_number=receipt.receipt_number,
        order_id=receipt.order_id,
        store_id=receipt.store_id,
        issued_at=receipt.issued_at.isoformat(),
        lines=receipt.lines,
        total_units=receipt.total_units,
    )