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
from database.models import Inventory, Order, OrderItem, Receipt, Store, StoreMember
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
    StoreMemberCreate,
    StoreMemberResponse,
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

def get_current_user_sub(user: dict = Depends(verify_oidc_token)) -> str:
    """Extracts the stable Auth0 user id (`sub`) used as the authorization principal."""
    sub = user.get("sub")
    if not sub:
        raise HTTPException(status_code=401, detail="Token missing subject claim")
    return sub

def _get_store_membership(store_id: int, user_sub: str, db: Session) -> StoreMember | None:
    return (
        db.query(StoreMember)
        .filter(StoreMember.store_id == store_id, StoreMember.user_sub == user_sub)
        .first()
    )

def _require_store_member(store_id: int, user_sub: str, db: Session) -> StoreMember:
    member = _get_store_membership(store_id, user_sub, db)
    if not member:
        # 404 (not 403) so non-members can't distinguish "no access" from "doesn't exist"
        raise HTTPException(status_code=404, detail="Store not found")
    return member

def _require_store_owner(store_id: int, user_sub: str, db: Session) -> StoreMember:
    member = _require_store_member(store_id, user_sub, db)
    if member.role != "owner":
        raise HTTPException(status_code=403, detail="Owner role required for this action")
    return member

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
    user_sub: str = Depends(get_current_user_sub),
) -> StoreCreateResponse:
    db_store = Store(name=store.name, location_code=store.location_code, address=store.address)
    db.add(db_store)
    try:
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(status_code=409, detail="Location code already exists") from error
    db.refresh(db_store)

    # Creator is automatically granted ownership of the store they created
    db.add(StoreMember(store_id=db_store.id, user_sub=user_sub, role="owner"))
    db.commit()

    return {"message": "Store created successfully", "store_id": db_store.id}

@app.get("/stores/", response_model=list[StoreResponse])
def list_stores(
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    return (
        db.query(Store)
        .join(StoreMember, StoreMember.store_id == Store.id)
        .filter(StoreMember.user_sub == user_sub)
        .order_by(Store.id)
        .all()
    )

@app.get("/stores/{store_id}", response_model=StoreResponse)
def get_store(
    store_id: int,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_member(store_id, user_sub, db)
    return db.query(Store).filter(Store.id == store_id).first()

@app.patch("/stores/{store_id}", response_model=StoreResponse)
def update_store(
    store_id: int,
    changes: StoreUpdate,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_owner(store_id, user_sub, db)
    store = db.query(Store).filter(Store.id == store_id).first()

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
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_owner(store_id, user_sub, db)
    store = db.query(Store).filter(Store.id == store_id).first()

    for item in store.inventory:
        redis_client.delete(inventory_redis_key(item.sku, store_id))
    db.delete(store)
    db.commit()

@app.get("/stores/{store_id}/members", response_model=list[StoreMemberResponse])
def list_store_members(
    store_id: int,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_member(store_id, user_sub, db)
    return (
        db.query(StoreMember)
        .filter(StoreMember.store_id == store_id)
        .order_by(StoreMember.id)
        .all()
    )

@app.post("/stores/{store_id}/members", response_model=StoreMemberResponse, status_code=201)
def add_store_member(
    store_id: int,
    member: StoreMemberCreate,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_owner(store_id, user_sub, db)
    db_member = StoreMember(store_id=store_id, user_sub=member.user_sub, role=member.role)
    db.add(db_member)
    try:
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise HTTPException(status_code=409, detail="User is already a member of this store") from error
    db.refresh(db_member)
    return db_member

@app.delete("/stores/{store_id}/members/{member_id}", status_code=204)
def remove_store_member(
    store_id: int,
    member_id: int,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_owner(store_id, user_sub, db)
    member = (
        db.query(StoreMember)
        .filter(StoreMember.id == member_id, StoreMember.store_id == store_id)
        .first()
    )
    if not member:
        raise HTTPException(status_code=404, detail="Member not found")

    if member.role == "owner":
        owner_count = (
            db.query(StoreMember)
            .filter(StoreMember.store_id == store_id, StoreMember.role == "owner")
            .count()
        )
        if owner_count <= 1:
            raise HTTPException(status_code=400, detail="Cannot remove the last owner of a store")

    db.delete(member)
    db.commit()

@app.post("/inventory/", response_model=InventoryAddResponse)
def add_inventory(
    item: InventoryAdd, 
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
)-> InventoryAddResponse:
    _require_store_member(item.store_id, user_sub, db)

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
    user_sub: str = Depends(get_current_user_sub),
):
    if store_id is not None:
        _require_store_member(store_id, user_sub, db)
        query = db.query(Inventory).filter(Inventory.store_id == store_id)
    else:
        query = (
            db.query(Inventory)
            .join(StoreMember, StoreMember.store_id == Inventory.store_id)
            .filter(StoreMember.user_sub == user_sub)
        )
    return query.order_by(Inventory.id).all()

def _get_authorized_inventory(inventory_id: int, user_sub: str, db: Session) -> Inventory:
    item = db.query(Inventory).filter(Inventory.id == inventory_id).first()
    if not item or not _get_store_membership(item.store_id, user_sub, db):
        raise HTTPException(status_code=404, detail="Inventory item not found")
    return item

@app.get("/inventory/{inventory_id}", response_model=InventoryResponse)
def get_inventory(
    inventory_id: int,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    return _get_authorized_inventory(inventory_id, user_sub, db)

@app.patch("/inventory/{inventory_id}", response_model=InventoryResponse)
def update_inventory(
    inventory_id: int,
    changes: InventoryUpdate,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    item = _get_authorized_inventory(inventory_id, user_sub, db)

    item.quantity = changes.quantity
    db.commit()
    db.refresh(item)
    redis_client.set(inventory_redis_key(item.sku, item.store_id), item.quantity)
    return item

@app.delete("/inventory/{inventory_id}", status_code=204)
def delete_inventory(
    inventory_id: int,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    item = _get_authorized_inventory(inventory_id, user_sub, db)

    redis_client.delete(inventory_redis_key(item.sku, item.store_id))
    db.delete(item)
    db.commit()

def _get_authorized_order(order_id: str, user_sub: str, db: Session) -> Order:
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order or not _get_store_membership(order.store_id, user_sub, db):
        raise HTTPException(status_code=404, detail="Cart not found")
    return order

@app.post("/carts", response_model=CartResponse, status_code=201)
def create_cart(
    cart: CartCreate,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    _require_store_member(cart.store_id, user_sub, db)
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
    user_sub: str = Depends(get_current_user_sub),
):
    order = _get_authorized_order(order_id, user_sub, db)
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
    user_sub: str = Depends(get_current_user_sub),
):
    return _get_authorized_order(order_id, user_sub, db)

@app.post("/carts/{order_id}/confirm", response_model=OrderConfirmationResponse)
async def confirm_cart(
    order_id: str,
    db: Session = Depends(get_db),
    user_sub: str = Depends(get_current_user_sub),
):
    order = _get_authorized_order(order_id, user_sub, db)
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
    user_sub: str = Depends(get_current_user_sub),
):
    receipt = db.query(Receipt).filter(Receipt.id == receipt_id).first()
    if not receipt or not _get_store_membership(receipt.store_id, user_sub, db):
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