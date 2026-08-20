from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database.db import get_db, redis_client

app = FastAPI(title="Omni-Channel Inventory Mesh")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Omni-Channel Inventory Mesh API!"}

@app.get("/health/db")
def test_databases(db: Session = Depends(get_db)):
    try:
        # Test PostgreSQL connection
        db.execute(text("SELECT 1"))
        pg_status = "Connected successfully!"
    except Exception as e:
        pg_status = f"Failed: {str(e)}"
        
    try:
        # Test Redis connection
        redis_client.ping()
        redis_status = "Connected successfully!"
    except Exception as e:
        redis_status = f"Failed: {str(e)}"
        
    return {
        "postgres": pg_status,
        "redis": redis_status
    }