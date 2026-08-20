import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import redis

# PostgreSQL Connection URL from Docker Compose
POSTGRES_URL = "postgresql://postgres:password123@localhost:5432/inventory_db"

engine = create_engine(POSTGRES_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis Connection
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()