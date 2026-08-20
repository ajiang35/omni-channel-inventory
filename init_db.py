from database.db import engine, Base
# Import models so SQLAlchemy registers them
import database.models 

print("Creating database tables...")
Base.metadata.create_all(bind=engine)
print("Tables created successfully!")