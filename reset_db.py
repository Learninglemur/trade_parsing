#!/usr/bin/env python3

import os
import sys
from sqlmodel import SQLModel, create_engine, text
from dotenv import load_dotenv

# Add the current directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import models to ensure they are part of SQLModel metadata
from models import User, Trade, OHLCVData

# Load environment variables
load_dotenv()

# Database connection parameters
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'ooln2025!')
DB_NAME = os.getenv('DB_NAME', 'tradedb')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# Updated to use pg8000 driver
DATABASE_URL = f"postgresql+pg8000://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Explicitly list tables we want to manage
MANAGED_TABLES = ['user', 'trade', 'ohlcvdata']

def reset_database():
    """Reset the database by dropping and recreating all SQLModel tables"""
    print("Connecting to database:", DATABASE_URL)
    
    engine = create_engine(DATABASE_URL)
    
    print("Dropping SQLModel tables...")
    SQLModel.metadata.drop_all(engine)
    
    print("Creating SQLModel tables...")
    SQLModel.metadata.create_all(engine)
    
    # Verify that only desired tables were created
    with engine.connect() as conn:
        sql_query = text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)
        result = conn.execute(sql_query)
        tables = [row[0] for row in result]
        print(f"Tables in database: {tables}")
        
        # Check for any unexpected tables
        unexpected = [t for t in tables if t.lower() not in MANAGED_TABLES]
        if unexpected:
            print(f"Warning: Found unexpected tables: {unexpected}")
    
    print("Database reset successfully!")

if __name__ == "__main__":
    reset_database() 