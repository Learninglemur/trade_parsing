import os
from sqlmodel import create_engine, Session, SQLModel, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL Configuration
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'ooln2025!')
DB_NAME = os.getenv('DB_NAME', 'tradedb')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')

# Updated to use pg8000 driver
DATABASE_URL = f"postgresql+pg8000://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine with connection pooling configured for better stability
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Check connection before using from pool
    pool_recycle=3600,   # Recycle connections after 1 hour
    echo=False           # Set to True for SQL query logging
)

# Tables managed by SQLModel
MANAGED_TABLES = ['user', 'trade', 'ohlcvdata']

def create_db_and_tables():
    """Create all tables defined in SQLModel metadata"""
    SQLModel.metadata.create_all(engine)
    
    # Verify that only desired tables were created
    try:
        with engine.connect() as conn:
            sql_query = text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            result = conn.execute(sql_query)
            tables = [row[0] for row in result]
            
            # Check for any unexpected tables
            unexpected = [t for t in tables if t.lower() not in MANAGED_TABLES]
            if unexpected:
                print(f"Warning: Found unexpected tables: {unexpected}")
    except Exception as e:
        print(f"Warning: Could not verify tables: {e}")
        print("Database tables created but table verification failed")

def get_session():
    """Get a database session"""
    with Session(engine) as session:
        yield session

# Function to reset the database (delete all rows)
def reset_database():
    """Reset all tables by dropping and recreating them"""
    print("Starting database reset...")
    try:
        # Drop all tables (this will remove all data)
        SQLModel.metadata.drop_all(engine)
        
        # Recreate all tables (this will create empty tables with the schema intact)
        SQLModel.metadata.create_all(engine)
        
        # Verify that only desired tables were created
        try:
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
        except Exception as e:
            print(f"Warning: Could not verify tables: {e}")
        
        print("Database reset completed successfully!")
    except Exception as e:
        print(f"Error resetting database: {e}") 