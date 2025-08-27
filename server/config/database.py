import os
import urllib
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'driver': os.getenv('DB_DRIVER'),
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'username': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'trust_cert': os.getenv('DB_TRUST_CERT', 'no'),
}


DB_SCHEMA = "dbo"
Base = declarative_base()

def create_connection_string():
    """Create a properly formatted connection string for MS SQL Server"""
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate={'yes' if DB_CONFIG['trust_cert'].lower() == 'yes' else 'no'};"
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"

# Create engine
engine = create_engine(
    create_connection_string(),
    echo=True,  # Set to False in production
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=3600,
)

# Event listener to create schema if it doesn't exist
@event.listens_for(engine, 'connect')
def create_schema(dbapi_connection, connection_record):
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = '{DB_SCHEMA}'
            )
            BEGIN
                EXEC('CREATE SCHEMA {DB_SCHEMA}')
            END
        """)
        cursor.close()
        dbapi_connection.commit()
    except Exception as e:
        logger.error(f"Error creating schema: {e}")

# Create session factory
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False
)

def get_db():
    """Provide a database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def test_connection():
    """Test database connection"""
    try:
        with engine.connect() as connection:
            logger.info("Successfully connected to the database!")
            return True
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return False
test_connection()
