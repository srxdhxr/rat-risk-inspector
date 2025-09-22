import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
import glob
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgresLoader:
    def __init__(self):
        # PostgreSQL connection parameters
        self.host = os.getenv("PG_HOST")
        self.port = os.getenv("PG_PORT")
        self.database = os.getenv("PG_DB")
        self.username = os.getenv("PG_USERNAME")
        self.password = os.getenv("PG_PASSWORD")
        
        # Data directory
        self.data_dir = "data/clean/MY_WH/main_mart/20250921"
        
        # Create SQLAlchemy engine
        self.engine = create_engine(
            f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode=require",
            pool_pre_ping=True,
            pool_recycle=3600
        )
        
        # Table definitions
        self.tables = {
            "mart_rat_inspection": {
                "file_pattern": "mart_rat_inspection.parquet",
                "schema": "public"
            },
            "mart_rat_risk_model": {
                "file_pattern": "mart_rat_risk_model.parquet", 
                "schema": "public"
            },
            "mart_restaurant_inspection": {
                "file_pattern": "mart_restaurant_inspection.parquet",
                "schema": "public"
            }
        }

    def create_tables(self):
        """Create tables in PostgreSQL if they don't exist"""
        create_statements = {
            "mart_rat_inspection": """
                CREATE TABLE IF NOT EXISTS public.mart_rat_inspection(
                    inspection_type VARCHAR,
                    work_order_id INTEGER,
                    job_id VARCHAR,
                    job_progress INTEGER,
                    bbl BIGINT,
                    boro_code INTEGER,
                    block VARCHAR,
                    lot VARCHAR,
                    house_number VARCHAR,
                    street VARCHAR,
                    zipcode VARCHAR,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    borough VARCHAR,
                    inspection_date TIMESTAMP,
                    result VARCHAR,
                    approved_date TIMESTAMP,
                    bin INTEGER,
                    nta VARCHAR,
                    extract_date DATE
                );
            """,
            "mart_rat_risk_model": """
                CREATE TABLE IF NOT EXISTS public.mart_rat_risk_model(
                    camis INTEGER,
                    dba VARCHAR,
                    boro VARCHAR,
                    building VARCHAR,
                    bin INTEGER,
                    cuisine_description VARCHAR,
                    latitude DECIMAL(18,12),
                    longitude DECIMAL(18,12),
                    zipcode VARCHAR,
                    most_recent_inspection_date TIMESTAMP,
                    restaurant_risk_score FLOAT,
                    street_risk_score DOUBLE PRECISION,
                    zip_risk_score DOUBLE PRECISION,
                    bin_rat_score FLOAT,
                    street_rat_score FLOAT,
                    zip_rat_score FLOAT,
                    rat_risk_score DOUBLE PRECISION,
                    risk_category VARCHAR,
                    extract_date DATE
                );
            """,
            "mart_restaurant_inspection": """
                CREATE TABLE IF NOT EXISTS public.mart_restaurant_inspection(
                    inspection_order_id VARCHAR,
                    camis INTEGER,
                    dba VARCHAR,
                    dba_lower VARCHAR,
                    boro VARCHAR,
                    building VARCHAR,
                    street VARCHAR,
                    zipcode VARCHAR,
                    inspection_date TIMESTAMP,
                    critical_flag VARCHAR,
                    record_date TIMESTAMP,
                    bin INTEGER,
                    bbl VARCHAR,
                    nta VARCHAR,
                    cuisine_description VARCHAR,
                    "action" VARCHAR,
                    violation_code VARCHAR,
                    violation_description VARCHAR,
                    score INTEGER,
                    computed_score DOUBLE PRECISION,
                    grade VARCHAR,
                    computed_grade VARCHAR,
                    grade_date TIMESTAMP,
                    latitude DECIMAL(18,12),
                    longitude DECIMAL(18,12),
                    extract_date DATE
                );
            """
        }
        
        with self.engine.connect() as conn:
            for table_name, create_sql in create_statements.items():
                try:
                    conn.execute(text(create_sql))
                    conn.commit()
                    logger.info(f"Created/verified table: {table_name}")
                except Exception as e:
                    logger.error(f"Error creating table {table_name}: {e}")

    def load_table(self, table_name, file_path):
        """Load a single parquet file into PostgreSQL table"""
        try:
            logger.info(f"Loading {table_name} from {file_path}")
            
            # Read parquet file
            df = pd.read_parquet(file_path)
            logger.info(f"Read {len(df)} rows from {file_path}")
            
            # Clean data for PostgreSQL
            df = self.clean_dataframe(df)
            
            # Truncate existing data
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE public.{table_name}"))
                conn.commit()
            
            # Load data using pandas to_sql
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema='public',
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Successfully loaded {len(df)} rows into {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading {table_name}: {e}")
            return False

    def clean_dataframe(self, df):
        """Clean DataFrame for PostgreSQL compatibility"""
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Handle datetime columns
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].dt.tz_localize(None)  # Remove timezone
                df_clean[col] = df_clean[col].replace(pd.NaT, None)
            
            # Handle nullable integers
            elif str(df_clean[col].dtype).startswith('Int'):
                df_clean[col] = df_clean[col].astype('object').where(df_clean[col].notna(), None)
            
            # Handle nullable floats
            elif str(df_clean[col].dtype).startswith('Float'):
                df_clean[col] = df_clean[col].astype('object').where(df_clean[col].notna(), None)
            
            # Handle object columns
            elif df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].replace({pd.NaT: None, '': None})
        
        return df_clean

    def load_all_tables(self):
        """Load all tables from parquet files"""
        logger.info(f"Starting data load from {self.data_dir}")
        
        # Create tables first
        self.create_tables()
        
        success_count = 0
        total_tables = len(self.tables)
        
        for table_name, config in self.tables.items():
            file_pattern = os.path.join(self.data_dir, config["file_pattern"])
            
            if os.path.exists(file_pattern):
                if self.load_table(table_name, file_pattern):
                    success_count += 1
            else:
                logger.warning(f"File not found: {file_pattern}")
        
        logger.info(f"Completed: {success_count}/{total_tables} tables loaded successfully")
        return success_count == total_tables

    def test_connection(self):
        """Test PostgreSQL connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                logger.info("PostgreSQL connection successful")
                return True
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            return False

def main():
    loader = PostgresLoader()
    
    # Test connection first
    if not loader.test_connection():
        logger.error("Cannot connect to PostgreSQL. Exiting.")
        return
    
    # Load all tables
    success = loader.load_all_tables()
    
    if success:
        logger.info("All tables loaded successfully!")
    else:
        logger.error("Some tables failed to load. Check logs for details.")

if __name__ == "__main__":
    main()
