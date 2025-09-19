import duckdb
import os
import pandas as pd
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
import logging
from sqlalchemy import create_engine, text
import numpy as np

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

class DataTransporter:
    def __init__(self, local_dump=False):
        # MotherDuck connection
        self.md_conn = duckdb.connect(f"md:MY_WH", config={
            'motherduck_token': os.environ.get("MD_TOKEN")
        })
        
        # MySQL connection using SQLAlchemy (more robust)
        mysql_url = f"mysql+mysqlconnector://{os.environ.get('MYSQL_USER')}:{os.environ.get('MYSQL_PASSWORD')}@{os.environ.get('MYSQL_HOST')}/{os.environ.get('MYSQL_DB')}?charset=utf8mb4"
        self.mysql_engine = create_engine(mysql_url, pool_pre_ping=True, pool_recycle=3600)
        
        # Local dump flag
        self.local_dump = local_dump
        
        # Tables to sync
        self.tables = [
            # "mart_restaurant_inspection", 
            # "mart_rat_inspection", 
            "mart_rat_risk_model"
        ]

    def clean_dataframe(self, df):
        """Clean DataFrame for MySQL compatibility"""
        df_clean = df.copy()
        
        for col in df_clean.columns:
            # Convert datetime columns to strings
            if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].astype(str)
                df_clean[col] = df_clean[col].replace('NaT', None)
                logger.info(f"Converted datetime column: {col}")
            
            # Handle other problematic types
            elif df_clean[col].dtype == 'object':
                # Replace pandas NaT, NaN with None
                df_clean[col] = df_clean[col].replace({pd.NaT: None, np.nan: None})
            
            # Convert nullable integers
            elif str(df_clean[col].dtype).startswith(('Int', 'Float')):
                df_clean[col] = df_clean[col].astype('object').where(df_clean[col].notna(), None)
        
        return df_clean

    def sync_table(self, table):
        """Sync a single table using pandas to_sql"""
        try:
            # Extract from MotherDuck
            query = f"SELECT * FROM main_mart.{table}"
            df = self.md_conn.execute(query).df()
            
            if df.empty:
                logger.warning(f"No data found for {table}")
                return False
            
            logger.info(f"Extracted {len(df)} rows from {table}")
            
            # Clean the DataFrame
            df_clean = self.clean_dataframe(df)
            
            # Save to local CSV if local_dump is enabled
            if self.local_dump:
                csv_filename = f"{table}.csv"
                df_clean.to_csv(csv_filename, index=False)
                logger.info(f"Saved {table} to {csv_filename}")
            
            # Load to MySQL using pandas (handles everything automatically)
            df_clean.to_sql(
                name=table,
                con=self.mysql_engine,
                if_exists='replace',  # This handles truncate/recreate
                index=False,
                chunksize=1000,
                method='multi'  # Faster bulk inserts
            )
            
            logger.info(f"Successfully synced {table}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to sync {table}: {e}")
            return False

    def sync_all(self):
        """Sync all tables"""
        logger.info("Starting data sync...")
        
        success_count = 0
        for table in self.tables:
            if self.sync_table(table):
                success_count += 1
        
        logger.info(f"Completed: {success_count}/{len(self.tables)} tables synced")

    def close(self):
        """Close connections"""
        self.md_conn.close()
        self.mysql_engine.dispose()

if __name__ == "__main__":
    import sys
    
    # Check if local_dump flag is passed
    local_dump = '--local-dump' in sys.argv
    
    transporter = DataTransporter(local_dump=local_dump)
    try:
        transporter.sync_all()
    finally:
        transporter.close()