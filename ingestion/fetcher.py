import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import logging
import json
import pyarrow.parquet as pq
import pyarrow as pa
import sys, duckdb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataFetcher:
    def __init__(self, name, api_url, headers, store_path, table_name):
        self.name = name
        self.API_URL = api_url
        self.headers = headers
        self.store = store_path
        self.today = datetime.now().strftime('%Y%m%d')
        self.log_file_name = f"{self.store}/{self.today}/logs.log"
        self.table_name = table_name

        self.__check_store()
        self.__setup_logger()


    def __setup_logger(self):
        logging.basicConfig(
            filename=self.log_file_name,
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logger Initialized Successfully")

    def __check_store(self):
        if not os.path.exists(self.store):
            try:
                os.makedirs(self.store)
            except Exception as e:
                print(f"Could not create directory '{self.store}': {e}")

        if not os.path.exists(self.store+'/'+self.today):
            try:
                os.makedirs(self.store+'/'+self.today)
            except Exception as e:
                print(f"Could not create directory '{self.store}/{self.today}': {e}")

    def __fetch_data_chunk(self, chunk_size=50000):
        offset = 0
        print(f"DEBUG: Starting data fetch from {self.API_URL}")

        while True:
            params = {"$limit": chunk_size, "$offset": offset}
            try:
                print(f'DEBUG: Fetching chunk at offset {offset}')
                response = requests.get(self.API_URL, params=params, headers=self.headers)
                print(f"DEBUG: Response status: {response.status_code}")
                data = response.json()
                print(f"DEBUG: Retrieved {len(data)} records")
                if not data:
                    print("DEBUG: No more data, breaking")
                    break
                yield data
                offset += chunk_size
                
            except Exception as e:
                print(f"DEBUG: Error in fetch_data_chunk: {e}")
                self.logger.error(e)
    
    def __save_as_json(self,data,index):
        current_date = datetime.now().strftime("%Y%m%d")
        folder = f"{self.store}/{current_date}/RAW/JSON"

        if not os.path.exists(folder):
            os.makedirs(folder)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{folder}/{self.name}_{timestamp}_{index}.json"
        with open(filename, "w") as f:
            json.dump(data, f)

    def __save_as_parquet(self, data, index):
        df = pd.DataFrame(data)
        current_date = datetime.now().strftime("%Y%m%d")
        folder = f"{self.store}/{current_date}/PARQUET"

        if not os.path.exists(folder):
            os.makedirs(folder)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{folder}/{self.name}_{timestamp}_{index}.parquet"
        
        # Use default schema for parquet
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, where=filename)
        self.logger.info(f"Parquet saved to {filename}")


    def fetch_all_data(self,save_as = 'json'):
        for i, chunk in enumerate(self.__fetch_data_chunk()):
            self.logger.info(f"Fetching chunk {i}")
            if save_as == 'json':
                self.__save_as_json(chunk,i)
            elif save_as == 'parquet':
                self.__save_as_parquet(chunk,i)
            else:
                self.logger.error(f"Unknown save_as: {save_as}")

    def copy_raw(self,date):
        md_token = os.environ.get("MD_TOKEN")
        parquet_file = f"{self.store}/{date}/PARQUET/*.parquet"

        conn = duckdb.connect(f"md:MY_WH", config={'motherduck_token': md_token})
        conn.execute(f"COPY raw.{self.table_name} FROM '{os.path.abspath(parquet_file)}' (FORMAT 'parquet')")
        print(f"Data copied to MotherDuck: {self.table_name}")


        
        