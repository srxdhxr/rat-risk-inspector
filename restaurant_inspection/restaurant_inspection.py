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

class RestaurantDataFetcher:
    def __init__(self,app_token):
        self.name = "RESTAURANT_INSPECTION"
        self.API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
        self.headers = {"X-App-Token": app_token}
        self.store = "data/restaurant_inspection"
        self.today = datetime.now().strftime('%Y%m%d')
        self.log_file_name = f"data/restaurant_inspection/{self.today}/logs.log"

        self.__check_store()
        self.__setup_logger()

        self.table_name = "restaurant_inspection"

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
            except:
                self.logger.error("Could not create directory '{}'".format(self.store))

        if not os.path.exists(self.store+'/'+self.today):
            try:
                os.makedirs(self.store+'/'+self.today)
            except:
                self.logger.error("Could not create directory '{}'".format(self.store+'/'+self.today))

    def __fetch_data_chunk(self, chunk_size=50000):
        offset = 0

        while True:
            params = {"$limit": chunk_size, "$offset": offset}
            try:
                response = requests.get(self.API_URL, params=params, headers=self.headers)
                data = response.json()
                if not data:
                    break
                yield data
                offset += chunk_size
            except Exception as e:
                self.logger.error(e)

    def __save_as_parquet(self, data,index):
        df = pd.DataFrame(data)

        schema  = pa.schema([
                            ("camis", pa.string()),
                            ("dba", pa.string()),
                            ("boro", pa.string()),
                            ("building", pa.string()),
                            ("street", pa.string()),
                            ("zipcode", pa.string()),
                            ("phone", pa.string()),
                            ("inspection_date", pa.string()),
                            ("critical_flag", pa.string()),               #
                            ("record_date", pa.string()),
                            ("bin", pa.string()),
                            ("bbl", pa.string()),
                            ("nta", pa.string()),
                            ("cuisine_description", pa.string()),
                            ("action", pa.string()),
                            ("violation_code", pa.string()),
                            ("violation_description", pa.string()),
                            ("score", pa.string()),
                            ("grade", pa.string()),
                            ("grade_date",pa.string()),
                            ('latitude',pa.string()),
                            ('longitude',pa.string()),
                            ("inspection_type", pa.string()),
                        ])

        current_date = datetime.now().strftime("%Y%m%d")
        folder = f"{self.store}/{current_date}/RAW/PARQUET"

        if not os.path.exists(folder):
            os.makedirs(folder)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{folder}/{self.name}_{timestamp}_{index}.parquet"
        table = pa.Table.from_pandas(df, schema= schema, preserve_index=False)
        pq.write_table(table, where = filename)
        self.logger.info(f"Parquet saved to {filename}")


    def __save_as_json(self,data,index):
        current_date = datetime.now().strftime("%Y%m%d")
        folder = f"{self.store}/{current_date}/RAW/JSON"

        if not os.path.exists(folder):
            os.makedirs(folder)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{folder}/{self.name}_{timestamp}_{index}.json"
        with open(filename, "w") as f:
            json.dump(data, f)


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
        parquet_file = f"{self.store}/{date}/RAW/PARQUET/*.parquet"

        conn = duckdb.connect(f"md:MY_WH", config={'motherduck_token': md_token})
        conn.execute(f"COPY raw.restaurant_inspection FROM '{os.path.abspath(parquet_file)}' (FORMAT 'parquet')")
        print("Data copied to MotherDuck: restaurant_inspection_raw")


app_token = os.getenv("APP_TOKEN")
rdf = RestaurantDataFetcher(app_token)
#rdf.fetch_all_data(save_as='parquet')
rdf.copy_raw(20250915)
