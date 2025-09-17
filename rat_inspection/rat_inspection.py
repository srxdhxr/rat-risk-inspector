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

class RatInspectionDataFetcher:
    def __init__(self,app_token):
        self.name = "RAT_INSPECTION"
        self.API_URL = "https://data.cityofnewyork.us/resource/p937-wjvj.json"
        self.headers = {"X-App-Token": app_token}
        self.store = "data/rat_inspection"
        self.today = datetime.now().strftime('%Y%m%d')
        self.log_file_name = f"data/rat_inspection/{self.today}/logs.log"

        self.__check_store()
        self.__setup_logger()

        self.table_name = "rat_inspection"

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
                print('Retrieved data')
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
        print(df.dtypes)
        schema  = pa.schema([
                            ("inspection_type", pa.string()),
                            ("job_ticket_or_work_order_id", pa.string()),
                            ("job_id", pa.string()),
                            ("job_progress", pa.string()),
                            ("bbl", pa.string()),
                            ("boro_code", pa.string()),
                            ("block", pa.string()),
                            ("lot", pa.string()),
                            ("house_number", pa.string()),               #
                            ("street_name", pa.string()),
                            ("zip_code", pa.string()),
                            ("x_coord", pa.string()),
                            ("y_coord", pa.string()),
                            ("latitude", pa.string()),
                            ("longitude", pa.string()),
                            ("borough", pa.string()),
                            ("inspection_date", pa.string()),
                            ("result", pa.string()),
                            ("approved_date", pa.string()),
                            ("location", pa.struct([
                                ("lat", pa.float64()),
                                ("lon", pa.float64())
                            ])),
                            ('bin',pa.string()),
                            ('nta',pa.string()),
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
        conn.execute(f"COPY raw.{self.table_name} FROM '{os.path.abspath(parquet_file)}' (FORMAT 'parquet')")
        print(f"Data copied to MotherDuck: {self.table_name}")


app_token = os.getenv("APP_TOKEN")
ridf = RatInspectionDataFetcher(app_token)
#ridf.fetch_all_data(save_as='parquet')
ridf.copy_raw("20250915")
