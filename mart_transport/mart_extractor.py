import os
import duckdb
from datetime import datetime


class MartExtractor:
    def __init__(self, warehouse_name):
        self.warehouse_name = warehouse_name
        self.today = datetime.now().strftime('%Y%m%d')
        self.md_token = os.environ.get("MD_TOKEN")
        self.md_conn = duckdb.connect(f"md:{self.warehouse_name}", config={'motherduck_token': self.md_token})
        self.file_store = f"data/clean/{self.warehouse_name}"

        
    def dump_mart(self,schema_name,table_name):
        table_location = f"{self.file_store}/{schema_name}/{self.today}/{table_name}.parquet"

        os.makedirs(os.path.dirname(table_location),exist_ok=True)

        self.md_conn.execute(f"COPY {schema_name}.{table_name} TO '{table_location}' (FORMAT 'parquet')")
        print(f"Mart {table_name} dumped to {table_location}")



if __name__ == "__main__":
    mart_extractor = MartExtractor("MY_WH")
    mart_extractor.dump_mart("main_mart","mart_rat_risk_model")
    mart_extractor.dump_mart("main_mart","mart_rat_inspection")
    mart_extractor.dump_mart("main_mart","mart_restaurant_inspection")
    