import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from dotenv import load_dotenv
import duckdb
from ingestion.fetcher import DataFetcher
import sys

load_dotenv()

class RatInspectionDataFetcher(DataFetcher):
    def __init__(self, app_token):
        super().__init__(
            name="RAT_INSPECTION",
            api_url="https://data.cityofnewyork.us/resource/p937-wjvj.json",
            headers={"X-App-Token": app_token},
            store_path="data/raw/rat_inspection",
            table_name="rat_inspection"
        )

        self.schema = pa.schema([
            ("inspection_type", pa.string()),
            ("job_ticket_or_work_order_id", pa.string()),
            ("job_id", pa.string()),
            ("job_progress", pa.string()),
            ("bbl", pa.string()),
            ("boro_code", pa.string()),
            ("block", pa.string()),
            ("lot", pa.string()),
            ("house_number", pa.string()),
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
                ("human_address", pa.string()),
                ("latitude", pa.string()),
                ("longitude", pa.string())
            ])),
            ("community_board", pa.string()),
            ("council_district", pa.string()),
            ("census_tract", pa.string()),
            ("bin", pa.string()),
            ("nta", pa.string()),
            (":@computed_region_92fq_4b7q", pa.string()),
            (":@computed_region_f5dn_yrer", pa.string()),
            (":@computed_region_yeji_bk3q", pa.string()),
            (":@computed_region_efsh_h5xi", pa.string()),
            (":@computed_region_sbqj_enih", pa.string()),
        ])

    def __save_as_parquet(self, data, index):
        """Override parent method to use custom schema"""
        df = pd.DataFrame(data)
        
        current_date = datetime.now().strftime("%Y%m%d")
        folder = f"{self.store}/{current_date}/RAW/PARQUET"

        if not os.path.exists(folder):
            os.makedirs(folder)

        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{folder}/{self.name}_{timestamp}_{index}.parquet"
        
        # Use custom schema instead of default
        table = pa.Table.from_pandas(df, schema=self.schema, preserve_index=False)
        pq.write_table(table, where=filename)
        self.logger.info(f"Parquet saved to {filename}")

    def copy_raw(self, date):
        """Copy data to MotherDuck"""
        md_token = os.environ.get("MD_TOKEN")
        parquet_file = f"{self.store}/{date}/PARQUET/*.parquet"

        conn = duckdb.connect(f"md:MY_WH", config={'motherduck_token': md_token})
        conn.execute(f"COPY raw.{self.table_name} FROM '{os.path.abspath(parquet_file)}' (FORMAT 'parquet')")
        print(f"Data copied to MotherDuck: {self.table_name}")


# Usage example
if __name__ == "__main__":
    import sys
    
    app_token = os.getenv("APP_TOKEN")
    if not app_token:
        print("ERROR: APP_TOKEN environment variable not set!")
        print("Please create a .env file with: APP_TOKEN=your_token_here")
        exit(1)
    
    ridf = RatInspectionDataFetcher(app_token)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'fetch':
            print("Fetching rat inspection data...")
            ridf.fetch_all_data(save_as='parquet')
        elif sys.argv[1] == 'copy':
            if len(sys.argv) > 2:
                date = sys.argv[2]
                print(f"Copying rat inspection data for date: {date}")
                ridf.copy_raw(date)
            else:
                print("ERROR: Please provide a date for copy operation")
                print("Usage: python rat_inspection.py copy 20250915")
        else:
            print("ERROR: Unknown command")
            print("Usage:")
            print("  python rat_inspection.py fetch")
            print("  python rat_inspection.py copy 20250915")
    else:
        print("No command provided. Available commands:")
        print("  fetch - Fetch data from API and save as parquet")
        print("  copy <date> - Copy data to MotherDuck for specified date")
        print("Example: python rat_inspection.py fetch")
        print("Example: python rat_inspection.py copy 20250915")
 

