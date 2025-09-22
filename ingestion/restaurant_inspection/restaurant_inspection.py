import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
from dotenv import load_dotenv
import duckdb
from ingestion.fetcher import DataFetcher

load_dotenv()

class RestaurantDataFetcher(DataFetcher):
    def __init__(self, app_token):
        super().__init__(
            name="RESTAURANT_INSPECTION",
            api_url="https://data.cityofnewyork.us/resource/43nn-pn8j.json",
            headers={"X-App-Token": app_token},
            store_path="data/raw/restaurant_inspection",
            table_name="restaurant_inspection"
        )

        self.schema = pa.schema([
            ("camis", pa.string()),
            ("dba", pa.string()),
            ("boro", pa.string()),
            ("building", pa.string()),
            ("street", pa.string()),
            ("zipcode", pa.string()),
            ("phone", pa.string()),
            ("inspection_date", pa.string()),
            ("critical_flag", pa.string()),
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
            ("grade_date", pa.string()),
            ('latitude', pa.string()),
            ('longitude', pa.string()),
            ("inspection_type", pa.string()),
        ])

    def __save_as_parquet(self, data, index):
        """Override parent method to use custom schema"""
        df = pd.DataFrame(data)
        print(df.dtypes)
        
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
        print(self.store)
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
    
    rdf = RestaurantDataFetcher(app_token)
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'fetch':
            print("Fetching restaurant inspection data...")
            rdf.fetch_all_data(save_as='parquet')
        elif sys.argv[1] == 'copy':
            if len(sys.argv) > 2:
                date = sys.argv[2]
                print(f"Copying restaurant inspection data for date: {date}")
                rdf.copy_raw(date)
            else:
                print("ERROR: Please provide a date for copy operation")
                print("Usage: python restaurant_inspection.py copy 20250915")
        else:
            print("ERROR: Unknown command")
            print("Usage:")
            print("  python restaurant_inspection.py fetch")
            print("  python restaurant_inspection.py copy 20250915")
    else:
        print("No command provided. Available commands:")
        print("  fetch - Fetch data from API and save as parquet")
        print("  copy <date> - Copy data to MotherDuck for specified date")
        print("Example: python restaurant_inspection.py fetch")
        print("Example: python restaurant_inspection.py copy 20250915")

