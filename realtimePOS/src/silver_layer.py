# src/silver_layer.py
import pandas as pd
import glob
import os
from datetime import datetime  # Change this line

class SilverLayer:
    def __init__(self, data_path):
        self.bronze_path = f"{data_path}/bronze"
        self.silver_path = f"{data_path}/silver"

    def process_bronze_data(self):
        bronze_files = glob.glob(f"{self.bronze_path}/*.parquet")
        dfs = []

        for file in bronze_files:
            df = pd.read_parquet(file)
            dfs.append(df)

        if not dfs:
            print("No new data to process")
            return

        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Perform basic transformations and validations
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
        combined_df['quantity'] = combined_df['quantity'].astype(int)
        
        # Remove any rows with missing values
        combined_df = combined_df.dropna()

        # Save to Silver layer
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        silver_file = f"{self.silver_path}/processed_data_{timestamp}.parquet"
        combined_df.to_parquet(silver_file, engine="pyarrow")  # Changed from "fastparquet" to "pyarrow"
        print(f"Processed {len(combined_df)} records to {silver_file}")

        # Remove processed Bronze files
        for file in bronze_files:
            os.remove(file)

# Test the Silver layer
if __name__ == "__main__":
    from data_generator import POSDataGenerator
    from bronze_layer import BronzeLayer
    
    generator = POSDataGenerator()
    bronze_layer = BronzeLayer("../data")
    silver_layer = SilverLayer("../data")
    
    # Generate and ingest some test data
    for _ in range(3):
        batch = generator.generate_batch(50)
        bronze_layer.ingest_batch(batch)
    
    # Process the Bronze data
    silver_layer.process_bronze_data()