# src/bronze_layer.py
import pandas as pd
from datetime import datetime  # Change this line

class BronzeLayer:
    def __init__(self, data_path):
        self.data_path = data_path

    def ingest_batch(self, batch):
        df = pd.DataFrame(batch)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{self.data_path}/bronze/pos_data_{timestamp}.parquet"
        df.to_parquet(file_name, engine="pyarrow")
        print(f"Ingested {len(df)} records to {file_name}")

# Test the Bronze layer
if __name__ == "__main__":
    from data_generator import POSDataGenerator
    
    generator = POSDataGenerator()
    bronze_layer = BronzeLayer("../data")
    
    batch = generator.generate_batch(100)
    bronze_layer.ingest_batch(batch)