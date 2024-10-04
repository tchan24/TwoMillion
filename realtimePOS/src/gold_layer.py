# src/gold_layer.py
import pandas as pd
import glob
from datetime import datetime

class GoldLayer:
    def __init__(self, data_path):
        self.silver_path = f"{data_path}/silver"
        self.gold_path = f"{data_path}/gold"
        self.inventory = pd.DataFrame(columns=['store', 'product', 'quantity'])

    def update_inventory(self):
        silver_files = glob.glob(f"{self.silver_path}/*.parquet")
        
        for file in silver_files:
            df = pd.read_parquet(file)
            
            # Group by store and product, sum the quantities
            inventory_changes = df.groupby(['store', 'product'])['quantity'].sum().reset_index()
            
            # Update the inventory
            for _, row in inventory_changes.iterrows():
                mask = (self.inventory['store'] == row['store']) & (self.inventory['product'] == row['product'])
                if mask.any():
                    self.inventory.loc[mask, 'quantity'] += row['quantity']
                else:
                    # Use concat instead of append
                    self.inventory = pd.concat([self.inventory, pd.DataFrame([row])], ignore_index=True)
            
            # Remove negative quantities (out of stock)
            self.inventory.loc[self.inventory['quantity'] < 0, 'quantity'] = 0
        
        # Save the updated inventory
        self.inventory.to_parquet(f"{self.gold_path}/current_inventory.parquet", engine="pyarrow")
        print(f"Updated inventory with {len(self.inventory)} records")

    def get_current_inventory(self):
        return self.inventory

# Test the Gold layer
if __name__ == "__main__":
    from data_generator import POSDataGenerator
    from bronze_layer import BronzeLayer
    from silver_layer import SilverLayer
    
    generator = POSDataGenerator()
    bronze_layer = BronzeLayer("../data")
    silver_layer = SilverLayer("../data")
    gold_layer = GoldLayer("../data")
    
    # Generate and process some test data
    for _ in range(3):
        batch = generator.generate_batch(50)
        bronze_layer.ingest_batch(batch)
    
    silver_layer.process_bronze_data()
    gold_layer.update_inventory()
    
    # Print the current inventory
    print(gold_layer.get_current_inventory())