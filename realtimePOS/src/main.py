# src/main.py
import time
from data_generator import POSDataGenerator
from bronze_layer import BronzeLayer
from silver_layer import SilverLayer
from gold_layer import GoldLayer

def main():
    data_path = "../data"
    generator = POSDataGenerator()
    bronze_layer = BronzeLayer(data_path)
    silver_layer = SilverLayer(data_path)
    gold_layer = GoldLayer(data_path)

    print("Starting POS Analytics PoC...")
    
    try:
        while True:
            # Generate and ingest a batch of transactions
            batch = generator.generate_batch(50)
            bronze_layer.ingest_batch(batch)
            
            # Process Bronze data to Silver
            silver_layer.process_bronze_data()
            
            # Update inventory in Gold layer
            gold_layer.update_inventory()
            
            # Print current inventory for a few random products
            current_inventory = gold_layer.get_current_inventory()
            print("\nCurrent Inventory (sample):")
            print(current_inventory.sample(5) if len(current_inventory) > 5 else current_inventory)
            
            # Wait for a short time before the next iteration
            time.sleep(5)
    
    except KeyboardInterrupt:
        print("\nStopping the simulation...")

if __name__ == "__main__":
    main()