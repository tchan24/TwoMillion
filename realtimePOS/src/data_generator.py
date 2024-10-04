# src/data_generator.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class POSDataGenerator:
    def __init__(self, num_products=100, num_stores=10):
        self.num_products = num_products
        self.num_stores = num_stores
        self.products = [f"Product_{i}" for i in range(num_products)]
        self.stores = [f"Store_{i}" for i in range(num_stores)]

    def generate_transaction(self):
        timestamp = datetime.now()
        store = np.random.choice(self.stores)
        product = np.random.choice(self.products)
        quantity = np.random.randint(1, 6)
        transaction_type = np.random.choice(["sale", "restock", "shrinkage"], p=[0.7, 0.2, 0.1])

        if transaction_type == "shrinkage":
            quantity = -quantity
        elif transaction_type == "restock":
            quantity *= 10

        return {
            "timestamp": timestamp,
            "store": store,
            "product": product,
            "quantity": quantity,
            "transaction_type": transaction_type
        }

    def generate_batch(self, num_transactions=100):
        return [self.generate_transaction() for _ in range(num_transactions)]

# Test the data generator
if __name__ == "__main__":
    generator = POSDataGenerator()
    batch = generator.generate_batch(10)
    df = pd.DataFrame(batch)
    print(df)