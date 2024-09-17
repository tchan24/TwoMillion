import random
from datetime import datetime, timedelta
import json

class POSDataGenerator:
    def __init__(self):
        self.store_ids = list(range(1, 11))  # 10 stores
        self.product_ids = list(range(1, 101))  # 100 products
        self.transaction_types = ['sale', 'return', 'adjustment']

    def generate_transaction(self):
        timestamp = datetime.now().isoformat()
        store_id = random.choice(self.store_ids)
        transaction_type = random.choice(self.transaction_types)
        
        items = []
        for _ in range(random.randint(1, 5)):  # 1 to 5 items per transaction
            product_id = random.choice(self.product_ids)
            quantity = random.randint(1, 10)
            price = round(random.uniform(1.0, 100.0), 2)
            items.append({
                'product_id': product_id,
                'quantity': quantity,
                'price': price
            })
        
        transaction = {
            'timestamp': timestamp,
            'store_id': store_id,
            'transaction_type': transaction_type,
            'items': items
        }
        
        return transaction

    def generate_batch(self, num_transactions):
        return [self.generate_transaction() for _ in range(num_transactions)]

# Usage
generator = POSDataGenerator()
batch = generator.generate_batch(10)  # Generate 10 transactions

# Print the generated data
for transaction in batch:
    print(json.dumps(transaction, indent=2))

if __name__ == "__main__":
    generator = POSDataGenerator()
    batch = generator.generate_batch(10)
    
    # Save to a JSON file
    with open('pos_data_sample.json', 'w') as f:
        json.dump(batch, f, indent=2)
    
    print("Sample POS data has been generated and saved to pos_data_sample.json")