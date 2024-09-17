from kafka import KafkaProducer
import json
import time
from pos_data_generator import POSDataGenerator

class POSKafkaProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='pos_transactions'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.generator = POSDataGenerator()

    def send_transaction(self, transaction):
        self.producer.send(self.topic, transaction)

    def send_batch(self, batch_size, delay=1):
        while True:
            batch = self.generator.generate_batch(batch_size)
            for transaction in batch:
                self.send_transaction(transaction)
                print(f"Sent transaction: {transaction['timestamp']}")
            self.producer.flush()
            time.sleep(delay)

if __name__ == "__main__":
    producer = POSKafkaProducer()
    producer.send_batch(batch_size=10, delay=5)