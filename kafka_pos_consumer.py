from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
from silver_layer_processor import SilverLayerProcessor

class POSKafkaConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='pos_transactions'):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.spark = SparkSession.builder \
            .appName("POS Data Consumer") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        self.silver_processor = SilverLayerProcessor()

    def consume_and_store(self):
        # Define the schema for our POS data
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("items", ArrayType(StructType([
                StructField("product_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("price", FloatType(), True)
            ])), True)
        ])

        # Create a streaming DataFrame from the Kafka topic
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "pos_transactions") \
            .load()

        # Parse the JSON data
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        # Write the stream to Delta Lake Bronze layer
        query = parsed_df \
            .writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/delta/pos_bronze_checkpoint") \
            .start("/tmp/delta/pos_bronze")

        query.awaitTermination()
    
    def process_micro_batch(self, batch_df, batch_id):
        # Write micro-batch to Bronze layer
        batch_df.write \
            .format("delta") \
            .mode("append") \
            .save("/tmp/delta/pos_bronze")

        # Trigger Silver layer processing
        self.silver_processor.process()

if __name__ == "__main__":
    consumer = POSKafkaConsumer()
    consumer.consume_and_store()