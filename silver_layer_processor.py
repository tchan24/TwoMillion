from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

class SilverLayerProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("POS Silver Layer Processor") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()

    def process(self):
        # Read from Bronze layer
        bronze_df = self.spark.read.format("delta").load("/tmp/delta/pos_bronze")

        # Perform transformations
        silver_df = bronze_df.select(
            col("timestamp"),
            col("store_id"),
            col("transaction_type"),
            explode("items").alias("item")
        ).select(
            "timestamp",
            "store_id",
            "transaction_type",
            col("item.product_id"),
            col("item.quantity"),
            col("item.price")
        )

        # Calculate total sales amount
        silver_df = silver_df.withColumn("total_amount", col("quantity") * col("price"))

        # Write to Silver layer
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save("/tmp/delta/pos_silver")

        # Calculate and update inventory
        self.update_inventory(silver_df)

    def update_inventory(self, silver_df):
        # Group by product_id and sum quantities
        inventory_changes = silver_df.groupBy("product_id") \
            .agg(spark_sum("quantity").alias("quantity_change"))

        # Read current inventory
        try:
            current_inventory = self.spark.read.format("delta").load("/tmp/delta/inventory")
        except:
            # If inventory doesn't exist, create an empty DataFrame
            schema = StructType([
                StructField("product_id", IntegerType(), True),
                StructField("quantity", IntegerType(), True)
            ])
            current_inventory = self.spark.createDataFrame([], schema)

        # Update inventory
        updated_inventory = current_inventory.join(inventory_changes, "product_id", "outer") \
            .fillna(0) \
            .withColumn("quantity", col("quantity") - col("quantity_change")) \
            .select("product_id", "quantity")

        # Write updated inventory
        updated_inventory.write \
            .format("delta") \
            .mode("overwrite") \
            .save("/tmp/delta/inventory")

if __name__ == "__main__":
    processor = SilverLayerProcessor()
    processor.process()