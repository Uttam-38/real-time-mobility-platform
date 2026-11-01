
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Project root directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")
CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "checkpoints", "bronze")

# Create Spark session
spark = (
    SparkSession.builder
    .appName("RealTimeMobilityBronze")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read live Kafka messages
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "vehicle-positions")
    .option("startingOffsets", "latest")
    .load()
)

# Preserve original JSON and Kafka metadata
bronze_df = kafka_stream.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_json"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingestion_timestamp")
)

# Store events in Parquet files
query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

print("\nBronze streaming started!")
print(f"Data location: {BRONZE_PATH}")
print(f"Checkpoint location: {CHECKPOINT_PATH}")
print("Waiting for Kafka messages... Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Bronze streaming...")
    query.stop()
finally:
    spark.stop()
