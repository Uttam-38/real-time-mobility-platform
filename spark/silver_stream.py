
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    from_unixtime,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver_stream")
CHECKPOINT_PATH = os.path.join(
    PROJECT_ROOT, "checkpoints", "silver"
)

spark = (
    SparkSession.builder
    .appName("RealTimeMobilitySilverStreaming")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

vehicle_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

# Bronze streaming schema must match the saved Parquet files.
bronze_schema = spark.read.parquet(BRONZE_PATH).schema

bronze_stream = (
    spark.readStream
    .schema(bronze_schema)
    .format("parquet")
    .load(BRONZE_PATH)
)

parsed = bronze_stream.withColumn(
    "vehicle",
    from_json(col("raw_json"), vehicle_schema)
)

silver = parsed.select(
    col("vehicle.vehicle_id").alias("vehicle_id"),
    col("vehicle.trip_id").alias("trip_id"),
    col("vehicle.route_id").alias("route_id"),
    col("vehicle.latitude").alias("latitude"),
    col("vehicle.longitude").alias("longitude"),
    col("vehicle.timestamp").alias("vehicle_timestamp_unix"),
    to_timestamp(
        col("vehicle.ingestion_timestamp")
    ).alias("producer_ingestion_timestamp"),
    col("vehicle.source").alias("source"),
    col("kafka_topic"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    col("ingestion_timestamp").alias(
        "bronze_ingestion_timestamp"
    )
)

silver = silver.withColumn(
    "vehicle_timestamp",
    to_timestamp(
        from_unixtime(col("vehicle_timestamp_unix"))
    )
)

# Geographic and required-field validation
silver = silver.filter(
    col("vehicle_id").isNotNull()
    & (col("vehicle_id") != "")
    & col("latitude").between(32.5, 34.5)
    & col("longitude").between(-113.5, -110.5)
    & col("vehicle_timestamp_unix").isNotNull()
    & (col("vehicle_timestamp_unix") > 0)
)

# Deduplicate by Kafka message identity.
# This is safe for streaming and prevents replayed Kafka
# messages from being written twice.
silver = silver.dropDuplicatesWithinWatermark([
    "kafka_topic",
    "kafka_partition",
    "kafka_offset"
]) if False else silver

# Append new cleaned observations to Silver.
# Observation-level deduplication will be handled in a
# later stateful stage after we define a suitable watermark.
silver = silver.withColumn(
    "silver_processed_timestamp",
    current_timestamp()
)

query = (
    silver.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

print("\n========== SILVER STREAM STARTED ==========")
print(f"Reading Bronze: {BRONZE_PATH}")
print(f"Writing Silver: {SILVER_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print("Waiting for new Bronze data...")
print("Press Control + C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Silver stream...")
    query.stop()
finally:
    spark.stop()
