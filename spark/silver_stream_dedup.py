
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    IntegerType
)

ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

BRONZE_PATH = os.path.join(ROOT, "data", "bronze")
SILVER_PATH = os.path.join(ROOT, "data", "silver_dedup")
CHECKPOINT_PATH = os.path.join(
    ROOT, "checkpoints", "silver_dedup"
)

spark = (
    SparkSession.builder
    .appName("RealTimeMobilitySilverDedup")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
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

bronze_schema = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("ingestion_timestamp", TimestampType(), True)
])

bronze = (
    spark.readStream
    .schema(bronze_schema)
    .format("parquet")
    .load(BRONZE_PATH)
)

parsed = bronze.withColumn(
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
    col("vehicle.source").alias("source"),
    col("kafka_timestamp"),
    col("kafka_partition"),
    col("kafka_offset")
)

silver = silver.withColumn(
    "vehicle_timestamp",
    to_timestamp(
        col("vehicle_timestamp_unix").cast("timestamp")
    )
)

silver = silver.filter(
    col("vehicle_id").isNotNull()
    & (col("vehicle_id") != "")
    & col("latitude").between(32.5, 34.5)
    & col("longitude").between(-113.5, -110.5)
    & col("vehicle_timestamp_unix").isNotNull()
    & (col("vehicle_timestamp_unix") > 0)
)

# Deduplicate by observation identity.
# Use Kafka ingestion time for the watermark because
# vehicle timestamps can be stale or arrive out of order.
silver = (
    silver
    .withWatermark("kafka_timestamp", "30 minutes")
    .dropDuplicatesWithinWatermark([
        "vehicle_id",
        "vehicle_timestamp_unix",
        "latitude",
        "longitude"
    ])
)

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

print("\n========== SILVER DEDUP STREAM STARTED ==========")
print(f"Bronze input: {BRONZE_PATH}")
print(f"Silver output: {SILVER_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print("Press Control + C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Silver dedup stream...")
    query.stop()
finally:
    spark.stop()
