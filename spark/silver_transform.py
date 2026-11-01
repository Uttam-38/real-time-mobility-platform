
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    from_unixtime,
    to_timestamp,
    to_utc_timestamp
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
SILVER_PATH = os.path.join(PROJECT_ROOT, "data", "silver")

spark = (
    SparkSession.builder
    .appName("RealTimeMobilitySilver")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Define the JSON schema based on inspected Bronze data
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

# 2. Read the stored Bronze Parquet files
bronze_df = spark.read.parquet(BRONZE_PATH)

print("\n========== SILVER TRANSFORMATION ==========")
print(f"Bronze records: {bronze_df.count()}")

# 3. Parse the raw JSON
parsed_df = bronze_df.withColumn(
    "vehicle",
    from_json(col("raw_json"), vehicle_schema)
)

# 4. Extract fields into separate columns
silver_df = parsed_df.select(
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
    col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
)

# 5. Convert Unix observation time to Spark timestamp
silver_df = silver_df.withColumn(
    "vehicle_timestamp",
    to_timestamp(
        from_unixtime(col("vehicle_timestamp_unix"))
    )
)

# 6. Validate essential fields and geographic coordinates
silver_df = silver_df.filter(
    col("vehicle_id").isNotNull()
    & (col("vehicle_id") != "")
    & col("latitude").isNotNull()
    & col("longitude").isNotNull()
    & col("latitude").between(32.5, 34.5)
    & col("longitude").between(-113.5, -110.5)
    & col("vehicle_timestamp_unix").isNotNull()
    & (col("vehicle_timestamp_unix") > 0)
)

# 7. Remove exact duplicate vehicle observations
silver_df = silver_df.dropDuplicates([
    "vehicle_id",
    "vehicle_timestamp_unix",
    "latitude",
    "longitude"
])

print(f"Valid unique observations: {silver_df.count()}")

# 8. Save Silver data
silver_df.write.mode("overwrite").parquet(SILVER_PATH)

print(f"\nSilver data saved to: {SILVER_PATH}")

# 9. Display results
print("\nSilver schema:")
silver_df.printSchema()

print("\nSample cleaned vehicle records:")
silver_df.select(
    "vehicle_id",
    "route_id",
    "latitude",
    "longitude",
    "vehicle_timestamp"
).show(10, truncate=False)

print("\n========== SILVER COMPLETE ==========")

spark.stop()
