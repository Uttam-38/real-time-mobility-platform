
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, count, countDistinct, lag,
    radians, sin, cos, asin, sqrt, least, lit,
    when
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType
)
from pyspark.sql.window import Window

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BRONZE_PATH = os.path.join(ROOT, "data", "bronze")
SILVER_PATH = os.path.join(ROOT, "data", "silver")

spark = (
    SparkSession.builder
    .appName("SilverQualityCheck")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("ingestion_timestamp", StringType()),
    StructField("source", StringType())
])

bronze = spark.read.parquet(BRONZE_PATH)
silver = spark.read.parquet(SILVER_PATH)

parsed = bronze.select(
    from_json(col("raw_json"), schema).alias("v")
).select("v.*")

valid_condition = (
    col("vehicle_id").isNotNull()
    & (col("vehicle_id") != "")
    & col("latitude").isNotNull()
    & col("longitude").isNotNull()
    & col("latitude").between(-90, 90)
    & col("longitude").between(-180, 180)
    & col("timestamp").isNotNull()
    & (col("timestamp") > 0)
)

valid = parsed.filter(valid_condition)

print("\n========== SILVER QUALITY REPORT ==========")

bronze_count = bronze.count()
valid_count = valid.count()
silver_count = silver.count()

print(f"Bronze records:             {bronze_count}")
print(f"Valid before deduplication: {valid_count}")
print(f"Invalid records removed:    {bronze_count - valid_count}")
print(f"Duplicates removed:         {valid_count - silver_count}")
print(f"Silver records:             {silver_count}")

print("\nMissing route/trip information:")
silver.select(
    count(when(col("route_id").isNull(), 1)).alias("missing_route_id"),
    count(when(col("trip_id").isNull(), 1)).alias("missing_trip_id"),
    countDistinct("vehicle_id").alias("unique_vehicle_ids")
).show()

print("\nChecking suspicious vehicle movement...")

window = Window.partitionBy("vehicle_id").orderBy(
    "vehicle_timestamp_unix",
    "kafka_offset"
)

movement = (
    silver
    .withColumn("prev_lat", lag("latitude").over(window))
    .withColumn("prev_lon", lag("longitude").over(window))
    .withColumn(
        "prev_time",
        lag("vehicle_timestamp_unix").over(window)
    )
    .withColumn(
        "seconds_elapsed",
        col("vehicle_timestamp_unix") - col("prev_time")
    )
)

# Haversine distance between consecutive observations
lat1 = radians(col("prev_lat"))
lon1 = radians(col("prev_lon"))
lat2 = radians(col("latitude"))
lon2 = radians(col("longitude"))

a = (
    sin((lat2 - lat1) / 2) ** 2
    + cos(lat1) * cos(lat2)
    * sin((lon2 - lon1) / 2) ** 2
)

movement = movement.withColumn(
    "distance_km",
    lit(6371.0) * 2 * asin(sqrt(least(lit(1.0), a)))
)

movement = movement.withColumn(
    "speed_kmh",
    when(
        col("seconds_elapsed") > 0,
        col("distance_km") * 3600 / col("seconds_elapsed")
    )
)

suspicious = movement.filter(
    (col("seconds_elapsed") > 0)
    & (col("speed_kmh") > 120)
)

print(f"Movement pairs exceeding 120 km/h: {suspicious.count()}")

suspicious.select(
    "vehicle_id",
    "seconds_elapsed",
    "distance_km",
    "speed_kmh"
).orderBy(col("speed_kmh").desc()).show(10, truncate=False)

print("\n========== REPORT COMPLETE ==========")

spark.stop()
