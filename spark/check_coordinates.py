
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, count
)

ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

SILVER_PATH = os.path.join(ROOT, "data", "silver")

spark = (
    SparkSession.builder
    .appName("CheckVehicleCoordinates")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(SILVER_PATH)

print("\n========== COORDINATE QUALITY CHECK ==========")

print("\nLatitude and longitude ranges:")

df.select(
    spark_min("latitude").alias("minimum_latitude"),
    spark_max("latitude").alias("maximum_latitude"),
    spark_min("longitude").alias("minimum_longitude"),
    spark_max("longitude").alias("maximum_longitude")
).show(truncate=False)

# Diagnostic bounds, not yet production filtering rules.
# Broadly cover the Phoenix metropolitan area.
suspect = df.filter(
    ~col("latitude").between(32.5, 34.5)
    | ~col("longitude").between(-113.5, -110.5)
)

print("\nRecords outside broad Phoenix-area bounds:")
print(suspect.count())

suspect.select(
    "vehicle_id",
    "route_id",
    "latitude",
    "longitude",
    "vehicle_timestamp"
).orderBy("vehicle_id", "vehicle_timestamp").show(
    30, truncate=False
)

print("\nAll observations for vehicle 101:")

df.filter(
    col("vehicle_id") == "101"
).select(
    "vehicle_id",
    "latitude",
    "longitude",
    "vehicle_timestamp"
).orderBy("vehicle_timestamp").show(
    50, truncate=False
)

print("\n========== CHECK COMPLETE ==========")

spark.stop()
