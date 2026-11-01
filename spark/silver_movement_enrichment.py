
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lag, radians, sin, cos, asin, sqrt,
    least, greatest, lit, when
)

# ============================================================
# 1. PROJECT PATHS
# ============================================================

ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)


INPUT_PATH = (
    sys.argv[1]
    if len(sys.argv) > 1
    else os.path.join(ROOT, "data", "silver_combined")
)

OUTPUT_PATH = (
    sys.argv[2]
    if len(sys.argv) > 2
    else os.path.join(ROOT, "data", "silver_enriched")
)


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SilverMovementEnrichment")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. READ SILVER DATA
# ============================================================

df = spark.read.parquet(INPUT_PATH)

print("\n========== SILVER MOVEMENT ENRICHMENT ==========")
print(f"Input observations: {df.count()}")

# ============================================================
# 4. FIND PREVIOUS VEHICLE POSITION
# ============================================================

vehicle_window = (
    Window.partitionBy("vehicle_id")
    .orderBy(
        "vehicle_timestamp_unix",
        "latitude",
        "longitude"
    )
)

df = (
    df
    .withColumn(
        "previous_latitude",
        lag("latitude").over(vehicle_window)
    )
    .withColumn(
        "previous_longitude",
        lag("longitude").over(vehicle_window)
    )
    .withColumn(
        "previous_timestamp",
        lag("vehicle_timestamp_unix").over(vehicle_window)
    )
)

# ============================================================
# 5. CALCULATE TIME DIFFERENCE
# ============================================================

df = df.withColumn(
    "time_difference_seconds",
    col("vehicle_timestamp_unix") - col("previous_timestamp")
)

# ============================================================
# 6. CALCULATE HAVERSINE DISTANCE
# ============================================================

EARTH_RADIUS_KM = 6371.0

lat1 = radians(col("previous_latitude"))
lon1 = radians(col("previous_longitude"))
lat2 = radians(col("latitude"))
lon2 = radians(col("longitude"))

a = (
    sin((lat2 - lat1) / 2) ** 2
    + cos(lat1) * cos(lat2)
    * sin((lon2 - lon1) / 2) ** 2
)

a = least(greatest(a, lit(0.0)), lit(1.0))

df = df.withColumn(
    "distance_km",
    2 * EARTH_RADIUS_KM * asin(sqrt(a))
)

# ============================================================
# 7. CALCULATE SPEED
# ============================================================

df = df.withColumn(
    "calculated_speed_kmh",
    when(
        col("time_difference_seconds") > 0,
        col("distance_km")
        / col("time_difference_seconds") * 3600.0
    )
)

# ============================================================
# 8. CLASSIFY MOVEMENTS
# ============================================================

SPEED_THRESHOLD_KMH = 120.0

df = df.withColumn(
    "movement_status",
    when(
        col("previous_timestamp").isNull(),
        "FIRST_OBSERVATION"
    )
    .when(
        col("time_difference_seconds") <= 0,
        "INVALID_TIME_INTERVAL"
    )
    .when(
        col("calculated_speed_kmh") > SPEED_THRESHOLD_KMH,
        "SUSPICIOUS_SPEED"
    )
    .otherwise("NORMAL")
)

# ============================================================
# 9. SAVE ENRICHED DATA
# ============================================================

print("\nSaving enriched Silver data...")

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .save(OUTPUT_PATH)
)

# ============================================================
# 10. VERIFY RESULTS
# ============================================================

verified_df = spark.read.parquet(OUTPUT_PATH)

print("\n========== VERIFICATION ==========")
print(f"Saved observations: {verified_df.count()}")

verified_df.groupBy("movement_status").count().show()

print("\nSample enriched observations:")

verified_df.select(
    "vehicle_id",
    "route_id",
    "vehicle_timestamp",
    "distance_km",
    "time_difference_seconds",
    "calculated_speed_kmh",
    "movement_status"
).show(10, truncate=False)

print("\n========== ENRICHMENT COMPLETE ==========")

spark.stop()
