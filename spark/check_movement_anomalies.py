
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lag,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    least,
    greatest,
    lit,
    when,
    avg,
    max as spark_max,
    round as spark_round
)

# ============================================================
# 1. PROJECT PATHS
# ============================================================

ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

SILVER_PATH = os.path.join(
    ROOT, "data", "silver_dedup"
)

# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("VehicleMovementAnomalyDetection")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. LOAD DEDUPLICATED SILVER DATA
# ============================================================

df = spark.read.parquet(SILVER_PATH)

print("\n========== MOVEMENT ANOMALY DETECTION ==========")
print(f"Total Silver observations: {df.count()}")

# ============================================================
# 4. ORDER OBSERVATIONS BY VEHICLE AND TIME
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
    col("vehicle_timestamp_unix")
    - col("previous_timestamp")
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
    + cos(lat1)
    * cos(lat2)
    * sin((lon2 - lon1) / 2) ** 2
)

# FIX: Use lit() for Spark numeric constants.
a = least(
    greatest(a, lit(0.0)),
    lit(1.0)
)

df = df.withColumn(
    "distance_km",
    2 * EARTH_RADIUS_KM * asin(sqrt(a))
)

# ============================================================
# 7. CALCULATE VEHICLE SPEED
# ============================================================

df = df.withColumn(
    "calculated_speed_kmh",
    when(
        col("time_difference_seconds") > 0,
        (
            col("distance_km")
            / col("time_difference_seconds")
        ) * 3600.0
    )
)

# ============================================================
# 8. CLASSIFY MOVEMENT QUALITY
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
# 9. DISPLAY MOVEMENT CLASSIFICATION
# ============================================================

print("\nMovement classification:")

(
    df.groupBy("movement_status")
    .count()
    .orderBy(col("count").desc())
    .show(truncate=False)
)

# ============================================================
# 10. DISPLAY SPEED STATISTICS
# ============================================================

print("\nSpeed statistics for valid time intervals:")

(
    df.filter(
        col("time_difference_seconds") > 0
    )
    .select(
        spark_round(
            avg("calculated_speed_kmh"), 2
        ).alias("average_speed_kmh"),
        spark_round(
            spark_max("calculated_speed_kmh"), 2
        ).alias("maximum_speed_kmh")
    )
    .show()
)

# ============================================================
# 11. DISPLAY SUSPICIOUS MOVEMENTS
# ============================================================

print("\nTop 20 suspicious vehicle movements:")

(
    df.filter(
        col("movement_status") == "SUSPICIOUS_SPEED"
    )
    .select(
        "vehicle_id",
        "route_id",
        "previous_timestamp",
        "vehicle_timestamp_unix",
        "time_difference_seconds",
        spark_round(
            col("distance_km"), 3
        ).alias("distance_km"),
        spark_round(
            col("calculated_speed_kmh"), 2
        ).alias("speed_kmh")
    )
    .orderBy(col("speed_kmh").desc())
    .show(20, truncate=False)
)

# ============================================================
# 12. COUNT SUSPICIOUS MOVEMENTS
# ============================================================

suspicious = df.filter(
    col("movement_status") == "SUSPICIOUS_SPEED"
)

print(
    "\nTotal suspicious movement pairs:",
    suspicious.count()
)

print(
    "Vehicles with suspicious movements:",
    suspicious.select("vehicle_id").distinct().count()
)

# ============================================================
# 13. FINISH
# ============================================================

print("\n========== ANALYSIS COMPLETE ==========")

spark.stop()
