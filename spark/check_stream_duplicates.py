
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# ============================================================
# 1. PROJECT FOLDER PATHS
# ============================================================

ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

ORIGINAL_SILVER_PATH = os.path.join(
    ROOT, "data", "silver_stream"
)

DEDUP_SILVER_PATH = os.path.join(
    ROOT, "data", "silver_dedup"
)

# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("CompareSilverDuplicates")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. DUPLICATE IDENTIFICATION COLUMNS
# ============================================================

duplicate_keys = [
    "vehicle_id",
    "vehicle_timestamp_unix",
    "latitude",
    "longitude"
]

# ============================================================
# 4. FUNCTION TO ANALYZE DUPLICATES
# ============================================================

def analyze_duplicates(folder_path, dataset_name):

    print("\n" + "=" * 60)
    print(dataset_name)
    print("=" * 60)

    if not os.path.isdir(folder_path):
        print("Dataset folder does not exist yet.")
        print(f"Expected location: {folder_path}")
        return None

    parquet_files = [
        name for name in os.listdir(folder_path)
        if name.endswith(".parquet")
    ]

    if not parquet_files:
        print("No Parquet files found in this folder.")
        return None

    df = spark.read.parquet(folder_path)

    total_records = df.count()

    unique_records = (
        df.dropDuplicates(duplicate_keys).count()
    )

    duplicate_records = total_records - unique_records

    duplicate_percentage = (
        (duplicate_records / total_records) * 100
        if total_records > 0
        else 0
    )

    print(f"Folder: {folder_path}")
    print(f"Total observations:       {total_records}")
    print(f"Unique observations:      {unique_records}")
    print(f"Duplicate observations:   {duplicate_records}")
    print(f"Duplicate percentage:     {duplicate_percentage:.2f}%")

    print("\nTop 10 repeated observations:")

    (
        df.groupBy(*duplicate_keys)
        .agg(count("*").alias("occurrences"))
        .filter(col("occurrences") > 1)
        .orderBy(col("occurrences").desc())
        .show(10, truncate=False)
    )

    return {
        "total": total_records,
        "unique": unique_records,
        "duplicates": duplicate_records,
        "percentage": duplicate_percentage
    }


# ============================================================
# 5. CHECK ORIGINAL SILVER STREAM
# ============================================================

original_results = analyze_duplicates(
    ORIGINAL_SILVER_PATH,
    "ORIGINAL SILVER STREAM - BEFORE DEDUPLICATION"
)

# ============================================================
# 6. CHECK NEW DEDUPLICATED SILVER STREAM
# ============================================================

dedup_results = analyze_duplicates(
    DEDUP_SILVER_PATH,
    "NEW SILVER STREAM - AFTER DEDUPLICATION"
)

# ============================================================
# 7. COMPARISON REPORT
# ============================================================

print("\n" + "=" * 60)
print("FINAL DUPLICATE COMPARISON")
print("=" * 60)

if original_results and dedup_results:

    print(
        f"Original duplicate percentage: "
        f"{original_results['percentage']:.2f}%"
    )

    print(
        f"New duplicate percentage:      "
        f"{dedup_results['percentage']:.2f}%"
    )

    improvement = (
        original_results["percentage"]
        - dedup_results["percentage"]
    )

    print(
        f"Reduction in duplicate rate:   "
        f"{improvement:.2f} percentage points"
    )

else:
    print(
        "Both datasets must exist before comparison "
        "can be completed."
    )

print("\n========== DUPLICATE CHECK COMPLETE ==========")

spark.stop()
