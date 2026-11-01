
import uuid
from pathlib import Path

from pyspark.sql import SparkSession

# ============================================================
# PROJECT PATHS
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

# Original historical Silver backup
HISTORICAL = (
    ROOT
    / "backups"
    / "silver_before_recovery_20261029"
)

# Silver backup created before restarting Kafka
HISTORICAL_RECENT = (
    ROOT
    / "backups"
    / "silver_before_new_kafka_20261030_164246"
    / "silver"
)

# Current live Silver streaming output
LIVE = ROOT / "data" / "silver_dedup"

# Combined Silver snapshots
OUTPUT_ROOT = ROOT / "data" / "silver_combined_snapshots"

OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("RefreshCombinedSilver")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def has_parquet_files(directory):
    """Check whether a directory contains Parquet data."""
    return (
        directory.is_dir()
        and any(directory.glob("*.parquet"))
    )


def read_historical(directory):
    """Read historical Parquet files from a backup."""
    if not has_parquet_files(directory):
        print(f"Skipping empty historical directory: {directory}")
        return None

    print(f"Reading historical Silver: {directory}")

    return spark.read.parquet(
        str(directory / "*.parquet")
    )


def read_live(directory):
    """Read committed live Silver streaming data."""
    metadata = directory / "_spark_metadata"

    if not metadata.is_dir():
        print("No committed live Silver data available yet.")
        return None

    print(f"Reading live Silver: {directory}")

    return spark.read.parquet(str(directory))


# ============================================================
# COMBINE SILVER DATA
# ============================================================

try:
    print("\n========== COMBINED SILVER REFRESH ==========")

    datasets = []

    # 1. Original historical observations
    historical = read_historical(HISTORICAL)

    if historical is not None:
        datasets.append(historical)

    # 2. Recently backed-up observations
    historical_recent = read_historical(HISTORICAL_RECENT)

    if historical_recent is not None:
        datasets.append(historical_recent)

    # 3. Newly arriving live observations
    live = read_live(LIVE)

    if live is not None:
        datasets.append(live)

    if not datasets:
        raise RuntimeError(
            "No Silver observations available to combine."
        )

    # Combine all available datasets
    combined = datasets[0]

    for dataset in datasets[1:]:
        combined = combined.unionByName(
            dataset,
            allowMissingColumns=True
        )

    # Remove duplicate vehicle observations
    keys = [
        "vehicle_id",
        "vehicle_timestamp_unix",
        "latitude",
        "longitude"
    ]

    combined = combined.dropDuplicates(keys)

    # ========================================================
    # CREATE NEW SNAPSHOT
    # ========================================================

    snapshot_id = uuid.uuid4().hex[:12]

    snapshot_path = OUTPUT_ROOT / snapshot_id

    print("\nCreating Combined Silver snapshot...")
    print(f"Snapshot ID: {snapshot_id}")

    combined.write.mode("errorifexists").parquet(
        str(snapshot_path)
    )

    # ========================================================
    # VERIFY SAVED DATA
    # ========================================================

    print("\nVerifying Combined Silver snapshot...")

    verified = spark.read.parquet(str(snapshot_path))

    count = verified.count()

    unique = (
        verified
        .select(*keys)
        .distinct()
        .count()
    )

    if count != unique:
        raise RuntimeError(
            "Duplicate observations detected in Combined Silver."
        )

    if not (snapshot_path / "_SUCCESS").exists():
        raise RuntimeError(
            "Combined Silver snapshot is incomplete."
        )

    print("\n========== VERIFICATION ==========")

    print(f"Combined observations: {count}")
    print(f"Unique observations: {unique}")
    print(f"Snapshot location: {snapshot_path}")

    print(
        "\nCombined Silver snapshot created successfully"
    )

finally:
    spark.stop()
