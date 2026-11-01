
import json
import os
import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# ============================================================
# 1. PROJECT PATHS
# ============================================================

ROOT = Path(__file__).resolve().parent.parent

INPUT_PATH = (
    Path(sys.argv[1]).resolve()
    if len(sys.argv) > 1
    else ROOT / "data" / "silver_enriched"
)

SNAPSHOT_ROOT = ROOT / "data" / "gold_snapshots"
MANIFEST_PATH = SNAPSHOT_ROOT / "current.json"

SNAPSHOT_ROOT.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("MobilityGoldAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def log(message):
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(f"[{timestamp}] {message}", flush=True)


def write_dataset(df, path):
    df.write.mode("errorifexists").parquet(str(path))


# ============================================================
# 3. READ ENRICHED SILVER
# ============================================================

try:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Enriched Silver input not found: {INPUT_PATH}"
        )

    log(f"Reading enriched Silver: {INPUT_PATH}")

    silver = (
        spark.read.parquet(str(INPUT_PATH))
        .withColumn(
            "route_label",
            F.coalesce(
                F.col("route_id").cast("string"),
                F.lit("Unknown")
            )
        )
        .cache()
    )

    observation_count = silver.count()

    if observation_count == 0:
        raise RuntimeError(
            "Enriched Silver dataset is empty"
        )

    required_columns = {
        "vehicle_id",
        "route_id",
        "route_label",
        "latitude",
        "longitude",
        "vehicle_timestamp_unix",
        "movement_status",
        "calculated_speed_kmh",
    }

    missing = required_columns - set(silver.columns)

    if missing:
        raise RuntimeError(
            f"Missing required columns: {sorted(missing)}"
        )

    log(f"Input observations: {observation_count}")

    # ========================================================
    # 4. FLEET SUMMARY
    # ========================================================

    fleet_summary = silver.agg(

        F.count("*").alias(
            "total_observations"
        ),

        F.countDistinct("vehicle_id").alias(
            "unique_vehicles"
        ),

        F.countDistinct("route_id").alias(
            "unique_routes"
        ),

        F.avg(
            F.when(
                F.col("movement_status") == "NORMAL",
                F.col("calculated_speed_kmh")
            )
        ).alias(
            "average_valid_speed_kmh"
        ),

        F.sum(
            F.when(
                F.col("movement_status") == "NORMAL",
                1
            ).otherwise(0)
        ).alias(
            "normal_movements"
        ),

        F.sum(
            F.when(
                F.col("movement_status") == "SUSPICIOUS_SPEED",
                1
            ).otherwise(0)
        ).alias(
            "suspicious_movements"
        ),

        F.sum(
            F.when(
                F.col("movement_status") == "FIRST_OBSERVATION",
                1
            ).otherwise(0)
        ).alias(
            "first_observations"
        ),

        F.max("vehicle_timestamp_unix").alias(
            "latest_vehicle_timestamp_unix"
        )
    )

    # ========================================================
    # 5. ROUTE SUMMARY
    # ========================================================

    route_summary = (
        silver
        .groupBy("route_label")
        .agg(

            F.count("*").alias(
                "total_observations"
            ),

            F.countDistinct("vehicle_id").alias(
                "unique_vehicles"
            ),

            F.avg(
                F.when(
                    F.col("movement_status") == "NORMAL",
                    F.col("calculated_speed_kmh")
                )
            ).alias(
                "average_valid_speed_kmh"
            ),

            F.sum(
                F.when(
                    F.col("movement_status") == "NORMAL",
                    1
                ).otherwise(0)
            ).alias(
                "normal_movements"
            ),

            F.sum(
                F.when(
                    F.col("movement_status") == "SUSPICIOUS_SPEED",
                    1
                ).otherwise(0)
            ).alias(
                "suspicious_movements"
            ),

            F.max("vehicle_timestamp_unix").alias(
                "latest_vehicle_timestamp_unix"
            )
        )
    )

    # ========================================================
    # 6. LATEST VEHICLE POSITIONS
    # ========================================================

    latest_window = (
        Window
        .partitionBy("vehicle_id")
        .orderBy(
            F.col("vehicle_timestamp_unix").desc(),
            F.col("kafka_timestamp").desc_nulls_last(),
            F.col("latitude").desc(),
            F.col("longitude").desc()
        )
    )

    latest_vehicle_positions = (
        silver
        .withColumn(
            "row_number",
            F.row_number().over(latest_window)
        )
        .filter(
            F.col("row_number") == 1
        )
        .drop("row_number")
    )

    # ========================================================
    # 7. CREATE VERSIONED GOLD SNAPSHOT
    # ========================================================

    snapshot_id = (
        datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S"
        )
        + "_"
        + uuid.uuid4().hex[:8]
    )

    snapshot_path = SNAPSHOT_ROOT / snapshot_id

    snapshot_path.mkdir(
        parents=True,
        exist_ok=False
    )

    log(f"Creating Gold snapshot: {snapshot_id}")

    write_dataset(
        fleet_summary,
        snapshot_path / "fleet_summary"
    )

    write_dataset(
        route_summary,
        snapshot_path / "route_summary"
    )

    write_dataset(
        latest_vehicle_positions,
        snapshot_path / "latest_vehicle_positions"
    )

    # ========================================================
    # 8. VERIFY GOLD DATASETS
    # ========================================================

    fleet_count = spark.read.parquet(
        str(snapshot_path / "fleet_summary")
    ).count()

    route_count = spark.read.parquet(
        str(snapshot_path / "route_summary")
    ).count()

    latest_count = spark.read.parquet(
        str(snapshot_path / "latest_vehicle_positions")
    ).count()

    if fleet_count != 1:
        raise RuntimeError(
            "Invalid fleet summary row count"
        )

    if route_count == 0:
        raise RuntimeError(
            "Route summary is empty"
        )

    if latest_count == 0:
        raise RuntimeError(
            "Latest vehicle positions are empty"
        )

    log("Gold snapshot verification successful")

    # ========================================================
    # 9. ATOMICALLY PUBLISH SNAPSHOT
    # ========================================================

    published_at = datetime.now(
        timezone.utc
    ).isoformat()

    manifest = {
        "snapshot_id": snapshot_id,
        "snapshot_path": str(snapshot_path),
        "published_at_utc": published_at,
        "input_observations": observation_count,
        "datasets": {
            "fleet_summary": fleet_count,
            "route_summary": route_count,
            "latest_vehicle_positions": latest_count,
        },
        "fleet_summary": str(
            snapshot_path / "fleet_summary"
        ),
        "route_summary": str(
            snapshot_path / "route_summary"
        ),
        "latest_vehicle_positions": str(
            snapshot_path / "latest_vehicle_positions"
        ),
    }

    temporary_manifest = SNAPSHOT_ROOT / (
        f".current_{uuid.uuid4().hex}.json"
    )

    with open(
        temporary_manifest,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            manifest,
            file,
            indent=2
        )

    os.replace(
        temporary_manifest,
        MANIFEST_PATH
    )

    # ========================================================
    # 10. FINAL OUTPUT
    # ========================================================

    print(
        "\n========== GOLD ANALYTICS COMPLETE =========="
    )

    print(f"Snapshot ID: {snapshot_id}")
    print(f"Input observations: {observation_count}")
    print(f"Fleet summary rows: {fleet_count}")
    print(f"Route summary rows: {route_count}")
    print(f"Latest vehicle positions: {latest_count}")
    print(f"Published at: {published_at}")
    print(f"Manifest: {MANIFEST_PATH}")

    print(
        "=============================================\n"
    )

finally:
    spark.stop()
