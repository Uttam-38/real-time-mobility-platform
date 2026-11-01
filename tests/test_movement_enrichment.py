import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "spark" / "silver_movement_enrichment.py"


class TestMovementEnrichment(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("MovementEnrichmentTests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_movement_calculations_and_classification(self):
        with tempfile.TemporaryDirectory() as tmp:
            input_path = str(Path(tmp) / "input")
            output_path = str(Path(tmp) / "output")

            rows = [
                ("BUS_101", "R1", 33.4255, -111.9400, 1000),
                ("BUS_101", "R1", 33.4255, -111.9400, 1060),
                ("BUS_101", "R1", 33.4355, -111.9400, 1660),
                ("BUS_101", "R1", 34.4355, -111.9400, 1720),
            ]

            schema = """
                vehicle_id STRING,
                route_id STRING,
                latitude DOUBLE,
                longitude DOUBLE,
                vehicle_timestamp_unix LONG
            """

            df = self.spark.createDataFrame(rows, schema=schema)
            df = df.withColumn(
                "vehicle_timestamp",
                df.vehicle_timestamp_unix.cast("timestamp")
            )
            df.write.mode("overwrite").parquet(input_path)

            env = os.environ.copy()
            env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

            result = subprocess.run(
                [sys.executable, str(SCRIPT), input_path, output_path],
                cwd=str(ROOT),
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )

            self.assertEqual(
                result.returncode,
                0,
                msg=result.stdout + "\n" + result.stderr,
            )

            records = {
                row.vehicle_timestamp_unix: row
                for row in self.spark.read.parquet(output_path).collect()
            }

            self.assertEqual(len(records), 4)

            first = records[1000]
            self.assertEqual(first.movement_status, "FIRST_OBSERVATION")
            self.assertIsNone(first.calculated_speed_kmh)

            stationary = records[1060]
            self.assertEqual(stationary.movement_status, "NORMAL")
            self.assertAlmostEqual(stationary.distance_km, 0.0, places=6)
            self.assertAlmostEqual(
                stationary.calculated_speed_kmh, 0.0, places=6
            )

            normal = records[1660]
            self.assertEqual(normal.movement_status, "NORMAL")
            self.assertGreater(normal.distance_km, 1.0)
            self.assertLess(normal.calculated_speed_kmh, 120.0)

            suspicious = records[1720]
            self.assertEqual(
                suspicious.movement_status, "SUSPICIOUS_SPEED"
            )
            self.assertGreater(
                suspicious.calculated_speed_kmh, 120.0
            )


if __name__ == "__main__":
    unittest.main()
