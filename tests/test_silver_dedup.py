import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class TestSilverDedup(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("SilverDedupTests")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_duplicate_observations_removed(self):
        with tempfile.TemporaryDirectory() as tmp:
            input_path = str(Path(tmp) / "input")
            checkpoint_path = str(Path(tmp) / "checkpoint")
            output_path = str(Path(tmp) / "output")

            event_time = datetime(
                2026, 10, 30, 12, 0, tzinfo=timezone.utc
            )

            rows = [
                ("BUS_101", 1793361600, 33.4255, -111.9400, event_time),
                ("BUS_101", 1793361600, 33.4255, -111.9400, event_time),
                ("BUS_102", 1793361600, 33.4300, -111.9500, event_time),
            ]

            schema = """
                vehicle_id STRING,
                vehicle_timestamp_unix LONG,
                latitude DOUBLE,
                longitude DOUBLE,
                kafka_timestamp TIMESTAMP
            """

            source_df = self.spark.createDataFrame(rows, schema=schema)
            source_df.write.mode("overwrite").parquet(input_path)

            stream_df = (
                self.spark.readStream
                .schema(source_df.schema)
                .parquet(input_path)
            )

            deduplicated = (
                stream_df
                .withWatermark("kafka_timestamp", "30 minutes")
                .dropDuplicatesWithinWatermark([
                    "vehicle_id",
                    "vehicle_timestamp_unix",
                    "latitude",
                    "longitude",
                ])
            )

            query = (
                deduplicated.writeStream
                .format("parquet")
                .outputMode("append")
                .option("path", output_path)
                .option("checkpointLocation", checkpoint_path)
                .trigger(availableNow=True)
                .start()
            )

            try:
                query.awaitTermination(120)
                self.assertFalse(
                    query.isActive,
                    "Streaming query did not finish within 120 seconds",
                )
                self.assertIsNone(query.exception())
            finally:
                if query.isActive:
                    query.stop()

            result = self.spark.read.parquet(output_path)

            self.assertEqual(result.count(), 2)
            self.assertEqual(
                result.select("vehicle_id").distinct().count(),
                2,
            )


if __name__ == "__main__":
    unittest.main()
