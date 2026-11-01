
import os
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

BRONZE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze")

spark = (
    SparkSession.builder
    .appName("CheckBronzeData")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read saved Bronze data
df = spark.read.parquet(BRONZE_PATH)

print("\n========== BRONZE LAYER VERIFICATION ==========")

# Count stored records
total_records = df.count()
print(f"\nTotal vehicle records stored: {total_records}")

# Display schema
print("\nBronze Data Schema:")
df.printSchema()

# Display sample records
print("\nSample vehicle records:")
df.select(
    "kafka_key",
    "raw_json",
    "kafka_timestamp",
    "ingestion_timestamp"
).show(5, truncate=100)

# Count records per Kafka partition
print("\nRecords by Kafka partition:")
df.groupBy("kafka_partition").count().show()

print("\n===============================================")

spark.stop()
