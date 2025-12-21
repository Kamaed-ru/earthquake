import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

if len(sys.argv) < 2:
    raise ValueError("process_date is required (YYYY-MM-DD)")

process_date = sys.argv[1]

spark = (
    SparkSession.builder
    .appName("parquet_to_postgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# PostgreSQL env
pg_url = os.getenv("PG_URL")
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")

input_path = f"s3a://raw-data/raw_parquet/date_{process_date}/"

df = spark.read.parquet(input_path)

df_clean = (
    df.select(
        col("id").alias("event_id"),
        to_timestamp("time").alias("time_utc"),
        col("latitude"),
        col("longitude"),
        col("depth").alias("depth_km"),
        col("mag").alias("magnitude"),
        col("magType").alias("magnitude_type"),
        col("place"),
        col("net").alias("source_network"),
        to_timestamp("updated").alias("updated_utc")
    )
)

(
    df_clean.write
    .format("jdbc")
    .mode("append")
    .option("url", pg_url)
    .option("dbtable", "dwh.earthquakes")
    .option("user", pg_user)
    .option("password", pg_pass)
    .option("driver", "org.postgresql.Driver")
    .save()
)

spark.stop()
print("✅ Loaded to PostgreSQL")
