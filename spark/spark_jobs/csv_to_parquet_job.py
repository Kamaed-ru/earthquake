import sys
import os
from pyspark.sql import SparkSession

# --- args ---
if len(sys.argv) < 2:
    raise ValueError("process_date argument is required (YYYY-MM-DD)")

process_date = sys.argv[1]

# --- Spark session ---
spark = (
    SparkSession.builder
    .appName("csv_to_parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# --- env config ---
def get_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is not set")
    return value

s3_endpoint = get_env("S3_ENDPOINT_URL")
s3_access_key = get_env("MINIO_USER_ACCESS_KEY")
s3_secret_key = get_env("MINIO_USER_SECRET_KEY")

# --- S3A config ---
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)
hadoop_conf.set("fs.s3a.access.key", s3_access_key)
hadoop_conf.set("fs.s3a.secret.key", s3_secret_key)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# --- paths ---
input_path = f"s3a://raw-data/raw_csv/date_{process_date}/"
output_path = f"s3a://raw-data/raw_parquet/date_{process_date}/"

print(f"Reading from:  {input_path}")
print(f"Writing to:    {output_path}")

# --- read ---
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print(f"Input rows: {df.count()}")

# --- write ---
(
    df.write
    .mode("overwrite")
    .option("compression", "gzip")
    .parquet(output_path)
)

spark.stop()
print("✅ Job completed successfully")