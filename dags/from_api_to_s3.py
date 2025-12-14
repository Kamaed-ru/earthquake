from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pendulum
import logging
import requests
import gzip
import io

# Конфигурация DAG
OWNER = "omash"
DAG_ID = "from_api_to_s3"

# S3
S3_BUCKET = Variable.get("S3_BUCKET")
S3_CONN_ID = "minio_s3"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    
    return start_date, end_date



def fetch_and_upload(**context):
    """"""
    start_date, end_date = get_dates(**context)
    
    logging.info(f"Start load for dates: {start_date}/{end_date}")

    url = (
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
        f"?format=csv&starttime={start_date}&endtime={end_date}"
    )

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    # сжимаем CSV -> gzip
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(response.content)

    buffer.seek(0)

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    s3.load_bytes(
        bytes_data=buffer.read(),
        key=f"earthquakes/date={start_date}/earthquakes_{start_date}.csv.gz",
        bucket_name=S3_BUCKET,
        replace=True,
    )
    
    logging.info(f"✅ Download for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["extract", "api", "minio"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=fetch_and_upload,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> fetch_and_upload >> end

