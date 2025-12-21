from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import os

# Конфигурация DAG
OWNER = "omash"
DAG_ID = "csv_to_parquet"

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

START_DATE = os.getenv("DAGS_START_DATE", "2025-12-01")
START_DATE = pendulum.parse(START_DATE).in_timezone("Europe/Moscow")

args = {
    "owner": OWNER,
    "start_date": START_DATE,
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 10 * * *",
    default_args=args,
    tags=["transform", "spark", "minio"],
    description=SHORT_DESCRIPTION,
    concurrency=3,
    max_active_tasks=3,
    max_active_runs=3,
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )
    
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=1800,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )
    
    spark_csv_to_parquet = BashOperator(
        task_id="spark_csv_to_parquet",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark_jobs/csv_to_parquet_job.py {{ ds }}
        """
    )

    end = EmptyOperator(
        task_id="end",
    )
    
    start >> sensor_on_raw_layer >> spark_csv_to_parquet >> end
    