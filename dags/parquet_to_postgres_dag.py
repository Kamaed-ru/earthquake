from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import os

OWNER = "omash"
DAG_ID = "parquet_to_postgres"

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
    tags=["dwh", "spark", "postgres"],
    description=SHORT_DESCRIPTION,
    concurrency=3,
    max_active_tasks=3,
    max_active_runs=3,
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_parquet = ExternalTaskSensor(
        task_id="wait_for_parquet",
        external_dag_id="csv_to_parquet",   # <-- твой предыдущий DAG
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,    # раз в минуту
        timeout=1800,        # 30 минут
    )

    spark_load_to_pg = BashOperator(
        task_id="spark_load_to_pg",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark_jobs/parquet_to_postgres_job.py {{ ds }}
        """
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_parquet >> spark_load_to_pg >> end
