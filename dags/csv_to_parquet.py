from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="transform_csv_to_parquet",
    start_date=datetime(2025, 12, 1),
    schedule="0 10 * * *",
    catchup=True,
    max_active_runs=5,
) as dag:

    spark_csv_to_parquet = BashOperator(
        task_id="spark_csv_to_parquet",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark_jobs/csv_to_parquet_job.py {{ ds }}
        """
    )