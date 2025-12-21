ПЕРВЫМ ДЕЛОМ БИЛБИМ ОБРАЗЫ
docker compose build

ПОТОМ РАЗВОРАЧИВАЕМ КОНТЕЙНЕРЫ 
docker compose up -d


docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark_jobs/parquet_to_postgres_job.py 2025-12-01