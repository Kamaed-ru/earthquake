ПЕРВЫМ ДЕЛОМ БИЛБИМ ОБРАЗЫ
docker compose build

ПОТОМ РАЗВОРАЧИВАЕМ КОНТЕЙНЕРЫ 
docker compose up -d --scale spark-worker=4


airflow - http://localhost:8080/
minio - http://localhost:9001/
spark - http://localhost:8081/
jupyter - http://localhost:8888/
postgres_dwh - localhost:5433


