#!/usr/bin/env bash
set -e

mkdir -p /opt/spark/conf

cat > /opt/spark/conf/spark-defaults.conf <<EOF
spark.hadoop.fs.s3a.endpoint               ${S3_ENDPOINT_URL}
spark.hadoop.fs.s3a.access.key             ${MINIO_USER_ACCESS_KEY}
spark.hadoop.fs.s3a.secret.key             ${MINIO_USER_SECRET_KEY}
spark.hadoop.fs.s3a.path.style.access      true
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.impl                   org.apache.hadoop.fs.s3a.S3AFileSystem
EOF

exec "$@"
