#!/usr/bin/env bash
set -e

CONTAINER_NAME="spark-master"
SPARK_MASTER_URL="spark://spark-master:7077"

echo "Connecting to Spark Master: $CONTAINER_NAME"
echo "Starting IPython + PySpark"
echo

docker exec -it "$CONTAINER_NAME" bash -c "
  export PYSPARK_DRIVER_PYTHON=ipython
  export PYSPARK_PYTHON=python3
  export SPARK_HOME=/opt/spark
  export PATH=\$SPARK_HOME/bin:\$PATH

  exec pyspark --master $SPARK_MASTER_URL --name interactive-ipython
"
