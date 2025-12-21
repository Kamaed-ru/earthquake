#!/usr/bin/env bash
set -e

docker exec -it spark-master bash -c "
  export PYSPARK_PYTHON=python3
  export PYSPARK_DRIVER_PYTHON=jupyter
  export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --ServerApp.token=\"\" --ServerApp.password=\"\"'

  pyspark --master spark://spark-master:7077
"
