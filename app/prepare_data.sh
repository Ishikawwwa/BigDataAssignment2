#!/bin/bash

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python) 

unset PYSPARK_PYTHON

echo "Waiting for HDFS to be ready..."
until hdfs dfs -ls / > /dev/null 2>&1; do
    echo "Waiting for HDFS..."
    sleep 5
done

hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /index/data
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /index/data

echo "Copying parquet file to HDFS..."
hdfs dfs -put -f /app/n.parquet /n.parquet

echo "Running data preparation script..."
spark-submit prepare_data.py

echo "Copying prepared documents to HDFS..."
hdfs dfs -put -f data /

echo "Verifying data in HDFS..."
echo "Documents in /data:"
hdfs dfs -ls /data
echo "Index data in /index/data:"
hdfs dfs -ls /index/data

echo "done data preparation"