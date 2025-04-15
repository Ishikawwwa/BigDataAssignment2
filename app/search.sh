#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 \"<search query>\"" >&2
  exit 1
fi

QUERY="$1"

VENV_ARCHIVE=.venv.tar.gz
ENV_NAME=appenv
CONNECTOR_VERSION="3.4.1"
CONNECTOR_JAR_BASE="spark-cassandra-connector-assembly_2.12"
CONNECTOR_JAR_FILENAME="${CONNECTOR_JAR_BASE}-${CONNECTOR_VERSION}.jar"
CONNECTOR_URL="https://repo1.maven.org/maven2/com/datastax/spark/${CONNECTOR_JAR_BASE}/${CONNECTOR_VERSION}/${CONNECTOR_JAR_FILENAME}"
CONNECTOR_LOCAL_PATH="./${CONNECTOR_JAR_FILENAME}"
SPARK_HOME_JARS_PATH="/usr/local/spark/jars"
HADOOP_CONF_DIR_PATH="/usr/local/hadoop/etc/hadoop"

echo "--- Checking Dependencies ---"
if [ ! -f "$VENV_ARCHIVE" ]; then
  echo "Error: Virtual environment archive '$VENV_ARCHIVE' not found." >&2
  exit 1
fi
echo "Virtual env archive found: $VENV_ARCHIVE"

if [ ! -f "$CONNECTOR_LOCAL_PATH" ]; then
  echo "Spark Cassandra Connector JAR ($CONNECTOR_JAR_FILENAME) not found. Starting to do it..."
  curl -f -o "$CONNECTOR_LOCAL_PATH" "$CONNECTOR_URL"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to download Spark Cassandra Connector from $CONNECTOR_URL" >&2
    rm -f "$CONNECTOR_LOCAL_PATH"
    exit 1
  fi
  echo "Connector downloaded to $CONNECTOR_LOCAL_PATH"
else
  echo "Using existing Spark Cassandra Connector: $CONNECTOR_LOCAL_PATH"
fi

if [ ! -f "query.py" ]; then
    echo "Error: query.py script not found." >&2
    exit 1
fi
echo "Query script found: query.py"

echo "--- Checking Runtime Environment ---"
if [ ! -d "$SPARK_HOME_JARS_PATH" ]; then
    echo "Error: Spark JARs directory not found at $SPARK_HOME_JARS_PATH." >&2
    exit 1
fi
echo "Spark JARs directory found: $SPARK_HOME_JARS_PATH"

if [ ! -d "$HADOOP_CONF_DIR_PATH" ]; then
    echo "Warning: Hadoop config directory not found at $HADOOP_CONF_DIR_PATH." >&2
else
    echo "Hadoop config directory found: $HADOOP_CONF_DIR_PATH"
    echo "Listing Hadoop config files:"
    ls -l "$HADOOP_CONF_DIR_PATH"
fi

export HADOOP_CONF_DIR=$HADOOP_CONF_DIR_PATH

echo "--- Environment Variables ---"
echo "SPARK_HOME: $SPARK_HOME"
echo "HADOOP_HOME: $HADOOP_HOME"
echo "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
echo "PATH: $PATH"

echo "--- Submitting Spark Application ---"
echo "Query: \"$QUERY\""

echo "$QUERY" | spark-submit \
    --master yarn \
    --deploy-mode client \
    --name "SearchEngineRanker" \
    --verbose \
    --jars "$CONNECTOR_LOCAL_PATH" \
    --archives "${VENV_ARCHIVE}#${ENV_NAME}" \
    --conf spark.hadoop.yarn.resourcemanager.address=cluster-master:8032 \
    --conf spark.yarn.jars="local:${SPARK_HOME_JARS_PATH}/*" \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./${ENV_NAME}/bin/python3 \
    --conf spark.executorEnv.PYSPARK_PYTHON=./${ENV_NAME}/bin/python3 \
    query.py

EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  echo "Error: Spark application failed with exit code $EXIT_CODE." >&2
  exit $EXIT_CODE
fi

echo "Search finished successfully."