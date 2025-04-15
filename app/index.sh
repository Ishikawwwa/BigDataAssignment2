#!/bin/bash

INPUT_PATH=${1:-/index/data}

INTERMEDIATE_OUTPUT=/tmp/index_intermediate
FINAL_OUTPUT=/tmp/index_final

VENV_ARCHIVE=.venv.tar.gz
ENV_NAME=appenv


HADOOP_STREAMING_JAR=$(find /usr/local/hadoop/share/hadoop/tools/lib/ -name "hadoop-streaming-*.jar" | head -n 1)

if [ -z "$HADOOP_STREAMING_JAR" ]; then
  HADOOP_STREAMING_JAR=$(find /opt/hadoop/share/hadoop/tools/lib/ -name "hadoop-streaming-*.jar" | head -n 1)
fi

if [ -z "$HADOOP_STREAMING_JAR" ]; then
  echo "Error: Hadoop Streaming JAR not found in /usr/local/hadoop or /opt/hadoop. Please verify Hadoop installation path in the container." >&2
  exit 1
fi

echo "Hadoop Streaming JAR found at: $HADOOP_STREAMING_JAR"

if [ ! -f "$VENV_ARCHIVE" ]; then
  echo "Error: Virtual environment archive '$VENV_ARCHIVE' not found. Please ensure prepare_data.sh ran correctly and created it." >&2
  exit 1
fi

chmod +x mapreduce/*.py

IS_HDFS=true
if [[ ! "$INPUT_PATH" == /* ]]; then
  LOCAL_INPUT_PATH=$INPUT_PATH
  INPUT_PATH=/tmp/local_input_for_index
  echo "Input path '$LOCAL_INPUT_PATH' detected as local. Copying to HDFS '$INPUT_PATH'..."
  hdfs dfs -mkdir -p $(dirname $INPUT_PATH)
  hdfs dfs -put -f "$LOCAL_INPUT_PATH" "$INPUT_PATH"
  if [ $? -ne 0 ]; then
    echo "Error: Failed to copy local input '$LOCAL_INPUT_PATH' to HDFS '$INPUT_PATH'." >&2
    exit 1
  fi
  IS_HDFS=false
elif ! hdfs dfs -test -e "$INPUT_PATH"; then
  echo "Error: Input path '$INPUT_PATH' does not exist in HDFS." >&2
  exit 1
fi

echo "Using input path: $INPUT_PATH"

echo "Cleaning up previous HDFS directories..."
hdfs dfs -rm -r -f $INTERMEDIATE_OUTPUT $FINAL_OUTPUT

echo "Running MapReduce Pipeline 1: Document Processing and TF calculation..."
yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="Indexing_Pipeline_1_DocProcessingTF" \
    -archives ${VENV_ARCHIVE}#${ENV_NAME} \
    -files mapreduce/mapper1.py,mapreduce/reducer1.py \
    -mapper "${ENV_NAME}/bin/python3 mapper1.py" \
    -reducer "${ENV_NAME}/bin/python3 reducer1.py" \
    -input "$INPUT_PATH" \
    -output $INTERMEDIATE_OUTPUT

if [ $? -ne 0 ]; then
  echo "Error: MapReduce Pipeline 1 failed." >&2
  if [ "$IS_HDFS" = false ]; then
    echo "Cleaning up temporary HDFS input directory '$INPUT_PATH'..."
    hdfs dfs -rm -r -f "$INPUT_PATH"
  fi
  exit 1
fi

echo "MapReduce Pipeline 1 completed successfully."

echo "Running MapReduce Pipeline 2: Inverted Index and DF calculation..."
yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="Indexing_Pipeline_2_InvertedIndexDF" \
    -archives ${VENV_ARCHIVE}#${ENV_NAME} \
    -files mapreduce/mapper2.py,mapreduce/reducer2.py \
    -mapper "${ENV_NAME}/bin/python3 mapper2.py" \
    -reducer "${ENV_NAME}/bin/python3 reducer2.py" \
    -input $INTERMEDIATE_OUTPUT \
    -output $FINAL_OUTPUT

if [ $? -ne 0 ]; then
  echo "Error: MapReduce Pipeline 2 failed." >&2
  if [ "$IS_HDFS" = false ]; then
    echo "Cleaning up temporary HDFS input directory '$INPUT_PATH'..."
    hdfs dfs -rm -r -f "$INPUT_PATH"
  fi
  exit 1
fi

echo "MapReduce Pipeline 2 completed successfully."

echo "Cleaning up intermediate HDFS directory: $INTERMEDIATE_OUTPUT"
hdfs dfs -rm -r -f $INTERMEDIATE_OUTPUT

if [ "$IS_HDFS" = false ]; then
  echo "Cleaning up temporary HDFS input directory '$INPUT_PATH'..."
  hdfs dfs -rm -r -f "$INPUT_PATH"
fi

echo "Indexing process finished successfully! Data written to Cassandra."
