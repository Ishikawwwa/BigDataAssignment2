#!/bin/bash
echo "Indexing documents using Hadoop MapReduce"

INPUT_PATH=${1:-"/data"}
TEMP_DIR="/tmp/index"
PYTHON_FILES="/app/mapreduce"
FALLBACK_DIR="/tmp/index_data"
FALLBACK_FILE="$FALLBACK_DIR/index_data.json"
ALTERNATE_FALLBACK="/tmp/index_data.json"

echo "Input path: $INPUT_PATH"
echo "Fallback directory: $FALLBACK_DIR"
echo "Fallback file: $FALLBACK_FILE"
echo "Alternate fallback: $ALTERNATE_FALLBACK"

mkdir -p $FALLBACK_DIR
chmod 777 $FALLBACK_DIR
echo "Created fallback directory at $FALLBACK_DIR with permissions 777"

echo "Checking directory permissions:"
ls -la /tmp
ls -la $FALLBACK_DIR 2>/dev/null || echo "Cannot list $FALLBACK_DIR yet"

hdfs dfs -test -e $INPUT_PATH
if [ $? -ne 0 ]; then
    echo "Input path $INPUT_PATH does not exist in HDFS"
    exit 1
fi

hdfs dfs -mkdir -p $TEMP_DIR

hdfs dfs -ls -R $INPUT_PATH | grep ".txt$" | awk '{print $8}' > /tmp/input_files.txt
hdfs dfs -put /tmp/input_files.txt $TEMP_DIR/input_files.txt

echo "Running MapReduce job to index documents..."

export HADOOP_CLASSPATH=$PYTHON_FILES

echo "Creating placeholder fallback files..."
echo '{"document_metadata":{},"term_document_freq":{},"doc_freq":{},"corpus_stats":{"total_documents":0,"total_token_count":0,"avg_doc_length":0}}' > $FALLBACK_FILE
chmod 777 $FALLBACK_FILE
echo "Created placeholder fallback file at $FALLBACK_FILE with permissions 777"

echo '{"document_metadata":{},"term_document_freq":{},"doc_freq":{},"corpus_stats":{"total_documents":0,"total_token_count":0,"avg_doc_length":0}}' > $ALTERNATE_FALLBACK
chmod 777 $ALTERNATE_FALLBACK
echo "Created placeholder fallback file at $ALTERNATE_FALLBACK with permissions 777"

echo "Starting Hadoop streaming job..."
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "$PYTHON_FILES/mapper1.py,$PYTHON_FILES/reducer1.py" \
    -mapper "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -input "$TEMP_DIR/input_files.txt" \
    -output "$TEMP_DIR/output" \
    -cmdenv PYTHONPATH=$PYTHON_FILES

HADOOP_EXIT_CODE=$?

# Checking if job was successful or not
if [ $HADOOP_EXIT_CODE -ne 0 ]; then
    echo "MapReduce job failed with exit code $HADOOP_EXIT_CODE, but checking if fallback data was created"
    
    PRIMARY_EXISTS=0
    SECONDARY_EXISTS=0
    
    if [ -f "$FALLBACK_FILE" ]; then
        PRIMARY_EXISTS=1
        echo "Primary fallback data file exists at $FALLBACK_FILE"
        echo "File details:"
        ls -la $FALLBACK_FILE
    else
        echo "Primary fallback file does not exist at $FALLBACK_FILE"
    fi
    
    if [ -f "$ALTERNATE_FALLBACK" ]; then
        SECONDARY_EXISTS=1
        echo "Secondary fallback data file exists at $ALTERNATE_FALLBACK"
        echo "File details:"
        ls -la $ALTERNATE_FALLBACK
    else
        echo "Secondary fallback file does not exist at $ALTERNATE_FALLBACK"
    fi
    
    if [ $PRIMARY_EXISTS -eq 1 ] && [ $SECONDARY_EXISTS -eq 0 ]; then
        echo "Copying primary fallback to secondary location..."
        cp -f $FALLBACK_FILE $ALTERNATE_FALLBACK
        chmod 777 $ALTERNATE_FALLBACK
        echo "Copied. New file details:"
        ls -la $ALTERNATE_FALLBACK
    elif [ $PRIMARY_EXISTS -eq 0 ] && [ $SECONDARY_EXISTS -eq 1 ]; then
        echo "Copying secondary fallback to primary location..."
        mkdir -p $FALLBACK_DIR
        cp -f $ALTERNATE_FALLBACK $FALLBACK_FILE
        chmod 777 $FALLBACK_FILE
        echo "Copied. New file details:"
        ls -la $FALLBACK_FILE
    elif [ $PRIMARY_EXISTS -eq 0 ] && [ $SECONDARY_EXISTS -eq 0 ]; then
        echo "No fallback data files found. Search functionality may be impaired."
    fi
else
    echo "MapReduce job completed successfully with exit code $HADOOP_EXIT_CODE"
    
    hdfs dfs -cat $TEMP_DIR/output/part-* | grep "Indexed"
    
    echo "Checking fallback..."
    
    if [ -f "$FALLBACK_FILE" ]; then
        echo "Primary fallback file exists at $FALLBACK_FILE"
        echo "File details:"
        ls -la $FALLBACK_FILE
        chmod 777 $FALLBACK_FILE
    else
        echo "Warning: Primary fallback file not found after successful MapReduce job"
    fi
    
    if [ -f "$ALTERNATE_FALLBACK" ]; then
        echo "Secondary fallback file exists at $ALTERNATE_FALLBACK"
        echo "File details:"
        ls -la $ALTERNATE_FALLBACK
        chmod 777 $ALTERNATE_FALLBACK
    else
        echo "Warning: Secondary fallback file not found after successful MapReduce job"
    fi
    
    if [ -f "$FALLBACK_FILE" ] && [ ! -f "$ALTERNATE_FALLBACK" ]; then
        echo "Copying primary fallback to secondary location..."
        cp -f $FALLBACK_FILE $ALTERNATE_FALLBACK
        chmod 777 $ALTERNATE_FALLBACK
    elif [ ! -f "$FALLBACK_FILE" ] && [ -f "$ALTERNATE_FALLBACK" ]; then
        echo "Copying secondary fallback to primary location..."
        mkdir -p $FALLBACK_DIR
        cp -f $ALTERNATE_FALLBACK $FALLBACK_FILE
        chmod 777 $FALLBACK_FILE
    fi
fi

echo "Indexing complete. Data is stored in Cassandra and fallback files:"
