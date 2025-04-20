#!/bin/bash
echo "Preparing document data for indexing..."

source .venv/bin/activate

mkdir -p data

export PYSPARK_DRIVER_PYTHON=$(which python) 
unset PYSPARK_PYTHON

# Sanity check one more time
# Check for parquet file in various possible locations
echo "Looking for n.parquet file..."
echo "Checking in current directory: $(pwd)"
ls -la n.parquet 2>/dev/null || echo "Not found in current directory"

echo "Checking in root directory: /"
ls -la /n.parquet 2>/dev/null || echo "Not found in root directory"

echo "Checking in parent directory: ../"
ls -la ../n.parquet 2>/dev/null || echo "Not found in parent directory"

if [ -f "/n.parquet" ]; then
    echo "Creating symbolic link from /n.parquet to current directory"
    ln -sf /n.parquet n.parquet
elif [ -f "../n.parquet" ]; then
    echo "Creating symbolic link from ../n.parquet to current directory"
    ln -sf ../n.parquet n.parquet
else
    echo "Trying to find n.parquet anywhere on the system..."
    FOUND_FILE=$(find / -name "n.parquet" -not -path "*/\.*" -type f 2>/dev/null | head -n 1)
    
    if [ -n "$FOUND_FILE" ]; then
        echo "Found n.parquet at: $FOUND_FILE"
        ln -sf "$FOUND_FILE" n.parquet
    else
        echo "WARNING: n.parquet not found anywhere on the system!"
    fi
fi

echo "Current directory contents:"
ls -la

echo "Processing parquet file..."
spark-submit prepare_data.py

echo "Checking created documents:"
ls -la data | grep ".txt" | wc -l

echo "Uploading documents to HDFS..."
hdfs dfs -mkdir -p /data

if [ -n "$(ls -A data/*.txt 2>/dev/null)" ]; then
    hdfs dfs -put -f data/*.txt /data/
    echo "Upload to HDFS completed successfully."
else
    echo "ERROR: No text files found in data directory to upload to HDFS!"
    echo "Current directory contents:"
    ls -la
    echo "Data directory contents:"
    ls -la data
    exit 1
fi

echo "Checking HDFS data directory..."
hdfs dfs -ls /data | wc -l

echo "Data preparation complete!"