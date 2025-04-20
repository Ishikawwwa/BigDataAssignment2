#!/bin/bash
echo "Starting search application..."

FALLBACK_DIR="/tmp/index_data"
FALLBACK_FILE="$FALLBACK_DIR/index_data.json"
ALTERNATE_FALLBACK="/tmp/index_data.json"

echo "Checking for fallback data files..."
FALLBACK_EXISTS=false

if [ -f "$FALLBACK_FILE" ]; then
    FILE_SIZE=$(stat -c%s "$FALLBACK_FILE")
    if [ "$FILE_SIZE" -gt 1000 ]; then
        echo "Found valid fallback data at $FALLBACK_FILE (size: $FILE_SIZE bytes)"
        FALLBACK_EXISTS=true
    else
        echo "Fallback file at $FALLBACK_FILE is too small (size: $FILE_SIZE bytes)"
    fi
fi

if [ -f "$ALTERNATE_FALLBACK" ] && [ "$FALLBACK_EXISTS" = false ]; then
    FILE_SIZE=$(stat -c%s "$ALTERNATE_FALLBACK")
    if [ "$FILE_SIZE" -gt 1000 ]; then
        echo "Found valid fallback data at $ALTERNATE_FALLBACK (size: $FILE_SIZE bytes)"
        FALLBACK_EXISTS=true
    else
        echo "Fallback file at $ALTERNATE_FALLBACK is too small (size: $FILE_SIZE bytes)"
    fi
fi

if [ "$FALLBACK_EXISTS" = false ]; then
    echo "No valid fallback data found, generating sample data..."
    mkdir -p $FALLBACK_DIR
    python3 /app/test_fallback.py --docs "/app/data" --output "$FALLBACK_FILE" --copy --sample
    chmod 777 $FALLBACK_FILE
    chmod 777 $ALTERNATE_FALLBACK
    echo "Sample fallback data generated"
    FALLBACK_EXISTS=true
fi

export SPARK_HOME=/usr/local/spark
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH

QUERY=${1:-"national"}

echo "************************************************************"
echo "*                    SEARCH RESULTS                         *"
echo "************************************************************"
echo "* Searching for: '$QUERY'                                 *"
echo "************************************************************"

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --conf spark.driver.memory=1g \
  /app/search.py "$QUERY"

# If that fails, run directly (as a backup)
if [ $? -ne 0 ]; then
    echo "Spark submit failed, trying direct Python execution..."
    python3 /app/search.py "$QUERY"
fi