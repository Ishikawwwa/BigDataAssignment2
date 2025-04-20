#!/bin/bash
service ssh restart 

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt  

venv-pack -o .venv.tar.gz

echo "Preparing data..."
bash prepare_data.sh

hdfs dfs -mkdir -p /tmp/index
hdfs dfs -mkdir -p /data

# Generate fallback data directly (since I had some problems with Cassandra because of my laptop's performance, so this is sometimes faster). Cassandra is still running and used though.
echo "Generating fallback data directly from documents..."
mkdir -p /tmp/index_data
python3 /app/test_fallback.py --docs "/app/data" --output "/tmp/index_data/index_data.json" --copy --sample
chmod 777 /tmp/index_data/index_data.json
chmod 777 /tmp/index_data.json
echo "Fallback data generated successfully"

echo "Indexing documents..."
bash index.sh /data

# Doublecheck that fallback data exists and is valid
echo "Verifying fallback data..."
if [ -f "/tmp/index_data/index_data.json" ] && [ -f "/tmp/index_data.json" ]; then
    echo "Fallback data files exist. Checking sizes..."
    PRIMARY_SIZE=$(stat -c%s "/tmp/index_data/index_data.json")
    SECONDARY_SIZE=$(stat -c%s "/tmp/index_data.json")
    
    echo "Primary size: $PRIMARY_SIZE bytes, Secondary size: $SECONDARY_SIZE bytes"
    
    if [ "$PRIMARY_SIZE" -lt 1000 ] || [ "$SECONDARY_SIZE" -lt 1000 ]; then
        echo "Fallback files are too small. Regenerating with sample data..."
        python3 /app/test_fallback.py --docs "/app/data" --output "/tmp/index_data/index_data.json" --copy --sample
        chmod 777 /tmp/index_data/index_data.json
        chmod 777 /tmp/index_data.json
    fi
else
    echo "Fallback files missing. Regenerating with sample data..."
    python3 /app/test_fallback.py --docs "/app/data" --output "/tmp/index_data/index_data.json" --copy --sample
    chmod 777 /tmp/index_data/index_data.json
    chmod 777 /tmp/index_data.json
fi

# Give Cassandra some time because of the performance of my laptop again(
echo "Waiting for Cassandra to initialize..."
sleep 10

echo "Running example queries..."
bash search.sh "national"
bash search.sh "Northern"
bash search.sh "Notre Dame"

echo "All done"