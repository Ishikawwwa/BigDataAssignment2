#!/bin/bash
service ssh restart 

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt  

venv-pack -o .venv.tar.gz

# Give Cassandra more time to initialize properly
echo "Waiting for Cassandra to initialize (30 seconds)..."
sleep 30

# Check Cassandra connection and create keyspace if needed
echo "Verifying Cassandra connection and keyspace..."
python3 - << EOF
from cassandra.cluster import Cluster
try:
    # Connect to Cassandra
    cluster = Cluster(['cassandra-server'], connect_timeout=30)
    session = cluster.connect()
    
    # Create keyspace if it doesn't exist
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS search_engine
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    print("Successfully created/verified keyspace")
except Exception as e:
    print(f"Error setting up Cassandra: {str(e)}")
    print("Will continue with fallback data")
EOF

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

# Try to populate Cassandra with fallback data before running MapReduce
echo "Populating Cassandra with fallback data..."
python3 /app/populate_cassandra.py
CASSANDRA_POPULATE_EXIT=$?
echo "Cassandra population script exited with code $CASSANDRA_POPULATE_EXIT"

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
        
        # Try to populate Cassandra again with new fallback data
        echo "Trying to populate Cassandra again with regenerated fallback data..."
        python3 /app/populate_cassandra.py
    fi
else
    echo "Fallback files missing. Regenerating with sample data..."
    python3 /app/test_fallback.py --docs "/app/data" --output "/tmp/index_data/index_data.json" --copy --sample
    chmod 777 /tmp/index_data/index_data.json
    chmod 777 /tmp/index_data.json
    
    # Try to populate Cassandra again with new fallback data
    echo "Trying to populate Cassandra again with regenerated fallback data..."
    python3 /app/populate_cassandra.py
fi

echo "Running example queries..."
bash search.sh "national"
bash search.sh "Northern"
bash search.sh "Notre Dame"

echo "All done"