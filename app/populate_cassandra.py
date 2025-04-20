#!/usr/bin/env python3
import os
import json
import sys
import traceback
import time
from cassandra.cluster import Cluster

def setup_cassandra(fallback_data):
    """Set up Cassandra keyspace and tables, and populate with fallback data"""
    try:
        print("Connecting to Cassandra...")
        cluster = Cluster(['cassandra-server'], connect_timeout=30)
        session = cluster.connect()
        
        # Create keyspace if it doesn't exist
        print("Creating keyspace if it doesn't exist...")
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """)
        
        # Use the keyspace
        session.execute("USE search_engine")
        
        # Create tables if they don't exist
        print("Creating tables if they don't exist...")
        
        # Documents table
        session.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            length int
        )
        """)
        
        # Term frequency table
        session.execute("""
        CREATE TABLE IF NOT EXISTS term_frequency (
            term text,
            doc_id text,
            frequency int,
            PRIMARY KEY (term, doc_id)
        )
        """)
        
        # Document frequency table
        session.execute("""
        CREATE TABLE IF NOT EXISTS document_frequency (
            term text PRIMARY KEY,
            doc_count int
        )
        """)
        
        # Corpus stats table
        session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_name text PRIMARY KEY,
            value float
        )
        """)
        
        # Check if data already exists
        rows = session.execute("SELECT COUNT(*) FROM corpus_stats")
        count = rows.one()[0]
        if count > 0:
            print(f"Found {count} entries in corpus_stats table. Data already exists in Cassandra.")
            return True
        
        # Insert data
        print("Inserting document metadata...")
        document_metadata = fallback_data["document_metadata"]
        for doc_id, metadata in document_metadata.items():
            session.execute(
                "INSERT INTO documents (doc_id, title, length) VALUES (%s, %s, %s)",
                (doc_id, metadata["title"], metadata["length"])
            )
        
        print("Inserting term frequency data...")
        term_document_freq = fallback_data["term_document_freq"]
        for term, docs in term_document_freq.items():
            for doc_id, freq in docs.items():
                session.execute(
                    "INSERT INTO term_frequency (term, doc_id, frequency) VALUES (%s, %s, %s)",
                    (term, doc_id, freq)
                )
        
        print("Inserting document frequency data...")
        doc_freq = fallback_data["doc_freq"]
        for term, count in doc_freq.items():
            session.execute(
                "INSERT INTO document_frequency (term, doc_count) VALUES (%s, %s)",
                (term, count)
            )
        
        print("Inserting corpus stats...")
        corpus_stats = fallback_data["corpus_stats"]
        for stat_name, value in corpus_stats.items():
            session.execute(
                "INSERT INTO corpus_stats (stat_name, value) VALUES (%s, %s)",
                (stat_name, float(value))
            )
        
        print("Successfully inserted all data into Cassandra")
        return True
    except Exception as e:
        print(f"Error setting up Cassandra: {str(e)}")
        traceback.print_exc()
        return False

def load_fallback_data():
    """Load data from fallback files"""
    fallback_files = [
        "/tmp/index_data/index_data.json",
        "/tmp/index_data.json"
    ]
    
    for fallback_file in fallback_files:
        try:
            if os.path.exists(fallback_file):
                print(f"Trying to load fallback data from {fallback_file}")
                file_size = os.path.getsize(fallback_file)
                print(f"File size: {file_size} bytes")
                
                if file_size < 1000:
                    print(f"Warning: File {fallback_file} is too small, skipping")
                    continue
                    
                with open(fallback_file, 'r') as f:
                    data = json.loads(f.read())
                
                doc_count = len(data.get('document_metadata', {}))
                print(f"Loaded fallback data with {doc_count} documents")
                
                if doc_count > 0:
                    return data
        except Exception as e:
            print(f"Error loading data from {fallback_file}: {e}")
    
    print("No valid fallback data found")
    return None

def main():
    # Wait a bit to make sure Cassandra is ready
    time.sleep(5)
    
    # Load fallback data
    print("Loading fallback data...")
    fallback_data = load_fallback_data()
    if not fallback_data:
        print("Error: Could not load fallback data")
        sys.exit(1)
    
    # Set up Cassandra
    print("Setting up Cassandra...")
    success = setup_cassandra(fallback_data)
    
    if success:
        print("Successfully populated Cassandra with fallback data")
        sys.exit(0)
    else:
        print("Failed to populate Cassandra with fallback data")
        sys.exit(1)

if __name__ == "__main__":
    main() 